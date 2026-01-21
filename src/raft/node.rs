//! Raft Node
//!
//! The main Raft node implementation that orchestrates election and replication

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, info};

use super::state::{RaftState, NodeRole};
use super::log::{RaftLog, LogEntry, CommandType};
use super::election::{ElectionManager, ElectionConfig};
use super::replication::ReplicationManager;
use super::rpc::*;
use crate::{Error, Result};

/// Configuration for a Raft node
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID
    pub node_id: String,
    /// Peer node addresses (id -> address)
    pub peers: Vec<(String, String)>,
    /// Election configuration
    pub election: ElectionConfig,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: "node1".to_string(),
            peers: Vec::new(),
            election: ElectionConfig::default(),
            heartbeat_interval_ms: 50,
        }
    }
}

/// Commands that can be sent to the Raft node
#[derive(Debug)]
pub enum RaftCommand {
    /// Propose a new command to be replicated
    Propose {
        command: CommandType,
        response: oneshot::Sender<Result<u64>>,
    },
    /// Handle a RequestVote RPC
    RequestVote {
        request: RequestVoteRequest,
        response: oneshot::Sender<RequestVoteResponse>,
    },
    /// Handle an AppendEntries RPC
    AppendEntries {
        request: AppendEntriesRequest,
        response: oneshot::Sender<AppendEntriesResponse>,
    },
    /// Get current status
    Status {
        response: oneshot::Sender<NodeStatus>,
    },
    /// Shutdown the node
    Shutdown,
}

/// Status of a Raft node
#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub node_id: String,
    pub role: NodeRole,
    pub term: u64,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_length: usize,
}

/// A Raft consensus node
pub struct RaftNode {
    /// Node configuration
    config: RaftConfig,
    /// Raft state
    state: Arc<RaftState>,
    /// Raft log
    log: Arc<RaftLog>,
    /// Election manager
    election: Arc<ElectionManager>,
    /// Replication manager
    replication: Arc<ReplicationManager>,
    /// Channel to receive commands
    command_rx: mpsc::Receiver<RaftCommand>,
    /// Channel to send commands (for client interaction)
    command_tx: mpsc::Sender<RaftCommand>,
    /// Callback for applying committed entries
    apply_callback: Option<Box<dyn Fn(LogEntry) + Send + Sync>>,
}

impl RaftNode {
    /// Create a new Raft node
    pub fn new(config: RaftConfig) -> Self {
        let peer_ids: Vec<String> = config.peers.iter().map(|(id, _)| id.clone()).collect();
        
        let state = Arc::new(RaftState::new(config.node_id.clone(), peer_ids));
        let log = Arc::new(RaftLog::new());
        
        let election = Arc::new(ElectionManager::new(
            Arc::clone(&state),
            Arc::clone(&log),
            config.election.clone(),
        ));
        
        let replication = Arc::new(ReplicationManager::new(
            Arc::clone(&state),
            Arc::clone(&log),
        ));

        let (command_tx, command_rx) = mpsc::channel(1000);

        Self {
            config,
            state,
            log,
            election,
            replication,
            command_rx,
            command_tx,
            apply_callback: None,
        }
    }

    /// Set a callback for when entries are committed
    pub fn set_apply_callback<F>(&mut self, callback: F)
    where
        F: Fn(LogEntry) + Send + Sync + 'static,
    {
        self.apply_callback = Some(Box::new(callback));
    }

    /// Get a handle to send commands to this node
    pub fn command_sender(&self) -> mpsc::Sender<RaftCommand> {
        self.command_tx.clone()
    }

    /// Get the current status
    pub fn status(&self) -> NodeStatus {
        NodeStatus {
            node_id: self.config.node_id.clone(),
            role: self.state.role(),
            term: self.state.current_term(),
            leader_id: self.state.current_leader(),
            commit_index: self.state.commit_index(),
            last_applied: self.state.last_applied(),
            log_length: self.log.len(),
        }
    }

    /// Run the Raft node main loop
    pub async fn run(&mut self) {
        let mut heartbeat_interval = interval(Duration::from_millis(self.config.heartbeat_interval_ms));

        loop {
            tokio::select! {
                // Handle incoming commands
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        RaftCommand::Propose { command, response } => {
                            let result = self.handle_propose(command);
                            let _ = response.send(result);
                        }
                        RaftCommand::RequestVote { request, response } => {
                            let resp = self.handle_request_vote(request);
                            let _ = response.send(resp);
                        }
                        RaftCommand::AppendEntries { request, response } => {
                            let resp = self.handle_append_entries(request);
                            let _ = response.send(resp);
                        }
                        RaftCommand::Status { response } => {
                            let _ = response.send(self.status());
                        }
                        RaftCommand::Shutdown => {
                            info!("Raft node shutting down");
                            break;
                        }
                    }
                }

                // Send heartbeats if leader
                _ = heartbeat_interval.tick() => {
                    if self.state.is_leader() {
                        self.send_heartbeats().await;
                    }
                }

                // Check election timeout
                _ = tokio::time::sleep(self.election.time_until_timeout()) => {
                    if self.election.should_start_election() {
                        self.start_election().await;
                    }
                }
            }

            // Apply committed entries
            self.apply_committed_entries();
        }
    }

    /// Handle a propose command
    fn handle_propose(&self, command: CommandType) -> Result<u64> {
        self.replication.append_command(command)
    }

    /// Handle a RequestVote RPC
    fn handle_request_vote(&self, request: RequestVoteRequest) -> RequestVoteResponse {
        debug!("Received RequestVote from {} for term {}", request.candidate_id, request.term);
        self.election.handle_vote_request(&request)
    }

    /// Handle an AppendEntries RPC
    fn handle_append_entries(&self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        debug!(
            "Received AppendEntries from {} with {} entries",
            request.leader_id,
            request.entries.len()
        );
        
        // Reset election timeout on valid AppendEntries
        self.election.reset_timeout();
        
        self.replication.handle_append_entries(&request)
    }

    /// Start a new election
    async fn start_election(&self) {
        info!("Starting election for term {}", self.state.current_term() + 1);
        
        let _vote_request = self.election.start_election();
        let _votes = 1; // Vote for self

        // Request votes from all peers (in parallel in a real implementation)
        for (peer_id, _peer_addr) in &self.config.peers {
            // In a real implementation, this would send RPC to peer
            // For now, we'll simulate immediate responses
            debug!("Requesting vote from {}", peer_id);
        }

        // Check if we got enough votes (for testing, assume we become leader in single-node)
        if self.config.peers.is_empty() {
            // Single node cluster, become leader immediately
            self.state.become_leader(self.log.last_index());
            info!("Became leader (single node cluster)");
        }
    }

    /// Send heartbeats to all followers
    async fn send_heartbeats(&self) {
        for (peer_id, _peer_addr) in &self.config.peers {
            if let Some(_heartbeat) = self.replication.create_heartbeat(peer_id) {
                // In a real implementation, this would send RPC to peer
                debug!("Sending heartbeat to {}", peer_id);
            }
        }
    }

    /// Apply committed entries to the state machine
    fn apply_committed_entries(&self) {
        let entries = self.replication.get_entries_to_apply();
        
        for entry in entries {
            let index = entry.index;
            
            if let Some(ref callback) = self.apply_callback {
                callback(entry);
            }
            
            self.replication.mark_applied(index);
        }
    }
}

/// Handle for interacting with a Raft node
#[derive(Clone)]
pub struct RaftHandle {
    command_tx: mpsc::Sender<RaftCommand>,
}

impl RaftHandle {
    /// Create a new handle from a command sender
    pub fn new(command_tx: mpsc::Sender<RaftCommand>) -> Self {
        Self { command_tx }
    }

    /// Propose a command
    pub async fn propose(&self, command: CommandType) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(RaftCommand::Propose { command, response: tx })
            .await
            .map_err(|_| Error::Connection("Node not running".into()))?;
        
        rx.await.map_err(|_| Error::Connection("Node not responding".into()))?
    }

    /// Get the current status
    pub async fn status(&self) -> Result<NodeStatus> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(RaftCommand::Status { response: tx })
            .await
            .map_err(|_| Error::Connection("Node not running".into()))?;
        
        rx.await.map_err(|_| Error::Connection("Node not responding".into()))
    }

    /// Shutdown the node
    pub async fn shutdown(&self) -> Result<()> {
        self.command_tx
            .send(RaftCommand::Shutdown)
            .await
            .map_err(|_| Error::Connection("Node not running".into()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_single_node_becomes_leader() {
        let config = RaftConfig {
            node_id: "node1".to_string(),
            peers: vec![],
            ..Default::default()
        };

        let mut node = RaftNode::new(config);
        let handle = RaftHandle::new(node.command_sender());

        // Run node in background
        let node_task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(500), node.run()).await
        });

        // Wait a bit for election
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check status
        let status = handle.status().await.unwrap();
        assert_eq!(status.role, NodeRole::Leader);

        // Shutdown
        let _ = handle.shutdown().await;
        let _ = node_task.await;
    }

    #[tokio::test]
    async fn test_propose_command() {
        let config = RaftConfig {
            node_id: "node1".to_string(),
            peers: vec![],
            ..Default::default()
        };

        let mut node = RaftNode::new(config);
        let handle = RaftHandle::new(node.command_sender());

        let node_task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(500), node.run()).await
        });

        // Wait for leader election
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Propose a command
        let index = handle.propose(CommandType::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }).await.unwrap();

        assert_eq!(index, 1);

        let _ = handle.shutdown().await;
        let _ = node_task.await;
    }
}
