//! Raft State Machine
//!
//! Manages the state of a Raft node including current term, voted for, and role.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;

/// Role of a Raft node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Passive node that responds to requests from leaders and candidates
    Follower,
    /// Node that is trying to become a leader
    Candidate,
    /// Active node that handles all client requests and replicates to followers
    Leader,
}

impl Default for NodeRole {
    fn default() -> Self {
        NodeRole::Follower
    }
}

/// Persistent state on all servers (updated on stable storage before responding to RPCs)
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentState {
    /// Latest term server has seen (initialized to 0, increases monotonically)
    pub current_term: u64,
    /// CandidateId that received vote in current term (or None)
    pub voted_for: Option<String>,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

/// Volatile state on all servers
#[derive(Debug, Default)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed
    pub commit_index: AtomicU64,
    /// Index of highest log entry applied to state machine
    pub last_applied: AtomicU64,
}

/// Volatile state on leaders (reinitialized after election)
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each server, index of the next log entry to send to that server
    pub next_index: std::collections::HashMap<String, u64>,
    /// For each server, index of highest log entry known to be replicated on server
    pub match_index: std::collections::HashMap<String, u64>,
}

impl LeaderState {
    pub fn new(peers: &[String], last_log_index: u64) -> Self {
        let mut next_index = std::collections::HashMap::new();
        let mut match_index = std::collections::HashMap::new();

        for peer in peers {
            // Initialize nextIndex to leader last log index + 1
            next_index.insert(peer.clone(), last_log_index + 1);
            // Initialize matchIndex to 0
            match_index.insert(peer.clone(), 0);
        }

        Self {
            next_index,
            match_index,
        }
    }

    pub fn update_match_index(&mut self, peer: &str, index: u64) {
        if let Some(current) = self.match_index.get_mut(peer) {
            *current = index.max(*current);
        }
        if let Some(next) = self.next_index.get_mut(peer) {
            *next = index + 1;
        }
    }

    pub fn decrement_next_index(&mut self, peer: &str) {
        if let Some(next) = self.next_index.get_mut(peer) {
            if *next > 1 {
                *next -= 1;
            }
        }
    }
}

/// Complete Raft state for a node
pub struct RaftState {
    /// This node's ID
    pub node_id: String,
    /// Current role of this node
    role: RwLock<NodeRole>,
    /// Persistent state
    persistent: RwLock<PersistentState>,
    /// Volatile state
    pub volatile: VolatileState,
    /// Leader state (only valid when this node is leader)
    leader_state: RwLock<Option<LeaderState>>,
    /// Current leader ID (if known)
    current_leader: RwLock<Option<String>>,
    /// List of peer node IDs
    pub peers: Vec<String>,
}

impl RaftState {
    /// Create a new Raft state
    pub fn new(node_id: String, peers: Vec<String>) -> Self {
        Self {
            node_id,
            role: RwLock::new(NodeRole::Follower),
            persistent: RwLock::new(PersistentState::default()),
            volatile: VolatileState::default(),
            leader_state: RwLock::new(None),
            current_leader: RwLock::new(None),
            peers,
        }
    }

    /// Get current role
    pub fn role(&self) -> NodeRole {
        *self.role.read()
    }

    /// Set role
    pub fn set_role(&self, role: NodeRole) {
        *self.role.write() = role;
    }

    /// Get current term
    pub fn current_term(&self) -> u64 {
        self.persistent.read().current_term
    }

    /// Set current term (also clears voted_for)
    pub fn set_term(&self, term: u64) {
        let mut persistent = self.persistent.write();
        if term > persistent.current_term {
            persistent.current_term = term;
            persistent.voted_for = None;
        }
    }

    /// Increment term and become candidate
    pub fn start_election(&self) -> u64 {
        let mut persistent = self.persistent.write();
        persistent.current_term += 1;
        persistent.voted_for = Some(self.node_id.clone());
        *self.role.write() = NodeRole::Candidate;
        persistent.current_term
    }

    /// Get voted_for
    pub fn voted_for(&self) -> Option<String> {
        self.persistent.read().voted_for.clone()
    }

    /// Set voted_for
    pub fn vote_for(&self, candidate_id: String) -> bool {
        let mut persistent = self.persistent.write();
        if persistent.voted_for.is_none() {
            persistent.voted_for = Some(candidate_id);
            true
        } else {
            false
        }
    }

    /// Check if already voted for this candidate
    pub fn already_voted_for(&self, candidate_id: &str) -> bool {
        self.persistent.read().voted_for.as_deref() == Some(candidate_id)
    }

    /// Become leader
    pub fn become_leader(&self, last_log_index: u64) {
        *self.role.write() = NodeRole::Leader;
        *self.leader_state.write() = Some(LeaderState::new(&self.peers, last_log_index));
        *self.current_leader.write() = Some(self.node_id.clone());
    }

    /// Become follower
    pub fn become_follower(&self, leader_id: Option<String>) {
        *self.role.write() = NodeRole::Follower;
        *self.leader_state.write() = None;
        *self.current_leader.write() = leader_id;
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.role() == NodeRole::Leader
    }

    /// Get the current leader ID
    pub fn current_leader(&self) -> Option<String> {
        self.current_leader.read().clone()
    }

    /// Set the current leader
    pub fn set_leader(&self, leader_id: Option<String>) {
        *self.current_leader.write() = leader_id;
    }

    /// Get leader state (only valid when leader)
    pub fn leader_state(&self) -> Option<LeaderState> {
        self.leader_state.read().clone()
    }

    /// Update leader state
    pub fn with_leader_state<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut LeaderState) -> R,
    {
        let mut guard = self.leader_state.write();
        guard.as_mut().map(f)
    }

    /// Get commit index
    pub fn commit_index(&self) -> u64 {
        self.volatile.commit_index.load(Ordering::SeqCst)
    }

    /// Set commit index
    pub fn set_commit_index(&self, index: u64) {
        self.volatile.commit_index.store(index, Ordering::SeqCst);
    }

    /// Get last applied
    pub fn last_applied(&self) -> u64 {
        self.volatile.last_applied.load(Ordering::SeqCst)
    }

    /// Set last applied
    pub fn set_last_applied(&self, index: u64) {
        self.volatile.last_applied.store(index, Ordering::SeqCst);
    }

    /// Check if a quorum is achieved
    pub fn has_quorum(&self, count: usize) -> bool {
        count >= self.quorum_size()
    }

    /// Get the quorum size (majority)
    pub fn quorum_size(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    /// Get the total cluster size
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let state = RaftState::new("node1".to_string(), vec!["node2".to_string(), "node3".to_string()]);
        
        assert_eq!(state.role(), NodeRole::Follower);
        assert_eq!(state.current_term(), 0);
        assert_eq!(state.voted_for(), None);
        assert_eq!(state.cluster_size(), 3);
        assert_eq!(state.quorum_size(), 2);
    }

    #[test]
    fn test_start_election() {
        let state = RaftState::new("node1".to_string(), vec!["node2".to_string(), "node3".to_string()]);
        
        let term = state.start_election();
        
        assert_eq!(term, 1);
        assert_eq!(state.role(), NodeRole::Candidate);
        assert_eq!(state.voted_for(), Some("node1".to_string()));
    }

    #[test]
    fn test_become_leader() {
        let state = RaftState::new("node1".to_string(), vec!["node2".to_string(), "node3".to_string()]);
        
        state.become_leader(0);
        
        assert_eq!(state.role(), NodeRole::Leader);
        assert!(state.is_leader());
        assert_eq!(state.current_leader(), Some("node1".to_string()));
    }

    #[test]
    fn test_become_follower() {
        let state = RaftState::new("node1".to_string(), vec!["node2".to_string()]);
        
        state.become_leader(0);
        state.become_follower(Some("node2".to_string()));
        
        assert_eq!(state.role(), NodeRole::Follower);
        assert!(!state.is_leader());
        assert_eq!(state.current_leader(), Some("node2".to_string()));
    }
}
