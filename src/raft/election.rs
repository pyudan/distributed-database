//! Election Manager
//!
//! Handles leader election with randomized timeouts

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::Rng;

use super::state::{RaftState, NodeRole};
use super::log::RaftLog;
use super::rpc::{RequestVoteRequest, RequestVoteResponse};

/// Configuration for elections
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    /// Minimum election timeout in milliseconds
    pub min_timeout_ms: u64,
    /// Maximum election timeout in milliseconds
    pub max_timeout_ms: u64,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            min_timeout_ms: 150,
            max_timeout_ms: 300,
        }
    }
}

/// Manages leader election for a Raft node
pub struct ElectionManager {
    /// Raft state
    state: Arc<RaftState>,
    /// Raft log
    log: Arc<RaftLog>,
    /// Election configuration
    config: ElectionConfig,
    /// Last time we heard from leader or granted a vote
    last_heartbeat: Mutex<Instant>,
    /// Current election timeout
    election_timeout: Mutex<Duration>,
    /// Whether an election is in progress
    election_in_progress: AtomicBool,
}

impl ElectionManager {
    /// Create a new election manager
    pub fn new(state: Arc<RaftState>, log: Arc<RaftLog>, config: ElectionConfig) -> Self {
        let timeout = Self::random_timeout(&config);
        Self {
            state,
            log,
            config,
            last_heartbeat: Mutex::new(Instant::now()),
            election_timeout: Mutex::new(timeout),
            election_in_progress: AtomicBool::new(false),
        }
    }

    /// Generate a random election timeout
    fn random_timeout(config: &ElectionConfig) -> Duration {
        let mut rng = rand::thread_rng();
        let ms = rng.gen_range(config.min_timeout_ms..=config.max_timeout_ms);
        Duration::from_millis(ms)
    }

    /// Reset the election timeout (called when receiving valid heartbeat or granting vote)
    pub fn reset_timeout(&self) {
        *self.last_heartbeat.lock() = Instant::now();
        *self.election_timeout.lock() = Self::random_timeout(&self.config);
    }

    /// Check if election timeout has elapsed
    pub fn timeout_elapsed(&self) -> bool {
        let last = *self.last_heartbeat.lock();
        let timeout = *self.election_timeout.lock();
        last.elapsed() >= timeout
    }

    /// Get time until next timeout
    pub fn time_until_timeout(&self) -> Duration {
        let last = *self.last_heartbeat.lock();
        let timeout = *self.election_timeout.lock();
        timeout.saturating_sub(last.elapsed())
    }

    /// Start an election
    pub fn start_election(&self) -> RequestVoteRequest {
        self.election_in_progress.store(true, Ordering::SeqCst);
        
        // Increment term and vote for self
        let term = self.state.start_election();
        
        // Reset timeout for this election
        self.reset_timeout();

        // Create vote request
        RequestVoteRequest::new(
            term,
            self.state.node_id.clone(),
            self.log.last_index(),
            self.log.last_term(),
        )
    }

    /// Handle a vote response
    pub fn handle_vote_response(&self, response: &RequestVoteResponse, votes_received: &mut usize) -> bool {
        // If response term is higher, step down
        if response.term > self.state.current_term() {
            self.state.set_term(response.term);
            self.state.become_follower(None);
            self.election_in_progress.store(false, Ordering::SeqCst);
            return false;
        }

        // Check if we're still a candidate
        if self.state.role() != NodeRole::Candidate {
            return false;
        }

        // Count vote if granted
        if response.vote_granted {
            *votes_received += 1;
        }

        // Check if we have a quorum
        if self.state.has_quorum(*votes_received) {
            self.state.become_leader(self.log.last_index());
            self.election_in_progress.store(false, Ordering::SeqCst);
            return true;
        }

        false
    }

    /// Handle a vote request from another candidate
    pub fn handle_vote_request(&self, request: &RequestVoteRequest) -> RequestVoteResponse {
        let current_term = self.state.current_term();

        // If request term is lower, reject
        if request.term < current_term {
            return RequestVoteResponse::new(current_term, false);
        }

        // If request term is higher, update our term and become follower
        if request.term > current_term {
            self.state.set_term(request.term);
            self.state.become_follower(None);
        }

        let current_term = self.state.current_term();

        // Check if we can vote for this candidate
        let can_vote = {
            let voted_for = self.state.voted_for();
            voted_for.is_none() || voted_for.as_ref() == Some(&request.candidate_id)
        };

        // Check if candidate's log is at least as up-to-date as ours
        let log_ok = self.log.is_up_to_date(request.last_log_index, request.last_log_term);

        if can_vote && log_ok {
            self.state.vote_for(request.candidate_id.clone());
            self.reset_timeout(); // Reset timeout when granting vote
            RequestVoteResponse::new(current_term, true)
        } else {
            RequestVoteResponse::new(current_term, false)
        }
    }

    /// Check if this node should start an election
    pub fn should_start_election(&self) -> bool {
        // Don't start election if we're already leader
        if self.state.is_leader() {
            return false;
        }

        // Don't start if election is already in progress
        if self.election_in_progress.load(Ordering::SeqCst) {
            return false;
        }

        // Start election if timeout elapsed
        self.timeout_elapsed()
    }

    /// Cancel any in-progress election
    pub fn cancel_election(&self) {
        self.election_in_progress.store(false, Ordering::SeqCst);
    }

    /// Check if an election is in progress
    pub fn is_election_in_progress(&self) -> bool {
        self.election_in_progress.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manager() -> ElectionManager {
        let state = Arc::new(RaftState::new(
            "node1".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        ));
        let log = Arc::new(RaftLog::new());
        ElectionManager::new(state, log, ElectionConfig::default())
    }

    #[test]
    fn test_start_election() {
        let manager = create_test_manager();
        
        let request = manager.start_election();
        
        assert_eq!(request.term, 1);
        assert_eq!(request.candidate_id, "node1");
        assert_eq!(manager.state.role(), NodeRole::Candidate);
    }

    #[test]
    fn test_vote_granted() {
        let manager = create_test_manager();
        
        let request = RequestVoteRequest::new(1, "node2".to_string(), 0, 0);
        let response = manager.handle_vote_request(&request);
        
        assert!(response.vote_granted);
        assert_eq!(manager.state.voted_for(), Some("node2".to_string()));
    }

    #[test]
    fn test_vote_rejected_lower_term() {
        let manager = create_test_manager();
        manager.state.set_term(5);
        
        let request = RequestVoteRequest::new(3, "node2".to_string(), 0, 0);
        let response = manager.handle_vote_request(&request);
        
        assert!(!response.vote_granted);
        assert_eq!(response.term, 5);
    }

    #[test]
    fn test_become_leader() {
        let manager = create_test_manager();
        
        manager.start_election();
        
        // We vote for ourselves, so we have 1 vote
        let mut votes = 1;
        
        // Receive vote from one peer (quorum is 2 for 3 nodes)
        let response = RequestVoteResponse::new(1, true);
        let became_leader = manager.handle_vote_response(&response, &mut votes);
        
        assert!(became_leader);
        assert_eq!(manager.state.role(), NodeRole::Leader);
    }
}
