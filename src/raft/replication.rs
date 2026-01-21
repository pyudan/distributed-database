//! Log Replication Manager
//!
//! Handles replicating log entries from leader to followers

use std::sync::Arc;
use std::collections::HashMap;

use super::state::{RaftState, NodeRole};
use super::log::{RaftLog, LogEntry, CommandType};
use super::rpc::{AppendEntriesRequest, AppendEntriesResponse};
use crate::{Error, Result};

/// Manages log replication for a Raft leader
pub struct ReplicationManager {
    /// Raft state
    state: Arc<RaftState>,
    /// Raft log
    log: Arc<RaftLog>,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(state: Arc<RaftState>, log: Arc<RaftLog>) -> Self {
        Self { state, log }
    }

    /// Create an AppendEntries request for a specific peer
    pub fn create_append_entries(&self, peer_id: &str) -> Option<AppendEntriesRequest> {
        if !self.state.is_leader() {
            return None;
        }

        let leader_state = self.state.leader_state()?;
        let next_index = *leader_state.next_index.get(peer_id)?;
        
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

        // Get entries to send
        let entries = self.log.get_from(next_index);

        Some(AppendEntriesRequest::new(
            self.state.current_term(),
            self.state.node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            self.state.commit_index(),
        ))
    }

    /// Create a heartbeat (empty AppendEntries) for a specific peer
    pub fn create_heartbeat(&self, peer_id: &str) -> Option<AppendEntriesRequest> {
        if !self.state.is_leader() {
            return None;
        }

        let leader_state = self.state.leader_state()?;
        let next_index = *leader_state.next_index.get(peer_id)?;
        
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

        Some(AppendEntriesRequest::heartbeat(
            self.state.current_term(),
            self.state.node_id.clone(),
            prev_log_index,
            prev_log_term,
            self.state.commit_index(),
        ))
    }

    /// Handle an AppendEntries response from a peer
    pub fn handle_append_response(&self, peer_id: &str, response: &AppendEntriesResponse) -> Result<()> {
        // If response term is higher, step down
        if response.term > self.state.current_term() {
            self.state.set_term(response.term);
            self.state.become_follower(None);
            return Ok(());
        }

        // Ignore if we're not leader
        if !self.state.is_leader() {
            return Ok(());
        }

        self.state.with_leader_state(|leader_state| {
            if response.success {
                // Update match_index and next_index
                leader_state.update_match_index(peer_id, response.match_index);
            } else {
                // Decrement next_index and retry
                leader_state.decrement_next_index(peer_id);
            }
        });

        // Try to advance commit index
        self.try_advance_commit_index();

        Ok(())
    }

    /// Handle an incoming AppendEntries request (as a follower)
    pub fn handle_append_entries(&self, request: &AppendEntriesRequest) -> AppendEntriesResponse {
        let current_term = self.state.current_term();

        // Reply false if term < currentTerm
        if request.term < current_term {
            return AppendEntriesResponse::new(current_term, false, self.log.last_index());
        }

        // If request term is higher, update term
        if request.term > current_term {
            self.state.set_term(request.term);
        }

        // Recognize leader and become follower
        self.state.become_follower(Some(request.leader_id.clone()));

        // Check if log contains entry at prevLogIndex with prevLogTerm
        if request.prev_log_index > 0 {
            match self.log.term_at(request.prev_log_index) {
                Some(term) if term != request.prev_log_term => {
                    // Conflict: delete this and all following entries
                    self.log.truncate(request.prev_log_index);
                    return AppendEntriesResponse::new(
                        self.state.current_term(),
                        false,
                        self.log.last_index(),
                    );
                }
                None => {
                    // Don't have this entry yet
                    return AppendEntriesResponse::new(
                        self.state.current_term(),
                        false,
                        self.log.last_index(),
                    );
                }
                _ => {}
            }
        }

        // Append new entries
        if let Err(_) = self.log.append_entries(
            request.prev_log_index,
            request.prev_log_term,
            request.entries.clone(),
        ) {
            return AppendEntriesResponse::new(
                self.state.current_term(),
                false,
                self.log.last_index(),
            );
        }

        // Update commit index
        if request.leader_commit > self.state.commit_index() {
            let new_commit = request.leader_commit.min(self.log.last_index());
            self.state.set_commit_index(new_commit);
        }

        AppendEntriesResponse::new(
            self.state.current_term(),
            true,
            self.log.last_index(),
        )
    }

    /// Append a new command to the log (leader only)
    pub fn append_command(&self, command: CommandType) -> Result<u64> {
        if !self.state.is_leader() {
            return Err(Error::NotLeader(self.state.current_leader()));
        }

        let index = self.log.append(self.state.current_term(), command);
        Ok(index)
    }

    /// Try to advance the commit index based on replication state
    fn try_advance_commit_index(&self) {
        if !self.state.is_leader() {
            return;
        }

        let current_term = self.state.current_term();
        let last_index = self.log.last_index();
        let current_commit = self.state.commit_index();

        // Find the highest index replicated on a majority
        for n in (current_commit + 1)..=last_index {
            // Only commit entries from current term
            if self.log.term_at(n) != Some(current_term) {
                continue;
            }

            // Count how many servers have this entry
            let mut count = 1; // Leader has it

            if let Some(leader_state) = self.state.leader_state() {
                for match_idx in leader_state.match_index.values() {
                    if *match_idx >= n {
                        count += 1;
                    }
                }
            }

            if self.state.has_quorum(count) {
                self.state.set_commit_index(n);
            }
        }
    }

    /// Get entries that need to be applied to the state machine
    pub fn get_entries_to_apply(&self) -> Vec<LogEntry> {
        let last_applied = self.state.last_applied();
        let commit_index = self.state.commit_index();

        if commit_index <= last_applied {
            return Vec::new();
        }

        self.log.get_range(last_applied + 1, commit_index)
    }

    /// Mark entries as applied
    pub fn mark_applied(&self, up_to_index: u64) {
        self.state.set_last_applied(up_to_index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manager() -> ReplicationManager {
        let state = Arc::new(RaftState::new(
            "leader".to_string(),
            vec!["follower1".to_string(), "follower2".to_string()],
        ));
        let log = Arc::new(RaftLog::new());
        
        // Make this node the leader
        state.become_leader(0);
        
        ReplicationManager::new(state, log)
    }

    #[test]
    fn test_append_command() {
        let manager = create_test_manager();
        
        let index = manager.append_command(CommandType::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }).unwrap();
        
        assert_eq!(index, 1);
    }

    #[test]
    fn test_create_append_entries() {
        let manager = create_test_manager();
        
        // Append a command first
        manager.append_command(CommandType::Noop).unwrap();
        
        let request = manager.create_append_entries("follower1").unwrap();
        
        assert_eq!(request.term, 0); // Initial term is 0
        assert_eq!(request.leader_id, "leader");
        assert_eq!(request.entries.len(), 1);
    }

    #[test]
    fn test_handle_append_entries_as_follower() {
        let state = Arc::new(RaftState::new(
            "follower".to_string(),
            vec!["leader".to_string()],
        ));
        let log = Arc::new(RaftLog::new());
        let manager = ReplicationManager::new(state.clone(), log);

        let request = AppendEntriesRequest::new(
            1,
            "leader".to_string(),
            0,
            0,
            vec![LogEntry::new(1, 1, CommandType::Noop)],
            0,
        );

        let response = manager.handle_append_entries(&request);
        
        assert!(response.success);
        assert_eq!(response.match_index, 1);
        assert_eq!(state.current_leader(), Some("leader".to_string()));
    }
}
