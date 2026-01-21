//! Raft RPC Messages
//!
//! Defines the message types used for Raft inter-node communication.

use serde::{Deserialize, Serialize};
use super::log::LogEntry;

/// Raft RPC message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Request vote from peers
    RequestVote(RequestVoteRequest),
    /// Response to vote request
    RequestVoteResponse(RequestVoteResponse),
    /// Append entries (heartbeat or log replication)
    AppendEntries(AppendEntriesRequest),
    /// Response to append entries
    AppendEntriesResponse(AppendEntriesResponse),
    /// Install snapshot
    InstallSnapshot(InstallSnapshotRequest),
    /// Response to install snapshot
    InstallSnapshotResponse(InstallSnapshotResponse),
}

/// RequestVote RPC arguments (invoked by candidates to gather votes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// Candidate's term
    pub term: u64,
    /// Candidate requesting vote
    pub candidate_id: String,
    /// Index of candidate's last log entry
    pub last_log_index: u64,
    /// Term of candidate's last log entry
    pub last_log_term: u64,
}

/// RequestVote RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself
    pub term: u64,
    /// True means candidate received vote
    pub vote_granted: bool,
}

/// AppendEntries RPC arguments (invoked by leader to replicate log entries; also used as heartbeat)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// Leader's term
    pub term: u64,
    /// Leader's ID so follower can redirect clients
    pub leader_id: String,
    /// Index of log entry immediately preceding new ones
    pub prev_log_index: u64,
    /// Term of prev_log_index entry
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat)
    pub entries: Vec<LogEntry>,
    /// Leader's commit index
    pub leader_commit: u64,
}

/// AppendEntries RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself
    pub term: u64,
    /// True if follower contained entry matching prevLogIndex and prevLogTerm
    pub success: bool,
    /// The follower's last log index (for faster log backup)
    pub match_index: u64,
}

/// InstallSnapshot RPC arguments (invoked by leader to send snapshot chunks)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// Leader's term
    pub term: u64,
    /// Leader's ID
    pub leader_id: String,
    /// The snapshot replaces all entries up through and including this index
    pub last_included_index: u64,
    /// Term of last_included_index
    pub last_included_term: u64,
    /// Byte offset where chunk is positioned in the snapshot file
    pub offset: u64,
    /// Raw bytes of the snapshot chunk
    pub data: Vec<u8>,
    /// True if this is the last chunk
    pub done: bool,
}

/// InstallSnapshot RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// Current term, for leader to update itself
    pub term: u64,
}

impl RequestVoteRequest {
    pub fn new(term: u64, candidate_id: String, last_log_index: u64, last_log_term: u64) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

impl RequestVoteResponse {
    pub fn new(term: u64, vote_granted: bool) -> Self {
        Self { term, vote_granted }
    }
}

impl AppendEntriesRequest {
    pub fn heartbeat(term: u64, leader_id: String, prev_log_index: u64, prev_log_term: u64, leader_commit: u64) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit,
        }
    }

    pub fn new(
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }
}

impl AppendEntriesResponse {
    pub fn new(term: u64, success: bool, match_index: u64) -> Self {
        Self {
            term,
            success,
            match_index,
        }
    }
}
