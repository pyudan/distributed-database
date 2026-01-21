//! Raft Consensus Module
//!
//! Implementation of the Raft consensus algorithm for distributed agreement.
//! 
//! Features:
//! - Leader election with randomized timeouts
//! - Log replication to followers
//! - Safety guarantees for linearizable reads/writes

mod state;
mod log;
mod election;
mod replication;
mod node;
mod rpc;

pub use state::{RaftState, NodeRole};
pub use log::{LogEntry, RaftLog};
pub use election::ElectionManager;
pub use replication::ReplicationManager;
pub use node::{RaftNode, RaftConfig};
pub use rpc::{RaftMessage, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse};
