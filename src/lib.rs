//! PyuDB - A Distributed Database
//!
//! A production-quality distributed database demonstrating:
//! - Raft consensus algorithm for distributed agreement
//! - ACID transactions with MVCC
//! - Custom PyuSQL query language

pub mod storage;
pub mod raft;
pub mod txn;
pub mod query;
pub mod network;
pub mod error;

pub use error::{Error, Result};
