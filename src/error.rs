//! Error types for PyuDB

use thiserror::Error;

/// Main error type for PyuDB
#[derive(Error, Debug)]
pub enum Error {
    // Storage errors
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("WAL error: {0}")]
    Wal(String),
    
    #[error("Corruption detected: {0}")]
    Corruption(String),
    
    // Raft errors
    #[error("Not leader, leader is: {0:?}")]
    NotLeader(Option<String>),
    
    #[error("Election timeout")]
    ElectionTimeout,
    
    #[error("Log inconsistency at index {0}")]
    LogInconsistency(u64),
    
    // Transaction errors
    #[error("Transaction aborted: {0}")]
    TransactionAborted(String),
    
    #[error("Write conflict on key: {0}")]
    WriteConflict(String),
    
    #[error("Deadlock detected")]
    Deadlock,
    
    // Query errors
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    
    // Network errors
    #[error("Connection failed: {0}")]
    Connection(String),
    
    #[error("RPC timeout")]
    RpcTimeout,
    
    // IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    // Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for PyuDB operations
pub type Result<T> = std::result::Result<T, Error>;
