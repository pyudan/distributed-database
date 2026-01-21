//! Transaction Module
//!
//! Provides ACID transaction support with:
//! - Transaction lifecycle management
//! - MVCC (Multi-Version Concurrency Control)
//! - Lock management and deadlock detection

mod manager;
mod mvcc;
mod lock;

pub use manager::{TransactionManager, Transaction, TransactionStatus, IsolationLevel};
pub use mvcc::{MvccManager, ReadView};
pub use lock::{LockManager, LockMode, LockRequest};
