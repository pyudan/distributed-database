//! Transaction Manager
//!
//! Manages the lifecycle of database transactions

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::storage::StorageEngine;
use crate::{Error, Result};

/// Isolation level for transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Read uncommitted - can see uncommitted changes from other transactions
    ReadUncommitted,
    /// Read committed - only see committed changes
    ReadCommitted,
    /// Repeatable read - reads are repeatable within transaction
    RepeatableRead,
    /// Serializable - full isolation, transactions appear serial
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::RepeatableRead
    }
}

/// Status of a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionStatus {
    /// Transaction is active
    Active,
    /// Transaction is preparing to commit (2PC)
    Preparing,
    /// Transaction has committed
    Committed,
    /// Transaction has been aborted
    Aborted,
}

/// A write that was performed in a transaction (for rollback)
#[derive(Debug, Clone)]
struct TransactionWrite {
    key: Vec<u8>,
    /// Old value (None if key didn't exist before)
    old_value: Option<Vec<u8>>,
}

/// A database transaction
#[derive(Debug)]
pub struct Transaction {
    /// Unique transaction ID
    pub id: u64,
    /// Transaction status
    pub status: TransactionStatus,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Timestamp when transaction started
    pub start_timestamp: u64,
    /// Timestamp for reads (MVCC snapshot)
    pub read_timestamp: u64,
    /// Keys written in this transaction (for rollback)
    writes: Vec<TransactionWrite>,
    /// Keys read in this transaction (for conflict detection)
    reads: Vec<Vec<u8>>,
    /// When this transaction started
    started_at: Instant,
    /// Timeout for this transaction
    timeout: Duration,
}

impl Transaction {
    /// Create a new transaction
    fn new(id: u64, start_timestamp: u64, isolation_level: IsolationLevel) -> Self {
        Self {
            id,
            status: TransactionStatus::Active,
            isolation_level,
            start_timestamp,
            read_timestamp: start_timestamp,
            writes: Vec::new(),
            reads: Vec::new(),
            started_at: Instant::now(),
            timeout: Duration::from_secs(30),
        }
    }

    /// Check if transaction has timed out
    pub fn is_timed_out(&self) -> bool {
        self.started_at.elapsed() > self.timeout
    }

    /// Record a write
    fn record_write(&mut self, key: Vec<u8>, old_value: Option<Vec<u8>>) {
        self.writes.push(TransactionWrite { key, old_value });
    }

    /// Record a read
    fn record_read(&mut self, key: Vec<u8>) {
        if !self.reads.contains(&key) {
            self.reads.push(key);
        }
    }

    /// Get all writes for rollback
    fn get_writes(&self) -> &[TransactionWrite] {
        &self.writes
    }
}

/// Transaction manager
pub struct TransactionManager {
    /// Storage engine
    storage: Arc<StorageEngine>,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Active transactions
    active_txns: RwLock<HashMap<u64, Mutex<Transaction>>>,
    /// Default isolation level
    default_isolation: IsolationLevel,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            next_txn_id: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            default_isolation: IsolationLevel::RepeatableRead,
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Result<u64> {
        self.begin_with_isolation(self.default_isolation)
    }

    /// Begin a transaction with specific isolation level
    pub fn begin_with_isolation(&self, isolation_level: IsolationLevel) -> Result<u64> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_timestamp = self.storage.current_timestamp();
        
        let txn = Transaction::new(txn_id, start_timestamp, isolation_level);
        
        // Log transaction start
        self.storage.log_txn_begin(txn_id)?;
        
        // Store transaction
        self.active_txns.write().insert(txn_id, Mutex::new(txn));
        
        Ok(txn_id)
    }

    /// Commit a transaction
    pub fn commit(&self, txn_id: u64) -> Result<()> {
        // Get and remove transaction
        let txn = self.active_txns.write().remove(&txn_id)
            .ok_or_else(|| Error::TransactionAborted(format!("Transaction {} not found", txn_id)))?;
        
        let mut txn = txn.lock();
        
        // Check if already finished
        if txn.status != TransactionStatus::Active {
            return Err(Error::TransactionAborted(format!(
                "Transaction {} is not active: {:?}",
                txn_id, txn.status
            )));
        }
        
        // Check timeout
        if txn.is_timed_out() {
            txn.status = TransactionStatus::Aborted;
            return Err(Error::TransactionAborted("Transaction timed out".into()));
        }
        
        // Mark as committed
        txn.status = TransactionStatus::Committed;
        
        // Log commit
        self.storage.log_txn_commit(txn_id)?;
        
        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback(&self, txn_id: u64) -> Result<()> {
        // Get and remove transaction
        let txn = self.active_txns.write().remove(&txn_id);
        
        if let Some(txn) = txn {
            let mut txn = txn.lock();
            txn.status = TransactionStatus::Aborted;
            
            // Log abort
            self.storage.log_txn_abort(txn_id)?;
            
            // Note: In a real implementation, we would also undo the writes
            // Since we're using MVCC, uncommitted writes are simply not visible
            // to other transactions, so no explicit undo is needed
        }
        
        Ok(())
    }

    /// Get a value within a transaction
    pub fn get(&self, txn_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txns = self.active_txns.read();
        let txn = txns.get(&txn_id)
            .ok_or_else(|| Error::TransactionAborted(format!("Transaction {} not found", txn_id)))?;
        
        let mut txn = txn.lock();
        
        if txn.status != TransactionStatus::Active {
            return Err(Error::TransactionAborted("Transaction not active".into()));
        }
        
        // Record the read
        txn.record_read(key.to_vec());
        
        // Get value at transaction's read timestamp
        let read_ts = match txn.isolation_level {
            IsolationLevel::ReadUncommitted => self.storage.current_timestamp(),
            IsolationLevel::ReadCommitted => self.storage.current_timestamp(),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => txn.read_timestamp,
        };
        
        self.storage.get_at(key, read_ts)
    }

    /// Put a value within a transaction
    pub fn put(&self, txn_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let txns = self.active_txns.read();
        let txn = txns.get(&txn_id)
            .ok_or_else(|| Error::TransactionAborted(format!("Transaction {} not found", txn_id)))?;
        
        let mut txn = txn.lock();
        
        if txn.status != TransactionStatus::Active {
            return Err(Error::TransactionAborted("Transaction not active".into()));
        }
        
        // Get old value for rollback
        let old_value = self.storage.get(&key)?;
        
        // Record the write
        txn.record_write(key.clone(), old_value);
        
        // Perform the write with transaction ID
        self.storage.put_with_txn(key, value, txn_id)?;
        
        Ok(())
    }

    /// Delete a value within a transaction
    pub fn delete(&self, txn_id: u64, key: Vec<u8>) -> Result<()> {
        let txns = self.active_txns.read();
        let txn = txns.get(&txn_id)
            .ok_or_else(|| Error::TransactionAborted(format!("Transaction {} not found", txn_id)))?;
        
        let mut txn = txn.lock();
        
        if txn.status != TransactionStatus::Active {
            return Err(Error::TransactionAborted("Transaction not active".into()));
        }
        
        // Get old value for rollback
        let old_value = self.storage.get(&key)?;
        
        // Record the write
        txn.record_write(key.clone(), old_value);
        
        // Perform the delete with transaction ID
        self.storage.delete_with_txn(key, txn_id)?;
        
        Ok(())
    }

    /// Get all active transaction IDs
    pub fn active_transactions(&self) -> Vec<u64> {
        self.active_txns.read().keys().cloned().collect()
    }

    /// Check if a transaction is active
    pub fn is_active(&self, txn_id: u64) -> bool {
        self.active_txns.read().contains_key(&txn_id)
    }

    /// Abort all timed-out transactions
    pub fn abort_timed_out(&self) -> Vec<u64> {
        let mut aborted = Vec::new();
        
        let txn_ids: Vec<u64> = self.active_txns.read().keys().cloned().collect();
        
        for txn_id in txn_ids {
            let should_abort = {
                let txns = self.active_txns.read();
                if let Some(txn) = txns.get(&txn_id) {
                    txn.lock().is_timed_out()
                } else {
                    false
                }
            };
            
            if should_abort {
                if self.rollback(txn_id).is_ok() {
                    aborted.push(txn_id);
                }
            }
        }
        
        aborted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use tempfile::TempDir;

    fn create_test_manager() -> (TempDir, TransactionManager) {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_segment_size: 1024 * 1024,
            sync_on_write: false,
        };
        let storage = Arc::new(StorageEngine::new(config).unwrap());
        (temp_dir, TransactionManager::new(storage))
    }

    #[test]
    fn test_begin_and_commit() {
        let (_temp, manager) = create_test_manager();
        
        let txn_id = manager.begin().unwrap();
        assert!(manager.is_active(txn_id));
        
        manager.commit(txn_id).unwrap();
        assert!(!manager.is_active(txn_id));
    }

    #[test]
    fn test_begin_and_rollback() {
        let (_temp, manager) = create_test_manager();
        
        let txn_id = manager.begin().unwrap();
        manager.put(txn_id, b"key".to_vec(), b"value".to_vec()).unwrap();
        
        manager.rollback(txn_id).unwrap();
        assert!(!manager.is_active(txn_id));
    }

    #[test]
    fn test_get_and_put() {
        let (_temp, manager) = create_test_manager();
        
        let txn_id = manager.begin().unwrap();
        
        manager.put(txn_id, b"key".to_vec(), b"value".to_vec()).unwrap();
        let value = manager.get(txn_id, b"key").unwrap();
        
        assert_eq!(value, Some(b"value".to_vec()));
        
        manager.commit(txn_id).unwrap();
    }

    #[test]
    fn test_isolation() {
        let (_temp, manager) = create_test_manager();
        
        // Transaction 1 writes a value
        let txn1 = manager.begin().unwrap();
        manager.put(txn1, b"key".to_vec(), b"value1".to_vec()).unwrap();
        manager.commit(txn1).unwrap();
        
        // Transaction 2 starts and reads
        let txn2 = manager.begin().unwrap();
        let value = manager.get(txn2, b"key").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
        
        manager.commit(txn2).unwrap();
    }
}
