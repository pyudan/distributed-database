//! Storage Engine - Main interface for data storage
//!
//! Combines B-Tree indexing with WAL for a complete storage solution

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::btree::BTree;
use super::wal::{Wal, WalEntryType};
use crate::{Error, Result};

/// Configuration for the storage engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory for data files
    pub data_dir: PathBuf,
    /// Maximum WAL segment size in bytes
    pub wal_segment_size: u64,
    /// Whether to sync WAL on every write
    pub sync_on_write: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            wal_segment_size: 64 * 1024 * 1024, // 64 MB
            sync_on_write: true,
        }
    }
}

/// The main storage engine
pub struct StorageEngine {
    /// Configuration
    config: StorageConfig,
    /// B-Tree for key-value storage
    tree: Arc<BTree>,
    /// Write-ahead log
    wal: Arc<Wal>,
    /// Global timestamp counter
    timestamp: AtomicU64,
}

impl StorageEngine {
    /// Create a new storage engine
    pub fn new(config: StorageConfig) -> Result<Self> {
        // Create data directory if needed
        std::fs::create_dir_all(&config.data_dir)?;

        // Open WAL
        let wal_dir = config.data_dir.join("wal");
        let wal = Arc::new(Wal::open(&wal_dir, config.wal_segment_size)?);

        // Create B-Tree
        let tree = Arc::new(BTree::new("main"));

        let engine = Self {
            config,
            tree,
            wal,
            timestamp: AtomicU64::new(0),
        };

        // Recover state from WAL
        engine.recover()?;

        Ok(engine)
    }

    /// Recover state from WAL on startup
    fn recover(&self) -> Result<()> {
        let entries = self.wal.recover()?;
        let mut max_timestamp = 0u64;

        for entry in entries {
            max_timestamp = max_timestamp.max(entry.timestamp);

            match entry.entry_type {
                WalEntryType::Put => {
                    self.tree.put(entry.key, entry.value, entry.txn_id, entry.timestamp)?;
                }
                WalEntryType::Delete => {
                    self.tree.delete(entry.key, entry.txn_id, entry.timestamp)?;
                }
                _ => {
                    // Transaction control entries handled by transaction manager
                }
            }
        }

        // Update timestamp to be after any recovered entries
        self.timestamp.store(max_timestamp + 1, Ordering::SeqCst);

        Ok(())
    }

    /// Get the next timestamp
    pub fn next_timestamp(&self) -> u64 {
        self.timestamp.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current timestamp
    pub fn current_timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::SeqCst)
    }

    /// Put a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.put_with_txn(key, value, 0)
    }

    /// Put a key-value pair with transaction ID
    pub fn put_with_txn(&self, key: Vec<u8>, value: Vec<u8>, txn_id: u64) -> Result<()> {
        let timestamp = self.next_timestamp();

        // Write to WAL first
        self.wal.append(WalEntryType::Put, txn_id, key.clone(), value.clone(), timestamp)?;

        if self.config.sync_on_write {
            self.wal.sync()?;
        }

        // Then update in-memory B-Tree
        self.tree.put(key, value, txn_id, timestamp)?;

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let timestamp = self.current_timestamp();
        self.tree.get(key, timestamp)
    }

    /// Get a value at a specific timestamp (for MVCC)
    pub fn get_at(&self, key: &[u8], timestamp: u64) -> Result<Option<Vec<u8>>> {
        self.tree.get(key, timestamp)
    }

    /// Delete a key
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.delete_with_txn(key, 0)
    }

    /// Delete a key with transaction ID
    pub fn delete_with_txn(&self, key: Vec<u8>, txn_id: u64) -> Result<()> {
        let timestamp = self.next_timestamp();

        // Write to WAL first
        self.wal.append(WalEntryType::Delete, txn_id, key.clone(), vec![], timestamp)?;

        if self.config.sync_on_write {
            self.wal.sync()?;
        }

        // Then update in-memory B-Tree
        self.tree.delete(key, txn_id, timestamp)?;

        Ok(())
    }

    /// Scan a range of keys
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let timestamp = self.current_timestamp();
        self.tree.scan(start, end, timestamp)
    }

    /// Scan all keys with a given prefix
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let timestamp = self.current_timestamp();
        self.tree.scan_prefix(prefix, timestamp)
    }

    /// Force sync WAL to disk
    pub fn sync(&self) -> Result<()> {
        self.wal.sync()
    }

    /// Get the underlying B-Tree (for advanced operations)
    pub fn tree(&self) -> Arc<BTree> {
        Arc::clone(&self.tree)
    }

    /// Get the underlying WAL (for advanced operations)
    pub fn wal(&self) -> Arc<Wal> {
        Arc::clone(&self.wal)
    }

    /// Begin a transaction in the WAL
    pub fn log_txn_begin(&self, txn_id: u64) -> Result<()> {
        let timestamp = self.next_timestamp();
        self.wal.append(WalEntryType::TxnBegin, txn_id, vec![], vec![], timestamp)?;
        Ok(())
    }

    /// Commit a transaction in the WAL
    pub fn log_txn_commit(&self, txn_id: u64) -> Result<()> {
        let timestamp = self.next_timestamp();
        self.wal.append(WalEntryType::TxnCommit, txn_id, vec![], vec![], timestamp)?;
        self.wal.sync()?; // Always sync on commit
        Ok(())
    }

    /// Abort a transaction in the WAL
    pub fn log_txn_abort(&self, txn_id: u64) -> Result<()> {
        let timestamp = self.next_timestamp();
        self.wal.append(WalEntryType::TxnAbort, txn_id, vec![], vec![], timestamp)?;
        Ok(())
    }

    /// Garbage collect old versions
    pub fn gc(&self, before_timestamp: u64) -> Result<usize> {
        self.tree.gc(before_timestamp)
    }

    /// Compact WAL by truncating old segments
    pub fn compact_wal(&self, before_seq: u64) -> Result<()> {
        self.wal.truncate_before(before_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(temp_dir: &TempDir) -> StorageConfig {
        StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_segment_size: 1024 * 1024,
            sync_on_write: false, // Faster tests
        }
    }

    #[test]
    fn test_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_config(&temp_dir)).unwrap();

        // Put and get
        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        // Update
        engine.put(b"key1".to_vec(), b"value2".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value2".to_vec()));

        // Delete
        engine.delete(b"key1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Write some data
        {
            let engine = StorageEngine::new(test_config(&temp_dir)).unwrap();
            engine.put(b"persistent".to_vec(), b"data".to_vec()).unwrap();
            engine.sync().unwrap();
        }

        // Reopen and verify
        {
            let engine = StorageEngine::new(test_config(&temp_dir)).unwrap();
            assert_eq!(engine.get(b"persistent").unwrap(), Some(b"data".to_vec()));
        }
    }

    #[test]
    fn test_scan() {
        let temp_dir = TempDir::new().unwrap();
        let engine = StorageEngine::new(test_config(&temp_dir)).unwrap();

        engine.put(b"user:1".to_vec(), b"alice".to_vec()).unwrap();
        engine.put(b"user:2".to_vec(), b"bob".to_vec()).unwrap();
        engine.put(b"user:3".to_vec(), b"charlie".to_vec()).unwrap();
        engine.put(b"post:1".to_vec(), b"hello".to_vec()).unwrap();

        let users = engine.scan_prefix(b"user:").unwrap();
        assert_eq!(users.len(), 3);
    }
}
