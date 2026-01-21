//! B-Tree implementation for in-memory indexing
//!
//! A thread-safe B-Tree that supports:
//! - O(log n) insertions, deletions, and lookups
//! - Range scans
//! - MVCC versioning

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{Error, Result};

/// A versioned value with timestamp for MVCC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedValue {
    /// The actual data
    pub data: Vec<u8>,
    /// Transaction ID that created this version
    pub txn_id: u64,
    /// Timestamp when this version was created
    pub timestamp: u64,
    /// Whether this version represents a deletion
    pub deleted: bool,
}

/// Thread-safe B-Tree for key-value storage
#[derive(Debug)]
pub struct BTree {
    /// The underlying B-Tree map protected by a read-write lock
    inner: RwLock<BTreeMap<Vec<u8>, Vec<VersionedValue>>>,
    /// Name of this B-Tree (for debugging)
    name: String,
}

impl BTree {
    /// Create a new empty B-Tree
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            name: name.into(),
        }
    }

    /// Get the name of this B-Tree
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert a key-value pair with versioning
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, txn_id: u64, timestamp: u64) -> Result<()> {
        let versioned = VersionedValue {
            data: value,
            txn_id,
            timestamp,
            deleted: false,
        };

        let mut tree = self.inner.write();
        tree.entry(key)
            .or_insert_with(Vec::new)
            .push(versioned);

        Ok(())
    }

    /// Get the latest visible value for a key at a given timestamp
    pub fn get(&self, key: &[u8], read_timestamp: u64) -> Result<Option<Vec<u8>>> {
        let tree = self.inner.read();
        
        if let Some(versions) = tree.get(key) {
            // Find the latest version visible to this transaction
            let visible = versions
                .iter()
                .filter(|v| v.timestamp <= read_timestamp)
                .max_by_key(|v| v.timestamp);

            if let Some(version) = visible {
                if version.deleted {
                    return Ok(None);
                }
                return Ok(Some(version.data.clone()));
            }
        }

        Ok(None)
    }

    /// Get the latest value regardless of timestamp (for internal use)
    pub fn get_latest(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tree = self.inner.read();
        
        if let Some(versions) = tree.get(key) {
            if let Some(version) = versions.last() {
                if version.deleted {
                    return Ok(None);
                }
                return Ok(Some(version.data.clone()));
            }
        }

        Ok(None)
    }

    /// Delete a key by inserting a tombstone version
    pub fn delete(&self, key: Vec<u8>, txn_id: u64, timestamp: u64) -> Result<()> {
        let versioned = VersionedValue {
            data: Vec::new(),
            txn_id,
            timestamp,
            deleted: true,
        };

        let mut tree = self.inner.write();
        tree.entry(key)
            .or_insert_with(Vec::new)
            .push(versioned);

        Ok(())
    }

    /// Scan a range of keys
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        read_timestamp: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tree = self.inner.read();
        let mut results = Vec::new();

        for (key, versions) in tree.range(start.to_vec()..end.to_vec()) {
            let visible = versions
                .iter()
                .filter(|v| v.timestamp <= read_timestamp)
                .max_by_key(|v| v.timestamp);

            if let Some(version) = visible {
                if !version.deleted {
                    results.push((key.clone(), version.data.clone()));
                }
            }
        }

        Ok(results)
    }

    /// Scan all keys with a given prefix
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        read_timestamp: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tree = self.inner.read();
        let mut results = Vec::new();

        for (key, versions) in tree.iter() {
            if key.starts_with(prefix) {
                let visible = versions
                    .iter()
                    .filter(|v| v.timestamp <= read_timestamp)
                    .max_by_key(|v| v.timestamp);

                if let Some(version) = visible {
                    if !version.deleted {
                        results.push((key.clone(), version.data.clone()));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Get the number of keys in the tree (including deleted)
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Check if the tree is empty
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Garbage collect old versions before a given timestamp
    pub fn gc(&self, before_timestamp: u64) -> Result<usize> {
        let mut tree = self.inner.write();
        let mut collected = 0;

        for versions in tree.values_mut() {
            let original_len = versions.len();
            
            // Keep only the latest version before the cutoff, plus all versions after
            if let Some(latest_before) = versions
                .iter()
                .filter(|v| v.timestamp < before_timestamp)
                .max_by_key(|v| v.timestamp)
                .cloned()
            {
                versions.retain(|v| v.timestamp >= before_timestamp);
                if !versions.iter().any(|v| v.timestamp == latest_before.timestamp) {
                    versions.insert(0, latest_before);
                }
            }
            
            collected += original_len - versions.len();
        }

        // Remove keys with no versions
        tree.retain(|_, versions| !versions.is_empty());

        Ok(collected)
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new("default")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let tree = BTree::new("test");
        
        // Insert
        tree.put(b"key1".to_vec(), b"value1".to_vec(), 1, 100).unwrap();
        tree.put(b"key2".to_vec(), b"value2".to_vec(), 1, 100).unwrap();
        
        // Get
        assert_eq!(tree.get(b"key1", 100).unwrap(), Some(b"value1".to_vec()));
        assert_eq!(tree.get(b"key2", 100).unwrap(), Some(b"value2".to_vec()));
        assert_eq!(tree.get(b"key3", 100).unwrap(), None);
    }

    #[test]
    fn test_mvcc_versioning() {
        let tree = BTree::new("test");
        
        // Insert multiple versions
        tree.put(b"key".to_vec(), b"v1".to_vec(), 1, 100).unwrap();
        tree.put(b"key".to_vec(), b"v2".to_vec(), 2, 200).unwrap();
        tree.put(b"key".to_vec(), b"v3".to_vec(), 3, 300).unwrap();
        
        // Read at different timestamps
        assert_eq!(tree.get(b"key", 150).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(tree.get(b"key", 250).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(tree.get(b"key", 350).unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_delete() {
        let tree = BTree::new("test");
        
        tree.put(b"key".to_vec(), b"value".to_vec(), 1, 100).unwrap();
        assert_eq!(tree.get(b"key", 100).unwrap(), Some(b"value".to_vec()));
        
        tree.delete(b"key".to_vec(), 2, 200).unwrap();
        assert_eq!(tree.get(b"key", 200).unwrap(), None);
        
        // But we can still read the old version
        assert_eq!(tree.get(b"key", 150).unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn test_scan() {
        let tree = BTree::new("test");
        
        tree.put(b"a".to_vec(), b"1".to_vec(), 1, 100).unwrap();
        tree.put(b"b".to_vec(), b"2".to_vec(), 1, 100).unwrap();
        tree.put(b"c".to_vec(), b"3".to_vec(), 1, 100).unwrap();
        tree.put(b"d".to_vec(), b"4".to_vec(), 1, 100).unwrap();
        
        let results = tree.scan(b"b", b"d", 100).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }
}
