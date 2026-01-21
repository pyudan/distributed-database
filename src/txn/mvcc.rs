//! MVCC (Multi-Version Concurrency Control)
//!
//! Provides snapshot isolation for transactions

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

/// A read view for MVCC
/// 
/// Determines which versions of data are visible to a transaction
#[derive(Debug, Clone)]
pub struct ReadView {
    /// The timestamp when this view was created
    pub snapshot_ts: u64,
    /// Minimum active transaction ID when view was created
    pub min_active_txn: u64,
    /// Maximum transaction ID when view was created
    pub max_txn_id: u64,
    /// Set of active transaction IDs when view was created
    pub active_txns: HashSet<u64>,
}

impl ReadView {
    /// Create a new read view
    pub fn new(snapshot_ts: u64, active_txns: Vec<u64>, max_txn_id: u64) -> Self {
        let min_active_txn = active_txns.iter().min().copied().unwrap_or(max_txn_id);
        
        Self {
            snapshot_ts,
            min_active_txn,
            max_txn_id,
            active_txns: active_txns.into_iter().collect(),
        }
    }

    /// Check if a version created by txn_id is visible to this view
    pub fn is_visible(&self, txn_id: u64, committed: bool) -> bool {
        // Version created by current transaction is always visible
        // (This would be checked by the caller with their own txn_id)
        
        // Uncommitted versions from other transactions are not visible
        if !committed {
            return false;
        }
        
        // If txn_id is less than min_active_txn, it was committed before our snapshot
        if txn_id < self.min_active_txn {
            return true;
        }
        
        // If txn_id is greater than max_txn_id, it started after our snapshot
        if txn_id > self.max_txn_id {
            return false;
        }
        
        // If txn_id was in the active set, it wasn't committed at snapshot time
        if self.active_txns.contains(&txn_id) {
            return false;
        }
        
        // Otherwise, it was committed before our snapshot
        true
    }
}

/// MVCC manager
pub struct MvccManager {
    /// Current timestamp (logical clock)
    current_ts: AtomicU64,
    /// Current max transaction ID
    max_txn_id: AtomicU64,
    /// Active transaction IDs
    active_txns: RwLock<HashSet<u64>>,
    /// Committed transaction IDs (could be optimized with a more efficient structure)
    committed_txns: RwLock<HashSet<u64>>,
}

impl MvccManager {
    /// Create a new MVCC manager
    pub fn new() -> Self {
        Self {
            current_ts: AtomicU64::new(0),
            max_txn_id: AtomicU64::new(0),
            active_txns: RwLock::new(HashSet::new()),
            committed_txns: RwLock::new(HashSet::new()),
        }
    }

    /// Get the current timestamp
    pub fn current_timestamp(&self) -> u64 {
        self.current_ts.load(Ordering::SeqCst)
    }

    /// Advance the timestamp
    pub fn advance_timestamp(&self) -> u64 {
        self.current_ts.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Register a new transaction
    pub fn begin_transaction(&self) -> u64 {
        let txn_id = self.max_txn_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.active_txns.write().insert(txn_id);
        txn_id
    }

    /// Create a read view for a transaction
    pub fn create_read_view(&self) -> ReadView {
        let snapshot_ts = self.current_timestamp();
        let active_txns: Vec<u64> = self.active_txns.read().iter().cloned().collect();
        let max_txn_id = self.max_txn_id.load(Ordering::SeqCst);
        
        ReadView::new(snapshot_ts, active_txns, max_txn_id)
    }

    /// Mark a transaction as committed
    pub fn commit_transaction(&self, txn_id: u64) {
        self.active_txns.write().remove(&txn_id);
        self.committed_txns.write().insert(txn_id);
        self.advance_timestamp(); // Advance clock on commit
    }

    /// Mark a transaction as aborted
    pub fn abort_transaction(&self, txn_id: u64) {
        self.active_txns.write().remove(&txn_id);
        // Don't add to committed_txns
    }

    /// Check if a transaction is committed
    pub fn is_committed(&self, txn_id: u64) -> bool {
        self.committed_txns.read().contains(&txn_id)
    }

    /// Check if a transaction is active
    pub fn is_active(&self, txn_id: u64) -> bool {
        self.active_txns.read().contains(&txn_id)
    }

    /// Get all active transactions
    pub fn active_transactions(&self) -> Vec<u64> {
        self.active_txns.read().iter().cloned().collect()
    }

    /// Garbage collect old committed transaction records
    pub fn gc(&self, older_than_txn: u64) -> usize {
        let mut committed = self.committed_txns.write();
        let original_len = committed.len();
        committed.retain(|&txn_id| txn_id >= older_than_txn);
        original_len - committed.len()
    }
}

impl Default for MvccManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_view_visibility() {
        // Create a read view with transactions 2, 4 active
        let view = ReadView::new(10, vec![2, 4], 5);
        
        // Transaction 1 (committed before snapshot) - visible
        assert!(view.is_visible(1, true));
        
        // Transaction 2 (was active at snapshot) - not visible even if committed now
        assert!(!view.is_visible(2, true));
        
        // Transaction 3 (committed before snapshot) - visible
        assert!(view.is_visible(3, true));
        
        // Transaction 4 (was active at snapshot) - not visible
        assert!(!view.is_visible(4, true));
        
        // Transaction 6 (started after snapshot) - not visible
        assert!(!view.is_visible(6, true));
        
        // Uncommitted transaction - never visible
        assert!(!view.is_visible(1, false));
    }

    #[test]
    fn test_mvcc_manager() {
        let mvcc = MvccManager::new();
        
        // Begin two transactions
        let txn1 = mvcc.begin_transaction();
        let txn2 = mvcc.begin_transaction();
        
        assert!(mvcc.is_active(txn1));
        assert!(mvcc.is_active(txn2));
        
        // Commit txn1
        mvcc.commit_transaction(txn1);
        assert!(!mvcc.is_active(txn1));
        assert!(mvcc.is_committed(txn1));
        
        // Abort txn2
        mvcc.abort_transaction(txn2);
        assert!(!mvcc.is_active(txn2));
        assert!(!mvcc.is_committed(txn2));
    }

    #[test]
    fn test_read_view_creation() {
        let mvcc = MvccManager::new();
        
        // Begin some transactions
        let txn1 = mvcc.begin_transaction();
        let txn2 = mvcc.begin_transaction();
        mvcc.commit_transaction(txn1);
        
        // Create read view - should see txn2 as active
        let view = mvcc.create_read_view();
        
        assert!(view.active_txns.contains(&txn2));
        assert!(!view.active_txns.contains(&txn1));
    }
}
