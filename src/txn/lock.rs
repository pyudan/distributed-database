//! Lock Manager
//!
//! Provides locking for pessimistic concurrency control

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};

use crate::{Error, Result};

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Shared lock (for reads)
    Shared,
    /// Exclusive lock (for writes)
    Exclusive,
}

impl LockMode {
    /// Check if two lock modes are compatible
    pub fn compatible(&self, other: &LockMode) -> bool {
        match (self, other) {
            (LockMode::Shared, LockMode::Shared) => true,
            _ => false,
        }
    }
}

/// A lock request
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// Transaction ID
    pub txn_id: u64,
    /// Lock mode
    pub mode: LockMode,
    /// When the request was made
    pub requested_at: Instant,
}

impl LockRequest {
    pub fn new(txn_id: u64, mode: LockMode) -> Self {
        Self {
            txn_id,
            mode,
            requested_at: Instant::now(),
        }
    }
}

/// State of a lock on a particular key
#[derive(Debug, Default)]
struct LockState {
    /// Current lock holders (txn_id -> mode)
    holders: HashMap<u64, LockMode>,
    /// Waiting requests
    waiters: VecDeque<LockRequest>,
}

impl LockState {
    /// Check if a lock request can be granted
    fn can_grant(&self, request: &LockRequest) -> bool {
        // If requesting shared and all holders have shared, can grant
        // If requesting exclusive and no holders, can grant
        // If this txn already holds a compatible or same lock, can grant
        
        if self.holders.is_empty() {
            return true;
        }
        
        // Check if this transaction already holds the lock
        if let Some(held_mode) = self.holders.get(&request.txn_id) {
            // Already holds it - check if upgrade is needed
            match (held_mode, &request.mode) {
                (LockMode::Exclusive, _) => return true, // Already have exclusive
                (LockMode::Shared, LockMode::Shared) => return true, // Already have shared
                (LockMode::Shared, LockMode::Exclusive) => {
                    // Upgrade: only possible if we're the only holder
                    return self.holders.len() == 1;
                }
            }
        }
        
        // New lock request
        match request.mode {
            LockMode::Shared => {
                // Can grant if all current holders have shared locks
                self.holders.values().all(|m| *m == LockMode::Shared)
            }
            LockMode::Exclusive => {
                // Can only grant if no one holds the lock
                false
            }
        }
    }
    
    /// Grant a lock
    fn grant(&mut self, request: &LockRequest) {
        self.holders.insert(request.txn_id, request.mode);
    }
    
    /// Release a lock
    fn release(&mut self, txn_id: u64) {
        self.holders.remove(&txn_id);
    }
    
    /// Add a waiter
    fn add_waiter(&mut self, request: LockRequest) {
        self.waiters.push_back(request);
    }
    
    /// Try to grant waiting requests
    fn try_grant_waiters(&mut self) -> Vec<u64> {
        let mut granted = Vec::new();
        let mut still_waiting = VecDeque::new();
        
        while let Some(request) = self.waiters.pop_front() {
            if self.can_grant(&request) {
                self.grant(&request);
                granted.push(request.txn_id);
            } else {
                still_waiting.push_back(request);
                // In strict FIFO, we stop here
                // In fair scheduling, we might continue
                break;
            }
        }
        
        // Put back remaining waiters
        while let Some(waiter) = still_waiting.pop_front() {
            self.waiters.push_front(waiter);
        }
        while let Some(waiter) = self.waiters.pop_back() {
            still_waiting.push_front(waiter);
        }
        self.waiters = still_waiting;
        
        granted
    }
}

/// Lock manager for pessimistic concurrency control
pub struct LockManager {
    /// Lock state per key
    locks: RwLock<HashMap<Vec<u8>, Mutex<LockState>>>,
    /// Locks held by each transaction (for cleanup)
    txn_locks: RwLock<HashMap<u64, HashSet<Vec<u8>>>>,
    /// Lock wait timeout
    timeout: Duration,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            txn_locks: RwLock::new(HashMap::new()),
            timeout: Duration::from_secs(10),
        }
    }
    
    /// Create with custom timeout
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            txn_locks: RwLock::new(HashMap::new()),
            timeout,
        }
    }

    /// Acquire a lock (blocks until acquired or timeout)
    pub fn lock(&self, txn_id: u64, key: &[u8], mode: LockMode) -> Result<()> {
        let request = LockRequest::new(txn_id, mode);
        let key_vec = key.to_vec();
        
        // Note: In a real implementation, we would need proper waiting mechanism
        // For now, we do a simple spin-wait with timeout
        let deadline = Instant::now() + self.timeout;
        
        loop {
            {
                // Get or create lock state
                let locks = self.locks.read();
                
                if let Some(lock_mutex) = locks.get(&key_vec) {
                    let mut state = lock_mutex.lock();
                    
                    if state.can_grant(&request) {
                        state.grant(&request);
                        drop(state);
                        drop(locks);
                        
                        // Record lock for this transaction
                        self.txn_locks.write()
                            .entry(txn_id)
                            .or_insert_with(HashSet::new)
                            .insert(key_vec);
                        
                        return Ok(());
                    }
                    
                    // Add to waiters if not already waiting
                    let already_waiting = state.waiters.iter().any(|w| w.txn_id == txn_id);
                    if !already_waiting {
                        state.add_waiter(request.clone());
                    }
                } else {
                    drop(locks);
                    
                    // Create new lock state
                    let mut locks = self.locks.write();
                    let lock_state = locks.entry(key_vec.clone())
                        .or_insert_with(|| Mutex::new(LockState::default()));
                    
                    let mut state = lock_state.lock();
                    if state.can_grant(&request) {
                        state.grant(&request);
                        drop(state);
                        drop(locks);
                        
                        self.txn_locks.write()
                            .entry(txn_id)
                            .or_insert_with(HashSet::new)
                            .insert(key_vec);
                        
                        return Ok(());
                    }
                }
            }
            
            if Instant::now() > deadline {
                // Remove from waiters
                let locks = self.locks.read();
                if let Some(lock_mutex) = locks.get(&key_vec) {
                    let mut state = lock_mutex.lock();
                    state.waiters.retain(|w| w.txn_id != txn_id);
                }
                
                return Err(Error::Deadlock);
            }
            
            // Brief sleep to avoid busy-waiting
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Try to acquire a lock without blocking
    pub fn try_lock(&self, txn_id: u64, key: &[u8], mode: LockMode) -> Result<bool> {
        let request = LockRequest::new(txn_id, mode);
        let key_vec = key.to_vec();
        
        let locks = self.locks.read();
        
        if let Some(lock_mutex) = locks.get(&key_vec) {
            let mut state = lock_mutex.lock();
            
            if state.can_grant(&request) {
                state.grant(&request);
                drop(state);
                drop(locks);
                
                self.txn_locks.write()
                    .entry(txn_id)
                    .or_insert_with(HashSet::new)
                    .insert(key_vec);
                
                return Ok(true);
            }
            
            return Ok(false);
        }
        
        drop(locks);
        
        // No lock state exists, create and grant
        let mut locks = self.locks.write();
        let lock_state = locks.entry(key_vec.clone())
            .or_insert_with(|| Mutex::new(LockState::default()));
        
        let mut state = lock_state.lock();
        state.grant(&request);
        drop(state);
        drop(locks);
        
        self.txn_locks.write()
            .entry(txn_id)
            .or_insert_with(HashSet::new)
            .insert(key_vec);
        
        Ok(true)
    }

    /// Release a specific lock
    pub fn unlock(&self, txn_id: u64, key: &[u8]) -> Result<()> {
        let key_vec = key.to_vec();
        
        let locks = self.locks.read();
        if let Some(lock_mutex) = locks.get(&key_vec) {
            let mut state = lock_mutex.lock();
            state.release(txn_id);
            
            // Try to grant waiting requests
            let _granted = state.try_grant_waiters();
        }
        
        // Remove from transaction's lock set
        if let Some(keys) = self.txn_locks.write().get_mut(&txn_id) {
            keys.remove(&key_vec);
        }
        
        Ok(())
    }

    /// Release all locks held by a transaction
    pub fn release_all(&self, txn_id: u64) -> Result<()> {
        // Get all keys locked by this transaction
        let keys = self.txn_locks.write().remove(&txn_id);
        
        if let Some(keys) = keys {
            let locks = self.locks.read();
            
            for key in keys {
                if let Some(lock_mutex) = locks.get(&key) {
                    let mut state = lock_mutex.lock();
                    state.release(txn_id);
                    state.waiters.retain(|w| w.txn_id != txn_id);
                    
                    // Try to grant waiting requests
                    let _granted = state.try_grant_waiters();
                }
            }
        }
        
        Ok(())
    }

    /// Check if a transaction holds a lock on a key
    pub fn is_held(&self, txn_id: u64, key: &[u8]) -> bool {
        if let Some(keys) = self.txn_locks.read().get(&txn_id) {
            keys.contains(key)
        } else {
            false
        }
    }

    /// Get all locks held by a transaction
    pub fn locks_held(&self, txn_id: u64) -> Vec<Vec<u8>> {
        self.txn_locks.read()
            .get(&txn_id)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_locks() {
        let manager = LockManager::new();
        
        // Two transactions can hold shared locks
        assert!(manager.try_lock(1, b"key", LockMode::Shared).unwrap());
        assert!(manager.try_lock(2, b"key", LockMode::Shared).unwrap());
        
        assert!(manager.is_held(1, b"key"));
        assert!(manager.is_held(2, b"key"));
    }

    #[test]
    fn test_exclusive_lock() {
        let manager = LockManager::new();
        
        // First transaction gets exclusive lock
        assert!(manager.try_lock(1, b"key", LockMode::Exclusive).unwrap());
        
        // Second transaction cannot get any lock
        assert!(!manager.try_lock(2, b"key", LockMode::Shared).unwrap());
        assert!(!manager.try_lock(2, b"key", LockMode::Exclusive).unwrap());
    }

    #[test]
    fn test_release() {
        let manager = LockManager::new();
        
        manager.try_lock(1, b"key", LockMode::Exclusive).unwrap();
        manager.unlock(1, b"key").unwrap();
        
        // Now another transaction can get the lock
        assert!(manager.try_lock(2, b"key", LockMode::Exclusive).unwrap());
    }

    #[test]
    fn test_release_all() {
        let manager = LockManager::new();
        
        manager.try_lock(1, b"key1", LockMode::Exclusive).unwrap();
        manager.try_lock(1, b"key2", LockMode::Exclusive).unwrap();
        manager.try_lock(1, b"key3", LockMode::Shared).unwrap();
        
        assert_eq!(manager.locks_held(1).len(), 3);
        
        manager.release_all(1).unwrap();
        
        assert_eq!(manager.locks_held(1).len(), 0);
    }
}
