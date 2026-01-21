//! Raft Log
//!
//! Manages the replicated log of commands in the Raft consensus protocol.

use serde::{Deserialize, Serialize};
use parking_lot::RwLock;

use crate::{Error, Result};

/// Type of command in a log entry
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CommandType {
    /// No operation (used for leader liveness)
    Noop,
    /// Put a key-value pair
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Delete a key
    Delete { key: Vec<u8> },
    /// Configuration change
    ConfigChange { add: Vec<String>, remove: Vec<String> },
}

/// A single entry in the Raft log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Index of this entry (1-indexed)
    pub index: u64,
    /// Term when entry was received by leader
    pub term: u64,
    /// Command to apply to state machine
    pub command: CommandType,
    /// Optional client ID for deduplication
    pub client_id: Option<String>,
    /// Optional request sequence number for deduplication
    pub request_seq: Option<u64>,
}

impl LogEntry {
    /// Create a new log entry
    pub fn new(index: u64, term: u64, command: CommandType) -> Self {
        Self {
            index,
            term,
            command,
            client_id: None,
            request_seq: None,
        }
    }

    /// Create a new log entry with client tracking
    pub fn with_client(
        index: u64,
        term: u64,
        command: CommandType,
        client_id: String,
        request_seq: u64,
    ) -> Self {
        Self {
            index,
            term,
            command,
            client_id: Some(client_id),
            request_seq: Some(request_seq),
        }
    }

    /// Create a noop entry (used for leader to commit entries from previous terms)
    pub fn noop(index: u64, term: u64) -> Self {
        Self::new(index, term, CommandType::Noop)
    }
}

/// The Raft replicated log
pub struct RaftLog {
    /// Log entries (index 0 is unused, entries start at index 1)
    entries: RwLock<Vec<LogEntry>>,
    /// Index of last log entry included in snapshot
    snapshot_index: RwLock<u64>,
    /// Term of last log entry included in snapshot
    snapshot_term: RwLock<u64>,
}

impl RaftLog {
    /// Create a new empty Raft log
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            snapshot_index: RwLock::new(0),
            snapshot_term: RwLock::new(0),
        }
    }

    /// Get the last log index
    pub fn last_index(&self) -> u64 {
        let entries = self.entries.read();
        if entries.is_empty() {
            *self.snapshot_index.read()
        } else {
            entries.last().map(|e| e.index).unwrap_or(0)
        }
    }

    /// Get the last log term
    pub fn last_term(&self) -> u64 {
        let entries = self.entries.read();
        if entries.is_empty() {
            *self.snapshot_term.read()
        } else {
            entries.last().map(|e| e.term).unwrap_or(0)
        }
    }

    /// Get the term at a specific index
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }

        let snapshot_index = *self.snapshot_index.read();
        if index == snapshot_index {
            return Some(*self.snapshot_term.read());
        }

        let entries = self.entries.read();
        if index < snapshot_index {
            return None; // Compacted
        }

        let offset = (index - snapshot_index - 1) as usize;
        entries.get(offset).map(|e| e.term)
    }

    /// Get an entry at a specific index
    pub fn get(&self, index: u64) -> Option<LogEntry> {
        let snapshot_index = *self.snapshot_index.read();
        if index <= snapshot_index {
            return None; // Compacted
        }

        let entries = self.entries.read();
        let offset = (index - snapshot_index - 1) as usize;
        entries.get(offset).cloned()
    }

    /// Get entries from start_index to end_index (inclusive)
    pub fn get_range(&self, start_index: u64, end_index: u64) -> Vec<LogEntry> {
        let snapshot_index = *self.snapshot_index.read();
        let entries = self.entries.read();
        
        let start = if start_index <= snapshot_index {
            0
        } else {
            (start_index - snapshot_index - 1) as usize
        };
        
        let end = if end_index <= snapshot_index {
            0
        } else {
            ((end_index - snapshot_index - 1) as usize).min(entries.len())
        };

        if start >= entries.len() || start > end {
            return Vec::new();
        }

        entries[start..=end.min(entries.len() - 1)].to_vec()
    }

    /// Get all entries starting from an index
    pub fn get_from(&self, start_index: u64) -> Vec<LogEntry> {
        let snapshot_index = *self.snapshot_index.read();
        let entries = self.entries.read();
        
        if start_index <= snapshot_index {
            return entries.clone();
        }

        let offset = (start_index - snapshot_index - 1) as usize;
        if offset >= entries.len() {
            return Vec::new();
        }

        entries[offset..].to_vec()
    }

    /// Append a new entry (returns the index of the new entry)
    pub fn append(&self, term: u64, command: CommandType) -> u64 {
        let mut entries = self.entries.write();
        let index = if entries.is_empty() {
            *self.snapshot_index.read() + 1
        } else {
            entries.last().unwrap().index + 1
        };

        entries.push(LogEntry::new(index, term, command));
        index
    }

    /// Append multiple entries (used by followers receiving AppendEntries)
    pub fn append_entries(&self, prev_log_index: u64, prev_log_term: u64, entries: Vec<LogEntry>) -> Result<()> {
        // Check that prev_log matches
        if prev_log_index > 0 {
            match self.term_at(prev_log_index) {
                Some(term) if term != prev_log_term => {
                    return Err(Error::LogInconsistency(prev_log_index));
                }
                None if prev_log_index > *self.snapshot_index.read() => {
                    return Err(Error::LogInconsistency(prev_log_index));
                }
                _ => {}
            }
        }

        if entries.is_empty() {
            return Ok(());
        }

        let mut log = self.entries.write();
        let snapshot_index = *self.snapshot_index.read();

        for entry in entries {
            let offset = (entry.index - snapshot_index - 1) as usize;
            
            if offset < log.len() {
                // Check for conflict
                if log[offset].term != entry.term {
                    // Delete this entry and all following
                    log.truncate(offset);
                    log.push(entry);
                }
                // Otherwise entry already exists with same term, skip
            } else {
                // New entry
                log.push(entry);
            }
        }

        Ok(())
    }

    /// Truncate log to remove entries at and after the given index
    pub fn truncate(&self, from_index: u64) {
        let snapshot_index = *self.snapshot_index.read();
        if from_index <= snapshot_index {
            self.entries.write().clear();
            return;
        }

        let offset = (from_index - snapshot_index - 1) as usize;
        let mut entries = self.entries.write();
        if offset < entries.len() {
            entries.truncate(offset);
        }
    }

    /// Check if the log is at least as up-to-date as the given (last_log_index, last_log_term)
    pub fn is_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        let our_last_term = self.last_term();
        let our_last_index = self.last_index();

        if last_log_term != our_last_term {
            last_log_term > our_last_term
        } else {
            last_log_index >= our_last_index
        }
    }

    /// Compact the log up to (and including) the given index
    pub fn compact(&self, up_to_index: u64, up_to_term: u64) {
        let mut entries = self.entries.write();
        let snapshot_index = *self.snapshot_index.read();

        if up_to_index <= snapshot_index {
            return;
        }

        let offset = (up_to_index - snapshot_index) as usize;
        if offset < entries.len() {
            entries.drain(..offset);
        } else {
            entries.clear();
        }

        *self.snapshot_index.write() = up_to_index;
        *self.snapshot_term.write() = up_to_term;
    }

    /// Get snapshot metadata
    pub fn snapshot_info(&self) -> (u64, u64) {
        (*self.snapshot_index.read(), *self.snapshot_term.read())
    }

    /// Get the number of entries in the log (excluding snapshot)
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if the log is empty (excluding snapshot)
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_get() {
        let log = RaftLog::new();
        
        let idx1 = log.append(1, CommandType::Put { key: b"k1".to_vec(), value: b"v1".to_vec() });
        let idx2 = log.append(1, CommandType::Put { key: b"k2".to_vec(), value: b"v2".to_vec() });
        
        assert_eq!(idx1, 1);
        assert_eq!(idx2, 2);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1);
        
        let entry = log.get(1).unwrap();
        assert_eq!(entry.index, 1);
        assert_eq!(entry.term, 1);
    }

    #[test]
    fn test_is_up_to_date() {
        let log = RaftLog::new();
        log.append(1, CommandType::Noop);
        log.append(2, CommandType::Noop);
        
        // Same term, same index -> up to date
        assert!(log.is_up_to_date(2, 2));
        
        // Higher term -> up to date
        assert!(log.is_up_to_date(1, 3));
        
        // Lower term -> not up to date
        assert!(!log.is_up_to_date(5, 1));
        
        // Same term, lower index -> not up to date
        assert!(!log.is_up_to_date(1, 2));
    }

    #[test]
    fn test_truncate() {
        let log = RaftLog::new();
        log.append(1, CommandType::Noop);
        log.append(1, CommandType::Noop);
        log.append(1, CommandType::Noop);
        
        log.truncate(2);
        
        assert_eq!(log.last_index(), 1);
        assert!(log.get(2).is_none());
    }

    #[test]
    fn test_compact() {
        let log = RaftLog::new();
        log.append(1, CommandType::Noop);
        log.append(1, CommandType::Noop);
        log.append(2, CommandType::Noop);
        
        log.compact(2, 1);
        
        let (snap_index, snap_term) = log.snapshot_info();
        assert_eq!(snap_index, 2);
        assert_eq!(snap_term, 1);
        assert!(log.get(1).is_none());
        assert!(log.get(2).is_none());
        assert!(log.get(3).is_some());
    }
}
