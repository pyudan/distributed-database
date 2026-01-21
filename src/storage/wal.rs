//! Write-Ahead Log (WAL) for durability
//!
//! The WAL ensures durability by persisting all operations before
//! they are applied to the in-memory data structures. On crash recovery,
//! the WAL is replayed to restore state.

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::{Error, Result};

/// Type of WAL entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalEntryType {
    /// Insert or update a key-value pair
    Put,
    /// Delete a key
    Delete,
    /// Begin a transaction
    TxnBegin,
    /// Commit a transaction
    TxnCommit,
    /// Abort a transaction
    TxnAbort,
    /// Checkpoint marker
    Checkpoint,
}

/// A single entry in the WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Sequence number (monotonically increasing)
    pub seq: u64,
    /// Type of operation
    pub entry_type: WalEntryType,
    /// Transaction ID (0 for non-transactional operations)
    pub txn_id: u64,
    /// Key (empty for transaction control entries)
    pub key: Vec<u8>,
    /// Value (empty for deletes and transaction control)
    pub value: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
}

/// Header for each WAL record on disk
#[derive(Debug)]
struct WalRecordHeader {
    /// Length of the serialized entry
    len: u32,
    /// CRC32 checksum of the entry
    crc: u32,
}

impl WalRecordHeader {
    const SIZE: usize = 8; // 4 bytes for len + 4 bytes for crc
}

/// Write-Ahead Log
pub struct Wal {
    /// Directory containing WAL files
    dir: PathBuf,
    /// Current WAL file for writing
    writer: Mutex<BufWriter<File>>,
    /// Current segment number
    current_segment: AtomicU64,
    /// Next sequence number
    next_seq: AtomicU64,
    /// Maximum segment size in bytes
    max_segment_size: u64,
    /// Current segment size
    current_size: AtomicU64,
}

impl Wal {
    /// Create a new WAL or open existing one
    pub fn open(dir: impl AsRef<Path>, max_segment_size: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Find the latest segment and sequence number
        let (segment, next_seq) = Self::find_latest_segment(&dir)?;

        let segment_path = dir.join(format!("wal_{:08}.log", segment));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;

        let current_size = file.metadata()?.len();

        Ok(Self {
            dir,
            writer: Mutex::new(BufWriter::new(file)),
            current_segment: AtomicU64::new(segment),
            next_seq: AtomicU64::new(next_seq),
            max_segment_size,
            current_size: AtomicU64::new(current_size),
        })
    }

    /// Find the latest segment number and next sequence number
    fn find_latest_segment(dir: &Path) -> Result<(u64, u64)> {
        let mut max_segment = 0u64;
        let mut max_seq = 0u64;

        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                
                if name.starts_with("wal_") && name.ends_with(".log") {
                    if let Ok(segment) = name[4..12].parse::<u64>() {
                        max_segment = max_segment.max(segment);
                    }
                }
            }
        }

        // Read the last segment to find the max sequence number
        if max_segment > 0 || dir.join("wal_00000000.log").exists() {
            let segment_path = dir.join(format!("wal_{:08}.log", max_segment));
            if let Ok(entries) = Self::read_segment(&segment_path) {
                if let Some(last) = entries.last() {
                    max_seq = last.seq + 1;
                }
            }
        }

        Ok((max_segment, max_seq))
    }

    /// Read all entries from a segment file
    fn read_segment(path: &Path) -> Result<Vec<WalEntry>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::Io(e)),
        };

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            // Read header
            let mut header_buf = [0u8; WalRecordHeader::SIZE];
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(e)),
            }

            let len = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
            let expected_crc = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());

            // Read entry data
            let mut data = vec![0u8; len as usize];
            reader.read_exact(&mut data)?;

            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&data);
            let actual_crc = hasher.finalize();

            if actual_crc != expected_crc {
                return Err(Error::Corruption(format!(
                    "WAL checksum mismatch: expected {}, got {}",
                    expected_crc, actual_crc
                )));
            }

            // Deserialize entry
            let entry: WalEntry = bincode::deserialize(&data)
                .map_err(|e| Error::Serialization(e.to_string()))?;

            entries.push(entry);
        }

        Ok(entries)
    }

    /// Append an entry to the WAL
    pub fn append(&self, entry_type: WalEntryType, txn_id: u64, key: Vec<u8>, value: Vec<u8>, timestamp: u64) -> Result<u64> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        
        let entry = WalEntry {
            seq,
            entry_type,
            txn_id,
            key,
            value,
            timestamp,
        };

        // Serialize entry
        let data = bincode::serialize(&entry)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Calculate checksum
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let crc = hasher.finalize();

        // Write to WAL
        let mut writer = self.writer.lock();
        
        // Write header
        writer.write_all(&(data.len() as u32).to_le_bytes())?;
        writer.write_all(&crc.to_le_bytes())?;
        
        // Write data
        writer.write_all(&data)?;
        writer.flush()?;

        // Update size and check for rotation
        let record_size = (WalRecordHeader::SIZE + data.len()) as u64;
        let new_size = self.current_size.fetch_add(record_size, Ordering::SeqCst) + record_size;

        if new_size >= self.max_segment_size {
            drop(writer);
            self.rotate()?;
        }

        Ok(seq)
    }

    /// Rotate to a new segment
    fn rotate(&self) -> Result<()> {
        let new_segment = self.current_segment.fetch_add(1, Ordering::SeqCst) + 1;
        let segment_path = self.dir.join(format!("wal_{:08}.log", new_segment));
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)?;

        let mut writer = self.writer.lock();
        *writer = BufWriter::new(file);
        self.current_size.store(0, Ordering::SeqCst);

        Ok(())
    }

    /// Recover all entries from the WAL
    pub fn recover(&self) -> Result<Vec<WalEntry>> {
        let mut all_entries = Vec::new();

        // Read all segment files in order
        let mut segments: Vec<u64> = Vec::new();
        
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                
                if name.starts_with("wal_") && name.ends_with(".log") {
                    if let Ok(segment) = name[4..12].parse::<u64>() {
                        segments.push(segment);
                    }
                }
            }
        }

        segments.sort();

        for segment in segments {
            let segment_path = self.dir.join(format!("wal_{:08}.log", segment));
            let entries = Self::read_segment(&segment_path)?;
            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Sync the WAL to disk
    pub fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Get the current sequence number
    pub fn current_seq(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst)
    }

    /// Truncate WAL files up to a given sequence number (for compaction)
    pub fn truncate_before(&self, seq: u64) -> Result<()> {
        // Find segments that can be safely deleted
        let mut segments_to_delete = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                
                if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                    if let Ok(segment) = name_str[4..12].parse::<u64>() {
                        let segment_path = self.dir.join(format!("wal_{:08}.log", segment));
                        if let Ok(entries) = Self::read_segment(&segment_path) {
                            if let Some(last) = entries.last() {
                                if last.seq < seq {
                                    segments_to_delete.push(segment);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Delete old segments
        for segment in segments_to_delete {
            let segment_path = self.dir.join(format!("wal_{:08}.log", segment));
            if segment_path.exists() {
                fs::remove_file(segment_path)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_append_and_recover() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Wal::open(temp_dir.path(), 1024 * 1024).unwrap();

        // Append some entries
        wal.append(WalEntryType::Put, 1, b"key1".to_vec(), b"value1".to_vec(), 100).unwrap();
        wal.append(WalEntryType::Put, 1, b"key2".to_vec(), b"value2".to_vec(), 101).unwrap();
        wal.append(WalEntryType::Delete, 1, b"key1".to_vec(), vec![], 102).unwrap();

        // Recover
        let entries = wal.recover().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"key1".to_vec());
        assert_eq!(entries[1].key, b"key2".to_vec());
        assert_eq!(entries[2].entry_type, WalEntryType::Delete);
    }

    #[test]
    fn test_wal_persistence() {
        let temp_dir = TempDir::new().unwrap();
        
        // Write some entries
        {
            let wal = Wal::open(temp_dir.path(), 1024 * 1024).unwrap();
            wal.append(WalEntryType::Put, 1, b"key".to_vec(), b"value".to_vec(), 100).unwrap();
            wal.sync().unwrap();
        }

        // Reopen and recover
        {
            let wal = Wal::open(temp_dir.path(), 1024 * 1024).unwrap();
            let entries = wal.recover().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].key, b"key".to_vec());
        }
    }
}
