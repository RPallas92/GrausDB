use super::{
    db_command_serde::serialize_command,
    log_helpers::{get_log_ids, log_path, new_log_file},
    log_reader::LogReader,
};
use crate::{
    db_command::{Command, CommandPos},
    io_types::BufWriterWithPos,
};
use crate::{GrausError, Result};
use crossbeam_skiplist::SkipMap;
use log::error;
use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
    sync::atomic::Ordering,
};
use std::{fs::File, path::PathBuf, sync::Arc};

// If the log reaches 1 MB, trigger a compaction.
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// A log writer that is used by GrausDb to store new commands on the log.
///
/// It is used under a mutex to ensure only 1 write can happen at the same time.
/// Since GrausDB is lock-free, multiple reads can happen at the same time, even if
/// there is a write.
pub struct LogWriter {
    pub writer: BufWriterWithPos<File>,
    pub index: Arc<SkipMap<Vec<u8>, CommandPos>>,
    pub reader: LogReader,
    pub path: Arc<PathBuf>,
    pub current_log_id: u64,
    pub uncompacted: u64,
}

impl LogWriter {
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let command = Command::set(key, value);
        let pos = self.writer.pos;

        serialize_command(&command, &mut self.writer)?;

        if let Command::Set { key, .. } = command {
            if let Some(old_cmd) = self.index.get(&key) {
                self.uncompacted += old_cmd.value().len;
            }
            let command_pos = CommandPos {
                log_id: self.current_log_id,
                pos,
                len: self.writer.pos - pos,
            };
            self.index.insert(key, command_pos);
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    pub fn remove(&mut self, key: Vec<u8>) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(GrausError::KeyNotFound);
        }

        let command = Command::remove(key);
        let pos = self.writer.pos;

        serialize_command(&command, &mut self.writer)?;

        if let Command::Remove { key } = command {
            let old_cmd = self.index.remove(&key).expect("key not found");
            self.uncompacted += old_cmd.value().len;
            // the "remove" command itself can be deleted in the next compaction
            // so we add its length to `uncompacted`
            self.uncompacted += self.writer.pos - pos;
        }

        if self.uncompacted >= COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let compaction_log_id = self.current_log_id + 1;
        self.current_log_id += 2; // Increase current log by 2, as current_log+1 will be used for the compacted file.
        self.writer = new_log_file(&self.path, self.current_log_id)?;

        let mut compaction_writer = new_log_file(&self.path, compaction_log_id)?;

        let mut index_with_updated_positions: HashMap<Vec<u8>, CommandPos> = HashMap::new();
        // Write compacted entries in compaction log
        let mut new_pos = 0;
        for cmd_pos in self.index.iter() {
            // Removed values are not present in the index so they are not copied into the new log
            let len = self.reader.read_and(*cmd_pos.value(), |mut cmd_reader| {
                Ok(io::copy(&mut cmd_reader, &mut compaction_writer)?)
            })?;
            index_with_updated_positions.insert(
                cmd_pos.key().clone(),
                (compaction_log_id, new_pos..new_pos + len).into(),
            );
            new_pos += len;
        }
        compaction_writer.flush()?;

        // Now that all data is written into the new compacted log, we can update the lock-free index
        for (key, value) in index_with_updated_positions.iter() {
            self.index.insert(key.clone(), value.clone());
        }

        self.reader
            .safe_point
            .store(compaction_log_id, Ordering::SeqCst);
        self.reader.close_stale_readers();

        // remove stale log files
        // Note that actually these files are not deleted immediately because `LogReader`s
        // still keep open file handles. When `LogReader` is used next time, it will clear
        // its stale file handles. On Unix, the files will be deleted after all the handles
        // are closed. On Windows, the deletions below will fail and stale files are expected
        // to be deleted in the next compaction.
        let log_ids_to_remove: Vec<u64> = get_log_ids(&self.path)?
            .into_iter()
            .filter(|&log_id| log_id < compaction_log_id)
            .collect();

        for log_id_to_remove in log_ids_to_remove {
            let log_path = log_path(&self.path, log_id_to_remove);
            if let Err(e) = fs::remove_file(&log_path) {
                error!("{:?} cannot be deleted: {}", log_path, e);
            }
        }
        self.uncompacted = 0;

        Ok(())
    }
}
