use crate::db_command::{Command, CommandPos};
use crate::io_types::BufReaderWithPos;
use crate::log_storage::log_helpers::{get_log_ids, load_log, log_path, new_log_file};
use crate::log_storage::log_reader::LogReader;
use crate::log_storage::log_writer::LogWriter;
use crate::{GrausError, Result};
use bytes::{Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use std::cell::RefCell;
use std::fs::{self, File};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, path::PathBuf};

/// The `GrausDb` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `SkipMap` in memory stores the keys and the value locations for fast query.
///
/// GrausDb is thead-safe. It can be cloned to use it on new threads.
///
/// ```rust
/// # use graus_db::{GrausDb, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// use bytes::Bytes;
///
/// let store = GrausDb::open(current_dir()?)?;
/// store.set(Bytes::from_static(b"key"), Bytes::from_static(b"value"))?;
/// let val = store.get(&Bytes::from_static(b"key"))?;
/// assert_eq!(val, Some(Bytes::from_static(b"value")));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct GrausDb {
    // Index that maps every Key to a position in a log file.
    index: Arc<SkipMap<Bytes, CommandPos>>,
    // Writes new data into the file system logs. Protected by a mutex.
    writer: Arc<Mutex<LogWriter>>,
    // Reads data from the file system logs.
    reader: LogReader,
}

// TODO Ricardo update DOCS as now we don't use Strings

// TODO Ricardo add clippy as linter

impl GrausDb {
    /// Opens a `GrausDb` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<GrausDb> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = HashMap::new();
        let index = Arc::new(SkipMap::new());

        let log_ids = get_log_ids(&path)?;
        let mut uncompacted = 0;

        for &log_id in &log_ids {
            let log_path = log_path(&path, log_id);
            let mut reader = BufReaderWithPos::new(File::open(&log_path)?)?;
            uncompacted += load_log(log_id, &mut reader, &index)?;
            readers.insert(log_id, reader);
        }

        let new_log_id = log_ids.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, new_log_id)?;
        let safe_point = Arc::new(AtomicU64::new(0));

        let reader = LogReader {
            path: Arc::clone(&path),
            safe_point,
            readers: RefCell::new(readers),
        };

        let writer = LogWriter {
            writer,
            index: Arc::clone(&index),
            reader: reader.clone(),
            current_log_id: new_log_id,
            uncompacted,
            path: Arc::clone(&path),
        };

        Ok(GrausDb {
            reader,
            index,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        if let Some(cmd_pos) = self.index.get(key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(GrausError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a given key.
    ///
    /// Returns GrausError::KeyNotFound if the key does not exist.
    pub fn remove(&self, key: Bytes) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }

    /// Updates atomically an existing value.
    ///
    /// If predicate_key and predicate are provided, it won´t update the value if the predicate
    /// is not satisfied for predicate_key.
    pub fn update_if<F, P>(
        &self,
        key: Bytes,
        update_fn: F,
        predicate_key: Option<&Bytes>,
        predicate: Option<P>,
    ) -> Result<()>
    where
        F: FnOnce(&mut BytesMut),
        P: FnOnce(&Bytes) -> bool,
    {
        let mut writer = self.writer.lock().unwrap();
        let current_value = self.get(&key)?;
        let Some(current_value) = current_value else {
            return Err(GrausError::KeyNotFound);
        };

        if let (Some(predicate_key), Some(predicate)) = (predicate_key, predicate) {
            let current_predicate_key_value = self.get(predicate_key)?;
            let Some(current_predicate_key_value) = current_predicate_key_value else {
                return Err(GrausError::KeyNotFound);
            };
            if !predicate(&current_predicate_key_value) {
                return Err(GrausError::PredicateNotSatisfied);
            }
        }

        let mut current_value_mut = BytesMut::from(current_value);
        update_fn(&mut current_value_mut);
        writer.set(key, current_value_mut.freeze())
    }
}
