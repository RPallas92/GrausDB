use std::cell::RefCell;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, path::PathBuf};

use crossbeam_skiplist::SkipMap;
use log::error;
use serde_json::Deserializer;

use crate::command::{Command, CommandPos};
use crate::io_types::{BufReaderWithPos, BufWriterWithPos};
use crate::{GrausError, Result};

// If the log reaches 1 MB, trigger a compaction.
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `GrausDB` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `SkipMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use graus_db::{GrausDB, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let store = GrausDB::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct GrausDB {
    // Index that maps every Key to a position in the log
    index: Arc<SkipMap<String, CommandPos>>,
    // Writes new data into the file system
    writer: Arc<Mutex<GrausDbWriter>>,
    // Reads data from the file system
    reader: GrausDbReader,
}

impl GrausDB {
    /// Opens a `GrausDB` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<GrausDB> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = HashMap::new();
        let index = Arc::new(SkipMap::new());

        let log_ids = get_log_ids(&path)?;
        let mut uncompacted = 0;

        for &log_id in &log_ids {
            let log_path = log_path(&path, log_id);
            let mut reader = BufReaderWithPos::new(File::open(&log_path)?)?;
            uncompacted += read_log(log_id, &mut reader, &*index)?;
            readers.insert(log_id, reader);
        }

        let new_log_id = log_ids.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, new_log_id)?;
        let safe_point = Arc::new(AtomicU64::new(0));

        let reader = GrausDbReader {
            path: Arc::clone(&path),
            safe_point,
            readers: RefCell::new(readers),
        };

        let writer = GrausDbWriter {
            writer,
            index: Arc::clone(&index),
            reader: reader.clone(),
            current_log_id: new_log_id,
            uncompacted,
            path: Arc::clone(&path),
        };

        Ok(GrausDB {
            reader,
            index,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(GrausError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    pub fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }

    /// Updates atomically an existing value. If predicate_key and predicate are provided,
    /// it won´t update the value if the predicate is not satisfied for predicate_key.
    pub fn update_if<F, P>(
        &self,
        key: String,
        update_fn: F,
        predicate_key: Option<String>,
        predicate: Option<P>,
    ) -> Result<()>
    where
        F: FnOnce(String) -> String,
        P: FnOnce(String) -> bool,
    {
        let mut writer = self.writer.lock().unwrap();
        let current_value = self.get(key.to_owned())?;
        let Some(current_value) = current_value else {
            return Err(GrausError::KeyNotFound);
        };

        if let (Some(predicate_key), Some(predicate)) = (predicate_key, predicate) {
            let current_predicate_key_value = self.get(predicate_key)?;
            let Some(current_predicate_key_value) = current_predicate_key_value else {
                return Err(GrausError::KeyNotFound);
            };
            if !predicate(current_predicate_key_value) {
                return Err(GrausError::PredicateNotSatisfied);
            }
        }

        let updated_value = update_fn(current_value);
        writer.set(key, updated_value)
    }
}

/// A single thread reader.
///
/// Each `GrausDb` instance has its own `GrausDbReader` and
/// `GrausDbReader`s open the same files separately. So the user
/// can read concurrently through multiple `GrausDb`s in different
/// threads.
struct GrausDbReader {
    path: Arc<PathBuf>,
    safe_point: Arc<AtomicU64>,
    readers: RefCell<HashMap<u64, BufReaderWithPos<File>>>,
}

impl GrausDbReader {
    /// Close file handles with generation number less than safe_point.
    ///
    /// `safe_point` is updated to the latest compaction gen after a compaction finishes.
    /// The compaction generation contains the sum of all operations before it and the
    /// in-memory index contains no entries with generation number less than safe_point.
    /// So we can safely close those file handles and the stale files can be deleted.
    fn close_stale_readers(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_log_id = *readers.keys().next().unwrap();

            if self.safe_point.load(Ordering::SeqCst) <= first_log_id {
                break;
            }
            readers.remove(&first_log_id);
        }
    }

    /// Read the log file at the given `CommandPos` and execute a callback.
    fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>,
    {
        self.close_stale_readers();

        let mut readers = self.readers.borrow_mut();
        // Since each clone uses its own Map, maybe this log file was not opened in this instance
        if !readers.contains_key(&cmd_pos.log_id) {
            let log_path = log_path(&self.path, cmd_pos.log_id);
            let reader = BufReaderWithPos::new(File::open(log_path)?)?;
            readers.insert(cmd_pos.log_id, reader);
        }
        let reader = readers.get_mut(&cmd_pos.log_id).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);
        f(cmd_reader)
    }

    fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        self.read_and(cmd_pos, |cmd_reader| {
            Ok(serde_json::from_reader(cmd_reader)?)
        })
    }
}

impl Clone for GrausDbReader {
    fn clone(&self) -> GrausDbReader {
        GrausDbReader {
            path: Arc::clone(&self.path),
            safe_point: Arc::clone(&self.safe_point),
            // use a new map
            readers: RefCell::new(HashMap::new()),
        }
    }
}

struct GrausDbWriter {
    writer: BufWriterWithPos<File>,
    index: Arc<SkipMap<String, CommandPos>>,
    reader: GrausDbReader,
    path: Arc<PathBuf>,
    current_log_id: u64,
    uncompacted: u64,
}

impl GrausDbWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

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

    fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(GrausError::KeyNotFound);
        }

        let command = Command::remove(key);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;
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

        // Write compacted entries in compaction log
        let mut new_pos = 0;
        for cmd_pos in self.index.iter() {
            // Removed values are not present in the index so they are not copied into the new log
            let len = self.reader.read_and(*cmd_pos.value(), |mut cmd_reader| {
                Ok(io::copy(&mut cmd_reader, &mut compaction_writer)?)
            })?;
            self.index.insert(
                cmd_pos.key().clone(),
                (compaction_log_id, new_pos..new_pos + len).into(),
            );
            new_pos += len;
        }
        compaction_writer.flush()?;

        self.reader
            .safe_point
            .store(compaction_log_id, Ordering::SeqCst);
        self.reader.close_stale_readers();

        // remove stale log files
        // Note that actually these files are not deleted immediately because `KvStoreReader`s
        // still keep open file handles. When `KvStoreReader` is used next time, it will clear
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

// Returns sorted existing log ids in the given directory (path).
fn get_log_ids(path: &Path) -> Result<Vec<u64>> {
    let mut log_ids: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    log_ids.sort_unstable();
    Ok(log_ids)
}

fn new_log_file(path: &Path, log_id: u64) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, log_id);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    Ok(writer)
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn read_log(
    log_id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &SkipMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<serde_json::Value>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction.

    while let Some(value) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        let command: Command = serde_json::from_value(value?)?;
        match command {
            Command::Set { key, .. } => {
                let old_cmd = index.insert(
                    key,
                    CommandPos {
                        log_id,
                        pos,
                        len: new_pos - pos,
                    },
                );
                uncompacted += old_cmd.value().len;
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.value().len;
                }

                // the new "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                uncompacted += new_pos - pos;
            }
        }

        pos = new_pos;
    }
    Ok(uncompacted)
}

fn log_path(dir: &Path, log_id: u64) -> PathBuf {
    dir.join(format!("{}.log", log_id))
}
