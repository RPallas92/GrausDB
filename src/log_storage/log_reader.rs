use super::log_helpers::log_path;
use crate::db_command::Command;
use crate::Result;
use crate::{db_command::CommandPos, io_types::BufReaderWithPos};
use std::io::{self, Read, Seek};
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::SeekFrom,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// A single thread reader.
///
/// Each `GrausDb` instance has its own `LogReader` and
/// `LogReader`s open the same files separately. So the user
/// can read concurrently through multiple `GrausDb`s in different
/// threads.
pub struct LogReader {
    pub path: Arc<PathBuf>,
    pub safe_point: Arc<AtomicU64>,
    pub readers: RefCell<HashMap<u64, BufReaderWithPos<File>>>,
}

impl LogReader {
    /// Close file handles with generation number less than safe_point.
    ///
    /// `safe_point` is updated to the latest compaction gen after a compaction finishes.
    /// The compaction generation contains the sum of all operations before it and the
    /// in-memory index contains no entries with generation number less than safe_point.
    /// So we can safely close those file handles and the stale files can be deleted.
    pub fn close_stale_readers(&self) {
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
    pub fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R>
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

    pub fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        self.read_and(cmd_pos, |cmd_reader| {
            Ok(serde_json::from_reader(cmd_reader)?)
        })
    }
}

impl Clone for LogReader {
    fn clone(&self) -> LogReader {
        LogReader {
            path: Arc::clone(&self.path),
            safe_point: Arc::clone(&self.safe_point),
            // use a new map
            readers: RefCell::new(HashMap::new()),
        }
    }
}
