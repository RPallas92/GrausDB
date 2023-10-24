use crate::{
    io_types::BufReaderWithPos,
    log_storage::log_helpers::{
        get_commands_from_log, get_log_ids, get_log_size, last_log_id, log_path,
    },
    Result,
};
use std::{cell::RefCell, collections::HashMap, fs::File, path::PathBuf, sync::Arc};

use super::journal_position::{JournalPos, JournalUpdates};

/// A single thread journal reader.
///
/// Each `GrausDb` instance has its own `JournalReader` and
/// `JournalReader`s open the same files separately. So the user
/// can read concurrently through multiple `GrausDb`s in different
/// threads.
pub struct JournalReader {
    path: Arc<PathBuf>,
    readers: RefCell<HashMap<u64, BufReaderWithPos<File>>>,
}

impl JournalReader {
    pub fn new(db_path: &PathBuf) -> Result<Self> {
        let path = db_path.join("journal");
        let journal_ids: Vec<u64> = get_log_ids(&path)?;

        let mut readers = HashMap::new();

        for &journal_id in &journal_ids {
            let journal_path = log_path(&path, journal_id);
            let reader = BufReaderWithPos::new(File::open(&journal_path)?)?;
            readers.insert(journal_id, reader);
        }

        let journal_reader = JournalReader {
            path: Arc::new(path),
            readers: RefCell::new(readers),
        };

        Ok(journal_reader)
    }

    /// Returns all commands since journal_pos and
    /// the last journal pos until all commands were returned
    pub fn get_updates_since(&self, journal_pos: JournalPos) -> Result<JournalUpdates> {
        let mut commands = Vec::new();

        let mut readers = self.readers.borrow_mut();

        let last_journal_id = last_log_id(&self.path)?;
        let last_journal_id_size = get_log_size(&self.path, last_journal_id)?;

        for current_journal_id in journal_pos.journal_id..=last_journal_id {
            // Since each clone uses its own Map, maybe this log file was not opened in this instance
            if !readers.contains_key(&journal_pos.journal_id) {
                let journal_path = log_path(&self.path, journal_pos.journal_id);
                let reader = BufReaderWithPos::new(File::open(&journal_path)?)?;
                readers.insert(journal_pos.journal_id, reader);
            }

            let reader = readers.get_mut(&journal_pos.journal_id).unwrap();
            let start_pos = if current_journal_id == journal_pos.journal_id {
                journal_pos.pos
            } else {
                0
            };
            let end_pos = if current_journal_id == last_journal_id {
                Some(last_journal_id_size)
            } else {
                None
            };

            let mut current_journal_commands =
                get_commands_from_log(reader, current_journal_id, start_pos, end_pos)?;

            commands.append(&mut current_journal_commands);
        }

        Ok(JournalUpdates {
            commands,
            updated_until: JournalPos {
                journal_id: last_journal_id,
                pos: last_journal_id_size,
            },
        })
    }
}

impl Clone for JournalReader {
    fn clone(&self) -> JournalReader {
        JournalReader {
            path: Arc::clone(&self.path),
            // use a new map
            readers: RefCell::new(HashMap::new()),
        }
    }
}
