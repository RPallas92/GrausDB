use crate::{
    db_command::Command,
    io_types::BufWriterWithPos,
    log_storage::log_helpers::{get_log_ids, get_log_size, new_log_file},
    Result,
};
use std::{fs::File, io::Write, path::PathBuf, slice};

// If the journal reaches 1 MB, trigger a compaction.
const JOURNAL_SIZE_THRESHOLD: u64 = 1024 * 1024;

/// TODO Ricardo to be used behind a mutex
pub struct JournalWriter {
    path: PathBuf,
    current_journal_id: u64,
    writer: BufWriterWithPos<File>,
    current_journal_size: u64,
}

impl JournalWriter {
    pub fn new(db_path: &PathBuf) -> Result<Self> {
        let path = db_path.join("journal");
        let journal_ids: Vec<u64> = get_log_ids(&path)?;
        let current_journal_id = journal_ids.last().unwrap_or(&1).to_owned();
        let current_journal_size = get_log_size(&path, current_journal_id)?;
        let writer = new_log_file(&path, current_journal_id)?;

        let journal_writer = JournalWriter {
            path,
            current_journal_id,
            writer,
            current_journal_size,
        };

        Ok(journal_writer)
    }

    pub fn append_command(&mut self, command: Command) -> Result<()> {
        self.append_commands(slice::from_ref(&command))
    }

    pub fn append_commands(&mut self, commands: &[Command]) -> Result<()> {
        for command in commands {
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, command)?;
            self.current_journal_size += self.writer.pos - pos;
        }
        self.writer.flush()?;

        if self.current_journal_size > JOURNAL_SIZE_THRESHOLD {
            self.switch_to_new_journal_file();
        }

        Ok(())
    }

    fn switch_to_new_journal_file(&mut self) -> Result<()> {
        self.current_journal_size = 0;
        self.current_journal_id += 1;
        self.writer = new_log_file(&self.path, self.current_journal_id)?;
        Ok(())
    }
}
