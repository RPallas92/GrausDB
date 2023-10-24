use crate::Result;
use crate::{
    db_command::{Command, CommandPos},
    io_types::{BufReaderWithPos, BufWriterWithPos},
};
use crossbeam_skiplist::SkipMap;
use serde_json::Deserializer;
use std::io::Seek;
use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::SeekFrom,
    path::{Path, PathBuf},
};

// Returns sorted existing log ids in the given directory (path).
pub fn get_log_ids(path: &Path) -> Result<Vec<u64>> {
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

// Creates a new log file
pub fn new_log_file(path: &Path, log_id: u64) -> Result<BufWriterWithPos<File>> {
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
pub fn load_log(
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

// Returns the size of a given log
pub fn get_log_size(dir: &Path, log_id: u64) -> Result<u64> {
    let path = log_path(dir, log_id);
    let metadata = fs::metadata(path)?;
    Ok(metadata.len())
}

/// Returns the last log id in dir.
pub fn last_log_id(dir: &Path) -> Result<u64> {
    let log_ids = get_log_ids(dir)?;
    let last_log_id = log_ids.last().unwrap_or(&1).to_owned();
    Ok(last_log_id)
}

// TODO RICARDO
// Test reading from start = end of file
// test start = 0, start = middle, and end pos = middle

/// Returns a vector of all commands on the log since start_pos.
/// If end_pos is passed, it will return commands until it, otherwise until the end of the file.
pub fn get_commands_from_log(
    reader: &mut BufReaderWithPos<File>,
    log_id: u64,
    start_pos: u64,
    end_pos: Option<u64>,
) -> Result<Vec<Command>> {
    let mut commands = Vec::new();

    reader.seek(std::io::SeekFrom::Start(start_pos))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<serde_json::Value>();

    while let Some(value) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        let command: Command = serde_json::from_value(value?)?;
        commands.push(command);

        if end_pos.is_some_and(|end| end == new_pos) {
            break;
        }
    }

    Ok(commands)
}

// Returns the path of a log with log_id
pub fn log_path(dir: &Path, log_id: u64) -> PathBuf {
    dir.join(format!("{}.log", log_id))
}
