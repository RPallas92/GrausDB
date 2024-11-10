use crate::Result;
use crate::{
    db_command::{Command, CommandPos},
    io_types::{BufReaderWithPos, BufWriterWithPos},
};
use bytes::{Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use std::io::{Read, Seek};
use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::SeekFrom,
    path::{Path, PathBuf},
};

use super::db_command_serde::CommandDeserializer;

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
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction.

    let file_metadata = reader.get_metadata()?;
    let file_size = file_metadata.len() as usize;

    let mut buf = BytesMut::with_capacity(file_size);
    buf.resize(file_size, 0);
    reader.read_exact(&mut buf)?;

    // Create an iterator for deserializing commands.
    let mut deserializer = CommandDeserializer::new(Bytes::from(buf));

    // Iterate over the deserialized commands.
    while let Some(command) = deserializer.next() {
        let new_pos = deserializer.pos as u64;
        let command = command?;
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

// Returns the path of a log with log_id
pub fn log_path(dir: &Path, log_id: u64) -> PathBuf {
    dir.join(format!("{}.log", log_id))
}
