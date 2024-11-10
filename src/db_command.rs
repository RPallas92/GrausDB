use std::ops::Range;

use bytes::Bytes;

/// Struct representing a command to the database.
#[derive(Debug, PartialEq)]
pub enum Command {
    // TODO Ricardo &str instead?
    Set { key: String, value: Bytes },
    Remove { key: String },
}

impl Command {
    pub fn set<K: AsRef<str>>(key: K, value: Bytes) -> Command {
        Command::Set {
            key: key.as_ref().to_owned(),
            value,
        }
    }

    pub fn remove<K: AsRef<str>>(key: K) -> Command {
        Command::Remove {
            key: key.as_ref().to_owned(),
        }
    }
}
/// Struct representing the position of a command in a given file.
#[derive(Debug, Clone, Copy)]
pub struct CommandPos {
    pub log_id: u64, // the file where the command is stored.
    pub pos: u64,    // The position of the command's start in the file.
    pub len: u64,    // The length of the command.
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((log_id, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            log_id,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}
