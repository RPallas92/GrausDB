use serde::{Deserialize, Serialize};
use std::ops::Range;

/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    pub fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    pub fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CommandPos {
    pub log_id: u64,
    pub pos: u64,
    pub len: u64,
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