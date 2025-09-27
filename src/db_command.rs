use std::ops::Range;

/// Struct representing an owned command to the database.
#[derive(Debug, PartialEq, Clone)]
pub enum CommandOwned {
    Set { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
}

impl CommandOwned {
    pub fn set(key: Vec<u8>, value: Vec<u8>) -> CommandOwned {
        CommandOwned::Set { key, value }
    }

    pub fn remove(key: Vec<u8>) -> CommandOwned {
        CommandOwned::Remove { key }
    }
}

/// Struct representing a borrowed command to the database.
#[derive(Debug, PartialEq)]
pub enum CommandRef<'a> {
    Set { key: &'a [u8], value: &'a [u8] },
    Remove { key: &'a [u8] },
}

impl<'a> CommandRef<'a> {
    pub fn set(key: &'a [u8], value: &'a [u8]) -> CommandRef<'a> {
        CommandRef::Set { key, value }
    }

    pub fn remove(key: &'a [u8]) -> CommandRef<'a> {
        CommandRef::Remove { key }
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
