use crate::db_command::Command;

/// Struct representing the position in a given journal.
#[derive(Debug, Clone, Copy)]
pub struct JournalPos {
    pub journal_id: u64,
    pub pos: u64,
}

#[derive(Debug)]
pub struct JournalUpdates {
    pub commands: Vec<Command>,
    pub updated_until: JournalPos,
}
