use std::io::{Read, Seek, Write};

use crate::db_command::Command;
use crate::io_types::{BufReaderWithPos, BufWriterWithPos};
use crate::{GrausError, Result};

const SET_COMMAND_KEY: u8 = 0;
const REMOVE_COMMAND_KEY: u8 = 1;

pub(crate) fn serialize_command<W: Write + Seek>(
    command: &Command,
    writer: &mut BufWriterWithPos<W>,
) -> Result<()> {
    match command {
        Command::Set { key, value } => {
            let key_size = key.len() as u32;
            let value_size = value.len() as u32;

            writer.write_all(&[SET_COMMAND_KEY])?;
            writer.write_all(&key_size.to_be_bytes())?;
            writer.write_all(key.as_ref())?;
            writer.write_all(&value_size.to_be_bytes())?;
            writer.write_all(value.as_ref())?;
        }
        Command::Remove { key } => {
            let key_size = key.len() as u32;

            writer.write_all(&[REMOVE_COMMAND_KEY])?;
            writer.write_all(&key_size.to_be_bytes())?;
            writer.write_all(key.as_ref())?;
        }
    }
    writer.flush()?;
    Ok(())
}

pub(crate) fn deserialize_command<R: Read + Seek>(
    reader: &mut BufReaderWithPos<R>,
) -> Result<Command> {
    let mut command_type = [0u8; 1];
    reader.read_exact(&mut command_type)?;

    match command_type[0] {
        SET_COMMAND_KEY => {
            let key = read_word_from_reader(reader)?;
            let value = read_word_from_reader(reader)?;
            Ok(Command::set(key, value))
        }
        REMOVE_COMMAND_KEY => {
            let key = read_word_from_reader(reader)?;
            Ok(Command::remove(key))
        }
        _ => Err(GrausError::SerializationError(String::from(
            "Invalid command found",
        ))),
    }
}

fn read_word_from_reader<R: Read + Seek>(reader: &mut BufReaderWithPos<R>) -> Result<Vec<u8>> {
    // Read the length of the word as a u32
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let word_len = u32::from_be_bytes(len_buf) as usize;

    // Read the actual word data
    let mut word_buf = Vec::with_capacity(word_len as usize);
    word_buf.resize(word_len, 0);
    reader.read_exact(&mut word_buf)?;

    Ok(word_buf)
}

pub struct CommandDeserializer<'a, R: Read + Seek> {
    reader: &'a mut BufReaderWithPos<R>,
    pub pos: usize,
}

impl<'a, R: Read + Seek> CommandDeserializer<'a, R> {
    pub fn new(reader: &'a mut BufReaderWithPos<R>) -> Self {
        Self { reader, pos: 0 }
    }
}

impl<'a, R: Read + Seek> Iterator for CommandDeserializer<'a, R> {
    type Item = Result<Command>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.is_exhausted().unwrap_or(true) {
            return None;
        }

        match deserialize_command(self.reader) {
            Ok(command) => {
                let end_pos = self.reader.stream_position().unwrap() as usize;
                self.pos = end_pos;
                Some(Ok(command))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_serde_command() -> Result<()> {
        let key = b"key value".to_vec();
        let set_command = Command::set(key.clone(), b"Ricardo".to_vec());
        let remove_command = Command::remove(key);

        let mut buffer = Vec::new();

        {
            let mut writer = BufWriterWithPos::new(Cursor::new(&mut buffer))?;
            serialize_command(&set_command, &mut writer)?;
            serialize_command(&remove_command, &mut writer)?;
        }

        let mut reader = BufReaderWithPos::new(Cursor::new(&mut buffer))?;
        let deserialized_set_command = deserialize_command(&mut reader)?;
        let deserialized_remove_command = deserialize_command(&mut reader)?;

        assert_eq!(set_command, deserialized_set_command);
        assert_eq!(remove_command, deserialized_remove_command);

        Ok(())
    }
}
