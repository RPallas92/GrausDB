use bytes::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;

use crate::db_command::Command;
use crate::{GrausError, Result};

const SET_COMMAND_KEY: u8 = 0;
const REMOVE_COMMAND_KEY: u8 = 1;

pub(crate) fn serialize_command(command: &Command) -> Bytes {
    match command {
        Command::Set { key, value } => {
            let serialized_key = key.as_bytes();
            let key_size = serialized_key.len();
            let value_size = value.len();

            let command_size = 1  // Command type
                + 4  // Key size (u32)
                + key_size  // Serialized key bytes
                + 4  // Value size (u32)
                + value_size; // Serialized value bytes

            let mut buf = BytesMut::with_capacity(command_size);
            buf.put_u8(SET_COMMAND_KEY);
            buf.put_u32(key_size as u32);
            buf.put_slice(serialized_key);
            buf.put_u32(value_size as u32);
            buf.put_slice(value);

            buf.freeze()
        }
        Command::Remove { key } => {
            let serialized_key = key.as_bytes();
            let key_size = serialized_key.len();

            let command_size = 1  // Command type
                + 4  // Key size (u32)
                + key_size; // Serialized key bytes

            let mut buf = BytesMut::with_capacity(command_size);
            buf.put_u8(REMOVE_COMMAND_KEY);
            buf.put_u32(key_size as u32);
            buf.put_slice(serialized_key);

            buf.freeze()
        }
    }
}

pub(crate) fn deserialize_command(buf: Bytes) -> Result<(usize, Command)> {
    let pos = 0;
    match buf[pos] {
        SET_COMMAND_KEY => {
            let (key_bytes_read, key) = read_word(&buf, pos + 1)?;
            let (value_bytes_read, value) = read_word(&buf, pos + 1 + key_bytes_read)?;
            let key = unsafe { std::str::from_utf8_unchecked(&key).to_string() };
            let total_bytes_read = 1 + key_bytes_read + value_bytes_read;
            Ok((total_bytes_read, Command::set(key, value)))
        }
        REMOVE_COMMAND_KEY => {
            let (key_bytes_read, key) = read_word(&buf, pos + 1)?;
            let key = unsafe { std::str::from_utf8_unchecked(&key).to_string() };
            let total_bytes_read = 1 + key_bytes_read;
            Ok((total_bytes_read, Command::remove(key)))
        }
        _ => Err(GrausError::SerializationError(String::from(
            "Invalid command found",
        ))),
    }
}

pub struct CommandDeserializer {
    buf: Bytes,
    pub pos: usize,
}

impl<'a> CommandDeserializer {
    pub fn new(buf: Bytes) -> Self {
        Self { buf, pos: 0 }
    }
}

impl Iterator for CommandDeserializer {
    type Item = Result<Command>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.buf.len() {
            return None;
        }

        match deserialize_command(self.buf.slice(self.pos..)) {
            Ok((bytes_read, command)) => {
                self.pos += bytes_read;
                Some(Ok(command))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

// A word is composed of {word_size}{word} where:
// - word_size length is 4 bytes
// - and word length is word_size
fn read_word(buf: &Bytes, pos: usize) -> Result<(usize, Bytes)> {
    if pos >= buf.len() {
        return Err(GrausError::SerializationError(String::from(
            "Trying to read bytes outside the buffer len",
        )));
    }

    let word_len = u32::from_be_bytes(
        (&buf.slice(pos..pos + 4)[..])
            .try_into()
            .expect("Failed to convert slice to array"),
    );

    let word_start_pos = pos + 4;

    if word_start_pos + word_len as usize > buf.len() {
        return Err(GrausError::SerializationError(String::from(
            "Insufficient bytes to read word content",
        )));
    }

    let word_bytes = buf.slice(word_start_pos..word_start_pos + word_len as usize);
    let total_bytes_read = 4 + word_bytes.len();

    Ok((total_bytes_read, word_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_command::Command;

    #[test]
    fn test_serde_set_command() {
        let command = Command::Set {
            key: "test_key".to_string(),
            value: Bytes::from_static(b"test value"),
        };

        let serialized = serialize_command(&command);
        let (_, deserialized) = deserialize_command(serialized).unwrap();

        assert_eq!(command, deserialized);
    }

    #[test]
    fn test_serde_remove_command() {
        let command = Command::Remove {
            key: "test_key".to_string(),
        };

        let serialized = serialize_command(&command);
        let (_, deserialized) = deserialize_command(serialized).unwrap();

        assert_eq!(command, deserialized);
    }
}
