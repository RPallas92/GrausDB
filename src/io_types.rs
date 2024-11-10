use crate::Result;
use std::{
    fs::{File, Metadata},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

pub trait MetadataReader {
    fn metadata(&self) -> io::Result<Metadata>;
}

impl MetadataReader for File {
    fn metadata(&self) -> io::Result<Metadata> {
        self.metadata()
    }
}

/// A buffered reader that stores the current position
pub struct BufReaderWithPos<R: Read + Seek + MetadataReader> {
    pub pos: u64,
    reader: BufReader<R>,
}

impl<R: Read + Seek + MetadataReader> BufReaderWithPos<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }

    pub fn get_metadata(&self) -> io::Result<Metadata> {
        self.reader.get_ref().metadata()
    }
}

impl<R: Read + Seek + MetadataReader> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek + MetadataReader> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

/// A buffered writer that stores the current position
pub struct BufWriterWithPos<W: Write + Seek> {
    pub pos: u64,
    writer: BufWriter<W>,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
