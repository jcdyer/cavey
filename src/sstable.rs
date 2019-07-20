use std::fs::File;
use std::io::{self, BufReader, BufWriter, prelude::*};
use std::path::{Path, PathBuf};

use byteorder::{self, LittleEndian};
use byteorder::{ReadBytesExt, WriteBytesExt};


type Entry = (Vec<u8>, Vec<u8>);

// Each entry will be u32/u32/Vec<u8>(Key)/Vec<u8>(Value)
pub struct SSTable {
    path: PathBuf,
}


impl SSTable {
    /// Create an SSTable from a sorted iterator of Key/Value pairs.
    ///
    /// Invariant: The input must be sorted, or the SSTable will yield incorrect results.
    /// Invariant: The input must not be empty, or else first and last will not exist.
    pub fn from_sorted_iter<'a, P>(path: P, iter: impl Iterator<Item=&'a Entry>) -> io::Result<SSTable>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let mut writer = BufWriter::new(File::create(path)?);
        writer.write_all(&b"sst\0"[..])?;
        let mut written = false;
        for &(ref key, ref value) in iter {
            written = true;
            writer.write_u32::<LittleEndian>(key.len() as u32)?;
            writer.write_u32::<LittleEndian>(value.len() as u32)?;
            writer.write_all(key)?;
            writer.write_all(value)?;
        }
        if !written {
            Err(io::Error::new(io::ErrorKind::Other, "empty iterator"))
        } else {
            Ok(SSTable {
                path: path.to_owned(),
            })
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> io::Result<SSTable> {
        // TODO: Check if the file exists, and can be opened for reading
        Ok(SSTable {
            path: path.as_ref().to_owned(),
        })
    }

    pub fn at(&self, offset: u64) -> io::Result<SSTableCursor> {
        let mut f = File::open(&self.path)?;
        f.seek(io::SeekFrom::Start(offset))?;
        Ok(SSTableCursor::new(BufReader::new(f)))
    }
}

pub struct SSTableCursor {
    reader: BufReader<File>,
}

impl SSTableCursor {
    fn new(reader: BufReader<File>) -> SSTableCursor {
        SSTableCursor { reader }
    }

    fn read_next(&mut self) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        let key_len = match self.reader.read_u32::<LittleEndian>() {
            Ok(key_len) => key_len,
            Err(ref io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None)
            }
            err => err?,
        };
        let value_len = self.reader.read_u32::<LittleEndian>()?;
        let mut key = vec![0; key_len as usize];
        let mut value = vec![0; value_len as usize];
        self.reader.read_exact(&mut key[..])?;
        self.reader.read_exact(&mut value[..])?;
        Ok(Some((key, value)))
    }
}


impl Iterator for SSTableCursor {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next().unwrap_or_default()
    }
}
