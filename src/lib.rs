use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::io::{Seek, SeekFrom, Read, BufReader, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct Cavey {
    path: PathBuf,
    segments: Vec<Segment>,
    memstore: BTreeMap<String, (usize, u64)>,
}

impl Cavey {
    /// Creates a new cavey store at the named path.  The path must be
    /// accessible, to the user, but not yet exist, so a new directory
    /// can be created there.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Cavey> {
        fs::create_dir(&path)?;
        Ok(Cavey {
            path: path.as_ref().to_owned(),
            segments: Vec::new(),
            memstore: BTreeMap::new(),
        })
    }

    /// To open an existing Cavey instance:
    /// 1.  Verify the directory exists.
    /// 2.  read the directory.
    /// 3.  Add files to segment list.
    /// 4.  Read each segment file.
    /// 5.  fill out the memstore with keys and offsets, validating values
    ///     along the way.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Cavey> {
        unimplemented!()
    }

    #[must_use]
    pub fn put(&mut self, k: String, v: Value) -> Result<()> {
        if self.needs_new_segment() {
            self.create_segment()?;
        }
        let segment = self.segments.last().unwrap();
        let segment_no = self.segments.len() - 1;
        let mut f = fs::OpenOptions::new().append(true).open(&segment.filename)?;
        let here = f.seek(SeekFrom::End(0))?;
        let mut serialized = Vec::new();
        Value::Text(k.clone()).encode_into(&mut serialized);
        v.encode_into(&mut serialized);
        f.write_all(&serialized)?;
        self.memstore.insert(k, (segment_no, here));
        Ok(())
    }

    #[must_use]
    pub fn get<Q>(&self, k: &Q) -> Result<Option<Value>>
    where
        String: Borrow<Q>,
        Q: Ord + Eq + ?Sized,
    {
        let value = match self.memstore.get(k) {
            Some((segment_no, offset)) => {
                let segment = &self.segments[*segment_no];
                let mut f = BufReader::new(fs::File::open(&segment.filename)?);
                f.seek(SeekFrom::Start(*offset))?;
                let key = Value::read_value(&mut f)?;
                /* ToDO: Figure out types
                if key != k {
                    Err("Wrong key")?
                } */
                let value = Value::read_value(&mut f)?;
                Some(value)
            }
            None => None,
        };
        Ok(value)
    }

    fn needs_new_segment(&self) -> bool {
        self.segments.is_empty()
    }

    fn create_segment(&mut self) -> Result<()> {
        let len = self.segments.len();
        self.segments
            .push(Segment::new(self.path.join(format!("p{}", len)))?);
        Ok(())
    }
}

pub struct Segment {
    filename: PathBuf,
}

impl Segment {
    pub fn new(filename: PathBuf) -> Result<Segment> {
        fs::write(&filename, "")?;
        Ok(Segment { filename })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i64),
    Text(String),
}

impl Value {
    fn read_value<R: Read>(f: &mut R) -> Result<Value> {
        let byte = f.read_u8()?;
        let value = match byte {
            b'n' => Value::Null,
            b't' => Value::Boolean(true),
            b'f' => Value::Boolean(false),
            b'i' => Value::Int(f.read_i64::<LittleEndian>()?),
            b's' => {
                let len = f.read_u8()?;
                let mut text = vec![0; len as usize];
                f.read_exact(&mut text[..])?;
                Value::Text(String::from_utf8(text)?)
            },
            b'S' => {
                let len = f.read_u64::<LittleEndian>()?;
                let mut text = vec![0; len as usize];
                f.read_exact(&mut text[..])?;
                Value::Text(String::from_utf8(text)?)
            },
            _ => Err("O noe")?,
        };
        Ok(value)

    }
    fn encode_into(&self, buf: &mut Vec<u8>) -> usize {
        let initial = buf.len();
        match self {
            Value::Null => buf.push(b'n'),
            Value::Boolean(true) => buf.push(b't'),
            Value::Boolean(false) => buf.push(b'f'),
            Value::Int(n) => {
                buf.push(b'i');
                buf.write_i64::<LittleEndian>(*n).unwrap();
            }
            Value::Text(s) => {
                if s.len() < 256 {
                    buf.push(b's');
                    buf.push(s.len() as u8);
                } else {
                    buf.push(b'S');
                    buf
                        .write_u64::<LittleEndian>(s.len() as u64)
                        .unwrap();
                }
                buf.write_all(s.as_bytes()).unwrap();
            }
        }
        buf.len() - initial
    }

}

type Result<T> = std::result::Result<T, Box<Error>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let f = "/tmp/kv_roundtrip";
        let _ = fs::remove_dir_all(f);
        let mut kv = Cavey::new(f).unwrap();
        kv.put("foo".into(), Value::Null).unwrap();
        assert_eq!(kv.get("foo").unwrap().unwrap(), Value::Null);
    }

    #[test]
    fn roundtrip_text() {
        let f = "/tmp/kv_text";
        let _ = fs::remove_dir_all(f);
        let mut kv = Cavey::new(f).unwrap();
        kv.put("foo".into(), Value::Text("This".to_string())).unwrap();
        assert_eq!(kv.get("foo").unwrap().unwrap(), Value::Text("This".to_string()));
    }

    fn encode(val: Value) -> Vec<u8> {
        let mut vec = Vec::new();
        val.encode_into(&mut vec);
        vec
    }

    #[test]
    fn test_serialize_values() {
        assert_eq!(encode(Value::Null), b"n");
        assert_eq!(encode(Value::Boolean(true)), b"t");
        assert_eq!(encode(Value::Boolean(false)), b"f");
        assert_eq!(encode(Value::Int(513)), b"i\x01\x02\0\0\0\0\0\0");
        assert_eq!(encode(Value::Text("hello".into())), b"s\x05hello");
        let long_string = String::from_utf8(vec![0x54; 291]).unwrap();
        assert_eq!(&encode(Value::Text(long_string))[..10], b"S\x23\x01\0\0\0\0\0\0T");
    }

}
