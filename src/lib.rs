use std::cmp;
use std::collections::{BinaryHeap, BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, SeekFrom, prelude::*};
use std::path::{Path, PathBuf};
use std::time;

use failure::{format_err, Error};
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq)]
struct Neg<T: Ord>(T);

impl<T: Ord> Ord for Neg<T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

impl<T: Ord> PartialOrd for Neg<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

#[derive(Debug)]
struct FilePool {
    size: usize,
    files: BTreeMap<PathBuf, File>,
    lru: BinaryHeap<Neg<(time::Instant, PathBuf)>>,
}

impl FilePool {
    fn new(size: usize) -> FilePool {
        FilePool {
            size,
            files: Default::default(),
            lru: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct Cavey {
    map: BTreeMap<String, String>,
    file: File,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Command {
    Put { key: String, value: String },
    Remove { key: String },
}

impl Cavey {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Cavey> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.as_ref().join("caveystore"))?;

        let mut map = BTreeMap::new();
        file.seek(SeekFrom::Start(0))?;
        for line in BufReader::new(&mut file).lines() {
            let line = line?;
            let cmd = serde_json::from_str(&line)?;
            match cmd {
                Command::Put { key, value } => {
                    map.insert(key, value);
                }
                Command::Remove { key } => {
                    map.remove(&key);
                }
            }
        }

        Ok(Cavey { file, map })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).cloned())
    }

    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        let keycopy = key.clone();
        let valuecopy = value.clone();
        let cmd = Command::Put { key, value };
        let mut w = BufWriter::new(&mut self.file);
        serde_json::to_writer(&mut w, &cmd)?;
        w.write(b"\n")?;
        w.flush()?;
        self.map.insert(keycopy, valuecopy);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Remove { key: key.clone() };
        let mut w = BufWriter::new(&mut self.file);
        serde_json::to_writer(&mut w, &cmd)?;
        w.write(b"\n")?;
        w.flush()?;
        self.map
            .remove(&key)
            .and(Some(()))
            .ok_or_else(|| format_err!("Key not found"))
    }

    pub fn keys(&mut self) -> Result<Vec<String>> {
        unimplemented!()
    }
}
