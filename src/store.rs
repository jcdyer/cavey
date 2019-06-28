use std::collections::BTreeMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{prelude::*, BufReader, BufWriter, SeekFrom};
use std::path::{Path, PathBuf};

use failure::{format_err, Error};
use serde::{Deserialize, Serialize};

use crate::CaveyEngine;
use super::Result;


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogRecord {
    Put { key: String, value: String },
    Remove { key: String },
}


#[derive(Debug)]
pub struct CaveyStore {
    datadir: PathBuf,
    keymap: BTreeMap<String, (PathBuf, u64)>,
    file: BufWriter<File>,
    file_version: usize,
    entries: usize,
}

impl CaveyStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<CaveyStore> {
        let datadir = path.as_ref().join("data");
        create_dir_all(&datadir)?;
        let candidates = std::fs::read_dir(&datadir)?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>();
        let mut candidates = candidates?;
        candidates.sort();
        let filename = candidates
            .into_iter()
            .take(1)
            .next()
            .unwrap_or_else(|| datadir.join(&format!("{:08}", 0)));
        let file_version =
            usize::from_str_radix(&filename.file_name().unwrap().to_string_lossy(), 0x10)?;
        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)?,
        );
        file.seek(SeekFrom::End(0))?;

        let mut reader = OpenOptions::new().read(true).open(&filename)?;

        let mut keymap = BTreeMap::new();
        let mut offset = reader.seek(SeekFrom::Start(0))?;

        let mut entries = 0;
        for line in BufReader::new(&mut reader).lines() {
            let line = line?;
            let cmd = serde_json::from_str(&line)?;
            match cmd {
                LogRecord::Put { key, .. } => {
                    keymap.insert(key, (PathBuf::from(&filename), offset));
                }
                LogRecord::Remove { key } => {
                    keymap.remove(&key);
                }
            }
            offset += line.len() as u64 + 1;
            entries += 1;
        }
        Ok(CaveyStore {
            datadir,
            file,
            keymap,
            entries,
            file_version,
        })
    }

    fn should_compact(&mut self) -> bool {
        (self.entries >= 500) && (self.entries > (10 * self.keymap.len()))
    }

    fn compact(&mut self) -> Result<()> {
        // Collect old log file(s).
        let existing = std::fs::read_dir(&self.datadir)?.collect::<Vec<_>>();

        // Read out commands from old log file(s).
        let mut commands = Vec::with_capacity(self.keymap.len());
        for (_, (filename, offset)) in self.keymap.iter() {
            let mut f = File::open(filename)?;
            f.seek(SeekFrom::Start(*offset))?;
            let cmd = serde_json::Deserializer::from_reader(&mut f)
                .into_iter()
                .next();
            if let Some(Ok(cmd)) = cmd {
                // TODO: Validate command was Put
                if let LogRecord::Put { .. } = &cmd {
                    // TODO: Validate key in Command
                    commands.push(cmd);
                }
            } else {
                eprintln!("Something went wrong: {:?}", cmd);
            }
        }

        // Clear in-memory map
        self.keymap.clear();

        // Create new file
        self.file_version += 1;
        let newfilename = self.current_file();
        let mut w = BufWriter::new(File::create(&newfilename)?);

        // Read commands into new file, and populate in-memory map with new locations
        for cmd in commands {
            let offset = w.seek(SeekFrom::Current(0))?;
            serde_json::to_writer(&mut w, &cmd)?;
            w.write_all(b"\n")?;
            if let LogRecord::Put { key, .. } = cmd {
                self.keymap.insert(key, (newfilename.clone(), offset));
            };
        }
        w.flush()?;

        // Make the new file the primary log.
        self.file = w;

        // Remove the old file(s)
        for direntry in existing {
            std::fs::remove_file(direntry?.path())?;
        }

        // Reset the entry count
        self.entries = self.keymap.len();
        Ok(())
    }

    fn current_file(&self) -> PathBuf {
        self.datadir.join(format!("{:08x}", self.file_version))
    }

}

impl CaveyEngine for CaveyStore {

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some((filename, offset)) = self.keymap.get(&key) {
            let mut reader = File::open(filename)?;
            reader.seek(SeekFrom::Start(*offset))?;
            let cmd = serde_json::Deserializer::from_reader(&mut reader)
                .into_iter()
                .next();
            match cmd {
                Some(Ok(LogRecord::Put { value, .. })) => Ok(Some(value)),
                Some(Ok(LogRecord::Remove { .. })) => Err(format_err!("unexpected remove")),
                Some(Err(err)) => Err(err)?,
                None => Err(format_err!("unexpected eof")),
            }
        } else {
            Ok(None)
        }
    }

    fn put(&mut self, key: String, value: String) -> Result<()> {
        let keycopy = key.clone();
        let cmd = LogRecord::Put { key, value };
        let offset = self.file.seek(SeekFrom::Current(0))?;
        serde_json::to_writer(&mut self.file, &cmd)?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        self.entries += 1;
        self.keymap.insert(keycopy, (self.current_file(), offset));
        if self.should_compact() {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let cmd = LogRecord::Remove { key: key.clone() };
        serde_json::to_writer(&mut self.file, &cmd)?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        self.entries += 1;
        self.keymap
            .remove(&key)
            .and(Some(()))
            .ok_or_else(|| format_err!("Key not found"))
    }

}
