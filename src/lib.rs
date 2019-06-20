use std::collections::{BTreeMap};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufReader, BufWriter, SeekFrom, prelude::*};
use std::path::{Path, PathBuf};


use failure::{format_err, Error};
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Cavey {
    datadir: PathBuf,
    keymap: BTreeMap<String, (PathBuf, u64)>,
    file: BufWriter<File>,
    file_version: usize,
    entries: usize,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all="snake_case")]
pub enum Command {
    Put { key: String, value: String },
    Remove { key: String },
}

impl Cavey {
    fn current_file(&self) -> PathBuf {
        self.datadir.join(format!("{:08x}", self.file_version))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Cavey> {

        let datadir = path.as_ref().join("data");
        create_dir_all(&datadir)?;

        let filename = datadir.join(format!("{:08}", 0));

        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filename)?
        );
        file.seek(SeekFrom::End(0))?;

        let mut reader = OpenOptions::new()
            .read(true)
            .open(&filename)?;

        let mut keymap = BTreeMap::new();
        let mut offset = reader.seek(SeekFrom::Start(0))?;

        for line in BufReader::new(&mut reader).lines() {
            let line = line?;
            let cmd = serde_json::from_str(&line)?;
            match cmd {
                Command::Put { key, .. } => {
                    keymap.insert(key, (PathBuf::from(&filename), offset));
                }
                Command::Remove { key } => {
                    keymap.remove(&key);
                }
            }
            offset += line.len() as u64 + 1;
        }
        let entries = keymap.len();
        Ok(Cavey { datadir, file, keymap, entries, file_version: 0 })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some((filename, offset)) = self.keymap.get(&key) {
            let mut reader = File::open(filename)?;
            reader.seek(SeekFrom::Start(*offset))?;
            let cmd = serde_json::Deserializer::from_reader(&mut reader).into_iter().next();
            match cmd {
                Some(Ok(Command::Put { value, .. })) => Ok(Some(value)),
                Some(Ok(Command::Remove { .. })) => Err(format_err!("unexpected remove")),
                Some(Err(err)) => Err(err)?,
                None => Err(format_err!("unexpected eof")),
            }
        } else {
            Ok(None)
        }
    }

    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        let keycopy = key.clone();
        let cmd = Command::Put { key, value };
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

    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Remove { key: key.clone() };
        serde_json::to_writer(&mut self.file, &cmd)?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        self.entries += 1;
        self.keymap
            .remove(&key)
            .and(Some(()))
            .ok_or_else(|| format_err!("Key not found"))
    }

    fn should_compact(&mut self) -> bool {
        self.entries > 2000 && self.entries > 3 * self.keymap.len()
    }

    fn compact(&mut self) -> Result<()> {
        let existing = std::fs::read_dir(&self.datadir)?.collect::<Vec<_>>();
        let mut commands = Vec::with_capacity(self.keymap.len());
        for (_, (filename, offset)) in self.keymap.iter() {
            println!("{}", filename.to_string_lossy());
            let mut f = File::open(filename)?;
            f.seek(SeekFrom::Start(*offset))?;
            let cmd = serde_json::Deserializer::from_reader(&mut f).into_iter().next();
            if let Some(Ok(cmd)) = cmd {
                // TODO: Validate command was Put
                if let Command::Put { .. } = &cmd {
                    // TODO: Validate key in Command
                    commands.push(cmd);
                }
            }
        }

        self.file_version += 1;
        let newfilename = self.current_file();
        let mut w = BufWriter::new(File::create(&newfilename)?);
        for cmd in commands {
            let offset = self.file.seek(SeekFrom::Current(0))?;
            serde_json::to_writer(&mut w, &cmd)?;
            if let Command::Put { key, .. } = cmd {
                self.keymap.insert(key, (newfilename.clone(), offset));
            };
        }
        w.flush()?;

        std::mem::swap(&mut self.file, &mut w);
        for direntry in existing {
            std::fs::remove_file(direntry?.path())?;
        }
        Ok(())
    }

}
