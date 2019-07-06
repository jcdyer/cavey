use std::fs::create_dir_all;
use std::path::Path;

use failure::format_err;
use sled::Db;

use crate::{utils::check_engine, CaveyEngine, Result};

pub struct SledStore(Db);

impl SledStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<SledStore> {
        let datadir = path.as_ref().join("data");
        create_dir_all(&datadir)?;
        check_engine(&datadir, b"sled")?;
        Ok(SledStore(Db::start_default(&datadir)?))
    }
}

impl CaveyEngine for SledStore {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        //self.0.flush()?;
        if let Some(ivec) = self.0.get(key)? {
            Ok(Some(String::from_utf8(ivec.to_vec())?))
        } else {
            Ok(None)
        }
    }

    fn put(&mut self, key: String, value: String) -> Result<()> {
        self.0.set(key, value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.0.del(key)?.is_some() {
            self.0.flush()?;
            Ok(())
        } else {
            Err(format_err!("Key not found"))
        }
    }
}

impl Drop for SledStore {
    fn drop(&mut self) {
        self.0.flush().ok();
    }
}