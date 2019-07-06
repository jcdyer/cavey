use std::path::Path;
use failure::format_err;
use crate::Result;

pub(crate) fn check_engine(datadir: &Path, expected: &[u8]) -> Result<()> {
    let enginepath = datadir.join(".engine");
    if enginepath.exists() {
        let engine = std::fs::read(enginepath)?;
        if engine != expected {
            return Err(format_err!("cavey error: wrong data format: {}", String::from_utf8_lossy(&engine)));
        }
    } else {
        std::fs::write(enginepath, expected)?;
    }
    Ok(())

}
