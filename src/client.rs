
use std::io::prelude::*;
use std::net::{SocketAddr, TcpStream};

use super::Result;


pub struct CaveyClient {
    socket: TcpStream,
}

impl CaveyClient {
    pub fn new<S: Into<SocketAddr>>(sockaddr: S) -> Result<CaveyClient> {
        TcpStream::connect(sockaddr.into())
            .map(|socket| CaveyClient { socket })
            .map_err(|e| e.into())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(None)
    }

    pub fn put(&self, key: &str, value: &str) -> Result<()> {
        Ok(())
    }

    pub fn remove(&self, key: &str) -> Result<()> {
        Ok(())
    }
}