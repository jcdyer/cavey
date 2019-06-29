
use std::io::prelude::*;
use std::net::{SocketAddr, TcpStream};

use bincode::{serialize, deserialize};
use failure::format_err;
use crate::Result;
use crate::protocol::{ClientMessage, ServerMessage};

pub struct CaveyClient {
    socket: TcpStream,
}

impl CaveyClient {
    pub fn new<S: Into<SocketAddr>>(sockaddr: S) -> Result<CaveyClient> {
        TcpStream::connect(sockaddr.into())
            .map(|socket| CaveyClient { socket })
            .map_err(|e| e.into())
    }

    fn send(&mut self, msg: &ClientMessage) -> Result<()> {
        let payload = serialize(msg)?;
        self.socket.write_all(&payload)?;
        Ok(())
    }

    fn receive<'de>(&mut self, payload: &'de mut Vec<u8>) -> Result<ServerMessage<'de>> {
        self.socket.read_to_end(payload)?;
        Ok(deserialize(payload)?)
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let mut payload = Vec::new();
        let request = ClientMessage::Get { key };
        self.send(&request)?;
        let response: ServerMessage = self.receive(&mut payload)?;
        match response {
            ServerMessage::Success { value } => Ok(value.map(|val| val.to_owned())),
            ServerMessage::Error { err } => Err(format_err!("cavey error: {}", err)),
        }
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<()> {
        unimplemented!()
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        unimplemented!()
    }
}