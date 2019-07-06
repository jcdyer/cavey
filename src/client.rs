
use std::net::{SocketAddr, TcpStream};

use bincode::{serialize_into, deserialize_from};
use failure::format_err;
use log::debug;

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


    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = ClientMessage::Get { key };
        self.send(&request)?;
        self.receive_value()
    }

    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        let request = ClientMessage::Put { key, value };
        self.send(&request)?;
        self.receive_empty()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = ClientMessage::Remove { key };
        self.send(&request)?;
        self.receive_empty()
    }

    fn send(&mut self, msg: &ClientMessage) -> Result<()> {
        debug!("sending_message: {:?}", msg);
        Ok(serialize_into(&mut self.socket, msg)?)
    }

    fn receive_empty(&mut self) -> Result<()> {
        let response: ServerMessage = self.receive()?;
        match response {
            ServerMessage::Success { value } => match value {
                None => Ok(()),
                Some(val) => Err(format_err!("cavey error: unexpected response {:?}", val)),
            },
            ServerMessage::Error { err } => Err(format_err!("cavey error: {}", err)),
        }
    }

    fn receive_value(&mut self) -> Result<Option<String>> {

        let response: ServerMessage = self.receive()?;
        match response {
            ServerMessage::Success { value } => Ok(value),
            ServerMessage::Error { err } => Err(format_err!("cavey error: {}", err)),
        }
    }

    fn receive(&mut self) -> Result<ServerMessage> {
        let resp = deserialize_from(&mut self.socket)?;
        debug!("received message: {:?}", resp);
        Ok(resp)
    }
}
