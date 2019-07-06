use std::net::TcpListener;
use std::io::prelude::*;

use bincode::{deserialize_from, serialize_into};
use log::{trace, info, error};

use crate::CaveyEngine;
use crate::Result;
use crate::protocol::{ClientMessage, ServerMessage};

pub fn run_server(socket: &mut TcpListener, engine: &mut dyn CaveyEngine) -> Result<()> {
    for stream in socket.incoming() {
        match stream {
            Ok(mut stream) => {
                trace!("connection accepted");
                handle_connection(&mut stream, engine)?;
            }
            Err(err) => {
                error!("connection failed: {}", err);
            }
        }
    }
    Ok(())
}

fn handle_connection<R: Read + Write>(stream: &mut R, engine: &mut dyn CaveyEngine) -> Result<()> {
    let msg: ClientMessage = deserialize_from(&mut *stream)?;
    info!("caveyd: received msg: {:?}", msg);
    let response = match msg {
        ClientMessage::Get { key } => {
            match engine.get(key) {
                Ok( value ) => ServerMessage::Success { value },
                Err( err ) => ServerMessage::Error { err: format!("{}", err) },
            }
        },
        ClientMessage::Put { key, value } => {
            match engine.put(key, value) {
                Ok(()) => ServerMessage::Success { value: None },
                Err(err) => ServerMessage::Error { err: format!("{}", err) },
            }
        },
        ClientMessage::Remove { key } => {
            match engine.remove(key) {
                Ok(()) => ServerMessage::Success {value: None },
                Err(err) => ServerMessage::Error { err: format!("{}", err) },

            }

        },
    };
    serialize_into(stream, &response)?;
    Ok(())
}
