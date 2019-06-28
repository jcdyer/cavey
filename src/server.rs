use std::net::TcpListener;
use std::io::prelude::*;

use log::{trace, info, error};
use super::CaveyEngine;
use super::Result;

pub fn run_server(socket: &mut TcpListener, engine: &mut Box<dyn CaveyEngine>) -> Result<()> {
    for mut stream in socket.incoming() {
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

fn handle_connection<R: Read>(stream: &mut R, engine: &mut Box<dyn CaveyEngine>) -> Result<()> {
    let mut string = String::new();
    stream.read_to_string(&mut string)?;
    info!("Received string: {}", string);
    Ok(())
}