use std::net::{SocketAddr, TcpListener};

use env_logger;
use failure::Error;
use log::info;
use structopt::StructOpt;

use cavey::{CaveyEngine, CaveyStore, SledStore};

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(short = "V", long = "version")]
    version: bool,

    #[structopt(short = "a", long = "addr", default_value="[::1]:4000")]
    addr: SocketAddr,

    // kvs or sled
    #[structopt(short = "e", long = "engine", default_value="")]
    engine_name: String
}

#[derive(Debug, StructOpt)]
enum OptionsB {
    Info {
        #[structopt(short = "V", long = "version")]
        version: bool
    },
    Normal {
        #[structopt(short = "a", long = "addr", default_value="[::1]:4000")]
        addr: SocketAddr,

        // kvs or sled
        #[structopt(short = "e", long = "engine")]
        engine_name: String
    },
}

fn main() -> Result<(), Error> {
    let opts = Options::from_args();
    if opts.version {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(())
    }

    env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
    info!("version: {}", env!("CARGO_PKG_VERSION"));
    info!("engine: {}", opts.engine_name);
    let mut engine: Box<dyn CaveyEngine> = match &opts.engine_name[..] {
        "kvs" => Box::new(CaveyStore::open(".")?),
        "sled" => Box::new(SledStore::open(".")?),
        _ => panic!(r#"unknown engine. Valid options are "kvs" and "sled""#),
    };
    info!("binding to socket {}", opts.addr);
    let mut server = TcpListener::bind(opts.addr)?;
    cavey::run_server(&mut server, &mut *engine)?;
    Ok(())
}
