use std::net::{SocketAddr, TcpListener};

use env_logger;
use failure::Error;
use log::debug;
use structopt::StructOpt;

use cavey::{CaveyEngine, CaveyStore};

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
    env_logger::init();
    debug!("cavey-server called with options: {:?}", opts);
    let mut engine: Box<dyn CaveyEngine> = match &opts.engine_name[..] {
        "kvs" => Box::new(CaveyStore::open(".")?),
        "sled" => unimplemented!(),
        _ => panic!("unknown engine"),
    };
    debug!("binding to socket {}", opts.addr);
    let mut server = TcpListener::bind(opts.addr)?;
    cavey::run_server(&mut server, &mut engine)?;
    Ok(())
}
