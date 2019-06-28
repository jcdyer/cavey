use std::net::SocketAddr;

use failure::Error;
use structopt::StructOpt;

use cavey::{self, CaveyClient};

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(short = "a", long = "addr", default_value = "[::1]:4000")]
    addr: SocketAddr,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "get")]
    Get { key: String },

    #[structopt(name = "put")]
    Put { key: String, value: String },

    #[structopt(name = "rm")]
    Remove { key: String },
}

fn main() -> Result<(), Error> {
    let options = Options::from_args();
    let client = CaveyClient::new(options.addr)?;
    match options.cmd {
        Command::Get { key } => match client.get(&key)? {
            Some(value) => println!("{}", value),
            None => {
                println!("Key not found");
            }
        },
        Command::Put { key, value } => client.put(&key, &value)?,
        Command::Remove { key } => match client.remove(&key) {
            Ok(()) => eprintln!("Success"),
            Err(_) => {
                println!("Key not found");
                std::process::exit(1);
            }
        },
    }
    Ok(())
}
