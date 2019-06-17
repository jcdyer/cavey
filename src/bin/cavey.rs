use failure::Error;
use structopt::StructOpt;

use cavey::{self, Cavey};

#[derive(Clone, Debug, StructOpt)]
enum Options {
    #[structopt(name = "get")] Get { key: String },

    #[structopt(name = "put")] Put { key: String, value: String },

    #[structopt(name = "rm")] Remove { key: String },

    #[structopt(name = "keys")] Keys,
}

fn main() -> Result<(), Error> {
    let options = Options::from_args();
    let mut cavey = Cavey::open(".")?;
    match options {
        Options::Get { key } => match cavey.get(key)? {
            Some(value) => println!("{}", value),
            None => {
                println!("Key not found");
            }
        },
        Options::Put { key, value } => cavey.put(key, value)?,
        Options::Remove { key } => match cavey.remove(key) {
            Ok(()) => eprintln!("Success"),
            Err(_) => {
                println!("Key not found");
                std::process::exit(1);
            }
        },
        Options::Keys => for key in cavey.keys()? {
            println!("{}", key);
        },
    }
    Ok(())
}
