use structopt::StructOpt;

use cavey::Cavey;

#[derive(Clone, Debug, StructOpt)]
enum Options {

    #[structopt(name="get")]
    Get {
        key: String,
    },

    #[structopt(name="put")]
    Put {
        key: String,
        value: String,
    },

    #[structopt(name="rm")]
    Remove {
        key: String,
    },

    #[structopt(name="keys")]
    Keys,
}

fn main() {
    let options = Options::from_args();
    println!("{:?}", options);
    match options {
        Options::Get{ key } => println!("{}", Cavey::new().get(key).unwrap_or_else(|| "".into())),
        Options::Put{ key, value } => Cavey::new().put(key, value),
        Options::Remove{ key } => Cavey::new().remove(key),
        Options::Keys => {
            for key in Cavey::new().keys() {
                println!("{}", key);
            }
        },
    }
}
