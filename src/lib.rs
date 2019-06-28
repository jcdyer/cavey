use failure::Error;

pub use client::CaveyClient;
pub use store::CaveyStore;
pub use server::run_server;

mod client;
mod store;
mod server;

pub type Result<T> = std::result::Result<T, Error>;

pub trait CaveyEngine {
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn put(&mut self, key: String, value: String) -> Result<()>;
    fn remove(&mut self, key: String) -> Result<()>;

}
