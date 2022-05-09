pub mod client;
pub mod config;
pub mod crypto;
pub mod error;
pub mod proto;
pub mod server;
pub mod store;
pub mod utils;

pub use config::Config;
pub use error::{Error, Result};

use cached::proc_macro::once;

#[once]
pub fn get_config() -> Config {
    config::Config::load()
}
