pub mod config;
pub mod crypto;
pub mod error;

pub use config::Config;
pub use error::{Error, Result};

lazy_static::lazy_static! {
    pub static ref CONFIG: config::Config = config::Config::load();
}
