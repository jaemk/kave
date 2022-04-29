//! The commit log is a file-backed append-only log of transactions performed by the KV store.
//! It's used to recover unfinished transactions in the event of an unplanned shutdown.

use std::path::Path;

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use uuid::Uuid;

use crate::{store::Transaction, Error, Result};

pub struct CommitLog<'a> {
    log_path: &'a Path,
}

impl<'a> CommitLog<'a> {
    pub fn new(log_path: &'a Path) -> Self {
        Self { log_path }
    }

    async fn get_log_file(&mut self) -> Result<File> {
        OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(self.log_path)
            .await?
    }

    fn encode_tx(tx: &Transaction) -> Vec<u8> {
        let mut buf = Vec::new();
        tx.serialize(&mut Serializer::new(&mut buf));
        buf
    }

    fn decode_tx(encoded: &[u8]) -> Result<Transaction> {
        let mut de = Deserializer::new(encoded);
        match Deserialize::deserialize(&mut de) {
            Ok(res) => Ok(res),
            Err(e) => Err(Error::MsgPackDecode(e)),
        }
    }

    /// Writes a begin_transaction line to the commit log.
    pub async fn begin_transaction<'b>(&mut self, tx: Transaction<'b>) -> Result<()> {
        let mut bytes = b"begin_tx:".to_vec();
        bytes.append(&mut Self::encode_tx(&tx));
        let mut logfile = self.get_log_file().await?;
        logfile.write_all(&bytes).await?;
        // TODO this is expensive. Should we relax the durability guarantee a bit,
        // say by syncing the logfile every n seconds or something?
        logfile.sync_all().await?;
        Ok(())
    }

    /// Writes an end_transaction line to the commit log.
    pub async fn end_transaction(&mut self, tx_id: Uuid) -> Result<()> {
        let mut bytes = b"end_tx:".to_vec();
        bytes.append(&mut tx_id.as_bytes().to_vec());
        let mut logfile = self.get_log_file().await?;
        logfile.write_all(&bytes).await?;
        // TODO this is expensive. Should we relax the durability guarantee a bit,
        // say by syncing the logfile every n seconds or something?
        logfile.sync_all().await?;
        Ok(())
    }

    /// Returns any unfinished transactions found in the commit log.
    /// Should only be called on startup before the node starts receiving traffic.
    pub async fn get_unfinished_transaction<'b, K: AsRef<str> + Send>(
        &self,
    ) -> Vec<Transaction<'b>> {
    }
}
