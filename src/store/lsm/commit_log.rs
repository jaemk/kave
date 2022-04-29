//! The commit log is a file-backed append-only log of transactions performed by the KV store.
//! It's used to recover unfinished transactions in the event of an unplanned shutdown.

use std::{path::Path, collections::HashMap};

use itertools::Itertools;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufReader, AsyncBufReadExt},
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
    pub async fn get_unfinished_transaction<'b: 'a, K: AsRef<str> + Send>(
        &mut self,
    ) -> Result<Vec<&Transaction<'b>>> {
        let mut txs = HashMap::new();
        let logfile = self.get_log_file().await?;
        let mut lines = BufReader::new(logfile).lines();
        let mut i = 0;
        while let Some(line) = lines.next_line().await? {
            match line.split_once(':') {
                Some(("begin_tx", tx_ser)) => {
                    let tx = Self::decode_tx(tx_ser.as_bytes())?;
                    txs.insert(tx.id, (i, tx));
                },
                Some(("end_tx", tx_id_ser))  => {
                    let tx_id = Uuid::from_slice(tx_id_ser.as_bytes())?;
                    txs.remove(&tx_id);
                },
            }
            i += 1;
        }
        let res = txs.values().sorted_by_key(|v| v.0).map(|v| &v.1).collect_vec();
        Ok(res)
    }
}
