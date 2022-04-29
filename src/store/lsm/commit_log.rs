//! The commit log is a file-backed append-only log of transactions performed by the KV store.
//! It's used to recover unfinished transactions in the event of an unplanned shutdown.

use std::{collections::HashMap, path::Path};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
};
use uuid::Uuid;

use self::CommitLogLine::{BeginTx, EndTx};
use crate::{store::Transaction, Error, Result};

#[derive(Serialize, Deserialize)]
enum CommitLogLine {
    BeginTx(Transaction),
    EndTx(Uuid),
}

impl CommitLogLine {
    fn encode(&self) -> Result<Vec<u8>> {
        let size = bincode::serialized_size(&self)?;
        let mut buf = size.to_be_bytes().to_vec();
        buf.append(&mut bincode::serialize(&self)?);
        Ok(buf)
    }

    async fn decode_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let size = reader.read_u64().await?;
        let mut buf = Vec::new();
        reader.take(size).read_buf(&mut buf).await?;
        match bincode::deserialize(buf.as_slice()) {
            Ok(c) => Ok(c),
            Err(e) => Err(Error::BincodeError(e)),
        }
    }
}

pub struct CommitLog<'a> {
    log_path: &'a Path,
}

impl<'a> CommitLog<'a> {
    pub fn new(log_path: &'a Path) -> Self {
        Self { log_path }
    }

    async fn get_log_file(&mut self) -> Result<File> {
        match OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(self.log_path)
            .await
        {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Writes a begin_transaction line to the commit log.
    pub async fn begin_transaction(&mut self, tx: &Transaction) -> Result<()> {
        let line = BeginTx(tx.clone());
        let bytes = line.encode()?;
        let mut logfile = self.get_log_file().await?;
        logfile.write_all(bytes.as_slice()).await?;
        // TODO this is expensive. Should we relax the durability guarantee a bit,
        // say by syncing the logfile every n seconds or something?
        logfile.sync_all().await?;
        Ok(())
    }

    /// Writes an end_transaction line to the commit log.
    pub async fn end_transaction(&mut self, tx_id: &Uuid) -> Result<()> {
        let line = EndTx(tx_id.clone());
        let bytes = line.encode()?;
        let mut logfile = self.get_log_file().await?;
        logfile.write_all(bytes.as_slice()).await?;
        // TODO this is expensive. Should we relax the durability guarantee a bit,
        // say by syncing the logfile every n seconds or something?
        logfile.sync_all().await?;
        Ok(())
    }

    /// Returns any unfinished transactions found in the commit log.
    /// Should only be called on startup before the node starts receiving traffic.
    pub async fn get_unfinished_transactions(&mut self) -> Result<Vec<Transaction>> {
        let mut txs = HashMap::new();
        let logfile = self.get_log_file().await?;
        let mut i = 0;
        let mut reader = BufReader::new(logfile);
        // TODO this is swallowing errors, we need proper error handling that ignores
        // UnexpectedEof but handles everything else
        while let Ok(line) = CommitLogLine::decode_from(&mut reader).await {
            match line {
                BeginTx(tx) => {
                    txs.insert(tx.id, (i, tx));
                }
                EndTx(tx_id) => {
                    txs.remove(&tx_id);
                }
            };
            i += 1;
        }
        Ok(txs
            .into_values()
            .sorted_by_key(|v| v.0)
            .map(|v| v.1)
            .collect_vec())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        store::{TransactInstruction, Transaction},
        Error, Result,
    };
    use std::{
        env,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::CommitLog;

    fn get_tmp_log_path() -> Result<PathBuf> {
        let path = env::temp_dir();
        println!("path: {:?}", path);
        match SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| path.join(format!("commit_log_{}", d.as_millis())))
        {
            Ok(p) => Ok(p),
            Err(e) => Err(Error::SystemTimeError(e)),
        }
    }

    #[tokio::test]
    async fn test_end_to_end() -> Result<()> {
        let path = self::get_tmp_log_path()?;
        let mut commit_log = CommitLog::new(path.as_path());
        let tx1 = Transaction::with_random_id(vec![TransactInstruction::set("foo", b"bar")]);
        let tx2 = Transaction::with_random_id(vec![TransactInstruction::set("foo", b"bar")]);
        let tx3 = Transaction::with_random_id(vec![TransactInstruction::set("foo", b"bar")]);
        commit_log.begin_transaction(&tx1).await?;
        commit_log.begin_transaction(&tx2).await?;
        commit_log.begin_transaction(&tx3).await?;
        commit_log.end_transaction(&tx1.id).await?;
        let unfinished_txs = commit_log.get_unfinished_transactions().await?;
        assert_eq!(vec![tx2.clone(), tx3.clone()], unfinished_txs);
        commit_log.end_transaction(&tx3.id).await?;
        let unfinished_txs = commit_log.get_unfinished_transactions().await?;
        assert_eq!(vec![tx2.clone()], unfinished_txs);
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_log() -> Result<()> {
        let path = self::get_tmp_log_path()?;
        let mut commit_log = CommitLog::new(path.as_path());
        let unfinished_txs = commit_log.get_unfinished_transactions().await?;
        let empty: Vec<Transaction> = vec![];
        assert_eq!(empty, unfinished_txs);
        Ok(())
    }
}
