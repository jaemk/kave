//! The commit log is a file-backed append-only log of transactions performed by the KV store.
//! It's used to recover unfinished transactions in the event of an unplanned shutdown.

use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
};
use uuid::Uuid;

use self::CommitLogLine::{BeginTx, EndTx};
use crate::{store::Transaction, Error, Result};

#[derive(Serialize, Deserialize, Debug)]
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

    async fn decode_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Self>> {
        let size = match reader.read_u64().await {
            Ok(s) => Ok(Some(s)),
            Err(e) => match e.kind() {
                ErrorKind::UnexpectedEof => Ok(None),
                _ => Err(Error::IO(e)),
            },
        }?;
        match size {
            Some(s) => {
                let mut buf = Vec::new();
                reader.take(s).read_buf(&mut buf).await?;
                match bincode::deserialize(buf.as_slice()) {
                    Ok(c) => Ok(Some(c)),
                    Err(e) => Err(Error::BincodeError(e)),
                }
            }
            None => Ok(None),
        }
    }
}

pub struct CommitLog {
    log_path: PathBuf,
    logfile: File,
}

impl CommitLog {
    pub async fn initialize(log_path: &Path) -> Result<Self> {
        let logfile = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(log_path)
            .await?;
        Ok(Self {
            logfile,
            log_path: log_path.to_path_buf(),
        })
    }

    fn get_write_handle(&mut self) -> &mut File {
        &mut self.logfile
    }

    async fn get_read_handle(&self) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.log_path)
            .await?;
        Ok(file)
    }

    /// Writes a begin_transaction line to the commit log.
    pub async fn begin_transaction(&mut self, tx: &Transaction) -> Result<()> {
        let line = BeginTx(tx.clone());
        let bytes = line.encode()?;
        let logfile = self.get_write_handle();
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
        let logfile = self.get_write_handle();
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
        // Get an owned version of the file
        // This is safe since this method is only called before any writes are done
        let logfile = self.get_read_handle().await?;
        let mut i = 0;
        let mut reader = BufReader::new(logfile);
        loop {
            match CommitLogLine::decode_from(&mut reader).await {
                Ok(maybe_line) => match maybe_line {
                    Some(line) => match line {
                        BeginTx(tx) => {
                            txs.insert(tx.id, (i, tx));
                        }
                        EndTx(tx_id) => {
                            txs.remove(&tx_id);
                        }
                    },
                    None => {
                        break;
                    }
                },
                Err(e) => return Err(Error::from(e)),
            }
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
    use uuid::Uuid;

    use crate::{
        store::{TransactInstruction, Transaction},
        Result,
    };
    use std::env;

    use super::CommitLog;

    async fn get_commit_log() -> Result<CommitLog> {
        let path = env::temp_dir().join(format!("commit_log_{}", Uuid::new_v4()));
        CommitLog::initialize(path.as_path()).await
    }

    #[tokio::test]
    async fn test_end_to_end() -> Result<()> {
        let mut commit_log = self::get_commit_log().await?;
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
        let mut commit_log = self::get_commit_log().await?;
        let unfinished_txs = commit_log.get_unfinished_transactions().await?;
        let empty: Vec<Transaction> = vec![];
        assert_eq!(empty, unfinished_txs);
        Ok(())
    }
}
