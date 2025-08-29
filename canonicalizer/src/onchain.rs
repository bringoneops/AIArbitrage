use ethers_core::types::{Address, Bytes, Log, Transaction, H256, U256, U64};
use serde::{Deserialize, Serialize};

/// Canonical representation of an onchain transaction.
#[derive(Debug, Serialize, Deserialize)]
pub struct OnChainTx {
    pub hash: H256,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub block_number: Option<U64>,
}

/// Convert an [`ethers::types::Transaction`] into an [`OnChainTx`].
pub fn format_tx(tx: &Transaction) -> OnChainTx {
    OnChainTx {
        hash: tx.hash,
        from: tx.from,
        to: tx.to,
        value: tx.value,
        block_number: tx.block_number,
    }
}

/// Canonical representation of a log entry emitted by a transaction.
#[derive(Debug, Serialize, Deserialize)]
pub struct OnChainLog {
    pub address: Address,
    pub topics: Vec<H256>,
    pub data: Bytes,
    pub block_number: Option<U64>,
    pub tx_hash: Option<H256>,
}

/// Convert an [`ethers::types::Log`] into an [`OnChainLog`].
pub fn format_log(log: &Log) -> OnChainLog {
    OnChainLog {
        address: log.address,
        topics: log.topics.clone(),
        data: log.data.clone(),
        block_number: log.block_number,
        tx_hash: log.transaction_hash,
    }
}
