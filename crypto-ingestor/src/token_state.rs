use std::collections::HashMap;

use std::sync::Arc;

use ethers::prelude::*;

use crate::error::IngestorError;

abigen!(Erc20, "[
    function symbol() view returns (string)
    function decimals() view returns (uint8)
    function balanceOf(address) view returns (uint256)
    function allowance(address,address) view returns (uint256)
]");

#[derive(Clone, Debug)]
pub struct TokenInfo {
    pub symbol: String,
    pub decimals: u8,
    pub balance: U256,
    pub allowance: U256,
}

#[derive(Default)]
pub struct TokenState {
    pub entries: HashMap<(Address, Address), TokenInfo>, // (token, owner)
}

impl TokenState {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn refresh(
        &mut self,
        token: Address,
        owner: Address,
        spender: Address,
        provider: Arc<Provider<Ws>>,
    ) -> Result<(), IngestorError> {
        let contract = Erc20::new(token, provider);
        let symbol = contract
            .symbol()
            .call()
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))?;
        let decimals = contract
            .decimals()
            .call()
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))?;
        let balance = contract
            .balance_of(owner)
            .call()
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))?;
        let allowance = contract
            .allowance(owner, spender)
            .call()
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))?;

        self.entries.insert(
            (token, owner),
            TokenInfo {
                symbol,
                decimals,
                balance,
                allowance,
            },
        );
        Ok(())
    }
}
