use std::{collections::HashMap, fs};

use ethers::types::Address;

use crate::error::IngestorError;

/// Load an address label CSV file in the form `address,label` per line.
pub fn load_labels(path: &str) -> Result<HashMap<Address, String>, IngestorError> {
    let content = fs::read_to_string(path)?;
    let mut map = HashMap::new();
    for line in content.lines() {
        let mut parts = line.split(',');
        if let (Some(addr), Some(label)) = (parts.next(), parts.next()) {
            if let Ok(address) = addr.trim().parse::<Address>() {
                map.insert(address, label.trim().to_string());
            }
        }
    }
    Ok(map)
}
