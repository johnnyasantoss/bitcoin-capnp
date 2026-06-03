use bitcoin::{
    block::Block,
    hashes::{sha256d, Hash, HashEngine},
    Transaction,
};

#[derive(Clone)]
pub struct TemplateData {
    pub template_id: u64,
    pub block: Block,
    pub coinbase: Transaction,
}

impl TemplateData {
    /// Create a new TemplateData with computed coinbase merkle path
    pub fn new(template_id: u64, block: Block, coinbase: Transaction) -> Self {
        Self {
            template_id,
            block,
            coinbase,
        }
    }

    pub fn get_nbits(&self) -> u32 {
        self.block.header.bits.to_consensus()
    }

    pub fn get_ntime(&self) -> u32 {
        self.block.header.time
    }

    pub fn get_version(&self) -> u32 {
        self.block
            .header
            .version
            .to_consensus()
            .try_into()
            .expect("version converstion to u32 should never fail")
    }

    pub fn get_coinbase_tx_version(&self) -> u32 {
        self.coinbase
            .version
            .0
            .try_into()
            .expect("version converstion to u32 should never fail")
    }

    pub fn get_txs(&self) -> &Vec<Transaction> {
        &self.block.txdata
    }

    pub fn get_merkle_path(&self) -> Vec<Vec<u8>> {
        let tx_hashes: Vec<sha256d::Hash> = self
            .block
            .txdata
            .iter()
            .map(|tx| tx.compute_txid().to_raw_hash())
            .collect();

        if tx_hashes.len() == 1 {
            // If there's only the coinbase transaction, the path is empty
            return Vec::new();
        }

        let mut merkle_path = Vec::new();
        let mut current_level = tx_hashes;
        let mut target_index = 0; // Start with coinbase at index 0

        while current_level.len() > 1 {
            let mut next_level = Vec::new();
            let next_target_index = target_index / 2;

            // Find the sibling of the target transaction at this level
            let sibling_index = if target_index % 2 == 0 {
                // Target is left child, sibling is right child
                target_index + 1
            } else {
                // Target is right child, sibling is left child
                target_index - 1
            };

            // Add the sibling hash to the merkle path
            if sibling_index < current_level.len() {
                let hash_bytes: [u8; 32] = *current_level[sibling_index].as_byte_array();
                merkle_path.push(hash_bytes.to_vec());
            } else {
                // If no sibling (odd number of nodes), duplicate the last hash
                let hash_bytes: [u8; 32] = *current_level[target_index].as_byte_array();
                merkle_path.push(hash_bytes.to_vec());
            }

            // Calculate the next level of the merkle tree
            for i in (0..current_level.len()).step_by(2) {
                let left = current_level[i];
                let right = if i + 1 < current_level.len() {
                    current_level[i + 1]
                } else {
                    left // Duplicate if odd number of hashes
                };

                // Compute parent hash: SHA256(SHA256(left || right))
                let mut hasher = sha256d::Hash::engine();
                HashEngine::input(&mut hasher, left.as_byte_array());
                HashEngine::input(&mut hasher, right.as_byte_array());
                let parent_hash = sha256d::Hash::from_engine(hasher);
                next_level.push(parent_hash);
            }

            current_level = next_level;
            target_index = next_target_index;
        }

        merkle_path
    }
}
