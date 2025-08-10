use crate::values;
use crate::indexer::{ProcessingComponents, EthereumBlockData};
use crate::storage::writer::ClickhouseWriter as DbWriter;
use alloy::{sol, sol_types::SolEvent, primitives::{address, Address}};
use reth_node_api::FullNodeComponents;
use eyre::Result;
use chrono::Utc;
use reth_rpc_eth_api::helpers::FullEthApi;
use tracing::debug;

const UNIV4_FACTORY_CONTRACT_ADDRESS: Address = address!("0x000000000004444c5dc75cB358380D2e3dE08A90");

sol! {
    event ModifyLiquidity(
        bytes32 indexed id,
        address indexed sender,
        int24 tickLower,
        int24 tickUpper,
        int256 liquidityDelta,
        bytes32 salt
    );
}

pub async fn process_uni_v4_modify_liquidity<Node: FullNodeComponents, EthApi: FullEthApi>(
    block_data: &EthereumBlockData,
    _components: ProcessingComponents<Node, EthApi>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.num_hash().number;

    for (tx_idx, (tx, receipt)) in block.body().transactions.iter().zip(receipts.iter()).enumerate() {
        for (log_idx, log) in receipt.logs.iter().enumerate() {
            if log.address != UNIV4_FACTORY_CONTRACT_ADDRESS { continue; }
            if log.topics().get(0) != Some(&ModifyLiquidity::SIGNATURE_HASH) { continue; }

            match ModifyLiquidity::decode_raw_log(log.topics(), &log.data.data) {
                Ok(evt) => {
                    writer.write_record(values![
                        block_number as i64,
                        tx.hash(),
                        tx_idx as i64,
                        log_idx as i64,
                        log.address,
                        evt.id,
                        evt.sender,
                        evt.tickLower,
                        evt.tickUpper,
                        evt.liquidityDelta,
                        evt.salt,
                        Utc::now(),
                    ]).await?;
                }
                Err(e) => { debug!("Failed to decode univ4 modify liquidity event: {:?}", e); }
            }
        }
    }

    Ok(())
}

