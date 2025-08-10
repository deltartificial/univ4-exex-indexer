use crate::values;
use crate::indexer::{ProcessingComponents, EthereumBlockData};
use crate::storage::writer::ClickhouseWriter as DbWriter;
use crate::schema::get as get_table;
use alloy::{sol, sol_types::SolEvent, primitives::{address, Address}};
use reth_node_api::FullNodeComponents;
use eyre::Result;
use chrono::Utc;
use reth_rpc_eth_api::helpers::FullEthApi;
use tracing::debug;

const UNIV4_FACTORY_CONTRACT_ADDRESS: Address = address!("0x000000000004444c5dc75cB358380D2e3dE08A90");

sol! {
    event Initialize(
        bytes32 indexed id,
        address indexed currency0,
        address indexed currency1,
        uint24 fee,
        int24 tickSpacing,
        address hooks,
        uint160 sqrtPriceX96,
        int24 tick
    );
    event ModifyLiquidity(
        bytes32 indexed id,
        address indexed sender,
        int24 tickLower,
        int24 tickUpper,
        int256 liquidityDelta,
        bytes32 salt
    );
    event Swap(
        bytes32 indexed id,
        address indexed sender,
        int128 amount0,
        int128 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick,
        uint24 fee
    );
    event Donate(
        bytes32 indexed id,
        address indexed sender,
        uint256 amount0,
        uint256 amount1
    );
}

pub async fn process_uni_v4_pools<Node: FullNodeComponents, EthApi: FullEthApi>(
    block_data: &EthereumBlockData,
    components: ProcessingComponents<Node, EthApi>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.num_hash().number;

    let mut swaps_writer: Option<DbWriter> = None;
    let mut mods_writer: Option<DbWriter> = None;
    let mut donate_writer: Option<DbWriter> = None;

    for (tx_idx, (tx, receipt)) in block.body().transactions.iter().zip(receipts.iter()).enumerate() {
        for (log_idx, log) in receipt.logs.iter().enumerate() {
            if log.address != UNIV4_FACTORY_CONTRACT_ADDRESS { continue; }

            if log.topics().get(0) == Some(&Initialize::SIGNATURE_HASH) {
                match Initialize::decode_raw_log(log.topics(), &log.data.data) {
                    Ok(create) => {
                        writer
                            .write_record(values![
                                block_number as i64,
                                tx.hash(),
                                tx_idx as i64,
                                log_idx as i64,
                                log.address,
                                create.id,
                                create.currency0,
                                create.currency1,
                                create.fee,
                                create.tickSpacing,
                                create.hooks,
                                create.sqrtPriceX96,
                                create.tick,
                                Utc::now(),
                            ])
                            .await?;
                    }
                    Err(e) => { debug!("Failed to decode univ4 pool creation event: {:?}", e); }
                }
                continue;
            }

            if log.topics().get(0) == Some(&ModifyLiquidity::SIGNATURE_HASH) { continue; }
            if log.topics().get(0) == Some(&Swap::SIGNATURE_HASH) { continue; }
            if log.topics().get(0) == Some(&Donate::SIGNATURE_HASH) { continue; }
        }
    }

    if let Some(w) = swaps_writer { let _ = w.finish().await?; }
    if let Some(w) = mods_writer { let _ = w.finish().await?; }
    if let Some(w) = donate_writer { let _ = w.finish().await?; }

    Ok(())
}
