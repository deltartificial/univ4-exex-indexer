use crate::schema::get as get_table;
use crate::storage::writer::ClickhouseWriter as DbWriter;
use crate::processors::pools::process_uni_v4_pools;
use crate::processors::swaps::process_uni_v4_swaps;
use crate::processors::modify_liquidity::process_uni_v4_modify_liquidity;
use crate::processors::donations::process_uni_v4_donations;
use alloy_rpc_types::{BlockId, BlockNumberOrTag};
use alloy_rpc_types_trace::parity::{TraceResultsWithTransactionHash, TraceType};
use eyre::Result;
use reth_ethereum::{
    node::api::FullNodeComponents,
    rpc::api::eth::helpers::FullEthApi,
};
use reth_rpc_eth_api::EthApiTypes;
use reth_rpc_convert::RpcTypes;
use alloy_network::{Network, TransactionBuilder};
use reth_primitives::{RecoveredBlock, Block, Receipt};
use reth_tracing::tracing::{info, warn};
use std::{sync::Arc, time::Instant, collections::HashSet};
use reth_rpc::TraceApi;
use clickhouse::Client;

pub type EthereumBlock = RecoveredBlock<Block>;
pub type EthereumReceipts = Vec<Receipt>;
pub type EthereumBlockData = (EthereumBlock, EthereumReceipts);

#[derive(Clone)]
pub struct ProcessingComponents<Node: FullNodeComponents, EthApi: FullEthApi> {
    pub eth_api: Arc<EthApi>,
    pub block_traces: Option<Vec<TraceResultsWithTransactionHash>>,
    pub provider: Node::Provider,
    pub client: Arc<Client>,
}

struct ProcessorInfo<Node: FullNodeComponents, EthApi: FullEthApi> {
    table_name: &'static str,
    processor_name: &'static str,
    processor: for<'a> fn(
        &'a EthereumBlockData,
        ProcessingComponents<Node, EthApi>,
        &'a mut DbWriter
    ) -> futures::future::BoxFuture<'a, Result<()>>,
}

impl<Node: FullNodeComponents, EthApi: FullEthApi> ProcessorInfo<Node, EthApi> {
    fn new(
        table_name: &'static str,
        processor_name: &'static str,
        processor: for<'a> fn(
            &'a EthereumBlockData,
            ProcessingComponents<Node, EthApi>,
            &'a mut DbWriter
        ) -> futures::future::BoxFuture<'a, Result<()>>,
    ) -> Self {
        Self {
            table_name,
            processor_name,
            processor,
        }
    }
}

pub struct Indexer<Node: FullNodeComponents, EthApi: FullEthApi> {
    processors: Vec<ProcessorInfo<Node, EthApi>>,
}

impl<Node: FullNodeComponents, EthApi: FullEthApi> Indexer<Node, EthApi> {
    pub fn new() -> Self
    where
        EthApi: EthApiTypes,
        <EthApi as EthApiTypes>::NetworkTypes: RpcTypes + Network,
        <<EthApi as EthApiTypes>::NetworkTypes as RpcTypes>::TransactionRequest: Default + TransactionBuilder<<EthApi as EthApiTypes>::NetworkTypes>,
    {
        let mut indexer = Self {
            processors: Vec::new(),
        };

        indexer.add_processor("uni_v4_pools", "Pools");
        indexer.add_processor("uni_v4_swaps", "Swaps");
        indexer.add_processor("uni_v4_modify_liquidity", "ModifyLiquidity");
        indexer.add_processor("uni_v4_donations", "Donations");

        info!("Initialized indexer with processors: {:?}", indexer.list_processors());
        indexer
    }

    pub fn add_processor(&mut self, table_name: &'static str, processor_name: &'static str)
    where
        EthApi: EthApiTypes,
        <EthApi as EthApiTypes>::NetworkTypes: RpcTypes + Network,
        <<EthApi as EthApiTypes>::NetworkTypes as RpcTypes>::TransactionRequest: Default + TransactionBuilder<<EthApi as EthApiTypes>::NetworkTypes>,
    {
        let processor = match table_name {
            "uni_v4_pools" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v4_pools::<Node, EthApi>(block_data, components, writer))
            ),
            "uni_v4_swaps" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v4_swaps::<Node, EthApi>(block_data, components, writer))
            ),
            "uni_v4_modify_liquidity" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v4_modify_liquidity::<Node, EthApi>(block_data, components, writer))
            ),
            "uni_v4_donations" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v4_donations::<Node, EthApi>(block_data, components, writer))
            ),
            _ => return,
        };
        self.processors.push(processor);
    }

    pub fn list_processors(&self) -> Vec<&str> {
        self.processors.iter().map(|p| p.processor_name).collect()
    }

    pub async fn revert_blocks(&self, block_numbers: &[i64], client: &Arc<Client>) -> Result<()> {
        for processor in &self.processors {
            let table = get_table(processor.table_name)
                .expect(&format!("Table definition not found for {}", processor.table_name));

            let writer = DbWriter::new(client, table)?;

            if let Err(e) = writer.revert(block_numbers).await {
                warn!("Failed to revert {} for blocks: {}", processor.table_name, e);
            }
        }
        Ok(())
    }

    pub async fn process_blocks(
        &self,
        blocks_and_receipts: Vec<EthereumBlockData>,
        client: &Arc<Client>,
        provider: Node::Provider,
        eth_api: &EthApi,
        trace_api: &TraceApi<EthApi>,
    ) -> Result<()>
    where
        Node: FullNodeComponents,
        EthApi: FullEthApi + EthApiTypes,
        <EthApi as EthApiTypes>::NetworkTypes: RpcTypes + Network,
        <<EthApi as EthApiTypes>::NetworkTypes as RpcTypes>::TransactionRequest: Default + TransactionBuilder<<EthApi as EthApiTypes>::NetworkTypes>,
    {
        let eth_api_arc = Arc::new(eth_api.clone());
        for (block, receipts) in blocks_and_receipts {
            let block_number = block.num_hash().number;
            let block_id = BlockId::Number(BlockNumberOrTag::from(block_number));

            let block_traces = match trace_api.replay_block_transactions(
                block_id,
                HashSet::from_iter(vec![TraceType::Trace])
            ).await {
                Ok(traces) => traces,
                Err(e) => {
                    warn!("Failed to get traces for block {}: {}", block_number, e);
                    None
                }
            };

            let components = ProcessingComponents {
                eth_api: Arc::clone(&eth_api_arc),
                block_traces,
                provider: provider.clone(),
                client: Arc::clone(client),
            };

            let block_data = (block, receipts);
            if let Err(e) = self.process_block_data(&block_data, components).await {
                warn!("Failed to process block {}: {}", block_number, e);
            }
        }

        Ok(())
    }

    pub async fn process_block_data(
        &self,
        block_data: &EthereumBlockData,
        components: ProcessingComponents<Node, EthApi>,
    ) -> Result<()>
    where
        Node: FullNodeComponents,
        EthApi: FullEthApi + EthApiTypes,
        <EthApi as EthApiTypes>::NetworkTypes: RpcTypes + Network,
        <<EthApi as EthApiTypes>::NetworkTypes as RpcTypes>::TransactionRequest: Default + TransactionBuilder<<EthApi as EthApiTypes>::NetworkTypes>,
    {
        let block_number = block_data.0.num_hash().number;
        let shared_block_data = std::sync::Arc::new(block_data.clone());

        let mut tasks = Vec::with_capacity(self.processors.len());

        for processor in &self.processors {
            let processor_name = processor.processor_name;
            let processor_fn = processor.processor;
            let block_data = std::sync::Arc::clone(&shared_block_data);
            let components = components.clone();
            let table = get_table(processor.table_name)
                .expect(&format!("Table definition not found for {}", processor.table_name));

            let task = tokio::spawn(async move {
                let event_start_time = Instant::now();
                let mut writer = match DbWriter::new(&components.client, table) {
                    Ok(w) => w,
                    Err(e) => return Err((processor_name, e.to_string()))
                };
                match processor_fn(&block_data, components, &mut writer).await {
                    Ok(()) => {
                        match writer.finish().await {
                            Ok(records_written) => Ok((processor_name, records_written, event_start_time.elapsed())),
                            Err(e) => Err((processor_name, e.to_string()))
                        }
                    },
                    Err(e) => Err((processor_name, e.to_string()))
                }
            });

            tasks.push(task);
        }

        let mut total_records = 0usize;
        let mut event_results: Vec<(&str, usize, std::time::Duration)> = Vec::with_capacity(tasks.len());
        let mut failed_events: Vec<(&str, String)> = Vec::new();

        for task in tasks {
            match task.await {
                Ok(Ok((name, records, duration))) => {
                    total_records += records;
                    event_results.push((name, records, duration));
                }
                Ok(Err((name, error))) => {
                    failed_events.push((name, error));
                }
                Err(e) => {
                    warn!("Task join error: {}", e);
                }
            }
        }

        event_results.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        if !event_results.is_empty() {
            let events_summary: Vec<String> = event_results
                .iter()
                .map(|(name, records, time)| {
                    format!("{}({}, {:.2}s)", name, records, time.as_secs_f64())
                })
                .collect();

            info!(
                "exex{{id=\"univ4-exex-indexer\"}}: Block {} processed - Events: [{}], Total records: {}",
                block_number,
                events_summary.join(", "),
                total_records,
            );
        }

        if !failed_events.is_empty() {
            let failure_summary: Vec<String> = failed_events
                .iter()
                .map(|(name, error)| format!("{}: {}", name, error))
                .collect();

            warn!(
                "exex{{id=\"univ4-exex-indexer\"}}: Block {} failures - {}",
                block_number,
                failure_summary.join(", ")
            );
        }

        Ok(())
    }
}