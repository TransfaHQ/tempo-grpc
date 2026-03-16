#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

use clap::Parser;
use reth_exex::BackfillJobFactory;
use reth_tracing::tracing::info;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use tonic::transport::Server;

use eyre::Context;
use reth::{builder::WithLaunchContext, cli::NoSubCmd};
use reth_ethereum_cli::Cli;
use reth_rpc_server_types::DefaultRpcModuleValidator;
use tempo_chainspec::spec::{TempoChainSpec, TempoChainSpecParser};
use tempo_consensus::TempoConsensus;
use tempo_evm::TempoEvmConfig;
use tempo_node::{TempoNodeArgs, node::TempoNode};

mod defaults;
mod exex;
mod server;
use exex::ExEx;
use tokio::sync::broadcast;

use crate::server::{BlockStreamService, RemoteExExService};
use shared::proto::{
    self, block_stream_server::BlockStreamServer, remote_ex_ex_server::RemoteExExServer,
};

#[derive(Debug, Clone, clap::Args)]
struct TempoArgs {
    /// Follow this specific RPC node for block hashes.
    /// If provided without a value, defaults to the RPC URL for the selected chain.
    #[arg(long, value_name = "URL", default_missing_value = "auto", num_args(0..=1), env = "TEMPO_FOLLOW")]
    pub follow: Option<String>,

    #[arg(long = "grpc.addr", default_value = "127.0.0.1")]
    pub grpc_addr: IpAddr,

    #[arg(long = "grpc.port", default_value = "50051")]
    pub grpc_port: u16,

    #[command(flatten)]
    pub node_args: TempoNodeArgs,
}

fn main() -> eyre::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install default rustls crypto provider");

    reth_cli_util::sigsegv_handler::install();

    tempo_eyre::install()
        .expect("must install the eyre error hook before constructing any eyre reports");

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    tempo_node::init_version_metadata();
    defaults::init_defaults();

    let cli = Cli::<TempoChainSpecParser, TempoArgs, DefaultRpcModuleValidator, NoSubCmd>::parse();
    let components =
        |spec: Arc<TempoChainSpec>| (TempoEvmConfig::new(spec.clone()), TempoConsensus::new(spec));
    cli.run_with_components::<TempoNode>(components, async move |builder, args| {
        let (notifications_tx, _) = broadcast::channel(1);
        let notifications_tx = Arc::new(notifications_tx);
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        let handle = builder
            .node(TempoNode::new(&args.node_args, None))
            .apply(|mut builder: WithLaunchContext<_>| {
                builder
                    .config_mut()
                    .network
                    .discovery
                    .enable_discv5_discovery = true;
                if let Some(follow) = &args.follow {
                    let follow_url = if follow == "auto" {
                        builder
                            .config()
                            .chain
                            .default_follow_url()
                            .map(|s| s.to_string())
                    } else {
                        Some(follow.clone())
                    };
                    builder.config_mut().debug.rpc_consensus_url = follow_url;
                }
                builder
            })
            .install_exex("grpc-exex", {
                let notifications_tx = notifications_tx.clone();
                |ctx| async move {
                    let exex = ExEx {
                        ctx,
                        notifications_tx,
                    };
                    Ok(exex.start())
                }
            })
            .launch_with_debug_capabilities()
            .await
            .wrap_err("failed launching execution node")?;

        let server = Server::builder()
            .add_service(reflection_service)
            .add_service(
                RemoteExExServer::new(RemoteExExService {
                    exex_notifications: notifications_tx.clone(),
                    backfill_job_factory: BackfillJobFactory::new(
                        handle.node.evm_config().clone(),
                        handle.node.provider().clone(),
                    ),
                })
                .max_encoding_message_size(usize::MAX)
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(BlockStreamServer::new(BlockStreamService::new(
                notifications_tx.clone(),
                handle.node.provider().clone(),
            )))
            .serve(SocketAddr::new(args.grpc_addr, args.grpc_port));
        handle
            .node
            .task_executor
            .spawn_critical("grpc", async move {
                info!(
                    "GRPC server started at {}",
                    SocketAddr::new(args.grpc_addr, args.grpc_port)
                );
                server.await.expect("gRPC server crashed")
            });

        handle.wait_for_node_exit().await
    })
}
