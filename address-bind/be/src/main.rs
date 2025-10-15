mod bind;
mod error;
mod indexer;
mod verify;

#[macro_use]
extern crate tracing as logger;

use ckb_sdk::NetworkType;
use ckb_sdk::rpc::CkbRpcClient;
use ckb_types::H256;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "https://testnet.ckb.dev/")]
    ckb_url: String,
    #[arg(short, long, default_value = "ckb_testnet")]
    network: String,
    #[arg(short, long, default_value = "http://localhost:3000")]
    recovery_url: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Verify {
        #[arg(short, long)]
        tx_hash: String,
    },
    Indexer {
        #[arg(short, long, default_value = "info")]
        log_filter: String,
        #[arg(short, long, default_value = "18_587_462")]
        start_height: u64,
        #[arg(short, long)]
        db_url: String,
        #[arg(short, long, default_value = "9533")]
        port: u16,
    },
}

#[derive(Debug, Clone)]
pub struct Indexer {
    pub db: sqlx::Pool<sqlx::Postgres>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let network_type =
        NetworkType::from_raw_str(&cli.network).expect("network must be 'ckb' or 'ckb_testnet'");

    match &cli.command {
        Commands::Verify { tx_hash } => {
            let ckb_client = CkbRpcClient::new(cli.ckb_url.as_str());

            let mut tx_hash = tx_hash.clone();
            if tx_hash.starts_with("0x") {
                tx_hash = tx_hash[2..].to_string();
            }
            let tx_hash_bytes = hex::decode(tx_hash).unwrap();
            let tx_hash = H256::from_slice(&tx_hash_bytes).unwrap();
            let tx = verify::get_tx(&ckb_client, tx_hash.clone()).await.unwrap();

            let ret =
                verify::verify_tx(&ckb_client, network_type, &tx, cli.recovery_url.as_str()).await;
            match ret {
                Ok((from, to, timestamp)) => {
                    println!(
                        "tx {tx_hash} has valid bind info, from: {from}, to: {to}, timestamp: {timestamp}"
                    );
                }
                Err(e) => {
                    println!("tx {tx_hash} is invalid, err: {e}");
                }
            }
        }
        Commands::Indexer {
            log_filter,
            start_height,
            port,
            db_url,
        } => {
            common_x::log::init_log_filter(log_filter);
            info!("args: {:?}", cli);
            let ckb_client = CkbRpcClient::new(cli.ckb_url.as_str());
            let ret = indexer::server(
                &ckb_client,
                network_type,
                db_url,
                *start_height,
                *port,
                cli.recovery_url.as_str(),
            )
            .await;
            if let Err(e) = ret {
                info!("indexer server error: {e}");
            }
        }
    }
}
