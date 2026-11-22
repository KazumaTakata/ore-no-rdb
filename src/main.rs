mod buffer;
mod cli;
mod cluster;
mod constant;
mod database;
mod error;
mod index;
mod metadata;
mod parser;
mod parser_mapping;
mod query;
mod raft;
mod record;
mod serve;
mod storage;
mod tx;

use std::path::Path;

use crate::cli::handle_cli;
use crate::cluster::{handle_cluster, handle_cluster_test_clinet};
use crate::database::Database;
use crate::serve::handle_serve;
use clap::Parser;
use storage::block::BlockId;
use storage::page::Page;

#[derive(Parser)]
struct Args {
    /// 名前を指定
    #[arg(short, long)]
    file: Option<String>,
    #[arg(short, long)]
    cli: Option<bool>,
    #[arg(long)]
    cluster: Option<bool>,
    #[arg(long)]
    cluster_client: Option<bool>,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let cli = args.cli;

    let directory_path_name = format!("test_data_{}", uuid::Uuid::new_v4());
    let directory_path = Path::new(&directory_path_name);
    let database = Database::new(directory_path);

    if let Some(_) = cli {
        return handle_cli(args, &database);
    }

    if let Some(_) = args.cluster {
        handle_cluster().unwrap();
        return Ok(());
    }

    if let Some(_) = args.cluster_client {
        handle_cluster_test_clinet().unwrap();
        return Ok(());
    }

    return handle_serve(&database);
}
