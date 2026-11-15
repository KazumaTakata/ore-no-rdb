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
mod record;
mod serve;
mod storage;
mod tx;

use std::path::Path;

use crate::cli::handle_cli;
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
    #[arg(short, long)]
    cluster: Option<bool>,
    #[arg(short, long)]
    test_cluster_client: Option<bool>,
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
        return handle_cluster();
    }

    if let Some(_) = args.test_cluster_client {
        return handle_cluster_client();
    }

    return handle_serve(&database);
}
