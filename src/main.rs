use nu_ansi_term::{Color, Style};
use reedline::{
    default_vi_insert_keybindings, default_vi_normal_keybindings, ColumnarMenu, DefaultCompleter,
    DefaultHinter, DefaultPrompt, ExampleHighlighter, FileBackedHistory, KeyCode, KeyModifiers,
    MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal, Vi,
};
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::vec;

mod buffer;
mod cli;
mod constant;
mod database;
mod error;
mod index;
mod metadata;
mod parser;
mod parser_mapping;
mod query;
mod record;
mod storage;
mod tx;

use crate::cli::handle_cli;
use crate::parser::parser::{parse, ErrorResport, TableNameAndFieldName};
use crate::parser_mapping::sql_node_to_parsed_sql;
use clap::Parser;
use storage::block::BlockId;
use storage::page::Page;

use crate::database::Database;
use crate::metadata::metadata_manager::{MetadataManager, ValidateSQLResponse};
use crate::query::index_update_planner::IndexUpdatePlanner;
use crate::query::parser::{ParsedSQL, QueryData};
use crate::query::plan_v2::{create_query_plan, execute_create_table};
use crate::query::predicate::ConstantValue;
use crate::query::predicate_v3::PredicateV2;
use crate::query::query_handler::handle_select_query;
use crate::tx::transaction_v2::TransactionV2;

#[derive(Parser)]
struct Args {
    /// 名前を指定
    #[arg(short, long)]
    file: Option<String>,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    handle_cli(args)
}
