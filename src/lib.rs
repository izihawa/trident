#![deny(
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_must_use,
    clippy::unwrap_used
)]

use iroh::rpc_protocol::{ProviderRequest, ProviderResponse};
use quic_rpc::transport::flume::FlumeConnection;

pub mod config;
pub mod error;
pub mod file_shard;
mod hash_ring;
pub mod iroh_node;
mod table;
mod utils;

pub type IrohDoc = iroh::client::Doc<FlumeConnection<ProviderResponse, ProviderRequest>>;
pub type IrohClient = iroh::client::Iroh<FlumeConnection<ProviderResponse, ProviderRequest>>;
