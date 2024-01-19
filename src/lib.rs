#![warn(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
#![allow(
    clippy::implicit_return,
    clippy::missing_docs_in_private_items,
    clippy::multiple_unsafe_ops_per_block,
    clippy::question_mark_used
)]

use iroh::rpc_protocol::{ProviderRequest, ProviderResponse};
use quic_rpc::transport::flume::FlumeConnection;

pub mod config;
pub mod error;
pub mod file_shard;
mod hash_ring;
pub mod iroh_node;
mod sinks;
pub mod storages;
mod utils;

pub type IrohDoc = iroh::client::Doc<FlumeConnection<ProviderResponse, ProviderRequest>>;
pub type IrohClient = iroh::client::Iroh<FlumeConnection<ProviderResponse, ProviderRequest>>;
