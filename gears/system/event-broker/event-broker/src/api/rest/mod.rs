//! REST transport layer (`DESIGN.md` §1.3 Architecture Layers, §3.3 API
//! Contracts). Handler bodies and DTO wire shapes land with #4346; this
//! crate only holds the shells `module.rs` will eventually gate per mode.

pub mod dto;
pub mod error;
pub mod extractors;
pub mod handlers;
pub mod routes;
