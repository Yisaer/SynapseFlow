//! Codec module for encoding and decoding data
//!
//! This module provides abstractions for converting between different data formats,
//! such as converting bytes to RecordBatch.

pub mod decoder;

pub use decoder::Decoder;
