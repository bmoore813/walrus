//! Write a [`RecordBatch`] to Parquet with arrow-rs.
//!
//! The one rule (walrus-extractor.md §2.1): DuckDB reads Parquet's **native** logical types, so we
//! must **not** coerce temporals. arrow-rs already emits `TIMESTAMP(MICROS, isAdjustedToUTC=…)`
//! straight from `Timestamp(Microsecond, tz)` — coercing to NANOS/MILLIS is exactly the bug §2.1
//! warns about. So [`default_writer_properties`] sets compression and bounded byte-array
//! statistics while leaving temporal encoding to arrow-rs. The conformance tests prove the
//! round-trip through in-process DuckDB.

use crate::error::Error;
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Bound retained row-group min/max values. Reload objects intentionally contain many small row
/// groups; without truncation, one oversized TEXT/BYTEA value could be copied into footer metadata
/// for every group and defeat the streaming memory bound.
const STATISTICS_TRUNCATE_LENGTH: usize = 64;

/// The walrus writer settings: Snappy compression + arrow-rs's native MICROS temporal encoding
/// (no NANOS/MILLIS coercion).
#[must_use]
pub fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_truncate_length(Some(STATISTICS_TRUNCATE_LENGTH))
        .build()
}

/// Stream one batch to Parquet using the walrus writer properties.
///
/// # Errors
///
/// Returns [`Error::Parquet`] if the writer cannot be created, the batch cannot be encoded, or the
/// Parquet footer cannot be closed into `sink`.
pub fn write_parquet<W: std::io::Write + Send>(batch: &RecordBatch, sink: W) -> Result<(), Error> {
    let mut writer = ArrowWriter::try_new(sink, batch.schema(), Some(default_writer_properties()))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

/// Convenience: write one batch to an in-memory Parquet buffer.
///
/// # Errors
///
/// Returns [`Error::Parquet`] for any encoding or writer-finalization failure reported by
/// [`write_parquet`].
pub fn write_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    write_parquet(batch, &mut buf)?;
    Ok(buf)
}

#[cfg(test)]
#[path = "parquet_test.rs"]
mod tests;
