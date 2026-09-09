//! Arrow → Parquet → S3 PUT (§1.4) — **step (a) of the durability checkpoint** (§1.5).
//!
//! Encode a [`SealedBatch`] to Parquet with arrow-rs's [`AsyncArrowWriter`] and stream it straight
//! into an S3 object via `object_store`'s multipart [`BufWriter`] — never materialising the file on
//! local disk. `close()` (which completes the multipart) is the **durability point**: `put` returns a
//! [`WrittenObject`] only after it, because the manifest INSERT and the slot advance
//! must never get ahead of a batch that isn't durably in S3 (the WAL-bounding invariant).
//!
//! The object key is the epoch-namespaced layout `<epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet`,
//! with `lsn_end` as zero-padded 16-hex ([`common::Lsn`]'s `Display`) so keys sort in commit order.

use crate::batch::SealedBatch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use common::{EpochNo, Lsn, SchemaVersionNo};
use futures_util::FutureExt;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Multipart parts on the reload path are large enough for S3's 5 MiB minimum while still fitting
/// inside the router's roughly 8 MiB per-worker envelope. One part may be in flight at a time, so
/// awaiting a row-group flush applies backpressure all the way to the source COPY stream.
const RELOAD_MULTIPART_PART_BYTES: usize = 8 * 1024 * 1024;

/// Whether the object holds streamed WAL rows, fenced reload rows, or a **speculative open-txn
/// spill**. The canonical enum also retains the legacy `Snapshot` wire value for old manifests. A
/// `Spill` file is a *single* streamed transaction's rows written
/// before its commit LSN is known, so its rows carry a placeholder `commit_lsn`; the real commit LSN is
/// the file's `lsn_end`, stamped onto the manifest at `Stream Commit`. The transformer therefore treats
/// `lsn_end` — not the per-row placeholder — as the authoritative `commit_lsn` for a `Spill` file, which
/// keeps commit-order correct (architecture.md §1.6). A multi-txn `Stream` batch keeps its per-row LSNs.
///
/// This is `control::ManifestKind`, the canonical enum for the `file_manifest.kind` column;
/// re-exported here under the extractor-local name the writer path already uses.
pub use control::ManifestKind as FileKind;

/// The result of a durable S3 PUT — everything the manifest writer needs for its row.
#[derive(Debug, Clone)]
pub struct WrittenObject {
    /// The full `s3://…` URI, as it will be written to the manifest row.
    pub s3_uri: String,
    /// The same object as a store-relative key — what a later delete needs, since the store API
    /// takes a key rather than a URI.
    pub key: Path,
    /// Source schema of the rows in the file.
    pub source_schema: String,
    /// Source table of the rows in the file.
    pub source_table: String,
    /// Commit LSN of the first transaction in the file.
    pub lsn_start: Lsn,
    /// Commit LSN of the last transaction in the file — the transformer's claim-order key.
    pub lsn_end: Lsn,
    /// Rows written.
    pub row_count: u64,
    /// Exact number of Parquet bytes uploaded.
    pub object_size: u64,
    /// SHA-256 of the exact Parquet byte stream sent to object storage.
    pub sha256: [u8; 32],
    /// The shape the rows were encoded against.
    pub schema_version: SchemaVersionNo,
    /// Which producer wrote it, which is how the transformer routes the file.
    pub kind: FileKind,
}

/// Encodes sealed batches to Parquet and PUTs them to S3, epoch-namespaced. Cheap to clone (the
/// store is an `Arc`) — reload exporters each carry their own handle.
#[derive(Clone, Debug)]
pub struct ParquetStager {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    epoch: EpochNo,
}

/// One open reload Parquet object.
///
/// Unlike [`ParquetStager::put_with_kind`], this writer accepts a sequence of small Arrow batches.
/// Every [`write_batch`](Self::write_batch) call closes and awaits one Parquet row group, allowing a
/// reload worker to alternate source reads with remote writes instead of retaining a whole output
/// object in memory. The object does not become visible until [`finish`](Self::finish) succeeds.
pub struct ReloadParquetWriter {
    writer: AsyncArrowWriter<AbortableObjectWriter>,
    upload: AbortableObjectWriter,
    arrow_schema: SchemaRef,
    bucket: String,
    key: Path,
    source_schema: String,
    source_table: String,
    fence_lsn: Lsn,
    schema_version: SchemaVersionNo,
    row_count: u64,
    write_duration: Duration,
    terminal: bool,
}

impl std::fmt::Debug for ReloadParquetWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadParquetWriter")
            .field("key", &self.key)
            .field("source_schema", &self.source_schema)
            .field("source_table", &self.source_table)
            .field("fence_lsn", &self.fence_lsn)
            .field("schema_version", &self.schema_version)
            .field("row_count", &self.row_count)
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

/// Shared ownership lets the reload writer explicitly abort the multipart upload even though
/// `AsyncArrowWriter` owns the byte sink and does not expose it again.
#[derive(Clone, Debug)]
struct AbortableObjectWriter {
    state: Arc<Mutex<UploadState>>,
}

#[derive(Debug)]
struct UploadState {
    writer: Option<BufWriter>,
    /// `BufWriter` cannot abort once shutdown has started. Retain that fact if completion fails so
    /// cleanup never calls its documented panic path.
    completing: bool,
    /// Updated at the same choke point that hands bytes to `BufWriter`, so every producer path
    /// receives the same end-to-end fingerprint.
    hasher: Sha256,
    bytes_written: u64,
}

impl AbortableObjectWriter {
    fn new(writer: BufWriter) -> Self {
        Self {
            state: Arc::new(Mutex::new(UploadState {
                writer: Some(writer),
                completing: false,
                hasher: Sha256::new(),
                bytes_written: 0,
            })),
        }
    }

    async fn fingerprint(&self) -> Result<(u64, [u8; 32]), StagingError> {
        let state = self.state.lock().await;
        if state.writer.is_some() || !state.completing {
            return Err(StagingError::ClosedStream);
        }
        let digest = state.hasher.clone().finalize();
        let mut sha256 = [0_u8; 32];
        sha256.copy_from_slice(&digest);
        Ok((state.bytes_written, sha256))
    }

    async fn abort(&self) -> Result<(), object_store::Error> {
        let writer = {
            let mut state = self.state.lock().await;
            if state.completing {
                // A failed shutdown may have moved BufWriter into its non-abortable Flush state.
                // Dropping it is the only safe best effort at that point.
                state.writer.take();
                return Ok(());
            }
            state.writer.take()
        };
        if let Some(mut writer) = writer {
            writer.abort().await?;
        }
        Ok(())
    }
}

impl AsyncFileWriter for AbortableObjectWriter {
    fn write(
        &mut self,
        mut bytes: Bytes,
    ) -> futures_util::future::BoxFuture<'_, parquet::errors::Result<()>> {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().await;
            if state.completing {
                return Err(parquet::errors::ParquetError::General(
                    "reload object upload is already completing".into(),
                ));
            }
            state.hasher.update(&bytes);
            state.bytes_written = state
                .bytes_written
                .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            let writer = state.writer.as_mut().ok_or_else(|| {
                parquet::errors::ParquetError::General(
                    "reload object upload is already closed".into(),
                )
            })?;
            // BufWriter checks its concurrency limit before each `put`, while one very large put
            // could otherwise split into several concurrently-uploaded parts internally. Feed it
            // at most one part per call, then an empty write to await the last scheduled part. This
            // makes return from a Parquet row-group flush a true remote backpressure boundary.
            while !bytes.is_empty() {
                let take = bytes.len().min(RELOAD_MULTIPART_PART_BYTES);
                writer
                    .put(bytes.split_to(take))
                    .await
                    .map_err(|error| parquet::errors::ParquetError::External(Box::new(error)))?;
            }
            let result = writer
                .put(Bytes::new())
                .await
                .map_err(|error| parquet::errors::ParquetError::External(Box::new(error)));
            drop(state);
            result
        }
        .boxed()
    }

    fn complete(&mut self) -> futures_util::future::BoxFuture<'_, parquet::errors::Result<()>> {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().await;
            state.completing = true;
            let writer = state.writer.as_mut().ok_or_else(|| {
                parquet::errors::ParquetError::General(
                    "reload object upload is already closed".into(),
                )
            })?;
            writer
                .shutdown()
                .await
                .map_err(|error| parquet::errors::ParquetError::External(Box::new(error)))?;
            state.writer = None;
            drop(state);
            Ok(())
        }
        .boxed()
    }
}

impl ParquetStager {
    /// `bucket` is *stored*, so it is taken as `impl Into<String>` and converted exactly once here:
    /// `app::establish_stream` hands over the owned name from its
    /// [`ExtractorConfig`](crate::config::ExtractorConfig), fixtures pass a `&str` literal, and no call site
    /// has to spell `.to_string()` to reach an owned `String`.
    ///
    /// That same parameter is why `#[must_use]` is written out here while [`Self::object_key`] got
    /// it from the lint: `clippy::must_use_candidate` treats a generic argument as possibly
    /// side-effecting and skips the function. Constructing a sink PUTs nothing by itself.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<String>, epoch: EpochNo) -> Self {
        ParquetStager {
            store,
            bucket: bucket.into(),
            epoch,
        }
    }

    /// Best-effort delete of a staged object — used to clean up an aborted streamed txn's speculative
    /// files, which have no manifest row pointing at them.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::Store`] if the object store cannot delete `key`.
    pub async fn delete(&self, key: &Path) -> Result<(), StagingError> {
        self.store.delete(key).await?;
        Ok(())
    }

    /// `<epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet`. `lsn_end` is zero-padded 16-hex so keys
    /// sort by commit LSN; `uuid` matches the batch's `batch_id`-style file identity.
    #[must_use]
    pub fn object_key(&self, schema: &str, table: &str, lsn_end: Lsn, uuid: &str) -> Path {
        Path::from(format!(
            "{}/{}/{}/{}-{}.parquet",
            self.epoch, schema, table, lsn_end, uuid
        ))
    }

    /// Open one incrementally-written reload object at `fence_lsn`.
    ///
    /// Construction performs no remote I/O. Callers feed bounded Arrow micro-batches through
    /// [`ReloadParquetWriter::write_batch`], then publish the returned object in the control
    /// manifest only after [`ReloadParquetWriter::finish`] succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::Encode`] if the Arrow schema cannot initialize a Parquet writer.
    pub fn begin_reload_stream(
        &self,
        arrow_schema: SchemaRef,
        source_schema: impl Into<String>,
        source_table: impl Into<String>,
        fence_lsn: Lsn,
        schema_version: SchemaVersionNo,
    ) -> Result<ReloadParquetWriter, StagingError> {
        let source_schema = source_schema.into();
        let source_table = source_table.into();
        let uuid = uuid::Uuid::new_v4().to_string();
        let key = self.object_key(&source_schema, &source_table, fence_lsn, &uuid);
        // A single part in flight gives the COPY -> Arrow -> Parquet -> object-store pipeline a
        // concrete backpressure point. The capacity is fixed implementation policy, not a fourth
        // reload tuning knob alongside tables, workers, and rows per object.
        let buf_writer = BufWriter::with_capacity(
            Arc::clone(&self.store),
            key.clone(),
            RELOAD_MULTIPART_PART_BYTES,
        )
        .with_max_concurrency(1);
        let upload = AbortableObjectWriter::new(buf_writer);
        let writer = AsyncArrowWriter::try_new(
            upload.clone(),
            Arc::clone(&arrow_schema),
            Some(pg_to_arrow::default_writer_properties()),
        )?;

        Ok(ReloadParquetWriter {
            writer,
            upload,
            arrow_schema,
            bucket: self.bucket.clone(),
            key,
            source_schema,
            source_table,
            fence_lsn,
            schema_version,
            row_count: 0,
            write_duration: Duration::ZERO,
            terminal: false,
        })
    }

    /// Encode `batch` to Parquet (MICROS temporals + Snappy, inherited from the Arrow schema and the
    /// walrus writer properties) and stream it to S3 via multipart. Returns **only once durable**.
    /// Streamed WAL rows; reload and spill writers use [`Self::put_with_kind`].
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::Encode`] if Parquet encoding or multipart finalization fails, or
    /// [`StagingError::Store`] for an object-store transport failure.
    pub async fn put(&self, batch: SealedBatch) -> Result<WrittenObject, StagingError> {
        self.put_with_kind(batch, FileKind::Stream).await
    }

    /// As [`Self::put`], stamping the object's provenance in the manifest row's `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::Encode`] if the Arrow batch cannot be encoded/finalized as Parquet, or
    /// [`StagingError::Store`] if multipart upload to the object store fails.
    pub async fn put_with_kind(
        &self,
        batch: SealedBatch,
        kind: FileKind,
    ) -> Result<WrittenObject, StagingError> {
        let uuid = uuid::Uuid::new_v4().to_string();
        let key = self.object_key(&batch.schema, &batch.table, batch.lsn_end, &uuid);
        // Flush-latency + throughput instrumentation; no-op until a recorder is installed.
        let flush_start = std::time::Instant::now();
        let rows = u64::try_from(batch.record_batch.num_rows()).unwrap_or(u64::MAX);

        // Multipart streaming upload — no local temp file.
        let buf_writer = BufWriter::new(Arc::clone(&self.store), key.clone());
        let props = pg_to_arrow::default_writer_properties();
        // The ENCODE stays on this task: `AsyncArrowWriter` wraps a synchronous `ArrowWriter`, so
        // `write` and `close` compress the whole batch before they await — the `async` in that name is
        // the upload, not the compression. This remains on the task because no profile identifies
        // compression as a scheduler bottleneck; move it to the blocking pool if measurements do.
        let upload = AbortableObjectWriter::new(buf_writer);
        let mut writer =
            AsyncArrowWriter::try_new(upload.clone(), batch.record_batch.schema(), Some(props))?;
        writer.write(&batch.record_batch).await?;
        // close() finalises the Parquet footer AND completes the multipart upload — the durability
        // point. Nothing downstream may observe this batch before this returns Ok.
        writer.close().await?;
        let (object_size, sha256) = upload.fingerprint().await?;
        common::metrics::record_batch_flush(flush_start.elapsed().as_secs_f64(), rows);

        Ok(WrittenObject {
            s3_uri: format!("s3://{}/{}", self.bucket, key),
            key,
            source_schema: batch.schema,
            source_table: batch.table,
            lsn_start: batch.lsn_start,
            lsn_end: batch.lsn_end,
            row_count: batch.row_count,
            object_size,
            sha256,
            schema_version: batch.schema_version,
            kind,
        })
    }
}

impl ReloadParquetWriter {
    /// Store-relative key reserved for this object. It is useful for diagnostics and cleanup but
    /// does not imply that the object is durable or visible yet.
    #[must_use]
    pub const fn key(&self) -> &Path {
        &self.key
    }

    /// Rows successfully encoded and flushed into this open Parquet object.
    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Encode one Arrow micro-batch and explicitly flush its Parquet row group to the bounded
    /// multipart writer. Awaiting this call is the worker's backpressure boundary.
    ///
    /// Empty batches are harmless no-ops. A write failure automatically attempts to abort the
    /// multipart upload and permanently closes this writer.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::SchemaMismatch`] when `batch` does not have the schema supplied at
    /// construction, [`StagingError::ClosedStream`] after an earlier failure/finish/abort, or
    /// [`StagingError::Encode`] for Parquet or object-store write failures.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), StagingError> {
        if self.terminal {
            return Err(StagingError::ClosedStream);
        }
        if batch.schema().as_ref() != self.arrow_schema.as_ref() {
            self.terminal = true;
            if let Err(abort_error) = self.upload.abort().await {
                tracing::warn!(error = ?abort_error, key = %self.key, "failed to abort schema-mismatched reload upload");
            }
            return Err(StagingError::SchemaMismatch);
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let write_started = Instant::now();
        let result = async {
            self.writer.write(batch).await?;
            // AsyncArrowWriter otherwise retains an in-progress row group until it reaches the
            // writer property threshold. An explicit flush is what keeps reload memory bounded by
            // the caller's micro-batch rather than the output object's row count.
            self.writer.flush().await
        }
        .await;
        self.write_duration = self.write_duration.saturating_add(write_started.elapsed());
        if let Err(error) = result {
            self.terminal = true;
            if let Err(abort_error) = self.upload.abort().await {
                tracing::warn!(error = ?abort_error, key = %self.key, "failed to abort reload multipart upload");
            }
            return Err(StagingError::Encode(error));
        }

        self.row_count = self
            .row_count
            .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Finalize the Parquet footer and complete the multipart upload, returning only after the
    /// object is durable. This is the first point at which a manifest may reference the object.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::EmptyStream`] if no rows were written, [`StagingError::ClosedStream`] after
    /// an earlier failure/abort, or [`StagingError::Encode`] when footer/upload completion fails.
    pub async fn finish(mut self) -> Result<WrittenObject, StagingError> {
        if self.terminal {
            return Err(StagingError::ClosedStream);
        }
        if self.row_count == 0 {
            self.terminal = true;
            if let Err(error) = self.upload.abort().await {
                tracing::warn!(error = ?error, key = %self.key, "failed to abort empty reload upload");
            }
            return Err(StagingError::EmptyStream);
        }

        let finish_started = Instant::now();
        if let Err(error) = self.writer.finish().await {
            self.terminal = true;
            if let Err(abort_error) = self.upload.abort().await {
                tracing::warn!(error = ?abort_error, key = %self.key, "failed to abort reload upload after finalization error");
            }
            return Err(StagingError::Encode(error));
        }
        let (object_size, sha256) = self.upload.fingerprint().await?;
        self.write_duration = self.write_duration.saturating_add(finish_started.elapsed());
        self.terminal = true;
        common::metrics::record_batch_flush(self.write_duration.as_secs_f64(), self.row_count);

        Ok(WrittenObject {
            s3_uri: format!("s3://{}/{}", self.bucket, self.key),
            key: self.key.clone(),
            source_schema: self.source_schema.clone(),
            source_table: self.source_table.clone(),
            lsn_start: self.fence_lsn,
            lsn_end: self.fence_lsn,
            row_count: self.row_count,
            object_size,
            sha256,
            schema_version: self.schema_version,
            kind: FileKind::Reload,
        })
    }

    /// Explicitly cancel this object and clean up uploaded multipart parts where the store permits.
    /// Buffered, not-yet-uploaded bytes are simply dropped.
    ///
    /// # Errors
    ///
    /// Returns [`StagingError::Store`] when the object store rejects multipart cleanup.
    pub async fn abort(mut self) -> Result<(), StagingError> {
        if self.terminal {
            return Ok(());
        }
        self.terminal = true;
        self.upload.abort().await?;
        Ok(())
    }
}

impl Drop for ReloadParquetWriter {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        // Drop cannot await. Under a Tokio runtime, schedule best-effort multipart cleanup; outside
        // one, dropping the upload still leaves no completed/visible object. Store lifecycle rules
        // remain the final guard for process death, as with every multipart client.
        let upload = self.upload.clone();
        let key = self.key.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            std::mem::drop(runtime.spawn(async move {
                if let Err(error) = upload.abort().await {
                    tracing::warn!(error = ?error, %key, "failed to abort dropped reload multipart upload");
                }
            }));
        }
    }
}

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StagingError {
    /// The engine's error itself, not its rendering: only the typed value tells a footer write
    /// apart from an out-of-spec schema, and `ParquetError::External` nests a further cause that
    /// `to_string()` drops.
    #[error("parquet encode: {0}")]
    Encode(#[source] parquet::errors::ParquetError),
    /// The object store rejected or could not complete the request. `transparent` because
    /// `object_store::Error` already names the operation and the path.
    #[error(transparent)]
    Store(#[from] object_store::Error),
    /// A reload micro-batch does not match the schema used to open its Parquet object.
    #[error("reload record batch schema does not match the open Parquet object")]
    SchemaMismatch,
    /// A reload stream is empty; publishing an empty Parquet object would create bogus progress.
    #[error("cannot finish an empty reload Parquet stream")]
    EmptyStream,
    /// The reload stream already failed, finished, or was aborted.
    #[error("reload Parquet stream is already closed")]
    ClosedStream,
}

/// Every Parquet failure on the writer path — builder, `write`, footer/multipart `close` — is the
/// same class, so `?` does the wrapping instead of an identical closure at each call. Flattening to
/// `common::Error`'s stringly variants happens at the process edge below, so everything upstream of
/// it still sees the engine failure whole.
impl From<parquet::errors::ParquetError> for StagingError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        StagingError::Encode(error)
    }
}

impl From<StagingError> for common::Error {
    fn from(e: StagingError) -> Self {
        match e {
            StagingError::Store(inner) => common::Error::ObjectStore(inner.to_string()),
            StagingError::Encode(source) => {
                common::Error::Internal(format!("parquet encode: {source}"))
            }
            other => common::Error::Internal(other.to_string()),
        }
    }
}

#[cfg(test)]
#[path = "staging_test.rs"]
mod tests;
