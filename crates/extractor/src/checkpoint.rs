//! The durability checkpoint — **the heart of the whole extractor** (§1.5).
//!
//! Only *after* a batch's Parquet is durable in S3 **and** its `file_manifest` row is
//! committed does [`DurabilityCheckpoint::on_commit_durable`] translate the batch's commit-order LSN
//! to its pgoutput `end_lsn` and advance `confirmed_flush_lsn`; the next standby status update carries
//! that end position as `flush`/`apply`. That is the
//! WAL-bounding invariant: slot lag is bounded to at most one in-flight batch, and a crash before the
//! checkpoint just re-streams from the last confirmed LSN (at-least-once, no loss).
//!
//! **Two LSNs, two rules (§1.9):** `confirmed_flush` (durable) moves only here; the *received*
//! keepalive LSN — `write` — is owned by [`ReplicationStream`] and moves **unconditionally** on every
//! frame. Conflating them causes disconnects (if you gate keepalives on
//! durability) or data loss (if you advance `confirmed_flush` as your keepalive). We keep them apart:
//! this struct owns `confirmed_flush`; the stream owns `received`.

use crate::replication::{ReplicationStream, StandbyStatus};
use common::Lsn;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};

/// The streamed-transaction view in the durability checkpoint disagrees with the protocol demux.
/// Treating either case as recoverable could lift the WAL-retention fence while a transaction is
/// still open, so the decode loop stops instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CheckpointError {
    /// A second first segment tried to register an already-open top-level transaction.
    #[error("durability checkpoint already contains open streamed top xid {top_xid}")]
    AlreadyOpen {
        /// Top-level xid carried by `StreamStart`.
        top_xid: u32,
    },
    /// A commit or whole-transaction abort tried to remove a transaction that was never registered.
    #[error("durability checkpoint does not contain streamed top xid {top_xid}")]
    NotOpen {
        /// Top-level xid carried by `StreamCommit` or `StreamAbort`.
        top_xid: u32,
    },
    /// A pgoutput commit boundary cannot end at or before the commit record's start.
    #[error("invalid pgoutput commit boundary: commit_lsn={commit_lsn}, end_lsn={end_lsn}")]
    InvalidCommitBoundary {
        /// Start/identity LSN carried by `Commit` or `StreamCommit`.
        commit_lsn: Lsn,
        /// End-of-record feedback position carried by the same message.
        end_lsn: Lsn,
    },
    /// Re-observing one source commit with a different end position is protocol corruption.
    #[error(
        "conflicting pgoutput commit boundary at {commit_lsn}: observed end {observed_end_lsn}, incoming end {incoming_end_lsn}"
    )]
    ConflictingCommitBoundary {
        /// Source commit identity shared by both observations.
        commit_lsn: Lsn,
        /// End position retained from the first observation.
        observed_end_lsn: Lsn,
        /// Different end position carried by the later observation.
        incoming_end_lsn: Lsn,
    },
    /// Two distinct commit records claimed overlapping WAL ranges.
    #[error(
        "overlapping pgoutput commit boundaries: earlier {earlier_commit_lsn}..{earlier_end_lsn}, later starts at {later_commit_lsn}"
    )]
    OverlappingCommitBoundaries {
        /// Commit record that starts first.
        earlier_commit_lsn: Lsn,
        /// Claimed end of the earlier commit record.
        earlier_end_lsn: Lsn,
        /// Start of the next commit record.
        later_commit_lsn: Lsn,
    },
    /// Decoding moved behind a commit boundary already translated as durable in this process.
    #[error(
        "pgoutput commit {commit_lsn} regressed behind durable commit boundary {durable_commit_lsn}"
    )]
    CommitBehindDurableBoundary {
        /// Newly observed, older commit identity.
        commit_lsn: Lsn,
        /// Highest commit identity already marked durable.
        durable_commit_lsn: Lsn,
    },
    /// Durability was reported in the commit-order domain without its matching source feedback cursor.
    #[error("durable commit {commit_lsn} has no observed pgoutput end_lsn")]
    MissingCommitBoundary {
        /// Commit-order LSN that became durable.
        commit_lsn: Lsn,
    },
}

/// Owns the slot-advancing `confirmed_flush_lsn`. Distinct from the stream's received LSN.
#[allow(
    missing_copy_implementations,
    reason = "copying this mutable durability state could silently detach checkpoint advances"
)]
#[derive(Debug, Clone)]
pub struct DurabilityCheckpoint {
    confirmed_flush: Lsn,
    /// Highest LSN whose object/control durability has completed. It may run ahead while an open
    /// streamed transaction clamps feedback; remembering it lets commit/abort lift that clamp without
    /// waiting for another batch to become durable.
    durable_high_water: Lsn,
    /// Highest commit boundary already translated into [`Self::durable_high_water`]. Retaining both
    /// positions makes an exact re-observation verifiable after older pending mappings are pruned.
    durable_commit_boundary: Option<CommitBoundary>,
    /// Commit-order LSN → end-of-commit-record feedback position. Ordinary table batches can remain
    /// in memory across later transactions, so the mapping must survive until their commit frontier
    /// becomes durable (including a graceful-shutdown drain).
    commit_end_lsn: BTreeMap<Lsn, Lsn>,
    /// For each still-open streamed transaction, the position that was already confirmed *before*
    /// its first `StreamStart` was processed. Feedback is clamped to the oldest such ceiling.
    ///
    /// Using the `StreamStart` record's own LSN as the clamp is unsafe: acknowledging that exact LSN
    /// can let PostgreSQL restart after the record, leaving replay without the state needed to decode
    /// the transaction's continuation. Capturing the prior confirmed position is conservative and
    /// remains safe for interleaved proto-v2 transactions.
    open_stream_ceilings: HashMap<u32, Lsn>,
}

#[derive(Debug, Clone, Copy)]
struct CommitBoundary {
    commit_lsn: Lsn,
    end_lsn: Lsn,
}

impl DurabilityCheckpoint {
    /// A checkpoint starting at the LSN the slot is resuming from — the position already known
    /// durable, not zero, so a restart never re-confirms ground the source has moved past.
    #[must_use]
    pub fn new(resume_lsn: Lsn) -> Self {
        DurabilityCheckpoint {
            confirmed_flush: resume_lsn,
            durable_high_water: resume_lsn,
            durable_commit_boundary: None,
            commit_end_lsn: BTreeMap::new(),
            open_stream_ceilings: HashMap::new(),
        }
    }

    /// Retain the two positions carried by one successfully decoded pgoutput `Commit` or
    /// `StreamCommit`. `commit_lsn` remains the identity/order key stored in rows and manifests;
    /// `end_lsn` is the first source feedback position beyond the complete commit record.
    ///
    /// Calling this before any durability side effect makes a missing/invalid translation fail
    /// closed later. Re-observing the exact same boundary is idempotent; a conflicting boundary is
    /// corruption and leaves the original mapping intact.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::InvalidCommitBoundary`] when `end_lsn <= commit_lsn`, or
    /// [`CheckpointError::ConflictingCommitBoundary`] when the same commit was already paired with a
    /// different end position, [`CheckpointError::OverlappingCommitBoundaries`] when two mappings
    /// cannot both describe ordered WAL records, or
    /// [`CheckpointError::CommitBehindDurableBoundary`] when decoding regresses behind a mapping
    /// already consumed in this process.
    pub fn observe_commit(&mut self, commit_lsn: Lsn, end_lsn: Lsn) -> Result<(), CheckpointError> {
        if end_lsn <= commit_lsn {
            return Err(CheckpointError::InvalidCommitBoundary {
                commit_lsn,
                end_lsn,
            });
        }
        if let Some(durable) = self.durable_commit_boundary {
            if commit_lsn < durable.commit_lsn {
                return Err(CheckpointError::CommitBehindDurableBoundary {
                    commit_lsn,
                    durable_commit_lsn: durable.commit_lsn,
                });
            }
            if commit_lsn == durable.commit_lsn {
                if end_lsn != durable.end_lsn {
                    return Err(CheckpointError::ConflictingCommitBoundary {
                        commit_lsn,
                        observed_end_lsn: durable.end_lsn,
                        incoming_end_lsn: end_lsn,
                    });
                }
                return Ok(());
            }
            if commit_lsn < durable.end_lsn {
                return Err(CheckpointError::OverlappingCommitBoundaries {
                    earlier_commit_lsn: durable.commit_lsn,
                    earlier_end_lsn: durable.end_lsn,
                    later_commit_lsn: commit_lsn,
                });
            }
        }
        if let Some(observed_end_lsn) = self.commit_end_lsn.get(&commit_lsn).copied() {
            if observed_end_lsn != end_lsn {
                return Err(CheckpointError::ConflictingCommitBoundary {
                    commit_lsn,
                    observed_end_lsn,
                    incoming_end_lsn: end_lsn,
                });
            }
            return Ok(());
        }
        if let Some((earlier_commit_lsn, earlier_end_lsn)) =
            self.commit_end_lsn.range(..commit_lsn).next_back()
            && *earlier_end_lsn > commit_lsn
        {
            return Err(CheckpointError::OverlappingCommitBoundaries {
                earlier_commit_lsn: *earlier_commit_lsn,
                earlier_end_lsn: *earlier_end_lsn,
                later_commit_lsn: commit_lsn,
            });
        }
        if let Some((later_commit_lsn, _)) = self
            .commit_end_lsn
            .range((
                std::ops::Bound::Excluded(commit_lsn),
                std::ops::Bound::Unbounded,
            ))
            .next()
            && end_lsn > *later_commit_lsn
        {
            return Err(CheckpointError::OverlappingCommitBoundaries {
                earlier_commit_lsn: commit_lsn,
                earlier_end_lsn: end_lsn,
                later_commit_lsn: *later_commit_lsn,
            });
        }
        self.commit_end_lsn.insert(commit_lsn, end_lsn);
        Ok(())
    }

    /// The LSN it is safe to tell the source it may discard. This is what frees WAL on the source,
    /// so it must never run ahead of what walrus has actually made durable.
    #[must_use]
    pub const fn confirmed_flush(&self) -> Lsn {
        self.confirmed_flush
    }

    /// Capture the only safe ceiling for a new streamed transaction. Call this before processing its
    /// first `StreamStart`, then pass the value to [`Self::on_stream_start`] only after the protocol
    /// demux accepted that start. Merely receiving a `StreamStart` must never advance feedback.
    #[must_use]
    pub const fn capture_pre_stream_start_ceiling(&self) -> Lsn {
        self.confirmed_flush
    }

    /// Register a top-level streamed transaction after its first `StreamStart` was accepted by the
    /// protocol demux. `pre_start_ceiling` must have been captured with
    /// [`Self::capture_pre_stream_start_ceiling`] before that start was processed.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::AlreadyOpen`] if the top-level xid is already registered.
    /// State is unchanged on error.
    pub fn on_stream_start(
        &mut self,
        top_xid: u32,
        pre_start_ceiling: Lsn,
    ) -> Result<(), CheckpointError> {
        match self.open_stream_ceilings.entry(top_xid) {
            Entry::Occupied(_) => return Err(CheckpointError::AlreadyOpen { top_xid }),
            Entry::Vacant(entry) => {
                entry.insert(pre_start_ceiling);
            }
        }
        self.recompute_confirmed();
        Ok(())
    }

    /// Remove a committed or wholly aborted top-level streamed transaction from the feedback fence.
    ///
    /// Returns `true` when removing this ceiling made an already-durable high-water mark immediately
    /// confirmable. The caller must then send a standby status even though no new batch was written.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::NotOpen`] if the top-level xid is not registered. State is
    /// unchanged on error.
    pub fn on_stream_end(&mut self, top_xid: u32) -> Result<bool, CheckpointError> {
        if self.open_stream_ceilings.remove(&top_xid).is_none() {
            return Err(CheckpointError::NotOpen { top_xid });
        }
        let before = self.confirmed_flush;
        self.recompute_confirmed();
        Ok(self.confirmed_flush > before)
    }

    /// A source commit is durable (objects + control publication committed): translate its
    /// commit-order LSN through the boundary retained by [`Self::observe_commit`], then advance
    /// `confirmed_flush` to the matching pgoutput `end_lsn`, clamped to every open transaction's
    /// pre-start ceiling and never regressing.
    ///
    /// **Call only after every source effect through `commit_lsn` is durable.** The exact lookup is
    /// deliberate: treating a manifest commit LSN as a feedback position can restart PostgreSQL at
    /// the beginning of that commit record and replay the boundary transaction.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::MissingCommitBoundary`] rather than acknowledging an untranslated
    /// commit-order LSN.
    pub fn on_commit_durable(&mut self, commit_lsn: Lsn) -> Result<(), CheckpointError> {
        if self
            .durable_commit_boundary
            .is_some_and(|durable| commit_lsn <= durable.commit_lsn)
        {
            return Ok(());
        }
        let end_lsn = self
            .commit_end_lsn
            .get(&commit_lsn)
            .copied()
            .ok_or(CheckpointError::MissingCommitBoundary { commit_lsn })?;
        self.durable_commit_boundary = Some(CommitBoundary {
            commit_lsn,
            end_lsn,
        });
        self.commit_end_lsn
            .retain(|observed_commit, _| *observed_commit > commit_lsn);
        self.durable_high_water = self.durable_high_water.max(end_lsn);
        self.recompute_confirmed();
        Ok(())
    }

    fn recompute_confirmed(&mut self) {
        let oldest_ceiling = self.open_stream_ceilings.values().copied().min();
        let target = oldest_ceiling.map_or(self.durable_high_water, |ceiling| {
            self.durable_high_water.min(ceiling)
        });
        self.confirmed_flush = self.confirmed_flush.max(target);
    }

    /// The standby reply: `write` = the stream's received/keepalive LSN (unconditional), `flush`/`apply`
    /// = `confirmed_flush` (durable). A stalled flush advances `write` (via the stream) but not these.
    #[must_use]
    pub const fn standby_status(&self, received: Lsn, reply_requested: bool) -> StandbyStatus {
        StandbyStatus {
            write: received,
            flush: self.confirmed_flush,
            apply: self.confirmed_flush,
            reply_requested,
        }
    }

    /// Send a standby status carrying the durable `confirmed_flush`, and sync it onto the stream so the
    /// stream's own periodic keepalive reports the same `flush` (never a stale one).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the replication socket cannot write or flush the standby status.
    pub async fn send(
        &self,
        stream: &mut ReplicationStream,
        reply_requested: bool,
    ) -> anyhow::Result<()> {
        stream.set_durable(self.confirmed_flush);
        let status = self.standby_status(stream.last_received(), reply_requested);
        stream.send_standby_status(status).await
    }
}

#[cfg(test)]
#[path = "checkpoint_test.rs"]
mod tests;
