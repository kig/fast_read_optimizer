use iou::IoUring;
use libc::{c_uchar, size_t};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

use crate::common::{AlignedBuffer, IOMode};
use crate::io_util::open_reader_files;
use crate::mincore::is_first_page_resident;

#[link(name = "crypto")]
unsafe extern "C" {
    fn SHA256(data: *const c_uchar, len: size_t, md: *mut c_uchar) -> *mut c_uchar;
}

pub const BLOCK_HASH_SIZE: u64 = 1024 * 1024;
const MAX_DIGEST_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestGeometry {
    file_size: u64,
    block_size: u64,
    block_count: usize,
    hash_type: BlockHashAlgorithm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockHashAlgorithm {
    Xxh3,
    Sha256,
}

impl BlockHashAlgorithm {
    fn digest_len(self) -> usize {
        match self {
            Self::Xxh3 => 8,
            Self::Sha256 => 32,
        }
    }

    fn hash_block(self, data: &[u8]) -> BlockDigest {
        match self {
            Self::Xxh3 => BlockDigest::from_prefix(&xxh3_64(data).to_le_bytes()),
            Self::Sha256 => BlockDigest::from_prefix(&sha256_digest(data)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockDigest {
    len: u8,
    bytes: [u8; MAX_DIGEST_LEN],
}

impl BlockDigest {
    fn from_prefix(bytes: &[u8]) -> Self {
        let mut digest = [0u8; MAX_DIGEST_LEN];
        digest[..bytes.len()].copy_from_slice(bytes);
        Self {
            len: bytes.len() as u8,
            bytes: digest,
        }
    }

    fn is_valid_for(&self, hash_type: BlockHashAlgorithm) -> bool {
        self.len as usize == hash_type.digest_len()
            && self.bytes[hash_type.digest_len()..]
                .iter()
                .all(|byte| *byte == 0)
    }

    fn as_bytes_for(&self, hash_type: BlockHashAlgorithm) -> &[u8] {
        debug_assert!(self.is_valid_for(hash_type));
        &self.bytes[..hash_type.digest_len()]
    }

    fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(self.len as usize * 2);
        for byte in &self.bytes[..self.len as usize] {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{:02x}", byte);
        }
        hex
    }

    fn from_hex(value: &str) -> Result<Self, String> {
        if value.len() % 2 != 0 {
            return Err(format!(
                "expected an even number of hex characters, got {}",
                value.len(),
            ));
        }
        if value.len() / 2 > MAX_DIGEST_LEN {
            return Err(format!(
                "expected at most {} hex characters, got {}",
                MAX_DIGEST_LEN * 2,
                value.len()
            ));
        }
        let mut digest = [0u8; MAX_DIGEST_LEN];
        for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(chunk).map_err(|err| err.to_string())?;
            digest[index] = u8::from_str_radix(text, 16).map_err(|err| err.to_string())?;
        }
        Ok(Self {
            len: (value.len() / 2) as u8,
            bytes: digest,
        })
    }
}

impl std::fmt::Display for BlockDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for BlockDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for BlockDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockHashManifest {
    pub hash_type: BlockHashAlgorithm,
    pub file_size: u64,
    pub block_size: u64,
    pub bytes_hashed: u64,
    pub block_hashes: Vec<BlockDigest>,
    pub hash_of_hashes: BlockDigest,
}

impl BlockHashManifest {
    pub fn verify_integrity(&self) -> bool {
        if self.bytes_hashed != self.file_size {
            return false;
        }
        if validate_block_size(self.block_size).is_err() {
            return false;
        }
        if !self.hash_of_hashes.is_valid_for(self.hash_type) {
            return false;
        }
        if self
            .block_hashes
            .iter()
            .any(|digest| !digest.is_valid_for(self.hash_type))
        {
            return false;
        }
        match block_count_for_size(self.file_size, self.block_size) {
            Ok(expected_blocks) => {
                expected_blocks == self.block_hashes.len()
                    && self.hash_of_hashes == hash_hashes(self.hash_type, &self.block_hashes)
            }
            Err(_) => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockHashWitnessKind {
    FileCopy,
    HashReplica,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHashVote {
    pub hash: BlockDigest,
    pub total_votes: usize,
    pub file_copy_votes: usize,
    pub hash_replica_votes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockRecoveryBasis {
    IntactHash,
    FileAndFileAgreement,
    FileAndManifestAgreement,
}

impl BlockRecoveryBasis {
    fn label(&self) -> &'static str {
        match self {
            BlockRecoveryBasis::IntactHash => "intact hash",
            BlockRecoveryBasis::FileAndFileAgreement => "file+file agreement",
            BlockRecoveryBasis::FileAndManifestAgreement => "file+manifest agreement",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockRecoveryFailure {
    ManifestOnlyAgreement,
    NoBlockHashFound,
    IntactHashWithoutMatchingBlock,
    ConflictingIntactHashes,
}

impl BlockRecoveryFailure {
    fn label(&self) -> &'static str {
        match self {
            BlockRecoveryFailure::ManifestOnlyAgreement => "manifest+manifest hashes agree",
            BlockRecoveryFailure::NoBlockHashFound => "no block hash found either",
            BlockRecoveryFailure::IntactHashWithoutMatchingBlock => {
                "intact hash found but no matching file block"
            }
            BlockRecoveryFailure::ConflictingIntactHashes => "conflicting intact hashes",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecoveryDecision {
    pub block_index: usize,
    pub elected_hash: Option<BlockDigest>,
    pub repair_source_index: Option<usize>,
    pub basis: Option<BlockRecoveryBasis>,
    pub failure: Option<BlockRecoveryFailure>,
    pub votes: Vec<BlockHashVote>,
}

impl BlockRecoveryDecision {
    pub fn status_message(&self) -> String {
        if let Some(basis) = &self.basis {
            format!("recovered block based on {}", basis.label())
        } else if let Some(failure) = &self.failure {
            format!("failed to recover corrupt block [{}]", failure.label())
        } else {
            "failed to recover corrupt block [unknown reason]".to_string()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockVerifyIssue {
    pub block_index: usize,
    pub current_hash: BlockDigest,
    pub decision: BlockRecoveryDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    pub bytes_hashed: u64,
    pub total_blocks: usize,
    pub loaded_manifests: usize,
    pub ok_blocks: usize,
    pub bad_blocks: Vec<BlockVerifyIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecoverIssue {
    pub file_index: usize,
    pub file_path: String,
    pub block_index: usize,
    pub current_hash: BlockDigest,
    pub decision: BlockRecoveryDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverMode {
    Standard,
    Fast,
    InPlaceAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverReport {
    pub bytes_hashed: u64,
    pub repaired_blocks: usize,
    pub repaired_files: usize,
    pub sidecars_refreshed: usize,
    pub used_fast_path: bool,
    pub fell_back_to_full_scan: bool,
    pub failed_blocks: Vec<BlockRecoverIssue>,
}

fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let mut digest = [0u8; 32];
    let ret = unsafe { SHA256(data.as_ptr(), data.len(), digest.as_mut_ptr()) };
    assert!(!ret.is_null(), "SHA256 returned null");
    digest
}

fn hash_hashes(hash_type: BlockHashAlgorithm, values: &[BlockDigest]) -> BlockDigest {
    let mut bytes = Vec::with_capacity(values.len() * hash_type.digest_len());
    for value in values {
        if !value.is_valid_for(hash_type) {
            return BlockDigest::from_prefix(&[]);
        }
        bytes.extend_from_slice(value.as_bytes_for(hash_type));
    }
    hash_type.hash_block(&bytes)
}

fn validate_block_size(block_size: u64) -> std::io::Result<()> {
    if block_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "block hash block size must be greater than zero",
        ));
    }
    if block_size % 4096 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "block hash block size must be a multiple of 4096 bytes, got {}",
                block_size
            ),
        ));
    }
    Ok(())
}

fn block_count_for_size(file_size: u64, block_size: u64) -> std::io::Result<usize> {
    validate_block_size(block_size)?;
    Ok(if file_size == 0 {
        0
    } else {
        file_size.div_ceil(block_size) as usize
    })
}

pub fn default_hash_base(filename: &str) -> String {
    format!("{}.fro-hash", filename)
}

fn hash_replica_paths(base: &str) -> [String; 3] {
    [
        format!("{}.0.json", base),
        format!("{}.1.json", base),
        format!("{}.2.json", base),
    ]
}

fn json_error_to_io(err: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err)
}

pub fn save_manifest_replicas(base: &str, manifest: &BlockHashManifest) -> std::io::Result<()> {
    let data = serde_json::to_vec_pretty(manifest).map_err(json_error_to_io)?;
    for path in hash_replica_paths(base) {
        std::fs::write(path, &data)?;
    }
    Ok(())
}

pub fn load_manifest_replicas(base: &str) -> Vec<Option<BlockHashManifest>> {
    hash_replica_paths(base)
        .into_iter()
        .map(|path| {
            std::fs::read_to_string(path)
                .ok()
                .and_then(|data| serde_json::from_str::<BlockHashManifest>(&data).ok())
        })
        .collect()
}

fn hash_base_for_file(index: usize, path: &str, hash_base: Option<&str>) -> String {
    if index == 0 {
        hash_base
            .map(str::to_string)
            .unwrap_or_else(|| default_hash_base(path))
    } else {
        default_hash_base(path)
    }
}

fn manifest_geometry(
    manifests: &[Option<BlockHashManifest>],
    label: &str,
) -> std::io::Result<Option<ManifestGeometry>> {
    let mut expected: Option<ManifestGeometry> = None;

    for manifest in manifests.iter().flatten() {
        let block_count = block_count_for_size(manifest.file_size, manifest.block_size)?;
        if manifest.block_hashes.len() != block_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} has {} block hashes but file_size={} and block_size={} imply {}",
                    label,
                    manifest.block_hashes.len(),
                    manifest.file_size,
                    manifest.block_size,
                    block_count
                ),
            ));
        }

        let current = ManifestGeometry {
            file_size: manifest.file_size,
            block_size: manifest.block_size,
            block_count,
            hash_type: manifest.hash_type,
        };

        if let Some(previous) = expected {
            if previous.file_size != current.file_size
                || previous.block_size != current.block_size
                || previous.block_count != current.block_count
                || previous.hash_type != current.hash_type
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{} disagree on geometry; saw file_size={}, block_size={}, blocks={}, hash_type={:?} and file_size={}, block_size={}, blocks={}, hash_type={:?}",
                        label,
                        previous.file_size,
                        previous.block_size,
                        previous.block_count,
                        previous.hash_type,
                        current.file_size,
                        current.block_size,
                        current.block_count,
                        current.hash_type
                    ),
                ));
            }
        } else {
            expected = Some(current);
        }
    }

    Ok(expected)
}

fn consistent_intact_manifest(
    manifests: &[Option<BlockHashManifest>],
) -> Option<&BlockHashManifest> {
    let mut intact = manifests
        .iter()
        .flatten()
        .filter(|manifest| manifest.verify_integrity());
    let first = intact.next()?;
    if intact.all(|manifest| {
        manifest.file_size == first.file_size
            && manifest.block_size == first.block_size
            && manifest.hash_type == first.hash_type
            && manifest.block_hashes == first.block_hashes
    }) {
        Some(first)
    } else {
        None
    }
}

fn sidecars_are_fully_healthy_for_current(
    current: &BlockHashManifest,
    manifests: &[Option<BlockHashManifest>],
) -> bool {
    manifests.len() == 3
        && manifests.iter().all(|manifest| {
            manifest.as_ref().is_some_and(|manifest| {
                manifest.verify_integrity()
                    && manifest.hash_type == current.hash_type
                    && manifest.file_size == current.file_size
                    && manifest.block_size == current.block_size
                    && manifest.block_hashes == current.block_hashes
            })
        })
}

fn verify_report_from_current(
    current: &BlockHashManifest,
    manifests: &[Option<BlockHashManifest>],
) -> VerifyReport {
    let intact_manifest = consistent_intact_manifest(manifests);
    let loaded_manifests = manifests.iter().filter(|m| m.is_some()).count();
    let manifest_refs = manifests.iter().map(|m| m.as_ref()).collect::<Vec<_>>();

    let mut ok_blocks = 0;
    let mut bad_blocks = Vec::new();

    for (block_index, current_hash) in current.block_hashes.iter().copied().enumerate() {
        if intact_manifest
            .and_then(|manifest| manifest.block_hashes.get(block_index))
            .is_some_and(|expected_hash| *expected_hash == current_hash)
        {
            ok_blocks += 1;
            continue;
        }

        let decision = recover_block_hash(block_index, &[Some(current_hash)], &manifest_refs);
        let block_ok =
            decision.elected_hash == Some(current_hash) && decision.repair_source_index == Some(0);
        if block_ok {
            ok_blocks += 1;
        } else {
            bad_blocks.push(BlockVerifyIssue {
                block_index,
                current_hash,
                decision,
            });
        }
    }

    VerifyReport {
        bytes_hashed: current.bytes_hashed,
        total_blocks: current.block_hashes.len(),
        loaded_manifests,
        ok_blocks,
        bad_blocks,
    }
}

fn tally_hash_votes(values: &[(BlockDigest, BlockHashWitnessKind)]) -> Vec<BlockHashVote> {
    let mut votes = Vec::<BlockHashVote>::new();
    for (hash, kind) in values {
        if let Some(vote) = votes.iter_mut().find(|vote| vote.hash == *hash) {
            vote.total_votes += 1;
            match kind {
                BlockHashWitnessKind::FileCopy => vote.file_copy_votes += 1,
                BlockHashWitnessKind::HashReplica => vote.hash_replica_votes += 1,
            }
            continue;
        }

        votes.push(BlockHashVote {
            hash: *hash,
            total_votes: 1,
            file_copy_votes: usize::from(*kind == BlockHashWitnessKind::FileCopy),
            hash_replica_votes: usize::from(*kind == BlockHashWitnessKind::HashReplica),
        });
    }
    votes.sort_by(|a, b| {
        b.total_votes
            .cmp(&a.total_votes)
            .then_with(|| b.file_copy_votes.cmp(&a.file_copy_votes))
            .then_with(|| b.hash_replica_votes.cmp(&a.hash_replica_votes))
            .then_with(|| a.hash.cmp(&b.hash))
    });
    votes
}

fn first_matching_file_copy(
    file_copy_hashes: &[Option<BlockDigest>],
    expected_hash: BlockDigest,
) -> Option<usize> {
    file_copy_hashes
        .iter()
        .position(|hash| hash.is_some_and(|value| value == expected_hash))
}

fn unique_top_vote<F>(votes: &[BlockHashVote], predicate: F) -> Option<&BlockHashVote>
where
    F: Fn(&BlockHashVote) -> bool,
{
    let mut matching = votes.iter().filter(|vote| predicate(vote));
    let top = matching.next()?;
    let next = matching.next();
    if next
        .map(|other| other.total_votes < top.total_votes)
        .unwrap_or(true)
    {
        Some(top)
    } else {
        None
    }
}

pub fn recover_block_hash(
    block_index: usize,
    file_copy_hashes: &[Option<BlockDigest>],
    hash_manifests: &[Option<&BlockHashManifest>],
) -> BlockRecoveryDecision {
    let mut witnesses = Vec::<(BlockDigest, BlockHashWitnessKind)>::new();
    let mut intact_manifest_hashes = Vec::<BlockDigest>::new();

    for hash in file_copy_hashes.iter().flatten() {
        witnesses.push((*hash, BlockHashWitnessKind::FileCopy));
    }

    for manifest in hash_manifests.iter().flatten() {
        if let Some(hash) = manifest.block_hashes.get(block_index) {
            if manifest.verify_integrity() {
                intact_manifest_hashes.push(*hash);
            }
            witnesses.push((*hash, BlockHashWitnessKind::HashReplica));
        }
    }

    let votes = tally_hash_votes(&witnesses);

    if let Some(first) = intact_manifest_hashes.first().copied() {
        if intact_manifest_hashes.iter().any(|hash| *hash != first) {
            return BlockRecoveryDecision {
                block_index,
                elected_hash: None,
                repair_source_index: None,
                basis: None,
                failure: Some(BlockRecoveryFailure::ConflictingIntactHashes),
                votes,
            };
        }

        if let Some(top) = unique_top_vote(&votes, |vote| vote.file_copy_votes >= 2) {
            if top.hash != first {
                return BlockRecoveryDecision {
                    block_index,
                    elected_hash: None,
                    repair_source_index: None,
                    basis: None,
                    failure: Some(BlockRecoveryFailure::ConflictingIntactHashes),
                    votes,
                };
            }
        }

        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(first),
            repair_source_index: first_matching_file_copy(file_copy_hashes, first),
            basis: first_matching_file_copy(file_copy_hashes, first)
                .map(|_| BlockRecoveryBasis::IntactHash),
            failure: first_matching_file_copy(file_copy_hashes, first)
                .is_none()
                .then_some(BlockRecoveryFailure::IntactHashWithoutMatchingBlock),
            votes,
        };
    }

    if let Some(top) = unique_top_vote(&votes, |vote| vote.file_copy_votes >= 2) {
        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(top.hash),
            repair_source_index: first_matching_file_copy(file_copy_hashes, top.hash),
            basis: Some(BlockRecoveryBasis::FileAndFileAgreement),
            failure: None,
            votes,
        };
    }

    if let Some(top) = unique_top_vote(&votes, |vote| {
        vote.file_copy_votes >= 1 && vote.hash_replica_votes >= 1
    }) {
        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(top.hash),
            repair_source_index: first_matching_file_copy(file_copy_hashes, top.hash),
            basis: Some(BlockRecoveryBasis::FileAndManifestAgreement),
            failure: None,
            votes,
        };
    }

    if let Some(top) = unique_top_vote(&votes, |vote| vote.hash_replica_votes >= 2) {
        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(top.hash),
            repair_source_index: None,
            basis: None,
            failure: Some(BlockRecoveryFailure::ManifestOnlyAgreement),
            votes,
        };
    }

    BlockRecoveryDecision {
        block_index,
        elected_hash: None,
        repair_source_index: None,
        basis: None,
        failure: Some(BlockRecoveryFailure::NoBlockHashFound),
        votes,
    }
}

fn block_offset(
    thread_base: u64,
    block_id: u64,
    num_threads: u64,
    block_size: u64,
) -> std::io::Result<u64> {
    let stride = block_id
        .checked_mul(num_threads)
        .and_then(|value| value.checked_mul(block_size))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "hash offset calculation overflowed",
            )
        })?;
    thread_base.checked_add(stride).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hash offset calculation overflowed",
        )
    })
}

fn use_direct_for_hashing(filename: &str, io_mode: IOMode) -> bool {
    let file_cached = match is_first_page_resident(filename) {
        Ok(true) => io_mode != IOMode::Direct,
        _ => io_mode == IOMode::PageCache,
    };
    (!file_cached) || io_mode == IOMode::Direct
}

fn should_use_direct_io(use_direct: bool, offset: u64, len: usize, file_size: u64) -> bool {
    if use_direct {
        debug_assert_eq!(
            len % 4096,
            0,
            "Direct I/O requires a 4096-byte aligned read length"
        );
    }
    use_direct && (offset % 4096 == 0) && (len % 4096 == 0) && (offset + len as u64 <= file_size)
}

fn submit_read(
    io_uring: &mut IoUring,
    file: &File,
    file_direct: &File,
    buffer: &mut AlignedBuffer,
    offset: u64,
    block_id: u64,
    use_direct: bool,
    file_size: u64,
) -> std::io::Result<()> {
    let direct = should_use_direct_io(use_direct, offset, buffer.as_slice().len(), file_size);
    unsafe {
        let mut sqe = io_uring
            .prepare_sqe()
            .ok_or_else(|| std::io::Error::other("io_uring submission queue is full"))?;
        if direct {
            sqe.prep_read(file_direct.as_raw_fd(), buffer.as_mut_slice(), offset);
        } else {
            sqe.prep_read(file.as_raw_fd(), buffer.as_mut_slice(), offset);
        }
        sqe.set_user_data(block_id);
    }
    Ok(())
}

fn wait_for_ready(io_uring: &mut IoUring) -> std::io::Result<Vec<(u64, u32)>> {
    let cq = io_uring.wait_for_cqe().map_err(std::io::Error::other)?;
    let mut ready = vec![(cq.user_data(), cq.result()?)];

    while io_uring.cq_ready() > 0 {
        let cq = io_uring.peek_for_cqe().ok_or_else(|| {
            std::io::Error::other("completion queue reported ready but no CQE was available")
        })?;
        ready.push((cq.user_data(), cq.result()?));
    }

    Ok(ready)
}

fn thread_hash_reader(
    thread_id: u64,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    hash_type: BlockHashAlgorithm,
    use_direct: bool,
) -> std::io::Result<Vec<(usize, BlockDigest)>> {
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }

    let file_size = file.seek(SeekFrom::End(0))?;
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;
    let mut digests = Vec::new();

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
        if current_offset >= file_size {
            break;
        }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        )?;
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 {
        return Ok(digests);
    }
    io_uring.submit_sqes().map_err(std::io::Error::other)?;

    loop {
        let ready = wait_for_ready(io_uring)?;

        for (block_id, result) in ready {
            if result > 0 {
                let current_offset = block_offset(offset, block_id, num_threads, block_size)?;
                let hash_index = (current_offset / block_size) as usize;
                let buf = &buffers[block_id as usize % qd].as_slice()[..result as usize];
                digests.push((hash_index, hash_type.hash_block(buf)));
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size)?;
            if next_offset < file_size {
                submit_read(
                    io_uring,
                    file,
                    file_direct,
                    &mut buffers[block_num % qd],
                    next_offset,
                    block_num as u64,
                    use_direct,
                    file_size,
                )?;
                block_num += 1;
                inflight += 1;
            }
        }

        io_uring.submit_sqes().map_err(std::io::Error::other)?;
        if inflight == 0 {
            return Ok(digests);
        }
    }
}

fn hash_file_blocks_inner(
    filename: &str,
    hash_type: BlockHashAlgorithm,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<BlockHashManifest> {
    let read_count = Arc::new(AtomicU64::new(0));
    let use_direct = use_direct_for_hashing(filename, io_mode);

    let num_threads = if use_direct {
        num_threads_d
    } else {
        num_threads_p
    };
    let block_size = if use_direct {
        block_size_d
    } else {
        block_size_p
    };
    let qd = if use_direct { qd_d } else { qd_p };
    validate_block_size(block_size)?;

    let file_size = std::fs::metadata(filename)?.len();
    let block_count = block_count_for_size(file_size, block_size)?;
    let mut block_hashes = vec![BlockDigest::from_prefix(&[]); block_count];

    let mut threads = vec![];
    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let filename = filename.to_string();
        threads.push(std::thread::spawn(
            move || -> std::io::Result<Vec<(usize, BlockDigest)>> {
                let (mut file, mut file_direct) = open_reader_files(&filename, use_direct)?;
                let mut io_uring = IoUring::new(1024).map_err(std::io::Error::other)?;
                thread_hash_reader(
                    thread_id,
                    num_threads,
                    block_size,
                    qd,
                    &mut file,
                    &mut file_direct,
                    &mut io_uring,
                    read_count,
                    hash_type,
                    use_direct,
                )
            },
        ));
    }

    for thread in threads {
        for (index, digest) in thread
            .join()
            .map_err(|_| std::io::Error::other("hash worker thread panicked"))??
        {
            block_hashes[index] = digest;
        }
    }
    let hash_of_hashes = hash_hashes(hash_type, &block_hashes);

    Ok(BlockHashManifest {
        hash_type,
        file_size,
        block_size,
        bytes_hashed: read_count.load(Ordering::SeqCst),
        block_hashes,
        hash_of_hashes,
    })
}

pub fn hash_file_blocks(
    filename: &str,
    hash_type: BlockHashAlgorithm,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<BlockHashManifest> {
    hash_file_blocks_inner(
        filename,
        hash_type,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )
}

pub fn hash_file_to_replicas(
    filename: &str,
    hash_base: Option<&str>,
    hash_type: BlockHashAlgorithm,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<BlockHashManifest> {
    let manifest = hash_file_blocks(
        filename,
        hash_type,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;
    let base = hash_base
        .map(str::to_string)
        .unwrap_or_else(|| default_hash_base(filename));
    save_manifest_replicas(&base, &manifest)?;
    Ok(manifest)
}

pub fn verify_file_with_replicas(
    filename: &str,
    hash_base: Option<&str>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<VerifyReport> {
    let base = hash_base_for_file(0, filename, hash_base);
    let manifests = load_manifest_replicas(&base);
    let manifest_geometry =
        manifest_geometry(&manifests, &format!("hash replicas for {}", filename))?;
    let current = hash_file_blocks_inner(
        filename,
        manifest_geometry
            .map(|g| g.hash_type)
            .unwrap_or(BlockHashAlgorithm::Xxh3),
        num_threads_p,
        manifest_geometry
            .map(|g| g.block_size)
            .unwrap_or(block_size_p),
        qd_p,
        num_threads_d,
        manifest_geometry
            .map(|g| g.block_size)
            .unwrap_or(block_size_d),
        qd_d,
        io_mode,
    )?;
    Ok(verify_report_from_current(&current, &manifests))
}

fn read_block(
    path: &str,
    block_index: usize,
    file_size: u64,
    block_size: u64,
) -> std::io::Result<Vec<u8>> {
    let offset = block_index as u64 * block_size;
    let len = std::cmp::min(block_size, file_size.saturating_sub(offset)) as usize;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_block(
    path: &str,
    block_index: usize,
    block_size: u64,
    data: &[u8],
) -> std::io::Result<()> {
    let offset = block_index as u64 * block_size;
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}

fn load_manifest_sets(
    files: &[String],
    hash_base: Option<&str>,
) -> Vec<Vec<Option<BlockHashManifest>>> {
    files
        .iter()
        .enumerate()
        .map(|(index, path)| load_manifest_replicas(&hash_base_for_file(index, path, hash_base)))
        .collect()
}

fn ensure_matching_file_geometry(
    files: &[String],
    manifests: &[BlockHashManifest],
) -> std::io::Result<()> {
    let Some(first) = manifests.first() else {
        return Ok(());
    };

    for (path, manifest) in files.iter().zip(manifests.iter()).skip(1) {
        if manifest.file_size != first.file_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical file sizes; {} has {} bytes but target has {} bytes",
                    path, manifest.file_size, first.file_size
                ),
            ));
        }
        if manifest.block_size != first.block_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical block sizes; {} uses {} bytes but target uses {} bytes",
                    path, manifest.block_size, first.block_size
                ),
            ));
        }
        if manifest.block_hashes.len() != first.block_hashes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical block layouts; {} has {} hash blocks but target has {}",
                    path,
                    manifest.block_hashes.len(),
                    first.block_hashes.len()
                ),
            ));
        }
        if manifest.hash_type != first.hash_type {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical hash types; {} uses {:?} but target uses {:?}",
                    path, manifest.hash_type, first.hash_type
                ),
            ));
        }
    }

    Ok(())
}

fn common_manifest_geometry_for_recovery(
    files: &[String],
    stored_replicas: &[Vec<Option<BlockHashManifest>>],
) -> std::io::Result<Option<ManifestGeometry>> {
    let mut expected: Option<ManifestGeometry> = None;

    for (path, replicas) in files.iter().zip(stored_replicas.iter()) {
        let Some(current) = manifest_geometry(replicas, &format!("hash replicas for {}", path))?
        else {
            continue;
        };
        if let Some(previous) = expected {
            if previous.file_size != current.file_size
                || previous.block_size != current.block_size
                || previous.block_count != current.block_count
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "recover expects all sidecars to agree on geometry; {} uses file_size={}, block_size={}, blocks={} but another sidecar set uses file_size={}, block_size={}, blocks={}",
                        path,
                        current.file_size,
                        current.block_size,
                        current.block_count,
                        previous.file_size,
                        previous.block_size,
                        previous.block_count
                    ),
                ));
            }
        } else {
            expected = Some(current);
        }
    }

    Ok(expected)
}

fn repair_file_set(
    files: &[String],
    manifests_now: &mut [BlockHashManifest],
    stored_replicas: &[Vec<Option<BlockHashManifest>>],
    repair_targets: &[usize],
    hash_base: Option<&str>,
    refresh_all_sidecars: bool,
) -> std::io::Result<(usize, usize, usize, Vec<BlockRecoverIssue>)> {
    let stored_refs = stored_replicas
        .iter()
        .flat_map(|replicas| replicas.iter())
        .map(|m| m.as_ref())
        .collect::<Vec<_>>();
    let mut repaired_blocks = 0;
    let mut repaired_file_flags = vec![false; files.len()];
    let mut failed_blocks = Vec::new();

    for block_index in 0..manifests_now[0].block_hashes.len() {
        for &target_index in repair_targets {
            let file_copy_hashes = manifests_now
                .iter()
                .map(|manifest| manifest.block_hashes.get(block_index).copied())
                .collect::<Vec<_>>();
            let decision = recover_block_hash(block_index, &file_copy_hashes, &stored_refs);
            let target_hash = file_copy_hashes[target_index].unwrap();

            if decision.elected_hash == Some(target_hash) {
                continue;
            }

            if let Some(source_index) = decision.repair_source_index {
                if source_index != target_index {
                    let block = read_block(
                        &files[source_index],
                        block_index,
                        manifests_now[source_index].file_size,
                        manifests_now[source_index].block_size,
                    )?;
                    write_block(
                        &files[target_index],
                        block_index,
                        manifests_now[target_index].block_size,
                        &block,
                    )?;
                    manifests_now[target_index].block_hashes[block_index] =
                        decision.elected_hash.unwrap();
                    repaired_blocks += 1;
                    repaired_file_flags[target_index] = true;
                    continue;
                }
            }

            failed_blocks.push(BlockRecoverIssue {
                file_index: target_index,
                file_path: files[target_index].clone(),
                block_index,
                current_hash: target_hash,
                decision,
            });
        }
    }

    let mut sidecars_refreshed = 0;
    for (index, manifest) in manifests_now.iter_mut().enumerate() {
        let should_consider_file = refresh_all_sidecars || index == 0;
        if !should_consider_file {
            continue;
        }

        let file_has_failures = failed_blocks.iter().any(|issue| issue.file_index == index);
        if file_has_failures {
            continue;
        }

        manifest.hash_of_hashes = hash_hashes(manifest.hash_type, &manifest.block_hashes);
        if !sidecars_are_fully_healthy_for_current(manifest, &stored_replicas[index]) {
            let base = hash_base_for_file(index, &files[index], hash_base);
            save_manifest_replicas(&base, manifest)?;
            sidecars_refreshed += 1;
        }
    }

    let repaired_files = repaired_file_flags.into_iter().filter(|flag| *flag).count();
    Ok((
        repaired_blocks,
        repaired_files,
        sidecars_refreshed,
        failed_blocks,
    ))
}

pub fn recover_file_with_copies(
    target: &str,
    copies: &[String],
    hash_base: Option<&str>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    recover_mode: RecoverMode,
) -> std::io::Result<RecoverReport> {
    let mut files = Vec::with_capacity(1 + copies.len());
    files.push(target.to_string());
    files.extend(copies.iter().cloned());

    let stored_replicas = load_manifest_sets(&files, hash_base);
    let stored_geometry = common_manifest_geometry_for_recovery(&files, &stored_replicas)?;
    let mut used_fast_path = false;
    let mut fell_back_to_full_scan = false;

    let mut manifests_now = if recover_mode == RecoverMode::Fast {
        let mut current = hash_file_blocks_inner(
            target,
            stored_geometry
                .map(|g| g.hash_type)
                .unwrap_or(BlockHashAlgorithm::Xxh3),
            num_threads_p,
            stored_geometry
                .map(|g| g.block_size)
                .unwrap_or(block_size_p),
            qd_p,
            num_threads_d,
            stored_geometry
                .map(|g| g.block_size)
                .unwrap_or(block_size_d),
            qd_d,
            io_mode,
        )?;
        let verify_report = verify_report_from_current(&current, &stored_replicas[0]);
        if consistent_intact_manifest(&stored_replicas[0]).is_some()
            && verify_report.bad_blocks.is_empty()
        {
            used_fast_path = true;
            let mut sidecars_refreshed = 0;
            current.hash_of_hashes = hash_hashes(current.hash_type, &current.block_hashes);
            if !sidecars_are_fully_healthy_for_current(&current, &stored_replicas[0]) {
                let base = hash_base_for_file(0, target, hash_base);
                save_manifest_replicas(&base, &current)?;
                sidecars_refreshed = 1;
            }
            return Ok(RecoverReport {
                bytes_hashed: current.bytes_hashed,
                repaired_blocks: 0,
                repaired_files: 0,
                sidecars_refreshed,
                used_fast_path,
                fell_back_to_full_scan,
                failed_blocks: Vec::new(),
            });
        }

        fell_back_to_full_scan = true;
        let mut manifests = Vec::with_capacity(files.len());
        manifests.push(current);
        for path in files.iter().skip(1) {
            manifests.push(hash_file_blocks_inner(
                path,
                stored_geometry
                    .map(|g| g.hash_type)
                    .unwrap_or(BlockHashAlgorithm::Xxh3),
                num_threads_p,
                stored_geometry
                    .map(|g| g.block_size)
                    .unwrap_or(block_size_p),
                qd_p,
                num_threads_d,
                stored_geometry
                    .map(|g| g.block_size)
                    .unwrap_or(block_size_d),
                qd_d,
                io_mode,
            )?);
        }
        manifests
    } else {
        files
            .iter()
            .map(|path| {
                hash_file_blocks_inner(
                    path,
                    stored_geometry
                        .map(|g| g.hash_type)
                        .unwrap_or(BlockHashAlgorithm::Xxh3),
                    num_threads_p,
                    stored_geometry
                        .map(|g| g.block_size)
                        .unwrap_or(block_size_p),
                    qd_p,
                    num_threads_d,
                    stored_geometry
                        .map(|g| g.block_size)
                        .unwrap_or(block_size_d),
                    qd_d,
                    io_mode,
                )
            })
            .collect::<std::io::Result<Vec<_>>>()?
    };

    ensure_matching_file_geometry(&files, &manifests_now)?;
    let bytes_hashed = manifests_now
        .iter()
        .map(|manifest| manifest.bytes_hashed)
        .sum();
    let repair_targets = if recover_mode == RecoverMode::InPlaceAll {
        (0..files.len()).collect::<Vec<_>>()
    } else {
        vec![0]
    };
    let refresh_all_sidecars = recover_mode == RecoverMode::InPlaceAll;

    let (repaired_blocks, repaired_files, sidecars_refreshed, failed_blocks) = repair_file_set(
        &files,
        &mut manifests_now,
        &stored_replicas,
        &repair_targets,
        hash_base,
        refresh_all_sidecars,
    )?;

    Ok(RecoverReport {
        bytes_hashed,
        repaired_blocks,
        repaired_files,
        sidecars_refreshed,
        used_fast_path,
        fell_back_to_full_scan,
        failed_blocks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn unique_temp_file(prefix: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{}-{}-{}.bin", prefix, pid, nanos))
    }

    fn fixture_bytes(size: usize) -> Vec<u8> {
        (0..size).map(|i| ((i * 131) % 251) as u8).collect()
    }

    fn digest_u64(value: u64) -> BlockDigest {
        BlockDigest::from_prefix(&value.to_le_bytes())
    }

    fn digest_sha256(byte: u8) -> BlockDigest {
        BlockHashAlgorithm::Sha256.hash_block(&[byte; 17])
    }

    fn expected_hashes(data: &[u8], block_size: usize) -> Vec<BlockDigest> {
        data.chunks(block_size)
            .map(|chunk| BlockHashAlgorithm::Xxh3.hash_block(chunk))
            .collect()
    }

    #[test]
    fn hash_file_blocks_matches_sequential_hashes() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes((block_size * 3) + 12345);
        let path = unique_temp_file("fro-block-hash");
        fs::write(&path, &data).unwrap();

        let manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            3,
            BLOCK_HASH_SIZE,
            2,
            3,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(manifest.file_size, data.len() as u64);
        assert_eq!(manifest.block_size, block_size as u64);
        assert_eq!(manifest.bytes_hashed, data.len() as u64);
        assert_eq!(manifest.block_hashes, expected_hashes(&data, block_size));
        assert!(manifest.verify_integrity());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn verify_integrity_rejects_corrupted_block_hash() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes((block_size * 2) + 17);
        let path = unique_temp_file("fro-block-hash-corrupt");
        fs::write(&path, &data).unwrap();

        let mut manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            2,
            BLOCK_HASH_SIZE,
            2,
            2,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();
        manifest.block_hashes[1] = digest_u64(1);

        assert!(!manifest.verify_integrity());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn verify_integrity_accepts_single_bad_hash_of_hash_copy() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes(block_size + 99);
        let path = unique_temp_file("fro-block-hash-majority");
        fs::write(&path, &data).unwrap();

        let mut manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            2,
            BLOCK_HASH_SIZE,
            2,
            2,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert!(manifest.verify_integrity());

        manifest.hash_of_hashes = digest_u64(1);
        assert!(!manifest.verify_integrity());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn sha256_manifest_uses_variable_digest_size() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes(block_size + 31);
        let path = unique_temp_file("fro-block-hash-sha256");
        fs::write(&path, &data).unwrap();

        let manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Sha256,
            2,
            BLOCK_HASH_SIZE,
            2,
            2,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(manifest.hash_type, BlockHashAlgorithm::Sha256);
        assert!(manifest
            .block_hashes
            .iter()
            .all(|digest| digest.len as usize == BlockHashAlgorithm::Sha256.digest_len()));
        assert_eq!(
            manifest.hash_of_hashes.len as usize,
            BlockHashAlgorithm::Sha256.digest_len()
        );
        assert!(manifest.verify_integrity());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn verify_integrity_rejects_wrong_digest_lengths_and_partial_hashes() {
        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Sha256,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE - 1,
            block_hashes: vec![digest_u64(99)],
            hash_of_hashes: digest_sha256(7),
        };
        assert!(!manifest.verify_integrity());

        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Sha256,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_sha256(9)],
            hash_of_hashes: digest_u64(7),
        };
        assert!(!manifest.verify_integrity());
    }

    #[test]
    fn recover_block_hash_prefers_intact_hash_manifest() {
        let manifest_a = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(222)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(222)],
            ),
        };
        let manifest_b = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(999)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(999)],
            ),
        };

        let decision = recover_block_hash(
            0,
            &[Some(digest_u64(111)), Some(digest_u64(333))],
            &[Some(&manifest_a), Some(&manifest_b), None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(111)));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(decision.basis, Some(BlockRecoveryBasis::IntactHash));
        assert_eq!(decision.failure, None);
        assert_eq!(
            decision.status_message(),
            "recovered block based on intact hash"
        );
        assert_eq!(decision.votes[0].hash, digest_u64(111));
    }

    #[test]
    fn recover_block_hash_reports_manifest_only_agreement() {
        let manifest_a = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(111)],
            hash_of_hashes: digest_u64(0),
        };
        let manifest_b = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(111)],
            hash_of_hashes: digest_u64(0),
        };

        let decision = recover_block_hash(
            0,
            &[Some(digest_u64(222)), Some(digest_u64(333))],
            &[Some(&manifest_a), Some(&manifest_b), None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(111)));
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(decision.basis, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::ManifestOnlyAgreement)
        );
        assert_eq!(
            decision.status_message(),
            "failed to recover corrupt block [manifest+manifest hashes agree]"
        );
    }

    #[test]
    fn recover_block_hash_can_use_file_and_manifest_agreement() {
        let good_manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(555)],
            hash_of_hashes: digest_u64(0),
        };
        let bad_manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(777)],
            hash_of_hashes: digest_u64(0),
        };

        let decision = recover_block_hash(
            0,
            &[Some(digest_u64(555)), Some(digest_u64(999))],
            &[Some(&good_manifest), Some(&bad_manifest), None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(555)));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(
            decision.basis,
            Some(BlockRecoveryBasis::FileAndManifestAgreement)
        );
        assert_eq!(decision.failure, None);
        assert_eq!(
            decision.status_message(),
            "recovered block based on file+manifest agreement"
        );
    }

    #[test]
    fn recover_block_hash_can_use_file_and_file_agreement() {
        let decision = recover_block_hash(
            0,
            &[
                Some(digest_u64(444)),
                Some(digest_u64(444)),
                Some(digest_u64(999)),
            ],
            &[None, None, None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(444)));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(
            decision.basis,
            Some(BlockRecoveryBasis::FileAndFileAgreement)
        );
        assert_eq!(decision.failure, None);
        assert_eq!(
            decision.status_message(),
            "recovered block based on file+file agreement"
        );
    }

    #[test]
    fn recover_block_hash_rejects_intact_manifest_that_disagrees_with_file_majority() {
        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(999)],
            hash_of_hashes: hash_hashes(BlockHashAlgorithm::Xxh3, &[digest_u64(999)]),
        };

        let decision = recover_block_hash(
            0,
            &[
                Some(digest_u64(111)),
                Some(digest_u64(111)),
                Some(digest_u64(222)),
            ],
            &[Some(&manifest), None, None],
        );

        assert_eq!(decision.elected_hash, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::ConflictingIntactHashes)
        );
    }

    #[test]
    fn recover_block_hash_rejects_intact_manifest_when_one_file_matches_but_majority_disagrees() {
        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(111)],
            hash_of_hashes: hash_hashes(BlockHashAlgorithm::Xxh3, &[digest_u64(111)]),
        };

        let decision = recover_block_hash(
            0,
            &[
                Some(digest_u64(111)),
                Some(digest_u64(999)),
                Some(digest_u64(999)),
            ],
            &[Some(&manifest), None, None],
        );

        assert_eq!(decision.elected_hash, None);
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::ConflictingIntactHashes)
        );
    }

    #[test]
    fn recover_block_hash_reports_no_hash_found() {
        let decision = recover_block_hash(0, &[None, None], &[None, None, None]);

        assert_eq!(decision.elected_hash, None);
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(decision.basis, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::NoBlockHashFound)
        );
        assert_eq!(
            decision.status_message(),
            "failed to recover corrupt block [no block hash found either]"
        );
    }

    #[test]
    fn consistent_intact_manifest_requires_full_manifest_agreement() {
        let manifest_a = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(222)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(222)],
            ),
        };
        let manifest_b = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(999)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(999)],
            ),
        };

        assert!(consistent_intact_manifest(&[Some(manifest_a), Some(manifest_b)]).is_none());
    }

    #[test]
    fn document_block_recovery_decision_table() {
        struct Case {
            scenario: &'static str,
            file_copy_hashes: Vec<Option<BlockDigest>>,
            manifests: Vec<Option<BlockHashManifest>>,
            expected_elected_hash: Option<BlockDigest>,
            expected_repair_source_index: Option<usize>,
            expected_basis: Option<BlockRecoveryBasis>,
            expected_failure: Option<BlockRecoveryFailure>,
            expected_status: &'static str,
        }

        fn intact_manifest(block_hash: u64) -> BlockHashManifest {
            BlockHashManifest {
                hash_type: BlockHashAlgorithm::Xxh3,
                file_size: BLOCK_HASH_SIZE,
                block_size: BLOCK_HASH_SIZE,
                bytes_hashed: BLOCK_HASH_SIZE,
                block_hashes: vec![digest_u64(block_hash)],
                hash_of_hashes: hash_hashes(BlockHashAlgorithm::Xxh3, &[digest_u64(block_hash)]),
            }
        }

        fn corrupted_manifest(block_hash: u64) -> BlockHashManifest {
            BlockHashManifest {
                hash_type: BlockHashAlgorithm::Xxh3,
                file_size: BLOCK_HASH_SIZE,
                block_size: BLOCK_HASH_SIZE,
                bytes_hashed: BLOCK_HASH_SIZE,
                block_hashes: vec![digest_u64(block_hash)],
                hash_of_hashes: digest_u64(0),
            }
        }

        let cases = vec![
            Case {
                scenario: "target block matches an intact manifest",
                file_copy_hashes: vec![Some(digest_u64(111)), Some(digest_u64(333))],
                manifests: vec![
                    Some(intact_manifest(111)),
                    Some(corrupted_manifest(999)),
                    None,
                ],
                expected_elected_hash: Some(digest_u64(111)),
                expected_repair_source_index: Some(0),
                expected_basis: Some(BlockRecoveryBasis::IntactHash),
                expected_failure: None,
                expected_status: "recovered block based on intact hash",
            },
            Case {
                scenario: "intact manifests agree, but no available file copy matches them",
                file_copy_hashes: vec![Some(digest_u64(222)), Some(digest_u64(333))],
                manifests: vec![Some(intact_manifest(111)), Some(intact_manifest(111)), None],
                expected_elected_hash: Some(digest_u64(111)),
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::IntactHashWithoutMatchingBlock),
                expected_status:
                    "failed to recover corrupt block [intact hash found but no matching file block]",
            },
            Case {
                scenario: "intact manifests disagree with each other",
                file_copy_hashes: vec![Some(digest_u64(111)), Some(digest_u64(222))],
                manifests: vec![Some(intact_manifest(111)), Some(intact_manifest(222)), None],
                expected_elected_hash: None,
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::ConflictingIntactHashes),
                expected_status: "failed to recover corrupt block [conflicting intact hashes]",
            },
            Case {
                scenario: "two file copies agree and no intact manifest overrides them",
                file_copy_hashes: vec![
                    Some(digest_u64(444)),
                    Some(digest_u64(444)),
                    Some(digest_u64(999)),
                ],
                manifests: vec![None, None, None],
                expected_elected_hash: Some(digest_u64(444)),
                expected_repair_source_index: Some(0),
                expected_basis: Some(BlockRecoveryBasis::FileAndFileAgreement),
                expected_failure: None,
                expected_status: "recovered block based on file+file agreement",
            },
            Case {
                scenario: "one file copy agrees with a manifest witness",
                file_copy_hashes: vec![Some(digest_u64(555)), Some(digest_u64(999))],
                manifests: vec![
                    Some(corrupted_manifest(555)),
                    Some(corrupted_manifest(777)),
                    None,
                ],
                expected_elected_hash: Some(digest_u64(555)),
                expected_repair_source_index: Some(0),
                expected_basis: Some(BlockRecoveryBasis::FileAndManifestAgreement),
                expected_failure: None,
                expected_status: "recovered block based on file+manifest agreement",
            },
            Case {
                scenario: "two manifest witnesses agree, but no file copy matches them",
                file_copy_hashes: vec![Some(digest_u64(222)), Some(digest_u64(333))],
                manifests: vec![
                    Some(corrupted_manifest(111)),
                    Some(corrupted_manifest(111)),
                    None,
                ],
                expected_elected_hash: Some(digest_u64(111)),
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::ManifestOnlyAgreement),
                expected_status: "failed to recover corrupt block [manifest+manifest hashes agree]",
            },
            Case {
                scenario: "all witnesses are missing",
                file_copy_hashes: vec![None, None],
                manifests: vec![None, None, None],
                expected_elected_hash: None,
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::NoBlockHashFound),
                expected_status: "failed to recover corrupt block [no block hash found either]",
            },
        ];

        println!("| Scenario | Elected hash | Repair source | Basis | Failure | Status message |");
        println!("| --- | --- | --- | --- | --- | --- |");

        for case in cases {
            let manifest_refs = case
                .manifests
                .iter()
                .map(|m| m.as_ref())
                .collect::<Vec<_>>();
            let decision = recover_block_hash(0, &case.file_copy_hashes, &manifest_refs);

            assert_eq!(decision.elected_hash, case.expected_elected_hash);
            assert_eq!(
                decision.repair_source_index,
                case.expected_repair_source_index
            );
            assert_eq!(decision.basis, case.expected_basis);
            assert_eq!(decision.failure, case.expected_failure);
            assert_eq!(decision.status_message(), case.expected_status);

            let basis = decision
                .basis
                .as_ref()
                .map(|basis| format!("{basis:?}"))
                .unwrap_or_else(|| "-".to_string());
            let failure = decision
                .failure
                .as_ref()
                .map(|failure| format!("{failure:?}"))
                .unwrap_or_else(|| "-".to_string());
            let elected_hash = decision
                .elected_hash
                .map(|hash| hash.to_string())
                .unwrap_or_else(|| "-".to_string());
            let repair_source_index = decision
                .repair_source_index
                .map(|index| index.to_string())
                .unwrap_or_else(|| "-".to_string());

            println!(
                "| {} | {} | {} | {} | {} | {} |",
                case.scenario,
                elected_hash,
                repair_source_index,
                basis,
                failure,
                decision.status_message()
            );
        }
    }

    #[test]
    fn block_offset_rejects_overflow() {
        let err = block_offset(u64::MAX - 3, 4, 8, 1024).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
