use libc::{c_uchar, size_t};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use xxhash_rust::xxh3::xxh3_64;

use crate::common::IOMode;
use crate::config::{IOParams, LoadedConfig};
use crate::io_util::sync_parent_directory;
use crate::reader::{resolve_reader_params, ResolvedReadParams};
use crate::stream::ParallelFile;

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
    #[allow(dead_code)]
    pub fn hash_of_hashes_bytes(&self) -> &[u8] {
        &self.hash_of_hashes.bytes[..self.hash_type.digest_len()]
    }

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

fn write_manifest_replicas(
    base: &str,
    manifest: &BlockHashManifest,
    sync: bool,
) -> std::io::Result<()> {
    let data = serde_json::to_vec_pretty(manifest).map_err(json_error_to_io)?;
    let paths = hash_replica_paths(base);
    for path in &paths {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        file.write_all(&data)?;
        if sync {
            file.sync_all()?;
        }
    }
    if sync {
        for path in &paths {
            sync_parent_directory(path)?;
        }
    }
    Ok(())
}

pub fn save_manifest_replicas(base: &str, manifest: &BlockHashManifest) -> std::io::Result<()> {
    write_manifest_replicas(base, manifest, false)
}

pub fn save_manifest_replicas_durable(
    base: &str,
    manifest: &BlockHashManifest,
) -> std::io::Result<()> {
    write_manifest_replicas(base, manifest, true)
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


mod ops;
#[cfg(test)]
mod tests;

pub use ops::*;
