use iou::IoUring;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use memchr::memmem::Finder;
use xxhash_rust::xxh3::xxh3_64;
use crate::common::{AlignedBuffer, IOMode};
use crate::mincore::{is_first_page_resident};

pub const BLOCK_HASH_SIZE: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockHashManifest {
    pub file_size: u64,
    pub block_size: u64,
    pub bytes_hashed: u64,
    pub block_hashes: Vec<u64>,
    pub hash_of_hashes: u64,
}

impl BlockHashManifest {
    pub fn verify_integrity(&self) -> bool {
        self.hash_of_hashes == hash_hashes(&self.block_hashes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockHashWitnessKind {
    FileCopy,
    HashReplica,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHashVote {
    pub hash: u64,
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
            BlockRecoveryFailure::IntactHashWithoutMatchingBlock => "intact hash found but no matching file block",
            BlockRecoveryFailure::ConflictingIntactHashes => "conflicting intact hashes",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRecoveryDecision {
    pub block_index: usize,
    pub elected_hash: Option<u64>,
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
    pub current_hash: u64,
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
    pub block_index: usize,
    pub current_hash: u64,
    pub decision: BlockRecoveryDecision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverReport {
    pub bytes_hashed: u64,
    pub repaired_blocks: usize,
    pub failed_blocks: Vec<BlockRecoverIssue>,
}

fn hash_hashes(values: &[u64]) -> u64 {
    let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<u64>());
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    xxh3_64(&bytes)
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

fn tally_hash_votes(values: &[(u64, BlockHashWitnessKind)]) -> Vec<BlockHashVote> {
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

fn first_matching_file_copy(file_copy_hashes: &[Option<u64>], expected_hash: u64) -> Option<usize> {
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
    if next.map(|other| other.total_votes < top.total_votes).unwrap_or(true) {
        Some(top)
    } else {
        None
    }
}

pub fn recover_block_hash(
    block_index: usize,
    file_copy_hashes: &[Option<u64>],
    hash_manifests: &[Option<&BlockHashManifest>],
) -> BlockRecoveryDecision {
    let mut witnesses = Vec::<(u64, BlockHashWitnessKind)>::new();
    let mut intact_manifest_hashes = Vec::<u64>::new();
    let mut all_manifest_hashes = Vec::<u64>::new();

    for hash in file_copy_hashes.iter().flatten() {
        witnesses.push((*hash, BlockHashWitnessKind::FileCopy));
    }

    for manifest in hash_manifests.iter().flatten() {
        if let Some(hash) = manifest.block_hashes.get(block_index) {
            all_manifest_hashes.push(*hash);
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

    if let Some(top) = unique_top_vote(&votes, |vote| vote.file_copy_votes >= 1 && vote.hash_replica_votes >= 1) {
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

    let failure = if all_manifest_hashes.is_empty() && file_copy_hashes.iter().all(|hash| hash.is_none()) {
        BlockRecoveryFailure::NoBlockHashFound
    } else {
        BlockRecoveryFailure::NoBlockHashFound
    };

    BlockRecoveryDecision {
        block_index,
        elected_hash: None,
        repair_source_index: None,
        basis: None,
        failure: Some(failure),
        votes,
    }
}

fn block_offset(thread_base: u64, block_id: u64, num_threads: u64, block_size: u64) -> u64 {
    thread_base + block_id * num_threads * block_size
}

fn should_use_direct_io(use_direct: bool, offset: u64, len: usize, file_size: u64) -> bool {
    debug_assert_eq!(len % 4096, 0, "Direct I/O requires a 4096-byte aligned read length");
    use_direct
        && (offset % 4096 == 0)
        && (len % 4096 == 0)
        && (offset + len as u64 <= file_size)
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
) {
    let direct = should_use_direct_io(use_direct, offset, buffer.as_slice().len(), file_size);
    unsafe {
        let mut sqe = io_uring.prepare_sqe().unwrap();
        if direct {
            sqe.prep_read(file_direct.as_raw_fd(), buffer.as_mut_slice(), offset);
        } else {
            sqe.prep_read(file.as_raw_fd(), buffer.as_mut_slice(), offset);
        }
        sqe.set_user_data(block_id);
    }
}

fn open_reader_files(filename: &str, use_direct: bool) -> (File, File) {
    let file = File::open(filename).unwrap();
    let file_direct = if use_direct {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(filename)
            .unwrap()
    } else {
        File::open(filename).unwrap()
    };
    (file, file_direct)
}

// The thread reader function.
fn thread_reader(
    thread_id: u64,
    pattern: String,
    num_threads: u64,
    block_size: u64,
    qd: usize,
    file: &mut File,
    file_direct: &mut File,
    io_uring: &mut IoUring,
    read_count: Arc<AtomicU64>,
    use_direct: bool,
) -> Vec<u64> {
    let mut matches = Vec::new();
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }
    let file_size = file.seek(SeekFrom::End(0)).unwrap();
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size);
        if current_offset >= file_size { break; }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        );
        block_num += 1;
        inflight += 1;
    }
    
    if inflight == 0 { return matches; }
    io_uring.submit_sqes().unwrap();

    let finder = Finder::new(pattern.as_bytes());

    loop {
        let cq = io_uring.wait_for_cqe().unwrap();
        let mut ready = vec![(cq.user_data() as u64, cq.result().unwrap())];

        while io_uring.cq_ready() > 0 {
            let cq = io_uring.peek_for_cqe().unwrap();
            ready.push((cq.user_data() as u64, cq.result().unwrap()));
        }

        for (block_id, result) in ready {
            if result > 0 {
                read_count.fetch_add(result as u64, Ordering::Relaxed);
                if !pattern.is_empty() {
                    let buf = &buffers[block_id as usize % qd].as_slice()[..std::cmp::min(result as usize, (block_size as usize)+pattern.len())];
                    for idx in finder.find_iter(buf) {
                        matches.push(block_offset(offset, block_id, num_threads, block_size) + idx as u64);
                    }
                }
            }
            inflight -= 1;
            
            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size);
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
                );
                block_num += 1;
                inflight += 1;
            }
        }
        io_uring.submit_sqes().unwrap();
        if inflight == 0 { return matches; }
    }
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
    block_hashes: Arc<Vec<AtomicU64>>,
    use_direct: bool,
) {
    let mut buffers = Vec::new();
    for _ in 0..qd {
        buffers.push(AlignedBuffer::new(block_size as usize));
    }

    let file_size = file.seek(SeekFrom::End(0)).unwrap();
    let offset = thread_id * block_size;
    let mut block_num = 0;
    let mut inflight = 0;

    for _ in 0..qd {
        let current_offset = block_offset(offset, block_num as u64, num_threads, block_size);
        if current_offset >= file_size { break; }
        submit_read(
            io_uring,
            file,
            file_direct,
            &mut buffers[block_num % qd],
            current_offset,
            block_num as u64,
            use_direct,
            file_size,
        );
        block_num += 1;
        inflight += 1;
    }

    if inflight == 0 { return; }
    io_uring.submit_sqes().unwrap();

    loop {
        let cq = io_uring.wait_for_cqe().unwrap();
        let mut ready = vec![(cq.user_data() as u64, cq.result().unwrap())];

        while io_uring.cq_ready() > 0 {
            let cq = io_uring.peek_for_cqe().unwrap();
            ready.push((cq.user_data() as u64, cq.result().unwrap()));
        }

        for (block_id, result) in ready {
            if result > 0 {
                let current_offset = block_offset(offset, block_id, num_threads, block_size);
                let hash_index = (current_offset / block_size) as usize;
                let buf = &buffers[block_id as usize % qd].as_slice()[..result as usize];
                block_hashes[hash_index].store(xxh3_64(buf), Ordering::Relaxed);
                read_count.fetch_add(result as u64, Ordering::Relaxed);
            }
            inflight -= 1;

            let next_offset = block_offset(offset, block_num as u64, num_threads, block_size);
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
                );
                block_num += 1;
                inflight += 1;
            }
        }

        io_uring.submit_sqes().unwrap();
        if inflight == 0 { return; }
    }
}

pub fn hash_file_blocks(
    filename: &str,
    num_threads_p: u64, qd_p: usize,
    num_threads_d: u64, qd_d: usize,
    io_mode: IOMode,
) -> BlockHashManifest {
    let read_count = Arc::new(AtomicU64::new(0));

    let file_cached = match is_first_page_resident(filename) {
        Ok(true) => io_mode != IOMode::Direct,
        _ => io_mode == IOMode::PageCache,
    };
    let use_direct = (!file_cached) || io_mode == IOMode::Direct;

    let num_threads = if use_direct { num_threads_d } else { num_threads_p };
    let block_size = BLOCK_HASH_SIZE;
    let qd = if use_direct { qd_d } else { qd_p };

    let file_size = std::fs::metadata(filename).unwrap().len();
    let block_count = if file_size == 0 {
        0
    } else {
        ((file_size + block_size - 1) / block_size) as usize
    };
    let block_hashes = Arc::new((0..block_count).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());

    let mut threads = vec![];
    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let filename = filename.to_string();
        let block_hashes = block_hashes.clone();
        threads.push(std::thread::spawn(move || {
            let (mut file, mut file_direct) = open_reader_files(&filename, use_direct);
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_hash_reader(
                thread_id,
                num_threads,
                block_size,
                qd,
                &mut file,
                &mut file_direct,
                &mut io_uring,
                read_count,
                block_hashes,
                use_direct,
            )
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let block_hashes = block_hashes
        .iter()
        .map(|value| value.load(Ordering::Relaxed))
        .collect::<Vec<_>>();
    let hash_of_hashes = hash_hashes(&block_hashes);

    BlockHashManifest {
        file_size,
        block_size,
        bytes_hashed: read_count.load(Ordering::SeqCst),
        block_hashes,
        hash_of_hashes,
    }
}

pub fn hash_file_to_replicas(
    filename: &str,
    hash_base: Option<&str>,
    num_threads_p: u64, qd_p: usize,
    num_threads_d: u64, qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<BlockHashManifest> {
    let manifest = hash_file_blocks(filename, num_threads_p, qd_p, num_threads_d, qd_d, io_mode);
    let base = hash_base
        .map(str::to_string)
        .unwrap_or_else(|| default_hash_base(filename));
    save_manifest_replicas(&base, &manifest)?;
    Ok(manifest)
}

pub fn verify_file_with_replicas(
    filename: &str,
    hash_base: Option<&str>,
    num_threads_p: u64, qd_p: usize,
    num_threads_d: u64, qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<VerifyReport> {
    let current = hash_file_blocks(filename, num_threads_p, qd_p, num_threads_d, qd_d, io_mode);
    let base = hash_base
        .map(str::to_string)
        .unwrap_or_else(|| default_hash_base(filename));
    let manifests = load_manifest_replicas(&base);
    let loaded_manifests = manifests.iter().filter(|m| m.is_some()).count();
    let manifest_refs = manifests.iter().map(|m| m.as_ref()).collect::<Vec<_>>();

    let mut ok_blocks = 0;
    let mut bad_blocks = Vec::new();

    for (block_index, current_hash) in current.block_hashes.iter().copied().enumerate() {
        let decision = recover_block_hash(block_index, &[Some(current_hash)], &manifest_refs);
        let block_ok = decision.elected_hash == Some(current_hash) && decision.repair_source_index == Some(0);
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

    Ok(VerifyReport {
        bytes_hashed: current.bytes_hashed,
        total_blocks: current.block_hashes.len(),
        loaded_manifests,
        ok_blocks,
        bad_blocks,
    })
}

fn read_block(path: &str, block_index: usize, file_size: u64) -> std::io::Result<Vec<u8>> {
    let offset = block_index as u64 * BLOCK_HASH_SIZE;
    let len = std::cmp::min(BLOCK_HASH_SIZE, file_size.saturating_sub(offset)) as usize;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_block(path: &str, block_index: usize, data: &[u8]) -> std::io::Result<()> {
    let offset = block_index as u64 * BLOCK_HASH_SIZE;
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    Ok(())
}

pub fn recover_file_with_copies(
    target: &str,
    copies: &[String],
    hash_base: Option<&str>,
    num_threads_p: u64, qd_p: usize,
    num_threads_d: u64, qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<RecoverReport> {
    let mut files = Vec::with_capacity(1 + copies.len());
    files.push(target.to_string());
    files.extend(copies.iter().cloned());

    let manifests_now = files
        .iter()
        .map(|path| hash_file_blocks(path, num_threads_p, qd_p, num_threads_d, qd_d, io_mode))
        .collect::<Vec<_>>();

    let bytes_hashed = manifests_now.iter().map(|manifest| manifest.bytes_hashed).sum();
    let target_size = manifests_now[0].file_size;

    let mut stored_replicas = Vec::<Option<BlockHashManifest>>::new();
    for (index, path) in files.iter().enumerate() {
        let base = if index == 0 {
            hash_base.map(str::to_string).unwrap_or_else(|| default_hash_base(path))
        } else {
            default_hash_base(path)
        };
        stored_replicas.extend(load_manifest_replicas(&base));
    }
    let stored_refs = stored_replicas.iter().map(|m| m.as_ref()).collect::<Vec<_>>();

    let mut repaired_blocks = 0;
    let mut failed_blocks = Vec::new();

    for block_index in 0..manifests_now[0].block_hashes.len() {
        let file_copy_hashes = manifests_now
            .iter()
            .map(|manifest| manifest.block_hashes.get(block_index).copied())
            .collect::<Vec<_>>();
        let decision = recover_block_hash(block_index, &file_copy_hashes, &stored_refs);
        let target_hash = file_copy_hashes[0].unwrap();

        if decision.repair_source_index == Some(0) && decision.elected_hash == Some(target_hash) {
            continue;
        }

        if let Some(source_index) = decision.repair_source_index {
            if source_index > 0 {
                let block = read_block(&files[source_index], block_index, manifests_now[source_index].file_size)?;
                write_block(target, block_index, &block)?;
                repaired_blocks += 1;
                continue;
            }
        }

        failed_blocks.push(BlockRecoverIssue {
            block_index,
            current_hash: target_hash,
            decision,
        });
    }

    if repaired_blocks > 0 {
        let updated_manifest = hash_file_blocks(target, num_threads_p, qd_p, num_threads_d, qd_d, io_mode);
        let base = hash_base
            .map(str::to_string)
            .unwrap_or_else(|| default_hash_base(target));
        save_manifest_replicas(&base, &updated_manifest)?;
    }

    let _ = target_size;

    Ok(RecoverReport {
        bytes_hashed,
        repaired_blocks,
        failed_blocks,
    })
}

pub fn read_file(
    pattern: &str, filename: &str,
    num_threads_p: u64, block_size_p: u64, qd_p: usize, 
    num_threads_d: u64, block_size_d: u64, qd_d: usize, 
    io_mode: IOMode
) -> u64 {
    let mut threads = vec![];
    let read_count = Arc::new(AtomicU64::new(0));

    let file_cached = match is_first_page_resident(filename) { 
        Ok(true) => true && io_mode != IOMode::Direct, 
        _ => false || io_mode == IOMode::PageCache
    };
    let use_direct = (!file_cached) || io_mode == IOMode::Direct;

    let num_threads = if use_direct { num_threads_d } else { num_threads_p };
    let block_size = if use_direct { block_size_d } else { block_size_p };
    let qd = if use_direct { qd_d } else { qd_p };
    
    for thread_id in 0..num_threads {
        let read_count = read_count.clone();
        let filename = filename.to_string();
        let pattern = pattern.to_string();
        threads.push(std::thread::spawn(move || {
            let (mut file, mut file_direct) = open_reader_files(&filename, use_direct);
            let mut io_uring = IoUring::new(1024).unwrap();
            thread_reader(thread_id, pattern, num_threads, block_size, qd, &mut file, &mut file_direct, &mut io_uring, read_count, use_direct)
        }));
    }
    let mut all_matches = Vec::new();
    for thread in threads { all_matches.extend(thread.join().unwrap()); }
    if !pattern.is_empty() {
        all_matches.sort();
        for m in all_matches { println!("{}:{}", m, pattern); }
    }
    read_count.load(Ordering::SeqCst)
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

    fn expected_hashes(data: &[u8], block_size: usize) -> Vec<u64> {
        data.chunks(block_size).map(xxh3_64).collect()
    }

    #[test]
    fn hash_file_blocks_matches_sequential_hashes() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes((block_size * 3) + 12345);
        let path = unique_temp_file("fro-block-hash");
        fs::write(&path, &data).unwrap();

        let manifest = hash_file_blocks(
            path.to_str().unwrap(),
            3, 2,
            3, 2,
            IOMode::PageCache,
        );

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
            2, 2,
            2, 2,
            IOMode::PageCache,
        );
        manifest.block_hashes[1] ^= 1;

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
            2, 2,
            2, 2,
            IOMode::PageCache,
        );

        assert!(manifest.verify_integrity());

        manifest.hash_of_hashes ^= 1;
        assert!(!manifest.verify_integrity());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn recover_block_hash_prefers_intact_hash_manifest() {
        let manifest_a = BlockHashManifest {
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![111, 222],
            hash_of_hashes: hash_hashes(&[111, 222]),
        };
        let manifest_b = BlockHashManifest {
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![111, 999],
            hash_of_hashes: hash_hashes(&[111, 999]),
        };

        let decision = recover_block_hash(
            0,
            &[Some(111), Some(333)],
            &[Some(&manifest_a), Some(&manifest_b), None],
        );

        assert_eq!(decision.elected_hash, Some(111));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(decision.basis, Some(BlockRecoveryBasis::IntactHash));
        assert_eq!(decision.failure, None);
        assert_eq!(decision.status_message(), "recovered block based on intact hash");
        assert_eq!(decision.votes[0].hash, 111);
    }

    #[test]
    fn recover_block_hash_reports_manifest_only_agreement() {
        let manifest_a = BlockHashManifest {
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![111],
            hash_of_hashes: 0,
        };
        let manifest_b = BlockHashManifest {
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![111],
            hash_of_hashes: 0,
        };

        let decision = recover_block_hash(
            0,
            &[Some(222), Some(333)],
            &[Some(&manifest_a), Some(&manifest_b), None],
        );

        assert_eq!(decision.elected_hash, Some(111));
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(decision.basis, None);
        assert_eq!(decision.failure, Some(BlockRecoveryFailure::ManifestOnlyAgreement));
        assert_eq!(decision.status_message(), "failed to recover corrupt block [manifest+manifest hashes agree]");
    }

    #[test]
    fn recover_block_hash_can_use_file_and_manifest_agreement() {
        let good_manifest = BlockHashManifest {
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![555],
            hash_of_hashes: 0,
        };
        let bad_manifest = BlockHashManifest {
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![777],
            hash_of_hashes: 0,
        };

        let decision = recover_block_hash(
            0,
            &[Some(555), Some(999)],
            &[Some(&good_manifest), Some(&bad_manifest), None],
        );

        assert_eq!(decision.elected_hash, Some(555));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(decision.basis, Some(BlockRecoveryBasis::FileAndManifestAgreement));
        assert_eq!(decision.failure, None);
        assert_eq!(decision.status_message(), "recovered block based on file+manifest agreement");
    }

    #[test]
    fn recover_block_hash_can_use_file_and_file_agreement() {
        let decision = recover_block_hash(
            0,
            &[Some(444), Some(444), Some(999)],
            &[None, None, None],
        );

        assert_eq!(decision.elected_hash, Some(444));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(decision.basis, Some(BlockRecoveryBasis::FileAndFileAgreement));
        assert_eq!(decision.failure, None);
        assert_eq!(decision.status_message(), "recovered block based on file+file agreement");
    }

    #[test]
    fn recover_block_hash_reports_no_hash_found() {
        let decision = recover_block_hash(0, &[None, None], &[None, None, None]);

        assert_eq!(decision.elected_hash, None);
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(decision.basis, None);
        assert_eq!(decision.failure, Some(BlockRecoveryFailure::NoBlockHashFound));
        assert_eq!(decision.status_message(), "failed to recover corrupt block [no block hash found either]");
    }
}
