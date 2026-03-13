use iou::IoUring;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use memchr::memmem::Finder;
use xxhash_rust::xxh3::xxh3_64;
use crate::common::{AlignedBuffer, IOMode};
use crate::mincore::{is_first_page_resident};

pub const BLOCK_HASH_SIZE: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn disk_replicas(&self) -> [u64; 3] {
        [self.hash_of_hashes; 3]
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

fn hash_hashes(values: &[u64]) -> u64 {
    let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<u64>());
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    xxh3_64(&bytes)
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

        assert_eq!(manifest.disk_replicas(), [manifest.hash_of_hashes; 3]);
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
