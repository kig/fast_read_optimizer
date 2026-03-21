use crate::{open_with_mode, IOMode};
use blake3::hazmat::{
    left_subtree_len, merge_subtrees_non_root, merge_subtrees_root, ChainingValue, HasherExt, Mode,
};
use blake3::{Hash, Hasher};
use std::io;

const HASH_BLOCK_SIZE: u64 = 1024 * 1024;

#[derive(Clone, Copy)]
struct SubtreeHash {
    len: u64,
    cv: ChainingValue,
}

#[derive(Clone, Copy)]
struct MappedBlake3Block {
    subtree: SubtreeHash,
    root_hash: Option<Hash>,
}

fn hash_subtree(block_index: usize, data: &[u8]) -> MappedBlake3Block {
    let offset = block_index as u64 * HASH_BLOCK_SIZE;
    let mut hasher = Hasher::new();
    if offset != 0 {
        hasher.set_input_offset(offset);
    }
    hasher.update(data);
    MappedBlake3Block {
        subtree: SubtreeHash {
            len: data.len() as u64,
            cv: hasher.finalize_non_root(),
        },
        root_hash: (block_index == 0).then(|| {
            let mut root_hasher = Hasher::new();
            root_hasher.update(data);
            root_hasher.finalize()
        }),
    }
}

fn split_subtrees(parts: &[SubtreeHash], left_len: u64) -> io::Result<usize> {
    let mut seen = 0u64;
    for (index, part) in parts.iter().enumerate() {
        seen += part.len;
        if seen == left_len {
            return Ok(index + 1);
        }
        if seen > left_len {
            break;
        }
    }
    Err(io::Error::other(format!(
        "subtree partition does not match BLAKE3 split at {} bytes",
        left_len
    )))
}

fn reduce_non_root(parts: &[SubtreeHash], total_len: u64) -> io::Result<ChainingValue> {
    if parts.len() == 1 && parts[0].len == total_len {
        return Ok(parts[0].cv);
    }

    let left_len = left_subtree_len(total_len);
    let split = split_subtrees(parts, left_len)?;
    let left = reduce_non_root(&parts[..split], left_len)?;
    let right = reduce_non_root(&parts[split..], total_len - left_len)?;
    Ok(merge_subtrees_non_root(&left, &right, Mode::Hash))
}

fn reduce_root(parts: &[SubtreeHash], total_len: u64) -> io::Result<Hash> {
    let left_len = left_subtree_len(total_len);
    let split = split_subtrees(parts, left_len)?;
    let left = reduce_non_root(&parts[..split], left_len)?;
    let right = reduce_non_root(&parts[split..], total_len - left_len)?;
    Ok(merge_subtrees_root(&left, &right, Mode::Hash))
}

pub fn hash_file_blake3(path: &str, io_mode: IOMode) -> io::Result<Hash> {
    let file = open_with_mode(path, io_mode)?;
    file.map_reduce_blocks(
        HASH_BLOCK_SIZE,
        |block_index, data| Ok(hash_subtree(block_index, data)),
        |parts, report| {
            if report.file_size == 0 {
                return Ok(blake3::hash(&[]));
            }
            if parts.len() == 1 && parts[0].subtree.len == report.file_size {
                return parts[0]
                    .root_hash
                    .ok_or_else(|| io::Error::other("single-block BLAKE3 root hash missing"));
            }
            let subtrees = parts
                .into_iter()
                .map(|part| part.subtree)
                .collect::<Vec<_>>();
            reduce_root(&subtrees, report.file_size)
        },
    )
}
