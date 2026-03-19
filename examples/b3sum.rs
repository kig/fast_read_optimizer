use blake3::hazmat::{
    left_subtree_len, merge_subtrees_non_root, merge_subtrees_root, ChainingValue, HasherExt,
    Mode,
};
use blake3::{Hash, Hasher};
use fro::IOMode;
use std::io;
use std::sync::{Arc, Mutex};

const HASH_BLOCK_SIZE: u64 = 1024 * 1024;

#[derive(Clone, Copy)]
struct SubtreeHash {
    len: u64,
    cv: ChainingValue,
}

struct Options {
    io_mode: IOMode,
    filenames: Vec<String>,
}

fn usage(program: &str) {
    eprintln!(
        "USAGE: {} [--auto|--no-direct] <file> [file ...]",
        program
    );
    eprintln!("       --direct is intentionally unsupported in this example");
}

fn parse_args() -> Result<Options, String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage(&args[0]);
        return Err("missing file".to_string());
    }

    let mut io_mode = IOMode::Auto;
    let mut filenames = Vec::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--auto" => io_mode = IOMode::Auto,
            "--direct" => {
                return Err(
                    "--direct is unsupported in this example; use the main fro CLI for direct-I/O experiments"
                        .to_string(),
                )
            }
            "--no-direct" => io_mode = IOMode::PageCache,
            "-h" | "--help" => {
                usage(&args[0]);
                std::process::exit(0);
            }
            other => filenames.push(other.to_string()),
        }
        i += 1;
    }

    if filenames.is_empty() {
        return Err("missing file".to_string());
    }

    Ok(Options { io_mode, filenames })
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn hash_subtree(block_index: usize, data: &[u8]) -> SubtreeHash {
    let offset = block_index as u64 * HASH_BLOCK_SIZE;
    let mut hasher = Hasher::new();
    if offset != 0 {
        hasher.set_input_offset(offset);
    }
    hasher.update(data);
    SubtreeHash {
        len: data.len() as u64,
        cv: hasher.finalize_non_root(),
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

fn hash_file(path: &str, io_mode: IOMode) -> io::Result<Hash> {
    debug_assert!(io_mode != IOMode::Direct);
    let file = fro::open_with_mode(path, io_mode)?;
    let file_size = file.len()?;

    if file_size == 0 {
        return Ok(blake3::hash(&[]));
    }

    if file_size <= HASH_BLOCK_SIZE {
        let root = Arc::new(Mutex::new(None::<Hash>));
        let root_slot = root.clone();
        file.foreach_block_parallel(HASH_BLOCK_SIZE, move |block_index, data| {
            if block_index != 0 {
                return Err(io::Error::other("unexpected extra block for small file"));
            }
            let mut hasher = Hasher::new();
            hasher.update(data);
            *root_slot.lock().unwrap() = Some(hasher.finalize());
            Ok(())
        })?;
        return root
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| io::Error::other("small file hash was not produced"));
    }

    let block_count = file.block_count(HASH_BLOCK_SIZE)?;
    let subtrees = Arc::new(Mutex::new(vec![None::<SubtreeHash>; block_count]));
    let subtree_slots = subtrees.clone();

    file.foreach_block_parallel(HASH_BLOCK_SIZE, move |block_index, data| {
        let subtree = hash_subtree(block_index, data);
        subtree_slots.lock().unwrap()[block_index] = Some(subtree);
        Ok(())
    })?;

    let subtrees = Arc::into_inner(subtrees)
        .ok_or_else(|| io::Error::other("subtree storage still shared"))?
        .into_inner()
        .map_err(|_| io::Error::other("subtree storage poisoned"))?;
    let subtrees = subtrees
        .into_iter()
        .enumerate()
        .map(|(index, subtree)| {
            subtree.ok_or_else(|| io::Error::other(format!("missing subtree hash {index}")))
        })
        .collect::<io::Result<Vec<_>>>()?;

    reduce_root(&subtrees, file_size)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args().map_err(io::Error::other)?;

    for filename in &opts.filenames {
        let digest = hash_file(filename, opts.io_mode)?;
        println!("{}  {}", hex_digest(digest.as_bytes()), filename);
    }

    Ok(())
}
