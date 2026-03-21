use crate::block_hash::{hash_file_blocks, BlockHashAlgorithm};
use crate::config::{load_config, IOParams};
use crate::{hash_file_blake3, visit_blocks_with_mode, IOMode};
use openssl::hash::{Hasher, MessageDigest};
use std::collections::BTreeMap;
use std::io;
use std::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    Md5,
    Blake2b512,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
    Blake3,
    FroBlockXxh3,
    FroBlockSha256,
}

impl HashAlgorithm {
    fn openssl_name(self) -> Option<&'static str> {
        match self {
            Self::Md5 => Some("MD5"),
            Self::Blake2b512 => Some("BLAKE2b512"),
            Self::Sha224 => Some("SHA224"),
            Self::Sha256 => Some("SHA256"),
            Self::Sha384 => Some("SHA384"),
            Self::Sha512 => Some("SHA512"),
            Self::Blake3 | Self::FroBlockXxh3 | Self::FroBlockSha256 => None,
        }
    }
}

pub fn hash_file(path: &str, algorithm: HashAlgorithm, io_mode: IOMode) -> io::Result<Vec<u8>> {
    if let Some(name) = algorithm.openssl_name() {
        let message_digest = MessageDigest::from_name(name).ok_or_else(|| {
            io::Error::other(format!("OpenSSL digest algorithm not available: {name}"))
        })?;
        return hash_file_ordered(path, message_digest, io_mode);
    }
    match algorithm {
        HashAlgorithm::Blake3 => Ok(hash_file_blake3(path, io_mode)?.as_bytes().to_vec()),
        HashAlgorithm::FroBlockXxh3 => {
            hash_file_block_digest(path, BlockHashAlgorithm::Xxh3, io_mode)
        }
        HashAlgorithm::FroBlockSha256 => {
            hash_file_block_digest(path, BlockHashAlgorithm::Sha256, io_mode)
        }
        HashAlgorithm::Md5
        | HashAlgorithm::Blake2b512
        | HashAlgorithm::Sha224
        | HashAlgorithm::Sha256
        | HashAlgorithm::Sha384
        | HashAlgorithm::Sha512 => unreachable!(),
    }
}

fn hash_file_ordered(
    path: &str,
    message_digest: MessageDigest,
    io_mode: IOMode,
) -> io::Result<Vec<u8>> {
    let (tx, rx) = mpsc::channel::<(usize, Vec<u8>)>();

    let hash_thread = std::thread::spawn(move || -> io::Result<Vec<u8>> {
        let mut hasher = Hasher::new(message_digest).map_err(io::Error::other)?;
        let mut next_block = 0usize;
        let mut pending = BTreeMap::<usize, Vec<u8>>::new();

        while let Ok((block_index, data)) = rx.recv() {
            pending.insert(block_index, data);
            while let Some(block) = pending.remove(&next_block) {
                hasher.update(&block).map_err(io::Error::other)?;
                next_block += 1;
            }
        }

        if !pending.is_empty() {
            return Err(io::Error::other(
                "missing block data while finalizing digest",
            ));
        }

        Ok(hasher.finish().map_err(io::Error::other)?.to_vec())
    });

    let sender = tx.clone();
    let visit_result = visit_blocks_with_mode(path, io_mode, move |block_index, data| {
        sender
            .send((block_index, data.to_vec()))
            .map_err(|_| io::Error::other("failed to queue block for hashing"))
    });
    drop(tx);
    let hash_result = hash_thread
        .join()
        .map_err(|_| io::Error::other("hash worker thread panicked"))?;
    visit_result?;
    Ok(hash_result?)
}

fn hash_file_block_digest(
    path: &str,
    block_algorithm: BlockHashAlgorithm,
    io_mode: IOMode,
) -> io::Result<Vec<u8>> {
    let config = load_config(None);
    let page_cache = config.get_params_for_path("hash", false, path);
    let direct = config.get_params_for_path("hash", true, path);
    hash_file_block_digest_with_params(path, block_algorithm, &page_cache, &direct, io_mode)
}

fn hash_file_block_digest_with_params(
    path: &str,
    block_algorithm: BlockHashAlgorithm,
    page_cache: &IOParams,
    direct: &IOParams,
    io_mode: IOMode,
) -> io::Result<Vec<u8>> {
    let manifest = hash_file_blocks(
        path,
        block_algorithm,
        page_cache.num_threads,
        page_cache.block_size,
        page_cache.qd,
        direct.num_threads,
        direct.block_size,
        direct.qd,
        io_mode,
    )?;
    Ok(manifest.hash_of_hashes_bytes().to_vec())
}

pub fn hash_file_sha256(path: &str, io_mode: IOMode) -> io::Result<[u8; 32]> {
    let digest = hash_file(path, HashAlgorithm::Sha256, io_mode)?;
    digest.try_into().map_err(|digest: Vec<u8>| {
        io::Error::other(format!(
            "unexpected SHA-256 digest length: {} bytes",
            digest.len()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{hash_file, hash_file_block_digest_with_params, HashAlgorithm};
    use crate::block_hash::{hash_file_blocks, BlockHashAlgorithm};
    use crate::config::IOParams;
    use crate::IOMode;
    use openssl::hash::{hash, MessageDigest};
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

    #[test]
    fn hash_file_matches_openssl_for_sha256_md5_and_blake2b() {
        let path = unique_temp_file("fro-hash-file");
        let bytes = (0..(512 * 1024 + 123))
            .map(|i| ((i * 31) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &bytes).unwrap();

        let sha256 = hash_file(
            path.to_str().unwrap(),
            HashAlgorithm::Sha256,
            IOMode::PageCache,
        )
        .unwrap();
        let md5 = hash_file(
            path.to_str().unwrap(),
            HashAlgorithm::Md5,
            IOMode::PageCache,
        )
        .unwrap();
        let blake2b = hash_file(
            path.to_str().unwrap(),
            HashAlgorithm::Blake2b512,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(
            sha256,
            hash(MessageDigest::sha256(), &bytes).unwrap().to_vec()
        );
        assert_eq!(md5, hash(MessageDigest::md5(), &bytes).unwrap().to_vec());
        assert_eq!(
            blake2b,
            hash(MessageDigest::from_name("BLAKE2b512").unwrap(), &bytes)
                .unwrap()
                .to_vec()
        );

        let blake3 = hash_file(
            path.to_str().unwrap(),
            HashAlgorithm::Blake3,
            IOMode::PageCache,
        )
        .unwrap();
        assert_eq!(blake3, blake3::hash(&bytes).as_bytes().to_vec());

        let page_cache = IOParams {
            num_threads: 2,
            block_size: 64 * 1024,
            qd: 2,
        };
        let direct = IOParams {
            num_threads: 2,
            block_size: 64 * 1024,
            qd: 2,
        };
        let fro_block_xxh3 = hash_file_block_digest_with_params(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            &page_cache,
            &direct,
            IOMode::PageCache,
        )
        .unwrap();
        let fro_block_sha256 = hash_file_block_digest_with_params(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Sha256,
            &page_cache,
            &direct,
            IOMode::PageCache,
        )
        .unwrap();
        let manifest_xxh3 = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            page_cache.num_threads,
            page_cache.block_size,
            page_cache.qd,
            direct.num_threads,
            direct.block_size,
            direct.qd,
            IOMode::PageCache,
        )
        .unwrap();
        let manifest_sha256 = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Sha256,
            page_cache.num_threads,
            page_cache.block_size,
            page_cache.qd,
            direct.num_threads,
            direct.block_size,
            direct.qd,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(
            fro_block_xxh3,
            manifest_xxh3.hash_of_hashes_bytes().to_vec()
        );
        assert_eq!(
            fro_block_sha256,
            manifest_sha256.hash_of_hashes_bytes().to_vec()
        );

        let _ = fs::remove_file(path);
    }
}
