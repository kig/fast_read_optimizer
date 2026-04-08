use crate::{open_with_mode, IOMode};
use crc_fast::{
    checksum as crc_fast_checksum, checksum_combine as crc_fast_checksum_combine,
    CrcAlgorithm as FastCrcAlgorithm,
};
use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CksumChunk {
    crc: u64,
    len: u64,
}

pub fn cksum_crc_block(data: &[u8]) -> u64 {
    crc_fast_checksum(FastCrcAlgorithm::Crc32Cksum, data)
}

pub fn cksum_crc_combine(crc1: u64, crc2: u64, len2: u64) -> u64 {
    crc_fast_checksum_combine(FastCrcAlgorithm::Crc32Cksum, crc1, crc2, len2)
}

fn cksum_length_suffix(bytes: u64) -> ([u8; size_of::<u64>()], usize) {
    let mut suffix = [0_u8; size_of::<u64>()];
    let mut remaining = bytes;
    let mut len = 0usize;
    while remaining != 0 {
        suffix[len] = (remaining & 0xff) as u8;
        remaining >>= 8;
        len += 1;
    }
    (suffix, len)
}

pub fn finalize_cksum_crc(mut crc: u64, bytes: u64) -> u32 {
    let (suffix, suffix_len) = cksum_length_suffix(bytes);
    if suffix_len != 0 {
        crc = cksum_crc_combine(
            crc,
            cksum_crc_block(&suffix[..suffix_len]),
            suffix_len as u64,
        );
    }
    crc.try_into().unwrap()
}

fn reduce_cksum_chunks(chunks: Vec<CksumChunk>) -> u64 {
    let mut chunks = chunks.into_iter();
    match chunks.next() {
        Some(first) => chunks.fold(first.crc, |acc, chunk| {
            cksum_crc_combine(acc, chunk.crc, chunk.len)
        }),
        None => cksum_crc_block(&[]),
    }
}

pub fn hash_file_crc32(path: &str, io_mode: IOMode) -> io::Result<u32> {
    let file = open_with_mode(path, io_mode)?;
    let block_size = file.block_size()?;
    file.map_reduce_blocks(
        block_size,
        |_, data| {
            Ok(CksumChunk {
                crc: cksum_crc_block(data),
                len: data.len() as u64,
            })
        },
        |chunks, report| {
            Ok(finalize_cksum_crc(
                reduce_cksum_chunks(chunks),
                report.file_size,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_cksum_update_matches_posix_cksum_examples() {
        let crc = finalize_cksum_crc(cksum_crc_block(b"abc"), 3);
        assert_eq!(crc, 1_219_131_554);

        let chunks = [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()];
        let mut iter = chunks.into_iter();
        let mut crc = cksum_crc_block(iter.next().unwrap());
        for chunk in iter {
            crc = cksum_crc_combine(crc, cksum_crc_block(chunk), chunk.len() as u64);
        }
        assert_eq!(finalize_cksum_crc(crc, 14), 2_318_676_478);
    }

    #[test]
    fn checksum_combine_matches_sequential_crc() {
        let chunks = [
            b"alpha".as_slice(),
            b"beta".as_slice(),
            b"gamma".as_slice(),
            b"delta".as_slice(),
        ];
        let mut iter = chunks.iter();
        let mut combined = cksum_crc_block(iter.next().unwrap());
        for chunk in iter {
            combined = cksum_crc_combine(combined, cksum_crc_block(chunk), chunk.len() as u64);
        }
        let sequential = cksum_crc_block(&chunks.concat());
        assert_eq!(combined, sequential);
    }

    #[test]
    fn hash_file_crc32_matches_single_block_finalize() {
        let base = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let path = base.join(format!(
            "fro-cksum-regular-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let bytes = (0..(3 * 1024 * 1024 + 517))
            .map(|i| ((i * 37 + 11) % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &bytes).unwrap();

        let regular = hash_file_crc32(path.to_str().unwrap(), IOMode::PageCache).unwrap();
        assert_eq!(
            regular,
            finalize_cksum_crc(cksum_crc_block(&bytes), bytes.len() as u64)
        );
        let _ = std::fs::remove_file(path);
    }
}
