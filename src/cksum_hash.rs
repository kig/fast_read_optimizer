use crate::{open_with_mode, IOMode};
use crc_fast::{checksum as crc_fast_checksum, CrcAlgorithm as FastCrcAlgorithm};
use std::fs;
use std::io;
use std::mem::size_of;
use std::sync::OnceLock;

const CKSUM_WIDTH: usize = 32;
const CKSUM_POLY: u32 = 0x04c11db7;
const CKSUM_COMBINE_INPUT_XOR: u32 = 0xffff_ffff;
const SMALL_FILE_CKSUM_LIMIT: u64 = 64 * 1024;

type CksumOperatorMatrix = [u32; CKSUM_WIDTH];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CksumChunk {
    crc: u32,
    len: u64,
}

pub fn cksum_crc_block(data: &[u8]) -> u64 {
    crc_fast_checksum(FastCrcAlgorithm::Crc32Cksum, data)
}

fn cksum_matrix_times(mat: &CksumOperatorMatrix, mut vec: u32) -> u32 {
    let mut sum = 0u32;
    let mut idx = 0usize;
    while vec != 0 {
        if vec & 1 != 0 {
            sum ^= mat[idx];
        }
        vec >>= 1;
        idx += 1;
    }
    sum
}

fn cksum_matrix_square(mat: &CksumOperatorMatrix) -> CksumOperatorMatrix {
    let mut square = [0u32; CKSUM_WIDTH];
    for (index, slot) in square.iter_mut().enumerate() {
        *slot = cksum_matrix_times(mat, mat[index]);
    }
    square
}

fn cksum_zero_byte_operators() -> &'static [CksumOperatorMatrix; u64::BITS as usize] {
    static OPERATORS: OnceLock<[CksumOperatorMatrix; u64::BITS as usize]> = OnceLock::new();
    OPERATORS.get_or_init(|| {
        let mut one_zero_bit = [0u32; CKSUM_WIDTH];
        let mut col = 2u32;
        for slot in one_zero_bit.iter_mut().take(CKSUM_WIDTH - 1) {
            *slot = col;
            col <<= 1;
        }
        one_zero_bit[CKSUM_WIDTH - 1] = CKSUM_POLY;

        let two_zero_bits = cksum_matrix_square(&one_zero_bit);
        let four_zero_bits = cksum_matrix_square(&two_zero_bits);
        let one_zero_byte = cksum_matrix_square(&four_zero_bits);

        let mut operators = [[0u32; CKSUM_WIDTH]; u64::BITS as usize];
        operators[0] = one_zero_byte;
        for index in 1..operators.len() {
            operators[index] = cksum_matrix_square(&operators[index - 1]);
        }
        operators
    })
}

fn cksum_apply_zero_bytes(mut crc: u32, mut len: u64) -> u32 {
    let operators = cksum_zero_byte_operators();
    let mut index = 0usize;
    while len != 0 {
        if len & 1 != 0 {
            crc = cksum_matrix_times(&operators[index], crc);
        }
        len >>= 1;
        index += 1;
    }
    crc
}

fn cksum_crc_combine_u32(crc1: u32, crc2: u32, len2: u64) -> u32 {
    cksum_apply_zero_bytes(crc1 ^ CKSUM_COMBINE_INPUT_XOR, len2) ^ crc2
}

pub fn cksum_crc_combine(crc1: u64, crc2: u64, len2: u64) -> u64 {
    cksum_crc_combine_u32(crc1 as u32, crc2 as u32, len2) as u64
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

pub fn finalize_cksum_crc(crc: u64, bytes: u64) -> u32 {
    let mut crc = crc as u32;
    let (suffix, suffix_len) = cksum_length_suffix(bytes);
    if suffix_len != 0 {
        crc = cksum_crc_combine_u32(
            crc,
            cksum_crc_block(&suffix[..suffix_len]) as u32,
            suffix_len as u64,
        );
    }
    crc
}

fn reduce_cksum_chunks(chunks: Vec<CksumChunk>) -> u32 {
    let mut chunks = chunks.into_iter();
    match chunks.next() {
        Some(first) => chunks.fold(first.crc, |acc, chunk| {
            cksum_crc_combine_u32(acc, chunk.crc, chunk.len)
        }),
        None => cksum_crc_block(&[]) as u32,
    }
}

fn try_hash_small_file_crc32(path: &str, io_mode: IOMode) -> io::Result<Option<u32>> {
    if io_mode == IOMode::Direct {
        return Ok(None);
    }
    let metadata = fs::metadata(path)?;
    if metadata.len() > SMALL_FILE_CKSUM_LIMIT {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    Ok(Some(finalize_cksum_crc(
        cksum_crc_block(&bytes),
        bytes.len() as u64,
    )))
}

pub fn hash_file_crc32(path: &str, io_mode: IOMode) -> io::Result<u32> {
    if let Some(crc) = try_hash_small_file_crc32(path, io_mode)? {
        return Ok(crc);
    }
    let file = open_with_mode(path, io_mode)?;
    let block_size = file.block_size()?;
    file.map_reduce_blocks(
        block_size,
        |_, data| {
            Ok(CksumChunk {
                crc: cksum_crc_block(data) as u32,
                len: data.len() as u64,
            })
        },
        |chunks, report| {
            Ok(finalize_cksum_crc(
                reduce_cksum_chunks(chunks) as u64,
                report.file_size,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crc_fast::checksum_combine as crc_fast_checksum_combine;
    use std::fs;

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
    fn cached_cksum_combine_matches_crc_fast_reference() {
        let first = (0..(256 * 1024 + 17))
            .map(|i| ((i * 13 + 5) % 251) as u8)
            .collect::<Vec<_>>();
        let second = (0..(768 * 1024 + 29))
            .map(|i| ((i * 29 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        let first_crc = cksum_crc_block(&first);
        let second_crc = cksum_crc_block(&second);
        assert_eq!(
            cksum_crc_combine(first_crc, second_crc, second.len() as u64),
            crc_fast_checksum_combine(
                FastCrcAlgorithm::Crc32Cksum,
                first_crc,
                second_crc,
                second.len() as u64
            )
        );
    }

    #[test]
    fn hash_file_crc32_matches_single_block_finalize() {
        let base = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
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
        fs::write(&path, &bytes).unwrap();

        let regular = hash_file_crc32(path.to_str().unwrap(), IOMode::PageCache).unwrap();
        assert_eq!(
            regular,
            finalize_cksum_crc(cksum_crc_block(&bytes), bytes.len() as u64)
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn small_file_crc32_fast_path_matches_finalize() {
        let base = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("test-tmp");
        fs::create_dir_all(&base).unwrap();
        let path = base.join(format!(
            "fro-cksum-small-{}-{}.bin",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let bytes = (0..8192)
            .map(|i| ((i * 19 + 7) % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&path, &bytes).unwrap();

        let crc = hash_file_crc32(path.to_str().unwrap(), IOMode::Auto).unwrap();
        assert_eq!(
            crc,
            finalize_cksum_crc(cksum_crc_block(&bytes), bytes.len() as u64)
        );
        let _ = fs::remove_file(path);
    }
}
