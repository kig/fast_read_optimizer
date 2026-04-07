use super::{regular_stdin_path, *};

const CKSUM_CRC32_TABLE: [u32; 256] = [
    0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9, 0x130476dc, 0x17c56b6b, 0x1a864db2, 0x1e475005,
    0x2608edb8, 0x22c9f00f, 0x2f8ad6d6, 0x2b4bcb61, 0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd,
    0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9, 0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75,
    0x6a1936c8, 0x6ed82b7f, 0x639b0da6, 0x675a1011, 0x791d4014, 0x7ddc5da3, 0x709f7b7a, 0x745e66cd,
    0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039, 0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5,
    0xbe2b5b58, 0xbaea46ef, 0xb7a96036, 0xb3687d81, 0xad2f2d84, 0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d,
    0xd4326d90, 0xd0f37027, 0xddb056fe, 0xd9714b49, 0xc7361b4c, 0xc3f706fb, 0xceb42022, 0xca753d95,
    0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1, 0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d,
    0x34867077, 0x30476dc0, 0x3d044b19, 0x39c556ae, 0x278206ab, 0x23431b1c, 0x2e003dc5, 0x2ac12072,
    0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16, 0x018aeb13, 0x054bf6a4, 0x0808d07d, 0x0cc9cdca,
    0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde, 0x6b93dddb, 0x6f52c06c, 0x6211e6b5, 0x66d0fb02,
    0x5e9f46bf, 0x5a5e5b08, 0x571d7dd1, 0x53dc6066, 0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
    0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e, 0xbfa1b04b, 0xbb60adfc, 0xb6238b25, 0xb2e29692,
    0x8aad2b2f, 0x8e6c3698, 0x832f1041, 0x87ee0df6, 0x99a95df3, 0x9d684044, 0x902b669d, 0x94ea7b2a,
    0xe0b41de7, 0xe4750050, 0xe9362689, 0xedf73b3e, 0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
    0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686, 0xd5b88683, 0xd1799b34, 0xdc3abded, 0xd8fba05a,
    0x690ce0ee, 0x6dcdfd59, 0x608edb80, 0x644fc637, 0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb,
    0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f, 0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53,
    0x251d3b9e, 0x21dc2629, 0x2c9f00f0, 0x285e1d47, 0x36194d42, 0x32d850f5, 0x3f9b762c, 0x3b5a6b9b,
    0x0315d626, 0x07d4cb91, 0x0a97ed48, 0x0e56f0ff, 0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623,
    0xf12f560e, 0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7, 0xe22b20d2, 0xe6ea3d65, 0xeba91bbc, 0xef68060b,
    0xd727bbb6, 0xd3e6a601, 0xdea580d8, 0xda649d6f, 0xc423cd6a, 0xc0e2d0dd, 0xcda1f604, 0xc960ebb3,
    0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7, 0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b,
    0x9b3660c6, 0x9ff77d71, 0x92b45ba8, 0x9675461f, 0x8832161a, 0x8cf30bad, 0x81b02d74, 0x857130c3,
    0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640, 0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c,
    0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8, 0x68860bfd, 0x6c47164a, 0x61043093, 0x65c52d24,
    0x119b4be9, 0x155a565e, 0x18197087, 0x1cd86d30, 0x029f3d35, 0x065e2082, 0x0b1d065b, 0x0fdc1bec,
    0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088, 0x2497d08d, 0x2056cd3a, 0x2d15ebe3, 0x29d4f654,
    0xc5a92679, 0xc1683bce, 0xcc2b1d17, 0xc8ea00a0, 0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb, 0xdbee767c,
    0xe3a1cbc1, 0xe760d676, 0xea23f0af, 0xeee2ed18, 0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
    0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0, 0x9abc8bd5, 0x9e7d9662, 0x933eb0bb, 0x97ffad0c,
    0xafb010b1, 0xab710d06, 0xa6322bdf, 0xa2f33668, 0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4,
];

const fn crc32_cksum_update_byte(crc: u32, byte: u8) -> u32 {
    let index = ((crc >> 24) as u8) ^ byte;
    (crc << 8) ^ CKSUM_CRC32_TABLE[index as usize]
}

const fn build_cksum_crc32_slicing_table() -> [[u32; 256]; 8] {
    let mut tables = [[0_u32; 256]; 8];
    let mut index = 0;
    while index < 256 {
        tables[0][index] = CKSUM_CRC32_TABLE[index];
        index += 1;
    }

    let mut table = 1;
    while table < 8 {
        let mut entry = 0;
        while entry < 256 {
            let crc = tables[table - 1][entry];
            tables[table][entry] = (crc << 8) ^ CKSUM_CRC32_TABLE[((crc >> 24) & 0xff) as usize];
            entry += 1;
        }
        table += 1;
    }

    tables
}

const fn build_cksum_zero_byte_matrix() -> [u32; 32] {
    let mut matrix = [0_u32; 32];
    let mut index = 0;
    while index < 32 {
        matrix[index] = crc32_cksum_update_byte(1_u32 << (31 - index), 0);
        index += 1;
    }
    matrix
}

const CKSUM_CRC32_SLICING_TABLE: [[u32; 256]; 8] = build_cksum_crc32_slicing_table();
const CKSUM_ZERO_BYTE_MATRIX: [u32; 32] = build_cksum_zero_byte_matrix();

fn crc32_cksum_update(mut crc: u32, mut data: &[u8]) -> u32 {
    while data.len() >= 8 {
        let x = crc ^ u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        crc = CKSUM_CRC32_SLICING_TABLE[7][((x >> 24) & 0xff) as usize]
            ^ CKSUM_CRC32_SLICING_TABLE[6][((x >> 16) & 0xff) as usize]
            ^ CKSUM_CRC32_SLICING_TABLE[5][((x >> 8) & 0xff) as usize]
            ^ CKSUM_CRC32_SLICING_TABLE[4][(x & 0xff) as usize]
            ^ CKSUM_CRC32_SLICING_TABLE[3][data[4] as usize]
            ^ CKSUM_CRC32_SLICING_TABLE[2][data[5] as usize]
            ^ CKSUM_CRC32_SLICING_TABLE[1][data[6] as usize]
            ^ CKSUM_CRC32_SLICING_TABLE[0][data[7] as usize];
        data = &data[8..];
    }
    for &byte in data {
        crc = crc32_cksum_update_byte(crc, byte);
    }
    crc
}

fn crc32_cksum_gf2_matrix_times(matrix: &[u32; 32], vector: u32) -> u32 {
    let mut product = 0_u32;
    let mut index = 0;
    while index < 32 {
        if vector & (1_u32 << (31 - index)) != 0 {
            product ^= matrix[index];
        }
        index += 1;
    }
    product
}

fn crc32_cksum_gf2_matrix_square(square: &mut [u32; 32], matrix: &[u32; 32]) {
    let mut index = 0;
    while index < 32 {
        square[index] = crc32_cksum_gf2_matrix_times(matrix, matrix[index]);
        index += 1;
    }
}

fn checksum_combine(crc1: u32, crc2: u32, len2: u64) -> u32 {
    if len2 == 0 {
        return crc1 ^ crc2;
    }

    let mut shifted_crc = crc1;
    let mut power = CKSUM_ZERO_BYTE_MATRIX;
    let mut square = [0_u32; 32];
    let mut remaining = len2;

    while remaining != 0 {
        if remaining & 1 != 0 {
            shifted_crc = crc32_cksum_gf2_matrix_times(&power, shifted_crc);
        }
        remaining >>= 1;
        if remaining == 0 {
            break;
        }
        crc32_cksum_gf2_matrix_square(&mut square, &power);
        power = square;
    }

    shifted_crc ^ crc2
}

fn crc32_cksum_finalize(mut crc: u32, bytes: u64) -> u32 {
    let mut length = bytes;
    while length != 0 {
        crc = crc32_cksum_update_byte(crc, (length & 0xff) as u8);
        length >>= 8;
    }
    !crc
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CksumChunk {
    crc: u32,
    len: u64,
}

fn cksum_stream_input(input: &StreamInput, io_mode: IOMode) -> io::Result<(u32, u64)> {
    let mut crc = 0_u32;
    let mut bytes = 0_u64;
    visit_ordered_input(input, io_mode, |block| {
        crc = crc32_cksum_update(crc, block);
        bytes += block.len() as u64;
        Ok(())
    })?;
    Ok((crc, bytes))
}

fn cksum_regular_path(path: &str, io_mode: IOMode) -> io::Result<(u32, u64)> {
    let config = crate::config::load_config(None);
    let file = crate::stream::ParallelFile::open(&config, "read", path, internal_io_mode(io_mode))?;
    let block_size = file.block_size()?;
    file.map_reduce_blocks(
        block_size,
        |_, data| {
            Ok(CksumChunk {
                crc: crc32_cksum_update(0, data),
                len: data.len() as u64,
            })
        },
        |chunks, report| {
            let crc = chunks.into_iter().fold(0_u32, |acc, chunk| {
                checksum_combine(acc, chunk.crc, chunk.len)
            });
            Ok((crc, report.file_size))
        },
    )
}

pub(super) fn run_cksum(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    for input in inputs {
        let (crc, bytes) = match &input {
            StreamInput::File(file) if is_regular_input_path(file)? => {
                cksum_regular_path(file, io_mode)?
            }
            StreamInput::Stdin { .. } => match regular_stdin_path()? {
                Some(path) => cksum_regular_path(&path, io_mode)?,
                None => cksum_stream_input(&input, io_mode)?,
            },
            _ => cksum_stream_input(&input, io_mode)?,
        };
        let crc = crc32_cksum_finalize(crc, bytes);
        match input {
            StreamInput::File(file) => println!("{} {} {}", crc, bytes, file),
            StreamInput::Stdin { label: Some(label) } => println!("{} {} {}", crc, bytes, label),
            StreamInput::Stdin { label: None } => println!("{} {}", crc, bytes),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_cksum_update_matches_posix_cksum_examples() {
        let crc = crc32_cksum_finalize(crc32_cksum_update(0, b"abc"), 3);
        assert_eq!(crc, 1_219_131_554);

        let mut crc = 0_u32;
        for chunk in [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()] {
            crc = crc32_cksum_update(crc, chunk);
        }
        assert_eq!(crc32_cksum_finalize(crc, 14), 2_318_676_478);
    }

    #[test]
    fn checksum_combine_matches_sequential_crc() {
        let chunks = [
            b"alpha".as_slice(),
            b"beta".as_slice(),
            b"gamma".as_slice(),
            b"delta".as_slice(),
        ];
        let combined = chunks.iter().fold(0_u32, |acc, chunk| {
            checksum_combine(acc, crc32_cksum_update(0, chunk), chunk.len() as u64)
        });
        let mut sequential = 0_u32;
        for chunk in chunks {
            sequential = crc32_cksum_update(sequential, chunk);
        }
        assert_eq!(combined, sequential);
    }

    #[test]
    fn cksum_regular_path_matches_stream_path() {
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

        let regular = cksum_regular_path(path.to_str().unwrap(), IOMode::PageCache).unwrap();
        let streamed = cksum_stream_input(
            &StreamInput::File(path.to_string_lossy().into_owned()),
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(regular, streamed);
        assert_eq!(
            crc32_cksum_finalize(regular.0, regular.1),
            crc32_cksum_finalize(crc32_cksum_update(0, &bytes), bytes.len() as u64)
        );

        let _ = std::fs::remove_file(path);
    }
}
