use super::*;
use std::os::unix::fs::FileTypeExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FgrepDevicePolicy {
    Read,
    Skip,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FgrepDirectoryPolicy {
    Read,
    Skip,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FgrepInputPathKind {
    Regular,
    Directory,
    Device,
    Other,
}

pub(super) fn parse_devices_value(value: &str) -> io::Result<FgrepDevicePolicy> {
    match value {
        "read" => Ok(FgrepDevicePolicy::Read),
        "skip" => Ok(FgrepDevicePolicy::Skip),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: unsupported --devices action '{value}'"),
        )),
    }
}

pub(super) fn parse_directories_value(value: &str) -> io::Result<FgrepDirectoryPolicy> {
    match value {
        "read" => Ok(FgrepDirectoryPolicy::Read),
        "skip" => Ok(FgrepDirectoryPolicy::Skip),
        "recurse" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fgrep: --directories=recurse is not supported in this bounded slice",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fgrep: unsupported --directories action '{value}'"),
        )),
    }
}

pub(super) fn fgrep_input_path_kind(path: &str) -> io::Result<FgrepInputPathKind> {
    let file_type = fs::metadata(path)?.file_type();
    if file_type.is_file() {
        Ok(FgrepInputPathKind::Regular)
    } else if file_type.is_dir() {
        Ok(FgrepInputPathKind::Directory)
    } else if file_type.is_char_device()
        || file_type.is_block_device()
        || file_type.is_fifo()
        || file_type.is_socket()
    {
        Ok(FgrepInputPathKind::Device)
    } else {
        Ok(FgrepInputPathKind::Other)
    }
}
