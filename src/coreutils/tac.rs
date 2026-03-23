use super::*;

pub(super) fn run_tac(args: &[String]) -> io::Result<()> {
    let (io_mode, files) = parse_io_mode(&args[1..])?;
    let inputs = parse_stream_inputs(files);
    let out = stdout_buf_writer()?;
    for input in &inputs {
        let data = loaded_or_stream_bytes(input, io_mode)?;
        let mut parts = data
            .split_inclusive(|&byte| byte == b'\n')
            .collect::<Vec<_>>();
        if parts.is_empty() && !data.is_empty() {
            parts.push(data.as_slice());
        }
        for part in parts.into_iter().rev() {
            out.write_all(part)?;
        }
    }
    out.into_inner()
}
