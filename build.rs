use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const RUNNERS: &[(&str, &str)] = &[
    ("cat_tiered_exec.S", "cat"),
    ("cksum_tiered_exec.S", "cksum"),
    ("cmp_tiered_exec.S", "cmp"),
    ("cp_tiered_exec.S", "cp"),
    ("fgrep_tiered_exec.S", "fgrep"),
    ("find_tiered_exec.S", "find"),
    ("head_tiered_exec.S", "head"),
    ("mv_tiered_exec.S", "mv"),
    ("rm_tiered_exec.S", "rm"),
    ("tail_tiered_exec.S", "tail"),
    ("wc_l_tiered_exec.S", "wc"),
];

fn main() {
    if env::var_os("CARGO_CFG_TARGET_OS").as_deref() != Some(std::ffi::OsStr::new("linux")) {
        return;
    }

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=examples/fro_exec_path.inc");
    for (source, _) in RUNNERS {
        println!("cargo:rerun-if-changed=examples/{source}");
    }

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("missing CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("missing OUT_DIR"));
    let profile_dir = profile_dir_from_out_dir(&out_dir);
    let coreutils_dir = profile_dir.join("coreutils");
    fs::create_dir_all(&coreutils_dir).expect("create coreutils dir");

    let fro_path = profile_dir.join("fro");
    let compiler = cc::Build::new()
        .cargo_metadata(false)
        .warnings(false)
        .get_compiler();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    if target_arch == "aarch64" {
        for (_, output_name) in RUNNERS {
            build_runner_rust(&fro_path, output_name, &out_dir, &coreutils_dir);
        }
    } else {
        for (source, output_name) in RUNNERS {
            build_runner(
                &compiler,
                &manifest_dir.join("examples").join(source),
                &coreutils_dir.join(output_name),
                &fro_path,
            );
        }
    }

    link_fro(&coreutils_dir.join("fro"));
}

fn build_runner_rust(fro_path: &Path, output_name: &str, out_dir: &Path, coreutils_dir: &Path) {
    use std::process::Command;
    // Create a tiny Rust wrapper that execs the fro multicall path with argv[0] set to output_name
    let src = format!(r###"
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;
use std::process::exit;

extern "C" {
    fn execv(path: *const c_char, argv: *const *const c_char) -> i32;
}

fn main() {{
    let fro = r#\"{}\"#;
    let prog = r#\"{}\"#;
    let c_fro = CString::new(fro).expect("invalid fro path");
    let mut c_args: Vec<CString> = Vec::new();
    c_args.push(CString::new(prog).unwrap());
    for arg in std::env::args().skip(1) {{
        c_args.push(CString::new(arg).unwrap());
    }}
    let mut argv: Vec<*const c_char> = c_args.iter().map(|s| s.as_ptr()).collect();
    argv.push(ptr::null());
    unsafe {{
        execv(c_fro.as_ptr(), argv.as_ptr());
        let err = std::io::Error::last_os_error();
        eprintln!("execv failed: {}", err);
        exit(1);
    }}
}}
"###, fro_path.display(), output_name);

    let src_path = out_dir.join(format!("runner_{}.rs", output_name));
    std::fs::write(&src_path, src).expect("write runner source");
    let out_path = coreutils_dir.join(output_name);
    let status = Command::new("rustc")
        .arg(src_path)
        .arg("-C").arg("opt-level=s")
        .arg("-C").arg("lto")
        .arg("-C").arg("codegen-units=1")
        .arg("-C").arg("panic=abort")
        .arg("-C").arg("link-arg=-s")
        .arg("-o").arg(out_path)
        .status()
        .expect("failed to spawn rustc to build runner");
    assert!(status.success(), "failed to build rust runner {}", output_name);
}

fn profile_dir_from_out_dir(out_dir: &Path) -> PathBuf {
    out_dir
        .ancestors()
        .find(|path| path.file_name().is_some_and(|name| name == "build"))
        .and_then(Path::parent)
        .expect("OUT_DIR missing build ancestor")
        .to_path_buf()
}

fn build_runner(compiler: &cc::Tool, source: &Path, output: &Path, fro_path: &Path) {
    let mut command = Command::new(compiler.path());
    command.args(compiler.args());
    command
        .arg("-x")
        .arg("assembler-with-cpp")
        .arg("-nostdlib")
        .arg("-static")
        .arg("-no-pie")
        .arg("-s")
        .arg(format!("-DFRO_MULTICALL_PATH=\"{}\"", fro_path.display()))
        .arg(source)
        .arg("-o")
        .arg(output);
    let status = command
        .status()
        .unwrap_or_else(|err| panic!("failed to spawn {command:?}: {err}"));
    assert!(
        status.success(),
        "failed to build asm runner {}",
        output.display()
    );
}

fn link_fro(link_path: &Path) {
    match fs::remove_file(link_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => panic!("remove old {}: {err}", link_path.display()),
    }
    std::os::unix::fs::symlink(Path::new("..").join("fro"), link_path)
        .unwrap_or_else(|err| panic!("link {} -> ../fro: {err}", link_path.display()));
}
