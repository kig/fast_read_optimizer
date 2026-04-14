#!/usr/bin/env python3
import argparse
import json
import shutil
import statistics
import subprocess
import time
from pathlib import Path


DEFAULT_SIZES = ["1K", "4K", "16K", "64K", "256K", "1M"]
SUFFIXES = {
    "K": 1024,
    "M": 1024 * 1024,
    "G": 1024 * 1024 * 1024,
}


def parse_size(token: str) -> int:
    token = token.strip().upper()
    if token[-1] in SUFFIXES:
        return int(token[:-1]) * SUFFIXES[token[-1]]
    return int(token)


def repetition_count(size_bytes: int) -> int:
    if size_bytes <= 64 * 1024:
        return 31
    if size_bytes <= 1024 * 1024:
        return 15
    return 7


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


def warm_cache(path: Path):
    with path.open("rb", buffering=0) as handle:
        while handle.read(1 << 20):
            pass


def build_binary(source: Path, output_path: Path, fro_path: Path, defines: dict[str, str] | None = None):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "cc",
        "-x",
        "assembler-with-cpp",
        "-nostdlib",
        "-static",
        "-no-pie",
        "-s",
        f'-DFRO_MULTICALL_PATH="{fro_path}"',
    ]
    if defines:
        for key, value in defines.items():
            cmd.append(f"-D{key}={value}")
    cmd.extend([str(source), "-o", str(output_path)])
    run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def create_fixture(fro_bin: Path, path: Path, size_token: str):
    run(
        [str(fro_bin), "write", "--create", size_token, str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def ensure_absent(path: Path):
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()


def time_command(cmd):
    started = time.perf_counter_ns()
    run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.perf_counter_ns() - started) / 1e6


def validate(cp_bin: Path, mv_bin: Path, root: Path):
    validation_root = root / "validation"
    shutil.rmtree(validation_root, ignore_errors=True)
    (validation_root / "dst-dir").mkdir(parents=True, exist_ok=True)
    (validation_root / "mv-dir").mkdir(parents=True, exist_ok=True)
    src = validation_root / "src.bin"
    src.write_bytes(b"cp-tiered-check\n")

    file_target = validation_root / "file-target.bin"
    run([str(cp_bin), str(src), str(file_target)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if file_target.read_bytes() != src.read_bytes():
        raise SystemExit("cp file->file validation failed")

    dir_target = validation_root / "dst-dir"
    run([str(cp_bin), str(src), str(dir_target)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    copied = dir_target / src.name
    if copied.read_bytes() != src.read_bytes():
        raise SystemExit("cp file->dir validation failed")

    mv_src = validation_root / "mv-src.bin"
    mv_src.write_bytes(b"mv-tiered-check\n")
    mv_file_target = validation_root / "mv-file-target.bin"
    run([str(mv_bin), str(mv_src), str(mv_file_target)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if mv_src.exists() or mv_file_target.read_bytes() != b"mv-tiered-check\n":
        raise SystemExit("mv file->file validation failed")

    mv_dir_src = validation_root / "mv-dir-src.bin"
    mv_dir_src.write_bytes(b"mv-dir-check\n")
    run([str(mv_bin), str(mv_dir_src), str(validation_root / "mv-dir")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    moved = validation_root / "mv-dir" / mv_dir_src.name
    if mv_dir_src.exists() or moved.read_bytes() != b"mv-dir-check\n":
        raise SystemExit("mv file->dir validation failed")


def bench_cp_size(size_token: str, size_bytes: int, fixture: Path, commands: dict[str, list[str]], dest_dir: Path):
    rows = []
    target = dest_dir / fixture.name
    dest_arg = f"{dest_dir}/"
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count(size_bytes)):
            ensure_absent(target)
            warm_cache(fixture)
            samples.append(time_command(cmd + [str(fixture), dest_arg]))
            if target.read_bytes() != fixture.read_bytes():
                raise SystemExit(f"cp benchmark corrupted output for {label} {size_token}")
        rows.append(
            {
                "command": label,
                "family": "cp",
                "size": size_token,
                "size_bytes": size_bytes,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    ensure_absent(target)
    return rows


def bench_mv_size(size_token: str, size_bytes: int, fixture: Path, commands: dict[str, list[str]], staging_dir: Path, dest_dir: Path):
    rows = []
    target = dest_dir / fixture.name
    dest_arg = f"{dest_dir}/"
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count(size_bytes)):
            ensure_absent(target)
            staged = staging_dir / fixture.name
            ensure_absent(staged)
            shutil.copyfile(fixture, staged)
            warm_cache(staged)
            samples.append(time_command(cmd + [str(staged), dest_arg]))
            if staged.exists() or target.read_bytes() != fixture.read_bytes():
                raise SystemExit(f"mv benchmark corrupted output for {label} {size_token}")
            ensure_absent(target)
        rows.append(
            {
                "command": label,
                "family": "mv",
                "size": size_token,
                "size_bytes": size_bytes,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--test-dir", default=Path("/data/fro-test"), type=Path)
    parser.add_argument("--sizes", nargs="*", default=DEFAULT_SIZES)
    parser.add_argument("--cp-inline-max", default="256K")
    parser.add_argument("--cp-shadow-max", default="1M")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    fro_bin = repo_root / "target" / "release" / "fro"
    if not fro_bin.exists():
        raise SystemExit(f"missing release binary: {fro_bin}")
    if shutil.which("cc") is None:
        raise SystemExit("missing compiler: cc")

    startup_root = repo_root / "target" / "startup-floor"
    cp_tiered = startup_root / "cp_tiered_exec"
    cp_shadow = startup_root / "cp_shadow_exec"
    mv_tiered = startup_root / "mv_tiered_exec"
    mv_shadow = startup_root / "mv_shadow_exec"

    build_binary(
        repo_root / "examples" / "cp_tiered_exec.S",
        cp_tiered,
        fro_bin,
        {"INLINE_MAX_BYTES": str(parse_size(args.cp_inline_max))},
    )
    build_binary(
        repo_root / "examples" / "cp_tiered_exec.S",
        cp_shadow,
        fro_bin,
        {
            "INLINE_MAX_BYTES": str(parse_size(args.cp_shadow_max)),
            "NO_FALLBACK": "1",
        },
    )
    build_binary(repo_root / "examples" / "mv_tiered_exec.S", mv_tiered, fro_bin)
    build_binary(
        repo_root / "examples" / "mv_tiered_exec.S",
        mv_shadow,
        fro_bin,
        {"NO_FALLBACK": "1"},
    )

    run_root = args.test_dir.resolve() / "startup-floor-cp-mv"
    run_root.mkdir(parents=True, exist_ok=True)
    validate(cp_tiered, mv_tiered, run_root)

    fixtures_dir = run_root / "fixtures"
    cp_dest_dir = run_root / "cp-dest"
    mv_stage_dir = run_root / "mv-stage"
    mv_dest_dir = run_root / "mv-dest"
    for path in (fixtures_dir, cp_dest_dir, mv_stage_dir, mv_dest_dir):
        path.mkdir(parents=True, exist_ok=True)

    rows = []
    for size_token in args.sizes:
        size_bytes = parse_size(size_token)
        fixture = fixtures_dir / f"{size_token}.bin"
        if not fixture.exists() or fixture.stat().st_size != size_bytes:
            create_fixture(fro_bin, fixture, size_token)

        cp_commands = {
            "system-cp": ["/bin/cp"],
            "fro-cp-direct": [str(fro_bin), "cp", "--direct", "--direct-write"],
            "shadow-cp-inline": [str(cp_shadow)],
            f"tiered-cp-{args.cp_inline_max.lower()}": [str(cp_tiered)],
        }
        mv_commands = {
            "system-mv": ["/bin/mv"],
            "fro-mv": [str(fro_bin), "mv"],
            "shadow-mv-rename": [str(mv_shadow)],
            "tiered-mv": [str(mv_tiered)],
        }

        rows.extend(bench_cp_size(size_token, size_bytes, fixture, cp_commands, cp_dest_dir))
        rows.extend(bench_mv_size(size_token, size_bytes, fixture, mv_commands, mv_stage_dir, mv_dest_dir))

    output = {
        "test_dir": str(run_root),
        "cp_inline_max_bytes": parse_size(args.cp_inline_max),
        "cp_shadow_max_bytes": parse_size(args.cp_shadow_max),
        "sizes": args.sizes,
        "rows": rows,
    }
    if args.output is None:
        args.output = run_root / "summary.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print("family\tcommand\tsize\tmedian_ms")
    for row in rows:
        print(f"{row['family']}\t{row['command']}\t{row['size']}\t{row['median_ms']:.3f}")
    print(f"\nsummary-json\t{args.output}")


if __name__ == "__main__":
    main()
