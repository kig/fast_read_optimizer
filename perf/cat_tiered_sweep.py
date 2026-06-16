#!/usr/bin/env python3
import argparse
import ctypes
import json
import shutil
import statistics
import subprocess
import time
from pathlib import Path


DEFAULT_SIZES = ["1K", "4K", "16K", "64K", "256K", "1M", "4M", "16M", "64M", "256M", "1G"]
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
    if size_bytes <= 1 << 20:
        return 25
    if size_bytes <= 64 << 20:
        return 10
    return 3


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


LIBC = ctypes.CDLL(None, use_errno=True)
POSIX_FADV_DONTNEED = 4


def warm_cache(path: Path):
    with path.open("rb", buffering=0) as handle:
        while handle.read(1 << 20):
            pass


def evict_cache(path: Path):
    handle = path.open("rb", buffering=0)
    try:
        result = LIBC.posix_fadvise(
            handle.fileno(),
            ctypes.c_longlong(0),
            ctypes.c_longlong(0),
            POSIX_FADV_DONTNEED,
        )
        if result != 0:
            raise OSError(result, f"posix_fadvise(DONTNEED) failed for {path}")
    finally:
        handle.close()


def time_command(cmd):
    started = time.perf_counter_ns()
    run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.perf_counter_ns() - started) / 1e6


def build_binary(repo_root: Path, output_path: Path, inline_max_bytes: int):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    source = repo_root / "examples" / "cat_tiered_exec.S"
    fro_path = repo_root / "target" / "release" / "fro"
    cmd = [
        "cc",
        "-x",
        "assembler-with-cpp",
        "-nostdlib",
        "-static",
        "-no-pie",
        "-s",
        f"-DINLINE_MAX_BYTES={inline_max_bytes}",
        f'-DFRO_MULTICALL_PATH="{fro_path}"',
        str(source),
        "-o",
        str(output_path),
    ]
    run(cmd)


def create_fixture(fro_bin: Path, path: Path, size_token: str):
    run([str(fro_bin), "write", "--create", size_token, str(path)], stdout=subprocess.DEVNULL)


def prepare_cache(fixture: Path, cache_state: str):
    if cache_state == "hot":
        warm_cache(fixture)
    elif cache_state == "cold":
        evict_cache(fixture)
    else:
        raise ValueError(f"unsupported cache state: {cache_state}")


def bench_size(size_token: str, size_bytes: int, fixture: Path, commands, cache_state: str):
    rows = []
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count(size_bytes)):
            prepare_cache(fixture, cache_state)
            samples.append(time_command(cmd))
        median_ms = statistics.median(samples)
        gbps = size_bytes / (median_ms / 1000.0) / 1_000_000_000
        rows.append(
            {
                "command": label,
                "cache_state": cache_state,
                "size": size_token,
                "size_bytes": size_bytes,
                "median_ms": median_ms,
                "gbps": gbps,
                "samples_ms": samples,
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--inline-max", default="512K")
    parser.add_argument("--sizes", nargs="*", default=DEFAULT_SIZES)
    parser.add_argument("--test-dir", type=Path, default=None)
    parser.add_argument("--cache-state", choices=["hot", "cold", "both"], default="both")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    fro_bin = repo_root / "target" / "release" / "fro"
    if not fro_bin.exists():
        raise SystemExit(f"missing release binary: {fro_bin}")
    if shutil.which("cc") is None:
        raise SystemExit("missing compiler: cc")

    inline_max_bytes = parse_size(args.inline_max)
    tiered_bin = repo_root / "target" / "startup-floor" / "cat_tiered_exec"
    build_binary(repo_root, tiered_bin, inline_max_bytes)

    sweep_root = (
        args.test_dir.resolve()
        if args.test_dir is not None
        else (repo_root / "target" / "tmp-test" / "cat-tiered-sweep")
    )
    if sweep_root.exists() and args.test_dir is None:
        shutil.rmtree(sweep_root)
    sweep_root.mkdir(parents=True, exist_ok=True)
    cache_states = ["hot", "cold"] if args.cache_state == "both" else [args.cache_state]

    all_rows = []
    for size_token in args.sizes:
        size_bytes = parse_size(size_token)
        fixture = sweep_root / f"{size_token}.bin"
        if not fixture.exists() or fixture.stat().st_size != size_bytes:
            create_fixture(fro_bin, fixture, size_token)
        commands = {
            "system-cat": ["/bin/cat", str(fixture)],
            "fro-cat": [str(fro_bin), "cat", str(fixture)],
            "tiered-cat": [str(tiered_bin), str(fixture)],
        }
        for cache_state in cache_states:
            all_rows.extend(bench_size(size_token, size_bytes, fixture, commands, cache_state))

    output = {
        "inline_max_bytes": inline_max_bytes,
        "cache_states": cache_states,
        "test_dir": str(sweep_root),
        "sizes": args.sizes,
        "rows": all_rows,
    }
    if args.output is None:
        args.output = sweep_root / "summary.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print("cache\tcommand\tsize\tmedian_ms\tgbps")
    for row in all_rows:
        print(
            f"{row['cache_state']}\t{row['command']}\t{row['size']}\t{row['median_ms']:.3f}\t{row['gbps']:.3f}"
        )
    print(f"\nsummary-json\t{args.output}")


if __name__ == "__main__":
    main()
