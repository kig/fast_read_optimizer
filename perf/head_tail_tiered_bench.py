#!/usr/bin/env python3
import argparse
import ctypes
import json
import os
import shutil
import statistics
import subprocess
import time
from pathlib import Path


DEFAULT_COUNTS = ["1K", "4K", "16K", "64K", "256K", "512K"]
SUFFIXES = {
    "K": 1024,
    "M": 1024 * 1024,
    "G": 1024 * 1024 * 1024,
}
LIBC = ctypes.CDLL(None, use_errno=True)
POSIX_FADV_DONTNEED = 4


def parse_size(token: str) -> int:
    token = token.strip().upper()
    if token[-1] in SUFFIXES:
        return int(token[:-1]) * SUFFIXES[token[-1]]
    return int(token)


def repetition_count(size_bytes: int) -> int:
    if size_bytes <= 16 << 10:
        return 31
    if size_bytes <= 256 << 10:
        return 21
    return 11


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


def resolve_system_command(name: str) -> str:
    for prefix in ("/usr/bin", "/bin", "/usr/sbin", "/sbin"):
        candidate = Path(prefix) / name
        if candidate.is_file():
            return str(candidate)
    resolved = shutil.which(name)
    if resolved is None:
        raise SystemExit(f"missing system command: {name}")
    return resolved


def build_binary(source: Path, output_path: Path, fro_path: Path, inline_max_bytes: int):
    output_path.parent.mkdir(parents=True, exist_ok=True)
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
    run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def ensure_shadow_link(target: Path, link_path: Path):
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if link_path.exists() or link_path.is_symlink():
        link_path.unlink()
    os.symlink(target, link_path)


def generate_fixture(path: Path, size_bytes: int):
    if path.exists() and path.stat().st_size == size_bytes:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    pattern = bytes((idx % 251 for idx in range(4096)))
    with path.open("wb") as handle:
        remaining = size_bytes
        while remaining > 0:
            chunk = pattern[: min(len(pattern), remaining)]
            handle.write(chunk)
            remaining -= len(chunk)


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


def prepare_cache(path: Path, cache_state: str):
    if cache_state == "hot":
        warm_cache(path)
    elif cache_state == "cold":
        evict_cache(path)
    else:
        raise ValueError(f"unsupported cache state: {cache_state}")


def time_command(cmd):
    started = time.perf_counter_ns()
    run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.perf_counter_ns() - started) / 1e6


def verify_same_output(commands):
    baseline = None
    for label, cmd in commands:
        completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if completed.returncode != 0:
            raise RuntimeError(
                f"{label} failed with {completed.returncode}\n"
                f"stdout={completed.stdout!r}\n"
                f"stderr={completed.stderr!r}"
            )
        current = (completed.stdout, completed.stderr)
        if baseline is None:
            baseline = (label, current)
            continue
        if current != baseline[1]:
            raise RuntimeError(
                f"output mismatch: {label} vs {baseline[0]}\n"
                f"{label} stdout={completed.stdout!r}\n"
                f"{baseline[0]} stdout={baseline[1][0]!r}\n"
                f"{label} stderr={completed.stderr!r}\n"
                f"{baseline[0]} stderr={baseline[1][1]!r}"
            )


def bench_case(utility: str, size_token: str, size_bytes: int, fixture: Path, commands, cache_state: str):
    rows = []
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count(size_bytes)):
            prepare_cache(fixture, cache_state)
            samples.append(time_command(cmd))
        rows.append(
            {
                "utility": utility,
                "count": size_token,
                "count_bytes": size_bytes,
                "fixture": str(fixture),
                "cache_state": cache_state,
                "command": label,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    return rows


def fallback_validation_commands(fro_bin: Path, proto_bin: Path, utility: str, fixture: Path):
    return [
        ("direct-fro-fallback", [str(fro_bin), utility, "-n", "1", str(fixture)]),
        ("tiered-fallback", [str(proto_bin), "-n", "1", str(fixture)]),
    ]


def bounded_validation_commands(fro_bin: Path, proto_bin: Path, utility: str, fixture: Path, count: int):
    count_arg = str(count)
    return [
        ("system", [resolve_system_command(utility), "-c", count_arg, str(fixture)]),
        ("direct-fro", [str(fro_bin), utility, "--direct", "-c", count_arg, str(fixture)]),
        ("tiered", [str(proto_bin), "-c", count_arg, str(fixture)]),
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--inline-max", default="256K")
    parser.add_argument("--counts", nargs="*", default=DEFAULT_COUNTS)
    parser.add_argument("--test-dir", type=Path, default=Path("/data/fro-test/head-tail-tiered-bench"))
    parser.add_argument("--cache-state", choices=["hot", "cold", "both"], default="hot")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    fro_bin = repo_root / "target" / "release" / "fro"
    if not fro_bin.exists():
        raise SystemExit(f"missing release binary: {fro_bin}")
    if shutil.which("cc") is None:
        raise SystemExit("missing compiler: cc")

    inline_max_bytes = parse_size(args.inline_max)
    startup_root = repo_root / "target" / "startup-floor"
    head_proto = startup_root / "head_tiered_exec"
    tail_proto = startup_root / "tail_tiered_exec"
    shadow_head = startup_root / "shadow" / "head"
    shadow_tail = startup_root / "shadow" / "tail"
    build_binary(repo_root / "examples" / "head_tiered_exec.S", head_proto, fro_bin, inline_max_bytes)
    build_binary(repo_root / "examples" / "tail_tiered_exec.S", tail_proto, fro_bin, inline_max_bytes)
    ensure_shadow_link(fro_bin, shadow_head)
    ensure_shadow_link(fro_bin, shadow_tail)

    sweep_root = args.test_dir.resolve()
    sweep_root.mkdir(parents=True, exist_ok=True)
    cache_states = ["hot", "cold"] if args.cache_state == "both" else [args.cache_state]
    system_head = resolve_system_command("head")
    system_tail = resolve_system_command("tail")

    validation_fixture = sweep_root / "validation-1536.bin"
    generate_fixture(validation_fixture, 1536)
    bounded_cases = [
        ("head", 0),
        ("head", 37),
        ("head", 1024),
        ("head", 4096),
        ("tail", 0),
        ("tail", 37),
        ("tail", 1024),
        ("tail", 4096),
    ]
    for utility, count in bounded_cases:
        proto_bin = head_proto if utility == "head" else tail_proto
        verify_same_output(bounded_validation_commands(fro_bin, proto_bin, utility, validation_fixture, count))
        verify_same_output(fallback_validation_commands(fro_bin, proto_bin, utility, validation_fixture))

    all_rows = []
    for count_token in args.counts:
        count_bytes = parse_size(count_token)
        fixture = sweep_root / f"{count_token}.bin"
        generate_fixture(fixture, count_bytes)
        shared_args = ["--direct", "-c", str(count_bytes), str(fixture)]
        cases = {
            "head": {
                "system": [system_head, "-c", str(count_bytes), str(fixture)],
                "direct-fro": [str(fro_bin), "head", *shared_args],
                "shadow-fro": [str(shadow_head), *shared_args],
                "tiered-head": [str(head_proto), "-c", str(count_bytes), str(fixture)],
            },
            "tail": {
                "system": [system_tail, "-c", str(count_bytes), str(fixture)],
                "direct-fro": [str(fro_bin), "tail", *shared_args],
                "shadow-fro": [str(shadow_tail), *shared_args],
                "tiered-tail": [str(tail_proto), "-c", str(count_bytes), str(fixture)],
            },
        }
        verify_same_output([("system-head", cases["head"]["system"]), ("tiered-head", cases["head"]["tiered-head"])])
        verify_same_output([("system-tail", cases["tail"]["system"]), ("tiered-tail", cases["tail"]["tiered-tail"])])
        for cache_state in cache_states:
            all_rows.extend(bench_case("head", count_token, count_bytes, fixture, cases["head"], cache_state))
            all_rows.extend(bench_case("tail", count_token, count_bytes, fixture, cases["tail"], cache_state))

    output = {
        "inline_max_bytes": inline_max_bytes,
        "cache_states": cache_states,
        "test_dir": str(sweep_root),
        "counts": args.counts,
        "rows": all_rows,
    }
    if args.output is None:
        args.output = sweep_root / "head-tail-tiered-summary.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print("cache\tutility\tcount\tcommand\tmedian_ms")
    for row in all_rows:
        print(
            f"{row['cache_state']}\t{row['utility']}\t{row['count']}\t{row['command']}\t{row['median_ms']:.3f}"
        )
    print(f"\nsummary-json\t{args.output}")


if __name__ == "__main__":
    main()
