#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


LINE = b"needle alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu\n"


@dataclass(frozen=True)
class CommandSpec:
    label: str
    argv: tuple[str, ...]


@dataclass(frozen=True)
class CaseResult:
    label: str
    seconds: float
    metric: float


def parse_size(text: str) -> int:
    value = text.strip().lower()
    if not value:
        raise ValueError("empty size")
    split = 0
    while split < len(value) and value[split].isdigit():
        split += 1
    number = int(value[:split])
    suffix = value[split:].strip()
    mult = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gib": 1024**3,
    }.get(suffix)
    if mult is None:
        raise ValueError(f"unsupported size suffix: {text}")
    return number * mult


def ensure_text_fixture(path: Path, size_bytes: int) -> None:
    if path.exists() and path.stat().st_size == size_bytes:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as fh:
        remaining = size_bytes
        while remaining > 0:
            chunk = LINE[: min(len(LINE), remaining)]
            fh.write(chunk)
            remaining -= len(chunk)


def warm_cache(path: Path, passes: int = 2) -> None:
    for _ in range(passes):
        with path.open("rb") as fh:
            while fh.read(4 * 1024 * 1024):
                pass


def run_checked(argv: tuple[str, ...]) -> subprocess.CompletedProcess[bytes]:
    completed = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(argv)}\n"
            f"stdout:\n{completed.stdout.decode(errors='replace')}\n"
            f"stderr:\n{completed.stderr.decode(errors='replace')}"
        )
    return completed


def verify_wc_outputs(commands: list[CommandSpec]) -> None:
    baseline_tokens: list[str] | None = None
    baseline_stderr: bytes | None = None
    baseline_label: str | None = None
    for command in commands:
        completed = run_checked(command.argv)
        tokens = completed.stdout.decode(errors="replace").split()
        if baseline_tokens is None:
            baseline_tokens = tokens
            baseline_stderr = completed.stderr
            baseline_label = command.label
            continue
        if tokens != baseline_tokens or completed.stderr != baseline_stderr:
            raise RuntimeError(
                f"wc output mismatch: {command.label} vs {baseline_label}\n"
                f"{command.label} stdout={completed.stdout!r}\n"
                f"{baseline_label} stdout={baseline_tokens!r}\n"
                f"{command.label} stderr={completed.stderr!r}\n"
                f"{baseline_label} stderr={baseline_stderr!r}"
            )


def verify_exact_outputs(commands: list[CommandSpec]) -> None:
    baseline_stdout: bytes | None = None
    baseline_stderr: bytes | None = None
    baseline_label: str | None = None
    for command in commands:
        completed = run_checked(command.argv)
        if baseline_stdout is None:
            baseline_stdout = completed.stdout
            baseline_stderr = completed.stderr
            baseline_label = command.label
            continue
        if completed.stdout != baseline_stdout or completed.stderr != baseline_stderr:
            raise RuntimeError(
                f"output mismatch: {command.label} vs {baseline_label}\n"
                f"{command.label} stdout={completed.stdout!r}\n"
                f"{baseline_label} stdout={baseline_stdout!r}\n"
                f"{command.label} stderr={completed.stderr!r}\n"
                f"{baseline_label} stderr={baseline_stderr!r}"
            )


def best_elapsed(argv: tuple[str, ...], repeat: int, warm_path: Path) -> float:
    best = float("inf")
    for _ in range(repeat):
        warm_cache(warm_path)
        start = time.perf_counter()
        subprocess.run(
            argv,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        best = min(best, time.perf_counter() - start)
    return best


def find_uutils() -> str | None:
    candidate = os.environ.get("UUTILS_COREUTILS") or shutil.which("coreutils")
    return candidate


def subcommand_available(multicall: str, subcommand: str) -> bool:
    try:
        completed = subprocess.run(
            (multicall, subcommand, "--help"),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return False
    return completed.returncode == 0


def format_gbps(seconds: float, input_bytes: int) -> str:
    return f"{(input_bytes / seconds) / 1e9:.2f}"


def format_ms(seconds: float) -> str:
    return f"{seconds * 1000.0:.2f}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark a practical coreutils flag-parity slice against GNU coreutils and optional uutils."
    )
    parser.add_argument(
        "--work-dir",
        default="target/coreutils-flag-parity-bench",
        help="Directory for generated fixtures (default: %(default)s)",
    )
    parser.add_argument(
        "--size",
        default="256MiB",
        help="Generated text fixture size (default: %(default)s)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=5,
        help="Number of measured runs per command (default: %(default)s)",
    )
    parser.add_argument(
        "--fro",
        default="target/release/fro",
        help="Path to the built fro binary (default: %(default)s)",
    )
    args = parser.parse_args()

    work_dir = Path(args.work_dir)
    size_bytes = parse_size(args.size)
    fixture = work_dir / "wc-tail-text.txt"
    fro = Path(args.fro)
    if not fro.exists():
        raise SystemExit(f"missing fro binary: {fro}")

    ensure_text_fixture(fixture, size_bytes)

    system_wc = CommandSpec("system wc", ("wc", "-l", "-w", "-m", "-L", str(fixture)))
    system_tail_lines = CommandSpec("system tail -n 4096", ("tail", "-n", "4096", str(fixture)))
    system_tail_bytes = CommandSpec("system tail -c 65536", ("tail", "-c", "65536", str(fixture)))

    fro_wc = CommandSpec(
        "fro wc --no-direct",
        (str(fro), "wc", "--no-direct", "-l", "-w", "-m", "-L", str(fixture)),
    )
    fro_tail_lines = CommandSpec("fro tail -n 4096", (str(fro), "tail", "-n", "4096", str(fixture)))
    fro_tail_bytes = CommandSpec("fro tail -c 65536", (str(fro), "tail", "-c", "65536", str(fixture)))

    wc_commands = [fro_wc, system_wc]
    tail_line_commands = [fro_tail_lines, system_tail_lines]
    tail_byte_commands = [fro_tail_bytes, system_tail_bytes]

    uutils = find_uutils()
    uutils_note = "unavailable"
    if uutils and subcommand_available(uutils, "wc"):
        wc_commands.append(CommandSpec("uutils wc", (uutils, "wc", "-l", "-w", "-m", "-L", str(fixture))))
        uutils_note = f"available via {uutils}"
    if uutils and subcommand_available(uutils, "tail"):
        tail_line_commands.append(
            CommandSpec("uutils tail -n 4096", (uutils, "tail", "-n", "4096", str(fixture)))
        )
        tail_byte_commands.append(
            CommandSpec("uutils tail -c 65536", (uutils, "tail", "-c", "65536", str(fixture)))
        )
        if uutils_note == "unavailable":
            uutils_note = f"partial via {uutils}"
    grep_available = bool(uutils and subcommand_available(uutils, "grep"))

    verify_wc_outputs(wc_commands)
    verify_exact_outputs(tail_line_commands)
    verify_exact_outputs(tail_byte_commands)

    wc_results = [
        CaseResult(command.label, best_elapsed(command.argv, args.repeat, fixture), 0.0)
        for command in wc_commands
    ]
    wc_results = [
        CaseResult(result.label, result.seconds, size_bytes / result.seconds / 1e9)
        for result in wc_results
    ]
    tail_line_results = [
        CaseResult(command.label, best_elapsed(command.argv, args.repeat, fixture), 0.0)
        for command in tail_line_commands
    ]
    tail_byte_results = [
        CaseResult(command.label, best_elapsed(command.argv, args.repeat, fixture), 0.0)
        for command in tail_byte_commands
    ]

    print("coreutils flag-parity benchmark slice")
    print(f"work_dir: {work_dir}")
    print(f"fixture: {fixture} ({size_bytes} bytes)")
    print(f"repeat: {args.repeat}")
    print(f"uutils: {uutils_note}")
    print(f"uutils grep available: {'yes' if grep_available else 'no'}")
    print()
    print("wc -l -w -m -L (hot page cache, best of repeat)")
    print("| Command | Seconds | Effective GB/s |")
    print("| --- | ---: | ---: |")
    for result in wc_results:
        print(f"| {result.label} | {result.seconds:.4f} | {result.metric:.2f} |")
    print()
    print("tail -n 4096 (hot page cache, best of repeat)")
    print("| Command | Seconds | Best ms |")
    print("| --- | ---: | ---: |")
    for result in tail_line_results:
        print(f"| {result.label} | {result.seconds:.4f} | {format_ms(result.seconds)} |")
    print()
    print("tail -c 65536 (hot page cache, best of repeat)")
    print("| Command | Seconds | Best ms |")
    print("| --- | ---: | ---: |")
    for result in tail_byte_results:
        print(f"| {result.label} | {result.seconds:.4f} | {format_ms(result.seconds)} |")
    print()
    print("Notes:")
    print("- The harness verifies output parity before timing.")
    print("- `wc -c` is intentionally not included because GNU `wc` can answer it from metadata.")
    print("- Tail is reported as latency, not throughput, because regular-file tail does not read the whole file.")
    print("- `fgrep` is not part of this first slice because the local uutils multicall does not expose `grep`.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
