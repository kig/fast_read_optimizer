#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from shlex import quote


LINE = b"needle alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu\n"


@dataclass(frozen=True)
class CommandSpec:
    label: str
    argv: tuple[str, ...]
    warm_paths: tuple[Path, ...]


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


def best_elapsed(command: CommandSpec, repeat: int) -> float:
    best = float("inf")
    for _ in range(repeat):
        for warm_path in command.warm_paths:
            warm_cache(warm_path)
        start = time.perf_counter()
        subprocess.run(
            command.argv,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        best = min(best, time.perf_counter() - start)
    return best


def format_ms(seconds: float) -> str:
    return f"{seconds * 1000.0:.2f}"


def format_gbps(seconds: float, input_bytes: int) -> str:
    return f"{(input_bytes / seconds) / 1e9:.2f}"


def git_state() -> tuple[str, str]:
    head = run_checked(("git", "rev-parse", "--short", "HEAD")).stdout.decode().strip()
    branch = run_checked(("git", "rev-parse", "--abbrev-ref", "HEAD")).stdout.decode().strip()
    dirty = subprocess.run(
        ("git", "diff", "--quiet"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode
    suffix = "dirty" if dirty else "clean"
    return branch, f"{head} ({suffix} working tree)"


def shell_pipeline(script: str, warm_path: Path, label: str) -> CommandSpec:
    return CommandSpec(label, ("bash", "-lc", script), (warm_path,))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark head/tail against practical local rooflines (`bin/wc` and cat-style pipe ceilings)."
    )
    parser.add_argument(
        "--work-dir",
        default="target/head-tail-roofline-bench",
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
    fixture = work_dir / "head-tail-text.txt"
    prefix = work_dir / "head-n4096-prefix.txt"
    fro = Path(args.fro)
    if not fro.exists():
        raise SystemExit(f"missing fro binary: {fro}")

    ensure_text_fixture(fixture, size_bytes)

    system_head = CommandSpec("system head -n 4096", ("head", "-n", "4096", str(fixture)), (fixture,))
    prefix.write_bytes(run_checked(system_head.argv).stdout)

    fro_head = CommandSpec("fro head -n 4096", (str(fro), "head", "-n", "4096", str(fixture)), (fixture,))
    prefix_wc = CommandSpec("bin/wc -l prefix", ("bin/wc", "-l", str(prefix)), (prefix,))

    system_tail_lines = CommandSpec("system tail -n 4096", ("tail", "-n", "4096", str(fixture)), (fixture,))
    fro_tail_lines = CommandSpec("fro tail -n 4096", (str(fro), "tail", "-n", "4096", str(fixture)), (fixture,))
    full_wc = CommandSpec("bin/wc -l full file", ("bin/wc", "-l", str(fixture)), (fixture,))

    fro_tail_bytes_file = CommandSpec(
        "fro tail -c 65536", (str(fro), "tail", "-c", "65536", str(fixture)), (fixture,)
    )
    system_tail_bytes_file = CommandSpec("system tail -c 65536", ("tail", "-c", "65536", str(fixture)), (fixture,))

    quoted_fixture = quote(str(fixture))
    quoted_fro = quote(str(fro))
    fro_tail_bytes_pipe = shell_pipeline(
        f"bin/cat {quoted_fixture} | {quoted_fro} tail -c 65536",
        fixture,
        "bin/cat | fro tail -c 65536",
    )
    system_tail_bytes_pipe = shell_pipeline(
        f"bin/cat {quoted_fixture} | tail -c 65536",
        fixture,
        "bin/cat | system tail -c 65536",
    )
    cat_pipe_ceiling = shell_pipeline(f"bin/cat {quoted_fixture} >/dev/null", fixture, "bin/cat >/dev/null")
    cat_wc_pipe = shell_pipeline(
        f"bin/cat {quoted_fixture} | bin/wc -c >/dev/null",
        fixture,
        "bin/cat | bin/wc -c >/dev/null",
    )

    verify_exact_outputs([fro_head, system_head])
    verify_exact_outputs([fro_tail_lines, system_tail_lines])
    verify_exact_outputs([fro_tail_bytes_file, system_tail_bytes_file])
    verify_exact_outputs([fro_tail_bytes_pipe, system_tail_bytes_pipe])

    head_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in [fro_head, system_head, prefix_wc]
    ]
    tail_line_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in [fro_tail_lines, system_tail_lines, full_wc]
    ]
    tail_file_byte_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in [fro_tail_bytes_file, system_tail_bytes_file]
    ]
    tail_pipe_results = []
    for command in [fro_tail_bytes_pipe, system_tail_bytes_pipe, cat_pipe_ceiling, cat_wc_pipe]:
        seconds = best_elapsed(command, args.repeat)
        tail_pipe_results.append(CaseResult(command.label, seconds, size_bytes / seconds / 1e9))

    branch, git_desc = git_state()
    prefix_bytes = prefix.stat().st_size
    head_roofline = next(result.seconds for result in head_results if result.label == prefix_wc.label)
    tail_roofline = next(result.seconds for result in tail_line_results if result.label == full_wc.label)
    cat_roofline = next(result.seconds for result in tail_pipe_results if result.label == cat_pipe_ceiling.label)

    print("head/tail roofline benchmark slice")
    print(f"branch: {branch}")
    print(f"git: {git_desc}")
    print(f"work_dir: {work_dir}")
    print(f"fixture: {fixture} ({size_bytes} bytes)")
    print(f"head prefix fixture: {prefix} ({prefix_bytes} bytes)")
    print(f"repeat: {args.repeat}")
    print()
    print("head -n 4096 regular file (hot page cache, best of repeat)")
    print("| Command | Seconds | Best ms | Relative to `bin/wc -l prefix` |")
    print("| --- | ---: | ---: | ---: |")
    for result in head_results:
        print(
            f"| {result.label} | {result.seconds:.4f} | {format_ms(result.seconds)} | {result.seconds / head_roofline:.2f}x |"
        )
    print()
    print("tail -n 4096 regular file (hot page cache, best of repeat)")
    print("| Command | Seconds | Best ms | Relative to `bin/wc -l full file` |")
    print("| --- | ---: | ---: | ---: |")
    for result in tail_line_results:
        print(
            f"| {result.label} | {result.seconds:.4f} | {format_ms(result.seconds)} | {result.seconds / tail_roofline:.2f}x |"
        )
    print()
    print("tail -c 65536 regular file (hot page cache, best of repeat)")
    print("| Command | Seconds | Best ms |")
    print("| --- | ---: | ---: |")
    for result in tail_file_byte_results:
        print(f"| {result.label} | {result.seconds:.4f} | {format_ms(result.seconds)} |")
    print()
    print("tail -c 65536 pipe path (hot page cache producer, best of repeat)")
    print("| Command | Seconds | Effective GB/s | Relative to `bin/cat >/dev/null` |")
    print("| --- | ---: | ---: | ---: |")
    for result in tail_pipe_results:
        print(
            f"| {result.label} | {result.seconds:.4f} | {format_gbps(result.seconds, size_bytes)} | {result.seconds / cat_roofline:.2f}x |"
        )
    print()
    print("Notes:")
    print("- Output parity is verified before timing for the head/tail cases under test.")
    print("- `head -n 4096` is compared against `bin/wc -l` on the exact emitted prefix bytes, not the full file.")
    print("- `tail -n 4096` is compared against `bin/wc -l` on the full file because the suffix split still depends on newline discovery over the source file.")
    print("- The pipe slice treats `bin/cat >/dev/null` as the cat-style splice/vmsplice ceiling and also reports `bin/cat | bin/wc -c` as a scan-like pipe baseline.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
