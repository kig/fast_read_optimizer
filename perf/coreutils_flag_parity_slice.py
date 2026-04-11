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
from shlex import quote


LINE = b"needle alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu\n"


@dataclass(frozen=True)
class CommandSpec:
    label: str
    argv: tuple[str, ...]
    warm_paths: tuple[Path, ...] = ()


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


def verify_tokenized_outputs(commands: list[CommandSpec]) -> None:
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
                f"tokenized output mismatch: {command.label} vs {baseline_label}\n"
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


def table_cell(text: str) -> str:
    return text.replace("|", "\\|")


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
        description="Benchmark practical coreutils file/pipe proposition slices against GNU coreutils and optional uutils."
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
    quoted_fixture = str(fixture)
    quoted_fro = str(fro)

    cat_file_outputs = [
        CommandSpec("fro cat --no-direct", (quoted_fro, "cat", "--no-direct", quoted_fixture), (fixture,)),
        CommandSpec("system cat", ("cat", quoted_fixture), (fixture,)),
    ]
    cat_devnull_commands = [
        CommandSpec(
            "fro cat --no-direct -> /dev/null",
            (quoted_fro, "cat", "--no-direct", quoted_fixture),
            (fixture,),
        ),
        CommandSpec("system cat -> /dev/null", ("cat", quoted_fixture), (fixture,)),
    ]
    cat_pipe_verify_commands = [
        shell_pipeline(
            f"{quote(quoted_fro)} cat --no-direct {quote(quoted_fixture)} | bin/wc -c",
            fixture,
            "fro cat --no-direct | bin/wc -c",
        ),
        shell_pipeline(
            f"cat {quote(quoted_fixture)} | bin/wc -c",
            fixture,
            "system cat | bin/wc -c",
        ),
    ]
    cat_pipe_commands = [
        shell_pipeline(
            f"{quote(quoted_fro)} cat --no-direct {quote(quoted_fixture)} | bin/wc -c >/dev/null",
            fixture,
            "fro cat --no-direct | bin/wc -c >/dev/null",
        ),
        shell_pipeline(
            f"cat {quote(quoted_fixture)} | bin/wc -c >/dev/null",
            fixture,
            "system cat | bin/wc -c >/dev/null",
        ),
    ]

    system_wc = CommandSpec("system wc", ("wc", "-l", "-w", "-m", "-L", str(fixture)), (fixture,))
    system_tail_lines = CommandSpec("system tail -n 4096", ("tail", "-n", "4096", str(fixture)), (fixture,))
    system_tail_bytes = CommandSpec("system tail -c 65536", ("tail", "-c", "65536", str(fixture)), (fixture,))

    fro_wc = CommandSpec(
        "fro wc --no-direct",
        (str(fro), "wc", "--no-direct", "-l", "-w", "-m", "-L", str(fixture)),
        (fixture,),
    )
    fro_tail_lines = CommandSpec("fro tail -n 4096", (str(fro), "tail", "-n", "4096", str(fixture)), (fixture,))
    fro_tail_bytes = CommandSpec("fro tail -c 65536", (str(fro), "tail", "-c", "65536", str(fixture)), (fixture,))
    wc_pipe_commands = [
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | {quote(quoted_fro)} wc --no-direct -l -w -m -L >/dev/null",
            fixture,
            "bin/cat | fro wc --no-direct -l -w -m -L >/dev/null",
        ),
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | wc -l -w -m -L >/dev/null",
            fixture,
            "bin/cat | system wc -l -w -m -L >/dev/null",
        ),
    ]
    wc_pipe_verify_commands = [
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | {quote(quoted_fro)} wc --no-direct -l -w -m -L",
            fixture,
            "bin/cat | fro wc --no-direct -l -w -m -L",
        ),
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | wc -l -w -m -L",
            fixture,
            "bin/cat | system wc -l -w -m -L",
        ),
    ]
    fgrep_file_commands = [
        CommandSpec(
            "fro fgrep --no-direct --count",
            (quoted_fro, "fgrep", "--no-direct", "--count", "needle", quoted_fixture),
            (fixture,),
        ),
        CommandSpec("system fgrep --count", ("fgrep", "--count", "needle", quoted_fixture), (fixture,)),
    ]
    fgrep_pipe_commands = [
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | {quote(quoted_fro)} fgrep --count needle >/dev/null",
            fixture,
            "bin/cat | fro fgrep --count >/dev/null",
        ),
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | fgrep --count needle >/dev/null",
            fixture,
            "bin/cat | system fgrep --count >/dev/null",
        ),
    ]
    fgrep_pipe_verify_commands = [
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | {quote(quoted_fro)} fgrep --count needle",
            fixture,
            "bin/cat | fro fgrep --count",
        ),
        shell_pipeline(
            f"bin/cat {quote(quoted_fixture)} | fgrep --count needle",
            fixture,
            "bin/cat | system fgrep --count",
        ),
    ]

    wc_commands = [fro_wc, system_wc]
    tail_line_commands = [fro_tail_lines, system_tail_lines]
    tail_byte_commands = [fro_tail_bytes, system_tail_bytes]

    uutils = find_uutils()
    uutils_note = "unavailable"
    if uutils and subcommand_available(uutils, "cat"):
        cat_file_outputs.append(CommandSpec("uutils cat", (uutils, "cat", quoted_fixture), (fixture,)))
        cat_devnull_commands.append(
            CommandSpec("uutils cat -> /dev/null", (uutils, "cat", quoted_fixture), (fixture,))
        )
        cat_pipe_verify_commands.append(
            shell_pipeline(
                f"{quote(uutils)} cat {quote(quoted_fixture)} | bin/wc -c",
                fixture,
                "uutils cat | bin/wc -c",
            )
        )
        cat_pipe_commands.append(
            shell_pipeline(
                f"{quote(uutils)} cat {quote(quoted_fixture)} | bin/wc -c >/dev/null",
                fixture,
                "uutils cat | bin/wc -c >/dev/null",
            )
        )
        uutils_note = f"available via {uutils}"
    if uutils and subcommand_available(uutils, "wc"):
        wc_commands.append(CommandSpec("uutils wc", (uutils, "wc", "-l", "-w", "-m", "-L", str(fixture)), (fixture,)))
        wc_pipe_verify_commands.append(
            shell_pipeline(
                f"bin/cat {quote(quoted_fixture)} | {quote(uutils)} wc -l -w -m -L",
                fixture,
                "bin/cat | uutils wc -l -w -m -L",
            )
        )
        wc_pipe_commands.append(
            shell_pipeline(
                f"bin/cat {quote(quoted_fixture)} | {quote(uutils)} wc -l -w -m -L >/dev/null",
                fixture,
                "bin/cat | uutils wc -l -w -m -L >/dev/null",
            )
        )
        if uutils_note == "unavailable":
            uutils_note = f"partial via {uutils}"
    if uutils and subcommand_available(uutils, "tail"):
        tail_line_commands.append(
            CommandSpec("uutils tail -n 4096", (uutils, "tail", "-n", "4096", str(fixture)), (fixture,))
        )
        tail_byte_commands.append(
            CommandSpec("uutils tail -c 65536", (uutils, "tail", "-c", "65536", str(fixture)), (fixture,))
        )
        if uutils_note == "unavailable":
            uutils_note = f"partial via {uutils}"
    grep_available = bool(uutils and subcommand_available(uutils, "grep"))
    if grep_available and uutils:
        fgrep_file_commands.append(
            CommandSpec(
                "uutils grep -F -c",
                (uutils, "grep", "-F", "-c", "needle", quoted_fixture),
                (fixture,),
            )
        )
        fgrep_pipe_verify_commands.append(
            shell_pipeline(
                f"bin/cat {quote(quoted_fixture)} | {quote(uutils)} grep -F -c needle",
                fixture,
                "bin/cat | uutils grep -F -c",
            )
        )
        fgrep_pipe_commands.append(
            shell_pipeline(
                f"bin/cat {quote(quoted_fixture)} | {quote(uutils)} grep -F -c needle >/dev/null",
                fixture,
                "bin/cat | uutils grep -F -c >/dev/null",
            )
        )
        if uutils_note == "unavailable":
            uutils_note = f"partial via {uutils}"

    verify_exact_outputs(cat_file_outputs)
    verify_tokenized_outputs(cat_pipe_verify_commands)
    verify_wc_outputs(wc_commands)
    verify_tokenized_outputs(wc_pipe_verify_commands)
    verify_exact_outputs(tail_line_commands)
    verify_exact_outputs(tail_byte_commands)
    verify_exact_outputs(fgrep_file_commands)
    verify_tokenized_outputs(fgrep_pipe_verify_commands)

    cat_devnull_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in cat_devnull_commands
    ]
    cat_devnull_results = [
        CaseResult(result.label, result.seconds, size_bytes / result.seconds / 1e9)
        for result in cat_devnull_results
    ]
    cat_pipe_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in cat_pipe_commands
    ]
    cat_pipe_results = [
        CaseResult(result.label, result.seconds, size_bytes / result.seconds / 1e9)
        for result in cat_pipe_results
    ]
    wc_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in wc_commands
    ]
    wc_results = [
        CaseResult(result.label, result.seconds, size_bytes / result.seconds / 1e9)
        for result in wc_results
    ]
    wc_pipe_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in wc_pipe_commands
    ]
    wc_pipe_results = [
        CaseResult(result.label, result.seconds, size_bytes / result.seconds / 1e9)
        for result in wc_pipe_results
    ]
    tail_line_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in tail_line_commands
    ]
    tail_byte_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in tail_byte_commands
    ]
    fgrep_file_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in fgrep_file_commands
    ]
    fgrep_file_results = [
        CaseResult(result.label, result.seconds, size_bytes / result.seconds / 1e9)
        for result in fgrep_file_results
    ]
    fgrep_pipe_results = [
        CaseResult(command.label, best_elapsed(command, args.repeat), 0.0)
        for command in fgrep_pipe_commands
    ]
    fgrep_pipe_results = [
        CaseResult(result.label, result.seconds, size_bytes / result.seconds / 1e9)
        for result in fgrep_pipe_results
    ]

    branch, git_desc = git_state()
    print("coreutils proposition benchmark slice")
    print(f"branch: {branch}")
    print(f"git: {git_desc}")
    print(f"work_dir: {work_dir}")
    print(f"fixture: {fixture} ({size_bytes} bytes)")
    print(f"repeat: {args.repeat}")
    print(f"uutils: {uutils_note}")
    print(f"uutils grep available: {'yes' if grep_available else 'no'}")
    print()
    print("cat regular file -> /dev/null (hot page cache, best of repeat)")
    print("| Command | Seconds | Effective GB/s |")
    print("| --- | ---: | ---: |")
    for result in cat_devnull_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {result.metric:.2f} |")
    print()
    print("cat regular file -> fixed pipe sink (`bin/wc -c >/dev/null`, hot page cache, best of repeat)")
    print("| Command | Seconds | Effective GB/s |")
    print("| --- | ---: | ---: |")
    for result in cat_pipe_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {result.metric:.2f} |")
    print()
    print("wc -l -w -m -L (hot page cache, best of repeat)")
    print("| Command | Seconds | Effective GB/s |")
    print("| --- | ---: | ---: |")
    for result in wc_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {result.metric:.2f} |")
    print()
    print("wc -l -w -m -L pipe input (`bin/cat` producer, best of repeat)")
    print("| Command | Seconds | Effective GB/s |")
    print("| --- | ---: | ---: |")
    for result in wc_pipe_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {result.metric:.2f} |")
    print()
    print("tail -n 4096 (hot page cache, best of repeat)")
    print("| Command | Seconds | Best ms |")
    print("| --- | ---: | ---: |")
    for result in tail_line_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {format_ms(result.seconds)} |")
    print()
    print("tail -c 65536 (hot page cache, best of repeat)")
    print("| Command | Seconds | Best ms |")
    print("| --- | ---: | ---: |")
    for result in tail_byte_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {format_ms(result.seconds)} |")
    print()
    print("fgrep --count regular file (hot page cache, best of repeat)")
    print("| Command | Seconds | Effective GB/s |")
    print("| --- | ---: | ---: |")
    for result in fgrep_file_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {result.metric:.2f} |")
    print()
    print("fgrep --count pipe input (`bin/cat` producer, best of repeat)")
    print("| Command | Seconds | Effective GB/s |")
    print("| --- | ---: | ---: |")
    for result in fgrep_pipe_results:
        print(f"| {table_cell(result.label)} | {result.seconds:.4f} | {result.metric:.2f} |")
    print()
    print("Notes:")
    print("- The harness verifies output parity before timing.")
    print("- `cat` is split by sink on purpose: regular-file -> `/dev/null` and regular-file -> pipe are different propositions, and `fro cat` may choose different backends by mount/cache/sink.")
    print("- The pipe sections hold the producer (`bin/cat`) or sink (`bin/wc -c >/dev/null`) fixed so the compared command changes are explicit, but they are still end-to-end pipeline propositions.")
    print("- `wc -c` is intentionally not included as a regular-file slice because GNU `wc` can answer it from metadata.")
    print("- Tail is reported as latency, not throughput, because regular-file tail does not read the whole file.")
    print("- If local uutils lacks `grep`, the `fgrep` tables fall back to fro vs GNU only.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
