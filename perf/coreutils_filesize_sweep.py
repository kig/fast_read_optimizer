#!/usr/bin/env python3
from __future__ import annotations

import argparse
import filecmp
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from pathlib import Path
from shlex import quote


DEFAULT_FILE_SIZES = ["4K", "16K", "64K", "256K", "1M", "4M", "16M", "64M", "256M"]
DEFAULT_TREE_SIZES = ["64K", "256K", "1M", "4M", "16M", "64M"]
DEFAULT_SURFACES = [
    "cat",
    "cp",
    "cksum",
    "md5sum",
    "sha256sum",
    "b3sum",
    "base64",
    "fgrep",
    "sort",
    "tar-create",
    "tar-extract",
    "rm",
    "wc-l",
    "cmp",
    "head-c",
    "tail-c",
    "find",
    "mv",
]

FILE_SURFACES = {
    "cat",
    "cp",
    "cksum",
    "md5sum",
    "sha256sum",
    "b3sum",
    "base64",
    "fgrep",
    "sort",
    "wc-l",
    "cmp",
    "head-c",
    "tail-c",
    "mv",
}
TREE_SURFACES = {"find", "rm", "tar-create", "tar-extract"}

SUFFIXES = {
    "": 1,
    "B": 1,
    "K": 1024,
    "KB": 1024,
    "KIB": 1024,
    "M": 1024 * 1024,
    "MB": 1024 * 1024,
    "MIB": 1024 * 1024,
    "G": 1024 * 1024 * 1024,
    "GB": 1024 * 1024 * 1024,
    "GIB": 1024 * 1024 * 1024,
}

LINE_TEMPLATE = (
    "needle\t{index:08d}\t{scramble:016x}\t"
    "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu\n"
)


@dataclass(frozen=True)
class CommandEntry:
    label: str
    shell_command: str
    argv: tuple[str, ...]


@dataclass(frozen=True)
class Case:
    surface: str
    size_token: str
    size_bytes: int
    metric_bytes: int
    mount_label: str
    prepare_command: str
    export_json: Path
    commands: tuple[CommandEntry, ...]


def parse_size(token: str) -> int:
    text = token.strip().upper()
    if not text:
        raise ValueError("empty size")
    match = re.fullmatch(r"(\d+)([A-Z]*)", text)
    if match is None:
        raise ValueError(f"unsupported size token: {token}")
    number = int(match.group(1))
    suffix = match.group(2)
    multiplier = SUFFIXES.get(suffix)
    if multiplier is None:
        raise ValueError(f"unsupported size suffix: {token}")
    return number * multiplier


def size_token_slug(token: str) -> str:
    return token.strip().lower().replace("ib", "").replace("b", "")


def repetition_count(size_bytes: int) -> int:
    if size_bytes <= 64 * 1024:
        return 21
    if size_bytes <= 1024 * 1024:
        return 11
    if size_bytes <= 16 * 1024 * 1024:
        return 7
    return 3


def warm_cache(path: Path) -> None:
    with path.open("rb", buffering=0) as handle:
        while handle.read(4 * 1024 * 1024):
            pass


def warm_tree(root: Path) -> None:
    for path in sorted(root.rglob("*")):
        if path.is_file():
            warm_cache(path)


def run_checked(argv: tuple[str, ...] | list[str], **kwargs) -> subprocess.CompletedProcess[bytes]:
    completed = subprocess.run(argv, check=False, **kwargs)
    if completed.returncode != 0:
        stdout = getattr(completed, "stdout", b"")
        stderr = getattr(completed, "stderr", b"")
        raise RuntimeError(
            f"command failed ({completed.returncode}): {' '.join(map(str, argv))}\n"
            f"stdout:\n{stdout.decode(errors='replace')}\n"
            f"stderr:\n{stderr.decode(errors='replace')}"
        )
    return completed


def resolve_system_command(name: str) -> str:
    for prefix in ("/usr/bin", "/bin", "/usr/sbin", "/sbin"):
        candidate = Path(prefix) / name
        if candidate.is_file():
            return str(candidate)
    resolved = shutil.which(name)
    if resolved is None:
        raise RuntimeError(f"missing system command: {name}")
    return resolved


def find_uutils() -> str | None:
    candidate = os.environ.get("UUTILS_COREUTILS") or shutil.which("coreutils")
    return candidate


def subcommand_available(multicall: str, subcommand: str) -> bool:
    completed = subprocess.run(
        (multicall, subcommand, "--help"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return completed.returncode == 0


def git_state(repo_root: Path) -> tuple[str, str]:
    head = run_checked(
        ("git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.decode().strip()
    branch = run_checked(
        ("git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.decode().strip()
    dirty = subprocess.run(
        ("git", "-C", str(repo_root), "diff", "--quiet"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode
    suffix = "dirty" if dirty else "clean"
    return branch, f"{head} ({suffix} working tree)"


def sanitize_label(path: Path) -> str:
    rendered = str(path.resolve()).strip("/")
    if not rendered:
        return "root"
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", rendered)


def ensure_text_fixture(path: Path, size_bytes: int) -> None:
    if path.exists() and path.stat().st_size == size_bytes:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        remaining = size_bytes
        index = 0
        while remaining > 0:
            line = LINE_TEMPLATE.format(index=index, scramble=(index * 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF)
            encoded = line.encode()
            chunk = encoded[: min(len(encoded), remaining)]
            handle.write(chunk)
            remaining -= len(chunk)
            index += 1


def tree_plan(size_bytes: int) -> list[tuple[Path, int]]:
    file_count = max(1, min(256, math.ceil(size_bytes / (64 * 1024))))
    chunk_size = math.ceil(size_bytes / file_count)
    remaining = size_bytes
    plan: list[tuple[Path, int]] = []
    for index in range(file_count):
        file_size = min(chunk_size, remaining)
        rel_path = Path(f"dir_{index // 32:03d}") / f"sub_{index // 8:03d}" / f"file_{index:04d}.txt"
        plan.append((rel_path, file_size))
        remaining -= file_size
    return plan


def ensure_tree_fixture(root: Path, size_bytes: int) -> None:
    meta_path = root.parent / f"{root.name}.meta.json"
    if root.exists() and meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except json.JSONDecodeError:
            meta = {}
        if meta.get("size_bytes") == size_bytes:
            return
        shutil.rmtree(root)
        meta_path.unlink(missing_ok=True)

    root.mkdir(parents=True, exist_ok=True)
    for rel_path, file_size in tree_plan(size_bytes):
        ensure_text_fixture(root / rel_path, file_size)
    meta_path.write_text(json.dumps({"size_bytes": size_bytes}, indent=2))


def ensure_identical_copy(source: Path, target: Path) -> None:
    if target.exists() and target.stat().st_size == source.stat().st_size:
        if filecmp.cmp(source, target, shallow=False):
            return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()


def compare_directories(expected: Path, actual: Path) -> None:
    comparison = filecmp.dircmp(expected, actual)
    if comparison.left_only or comparison.right_only or comparison.funny_files:
        raise RuntimeError(
            f"directory mismatch: left_only={comparison.left_only}, "
            f"right_only={comparison.right_only}, funny={comparison.funny_files}"
        )
    _, mismatches, errors = filecmp.cmpfiles(
        expected,
        actual,
        comparison.common_files,
        shallow=False,
    )
    if mismatches or errors:
        raise RuntimeError(f"directory file mismatch: mismatches={mismatches}, errors={errors}")
    for common_dir in comparison.common_dirs:
        compare_directories(expected / common_dir, actual / common_dir)


def find_named_subdir(root: Path, leaf_name: str) -> Path:
    if root.name == leaf_name and root.is_dir():
        return root
    matches = [path for path in root.rglob(leaf_name) if path.is_dir()]
    if not matches:
        raise RuntimeError(f"missing extracted directory named {leaf_name} under {root}")
    return min(matches, key=lambda path: len(path.parts))


def sorted_relative_lines(output: bytes, root: Path) -> list[str]:
    lines = []
    for raw_line in output.decode(errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        candidate = Path(line)
        if candidate.is_absolute():
            rel = candidate.relative_to(root)
        else:
            rel = candidate
        lines.append(rel.as_posix())
    return sorted(lines)


def metric_gbps(case: Case, seconds: float) -> float | None:
    if seconds <= 0 or case.metric_bytes <= 0:
        return None
    if case.surface in {"find", "rm", "mv"}:
        return None
    return case.metric_bytes / seconds / 1_000_000_000


def command_entry(label: str, argv: list[str], redirect_stdout: bool = False) -> CommandEntry:
    rendered = " ".join(quote(part) for part in argv)
    if redirect_stdout:
        rendered = f"{rendered} > /dev/null"
    return CommandEntry(label=label, shell_command=rendered, argv=tuple(argv))


def competitors_for(
    fro_argv: list[str],
    system_subcommand: str,
    system_argv: list[str],
    uutils: str | None,
    uutils_subcommand: str | None,
    redirect_stdout: bool = False,
) -> tuple[CommandEntry, ...]:
    commands = [
        command_entry("fro", fro_argv, redirect_stdout=redirect_stdout),
        command_entry("gnu", [resolve_system_command(system_subcommand), *system_argv], redirect_stdout=redirect_stdout),
    ]
    if uutils and uutils_subcommand and subcommand_available(uutils, uutils_subcommand):
        commands.append(
            command_entry(
                "uutils",
                [uutils, uutils_subcommand, *system_argv],
                redirect_stdout=redirect_stdout,
            )
        )
    return tuple(commands)


def validate_exact(entries: tuple[CommandEntry, ...]) -> None:
    baseline_stdout: bytes | None = None
    baseline_stderr: bytes | None = None
    baseline_label: str | None = None
    for entry in entries:
        completed = run_checked(entry.argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if baseline_stdout is None:
            baseline_stdout = completed.stdout
            baseline_stderr = completed.stderr
            baseline_label = entry.label
            continue
        if completed.stdout != baseline_stdout or completed.stderr != baseline_stderr:
            raise RuntimeError(
                f"output mismatch: {entry.label} vs {baseline_label}\n"
                f"{entry.label} stdout={completed.stdout!r}\n"
                f"{baseline_label} stdout={baseline_stdout!r}\n"
                f"{entry.label} stderr={completed.stderr!r}\n"
                f"{baseline_label} stderr={baseline_stderr!r}"
            )


def validate_tokenized(entries: tuple[CommandEntry, ...]) -> None:
    baseline_tokens: list[str] | None = None
    baseline_stderr: bytes | None = None
    baseline_label: str | None = None
    for entry in entries:
        completed = run_checked(entry.argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tokens = completed.stdout.decode(errors="replace").split()
        if baseline_tokens is None:
            baseline_tokens = tokens
            baseline_stderr = completed.stderr
            baseline_label = entry.label
            continue
        if tokens != baseline_tokens or completed.stderr != baseline_stderr:
            raise RuntimeError(
                f"tokenized output mismatch: {entry.label} vs {baseline_label}\n"
                f"{entry.label} stdout={completed.stdout!r}\n"
                f"{baseline_label} stdout={baseline_tokens!r}\n"
                f"{entry.label} stderr={completed.stderr!r}\n"
                f"{baseline_label} stderr={baseline_stderr!r}"
            )


def validate_find(entries: tuple[CommandEntry, ...], root: Path) -> None:
    baseline: list[str] | None = None
    baseline_label: str | None = None
    for entry in entries:
        completed = run_checked(entry.argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        normalized = sorted_relative_lines(completed.stdout, root)
        if baseline is None:
            baseline = normalized
            baseline_label = entry.label
            continue
        if normalized != baseline:
            raise RuntimeError(
                f"find output mismatch: {entry.label} vs {baseline_label}\n"
                f"{entry.label}={normalized!r}\n"
                f"{baseline_label}={baseline!r}"
            )


def validate_cp(entries: tuple[CommandEntry, ...], source: Path, validation_root: Path) -> None:
    validation_root.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        dest_dir = validation_root / entry.label
        dest_dir.mkdir(parents=True, exist_ok=True)
        target = dest_dir / source.name
        remove_path(target)
        argv = list(entry.argv[:-1]) + [f"{dest_dir}/"]
        run_checked(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not target.exists():
            raise RuntimeError(f"cp validation failed for {entry.label}: missing {target}")
        if not filecmp.cmp(source, target, shallow=False):
            raise RuntimeError(f"cp validation failed for {entry.label}: content mismatch")


def validate_mv(entries: tuple[CommandEntry, ...], template: Path, validation_root: Path) -> None:
    validation_root.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        staged = validation_root / f"{entry.label}-staged.bin"
        target = validation_root / f"{entry.label}-moved.bin"
        remove_path(staged)
        remove_path(target)
        shutil.copyfile(template, staged)
        argv = list(entry.argv[:-2]) + [str(staged), str(target)]
        run_checked(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if staged.exists():
            raise RuntimeError(f"mv validation failed for {entry.label}: source still exists")
        if not target.exists() or not filecmp.cmp(template, target, shallow=False):
            raise RuntimeError(f"mv validation failed for {entry.label}: content mismatch")


def validate_rm(entries: tuple[CommandEntry, ...], template_tree: Path, validation_root: Path) -> None:
    validation_root.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        target = validation_root / entry.label
        remove_path(target)
        shutil.copytree(template_tree, target)
        argv = list(entry.argv[:-1]) + [str(target)]
        run_checked(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if target.exists():
            raise RuntimeError(f"rm validation failed for {entry.label}: target still exists")


def validate_tar_create(entries: tuple[CommandEntry, ...], source_tree: Path, validation_root: Path) -> None:
    validation_root.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        archive = validation_root / f"{entry.label}.tar"
        extract_root = validation_root / f"{entry.label}-extract"
        remove_path(archive)
        remove_path(extract_root)
        argv = list(entry.argv)
        argv[-2] = str(archive)
        argv[-1] = str(source_tree)
        run_checked(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        extract_root.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive) as handle:
            handle.extractall(extract_root)
        compare_directories(source_tree, find_named_subdir(extract_root, source_tree.name))


def validate_tar_extract(entries: tuple[CommandEntry, ...], source_tree: Path, archive: Path, validation_root: Path) -> None:
    validation_root.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        extract_root = validation_root / entry.label
        remove_path(extract_root)
        extract_root.mkdir(parents=True, exist_ok=True)
        argv = list(entry.argv)
        argv[-1] = str(extract_root)
        run_checked(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        compare_directories(source_tree, find_named_subdir(extract_root, source_tree.name))


def ensure_archive_fixture(source_tree: Path, archive: Path) -> None:
    archive.unlink(missing_ok=True)
    archive.parent.mkdir(parents=True, exist_ok=True)
    run_checked(
        (
            resolve_system_command("tar"),
            "--format=ustar",
            "-cf",
            str(archive),
            "-C",
            str(source_tree.parent),
            source_tree.name,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def prepare_command(script_path: Path, action: str, **kwargs: str | Path) -> str:
    pieces = [quote(sys.executable), quote(str(script_path)), "__prepare", quote(action)]
    for key, value in kwargs.items():
        pieces.extend([f"--{key.replace('_', '-')}", quote(str(value))])
    return " ".join(pieces)


def slice_count(size_bytes: int) -> int:
    return max(1, min(size_bytes, max(1, size_bytes // 4)))


def build_cases(
    script_path: Path,
    fro_bin: Path,
    mount_root: Path,
    mount_label: str,
    surfaces: list[str],
    file_sizes: list[str],
    tree_sizes: list[str],
    uutils: str | None,
) -> list[Case]:
    run_root = mount_root / "fro-coreutils-filesize-sweep"
    files_dir = run_root / "fixtures" / "files"
    trees_dir = run_root / "fixtures" / "trees"
    work_dir = run_root / "work"
    results_dir = run_root / "results"
    validation_root = run_root / "validation"
    for path in (files_dir, trees_dir, work_dir, results_dir, validation_root):
        path.mkdir(parents=True, exist_ok=True)

    cases: list[Case] = []

    file_surface_set = [surface for surface in surfaces if surface in FILE_SURFACES]
    for size_token in file_sizes:
        size_bytes = parse_size(size_token)
        size_slug = size_token_slug(size_token)
        fixture = files_dir / f"fixture-{size_slug}.txt"
        ensure_text_fixture(fixture, size_bytes)

        cmp_rhs = files_dir / f"fixture-{size_slug}.cmp.txt"
        ensure_identical_copy(fixture, cmp_rhs)

        if "cat" in file_surface_set:
            commands = competitors_for(
                [str(fro_bin), "cat", str(fixture)],
                "cat",
                [str(fixture)],
                uutils,
                "cat",
                redirect_stdout=True,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface="cat",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"cat-{size_slug}.json",
                    commands=commands,
                )
            )

        for surface in ("cksum", "md5sum", "sha256sum", "b3sum"):
            if surface not in file_surface_set:
                continue
            commands = competitors_for(
                [str(fro_bin), surface, str(fixture)],
                surface,
                [str(fixture)],
                uutils,
                surface,
                redirect_stdout=True,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface=surface,
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"{surface}-{size_slug}.json",
                    commands=commands,
                )
            )

        if "base64" in file_surface_set:
            commands = competitors_for(
                [str(fro_bin), "base64", str(fixture)],
                "base64",
                [str(fixture)],
                uutils,
                "base64",
                redirect_stdout=True,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface="base64",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"base64-{size_slug}.json",
                    commands=commands,
                )
            )

        if "fgrep" in file_surface_set:
            pattern = "needle"
            commands = competitors_for(
                [str(fro_bin), "fgrep", pattern, str(fixture)],
                "fgrep",
                [pattern, str(fixture)],
                uutils,
                "fgrep",
                redirect_stdout=True,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface="fgrep",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"fgrep-{size_slug}.json",
                    commands=commands,
                )
            )

        if "sort" in file_surface_set:
            commands = competitors_for(
                [str(fro_bin), "sort", str(fixture)],
                "sort",
                [str(fixture)],
                uutils,
                "sort",
                redirect_stdout=True,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface="sort",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"sort-{size_slug}.json",
                    commands=commands,
                )
            )

        if "wc-l" in file_surface_set:
            commands = competitors_for(
                [str(fro_bin), "wc", "-l", str(fixture)],
                "wc",
                ["-l", str(fixture)],
                uutils,
                "wc",
                redirect_stdout=True,
            )
            validate_tokenized(commands)
            cases.append(
                Case(
                    surface="wc-l",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"wc-l-{size_slug}.json",
                    commands=commands,
                )
            )

        if "cmp" in file_surface_set:
            commands = competitors_for(
                [str(fro_bin), "cmp", str(fixture), str(cmp_rhs)],
                "cmp",
                [str(fixture), str(cmp_rhs)],
                uutils,
                "cmp",
                redirect_stdout=False,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface="cmp",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=f"{fixture}:{cmp_rhs}"),
                    export_json=results_dir / f"cmp-{size_slug}.json",
                    commands=commands,
                )
            )

        if "head-c" in file_surface_set:
            count = slice_count(size_bytes)
            count_str = str(count)
            commands = competitors_for(
                [str(fro_bin), "head", "-c", count_str, str(fixture)],
                "head",
                ["-c", count_str, str(fixture)],
                uutils,
                "head",
                redirect_stdout=True,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface="head-c",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=count,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"head-c-{size_slug}.json",
                    commands=commands,
                )
            )

        if "tail-c" in file_surface_set:
            count = slice_count(size_bytes)
            count_str = str(count)
            commands = competitors_for(
                [str(fro_bin), "tail", "-c", count_str, str(fixture)],
                "tail",
                ["-c", count_str, str(fixture)],
                uutils,
                "tail",
                redirect_stdout=True,
            )
            validate_exact(commands)
            cases.append(
                Case(
                    surface="tail-c",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=count,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-files", paths=fixture),
                    export_json=results_dir / f"tail-c-{size_slug}.json",
                    commands=commands,
                )
            )

        if "cp" in file_surface_set:
            dest_dir = work_dir / "cp" / size_slug
            dest_dir.mkdir(parents=True, exist_ok=True)
            commands = competitors_for(
                [str(fro_bin), "cp", str(fixture), f"{dest_dir}/"],
                "cp",
                [str(fixture), f"{dest_dir}/"],
                uutils,
                "cp",
                redirect_stdout=False,
            )
            validate_cp(commands, fixture, validation_root / "cp")
            cases.append(
                Case(
                    surface="cp",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(
                        script_path,
                        "prepare-cp",
                        source=fixture,
                        dest_dir=dest_dir,
                        dest_name=fixture.name,
                    ),
                    export_json=results_dir / f"cp-{size_slug}.json",
                    commands=commands,
                )
            )

        if "mv" in file_surface_set:
            staged = work_dir / "mv" / size_slug / "staged.bin"
            target = work_dir / "mv" / size_slug / "moved.bin"
            staged.parent.mkdir(parents=True, exist_ok=True)
            commands = competitors_for(
                [str(fro_bin), "mv", str(staged), str(target)],
                "mv",
                [str(staged), str(target)],
                uutils,
                "mv",
                redirect_stdout=False,
            )
            validate_mv(commands, fixture, validation_root / "mv")
            cases.append(
                Case(
                    surface="mv",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=0,
                    mount_label=mount_label,
                    prepare_command=prepare_command(
                        script_path,
                        "prepare-mv",
                        template=fixture,
                        staged=staged,
                        target=target,
                    ),
                    export_json=results_dir / f"mv-{size_slug}.json",
                    commands=commands,
                )
            )

    tree_surface_set = [surface for surface in surfaces if surface in TREE_SURFACES]
    for size_token in tree_sizes:
        size_bytes = parse_size(size_token)
        size_slug = size_token_slug(size_token)
        tree_root = trees_dir / f"tree-{size_slug}"
        ensure_tree_fixture(tree_root, size_bytes)
        archive = trees_dir / f"tree-{size_slug}.tar"
        ensure_archive_fixture(tree_root, archive)

        if "find" in tree_surface_set:
            commands = competitors_for(
                [str(fro_bin), "find", str(tree_root), "-type", "f"],
                "find",
                [str(tree_root), "-type", "f"],
                uutils,
                "find",
                redirect_stdout=True,
            )
            validate_find(commands, tree_root)
            cases.append(
                Case(
                    surface="find",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=0,
                    mount_label=mount_label,
                    prepare_command=prepare_command(script_path, "warm-tree", root=tree_root),
                    export_json=results_dir / f"find-{size_slug}.json",
                    commands=commands,
                )
            )

        if "rm" in tree_surface_set:
            target_root = work_dir / "rm" / size_slug / tree_root.name
            commands = competitors_for(
                [str(fro_bin), "rm", "-rf", str(target_root)],
                "rm",
                ["-rf", str(target_root)],
                uutils,
                "rm",
                redirect_stdout=False,
            )
            validate_rm(commands, tree_root, validation_root / "rm")
            cases.append(
                Case(
                    surface="rm",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=0,
                    mount_label=mount_label,
                    prepare_command=prepare_command(
                        script_path,
                        "prepare-rm",
                        template=tree_root,
                        target=target_root,
                    ),
                    export_json=results_dir / f"rm-{size_slug}.json",
                    commands=commands,
                )
            )

        if "tar-create" in tree_surface_set:
            archive_path = work_dir / "tar-create" / size_slug / f"{tree_root.name}.tar"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            commands = competitors_for(
                [str(fro_bin), "tar", "-cf", str(archive_path), str(tree_root)],
                "tar",
                ["-cf", str(archive_path), str(tree_root)],
                uutils,
                "tar",
                redirect_stdout=False,
            )
            validate_tar_create(commands, tree_root, validation_root / "tar-create")
            cases.append(
                Case(
                    surface="tar-create",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(
                        script_path,
                        "prepare-tar-create",
                        source=tree_root,
                        archive=archive_path,
                    ),
                    export_json=results_dir / f"tar-create-{size_slug}.json",
                    commands=commands,
                )
            )

        if "tar-extract" in tree_surface_set:
            extract_root = work_dir / "tar-extract" / size_slug
            commands = competitors_for(
                [str(fro_bin), "tar", "-xf", str(archive), "-C", str(extract_root)],
                "tar",
                ["-xf", str(archive), "-C", str(extract_root)],
                uutils,
                "tar",
                redirect_stdout=False,
            )
            validate_tar_extract(commands, tree_root, archive, validation_root / "tar-extract")
            cases.append(
                Case(
                    surface="tar-extract",
                    size_token=size_token,
                    size_bytes=size_bytes,
                    metric_bytes=size_bytes,
                    mount_label=mount_label,
                    prepare_command=prepare_command(
                        script_path,
                        "prepare-tar-extract",
                        archive=archive,
                        extract_root=extract_root,
                    ),
                    export_json=results_dir / f"tar-extract-{size_slug}.json",
                    commands=commands,
                )
            )

    return cases


def run_hyperfine(hyperfine: str, case: Case, warmup: int, runs: int) -> dict:
    case.export_json.parent.mkdir(parents=True, exist_ok=True)
    case.export_json.unlink(missing_ok=True)
    command: list[str] = [
        hyperfine,
        "--shell=bash",
        "--style=basic",
        "--warmup",
        str(warmup),
        "--runs",
        str(runs),
        "--prepare",
        case.prepare_command,
        "--export-json",
        str(case.export_json),
    ]
    for entry in case.commands:
        command.extend(["--command-name", entry.label, entry.shell_command])
    run_checked(tuple(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return json.loads(case.export_json.read_text())


def prepare_entrypoint(args: argparse.Namespace) -> int:
    if args.action == "warm-files":
        for token in str(args.paths).split(":"):
            warm_cache(Path(token))
        return 0
    if args.action == "warm-tree":
        warm_tree(Path(args.root))
        return 0
    if args.action == "prepare-cp":
        dest = Path(args.dest_dir) / args.dest_name
        remove_path(dest)
        warm_cache(Path(args.source))
        return 0
    if args.action == "prepare-mv":
        staged = Path(args.staged)
        target = Path(args.target)
        remove_path(staged)
        remove_path(target)
        staged.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.template, staged)
        warm_cache(staged)
        return 0
    if args.action == "prepare-rm":
        target = Path(args.target)
        remove_path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(args.template, target)
        return 0
    if args.action == "prepare-tar-create":
        remove_path(Path(args.archive))
        warm_tree(Path(args.source))
        return 0
    if args.action == "prepare-tar-extract":
        remove_path(Path(args.extract_root))
        Path(args.extract_root).mkdir(parents=True, exist_ok=True)
        warm_cache(Path(args.archive))
        return 0
    raise SystemExit(f"unknown prepare action: {args.action}")


def sweep_entrypoint(args: argparse.Namespace) -> int:
    repo_root = args.repo_root.resolve()
    script_path = Path(__file__).resolve()
    fro_bin = Path(args.fro).resolve()
    if not fro_bin.exists():
        raise SystemExit(f"missing fro binary: {fro_bin}")
    hyperfine = shutil.which(args.hyperfine)
    if hyperfine is None:
        raise SystemExit(f"missing hyperfine binary: {args.hyperfine}")

    surfaces = DEFAULT_SURFACES if "all" in args.surfaces else args.surfaces
    unknown = sorted(set(surfaces) - set(DEFAULT_SURFACES))
    if unknown:
        raise SystemExit(f"unknown surfaces: {', '.join(unknown)}")

    file_sizes = args.file_sizes
    tree_sizes = args.tree_sizes
    if args.sizes:
        file_sizes = args.sizes
        tree_sizes = args.sizes

    mount_paths = [Path(path).resolve() for path in args.mount]
    uutils = find_uutils()
    branch, git_summary = git_state(repo_root)

    rows = []
    for mount_root in mount_paths:
        mount_root.mkdir(parents=True, exist_ok=True)
        mount_label = sanitize_label(mount_root)
        cases = build_cases(
            script_path=script_path,
            fro_bin=fro_bin,
            mount_root=mount_root,
            mount_label=mount_label,
            surfaces=surfaces,
            file_sizes=file_sizes,
            tree_sizes=tree_sizes,
            uutils=uutils,
        )
        for case in cases:
            export = run_hyperfine(
                hyperfine=hyperfine,
                case=case,
                warmup=args.warmup,
                runs=repetition_count(case.size_bytes),
            )
            for result in export["results"]:
                rows.append(
                    {
                        "mount": str(mount_root),
                        "mount_label": mount_label,
                        "surface": case.surface,
                        "size": case.size_token,
                        "size_bytes": case.size_bytes,
                        "metric_bytes": case.metric_bytes,
                        "command": result["command"],
                        "mean_s": result["mean"],
                        "stddev_s": result["stddev"],
                        "median_s": result["median"],
                        "min_s": result["min"],
                        "max_s": result["max"],
                        "runs": len(result["times"]),
                        "times_s": result["times"],
                        "export_json": str(case.export_json),
                        "gbps": metric_gbps(case, result["median"]),
                    }
                )

    output = {
        "repo_root": str(repo_root),
        "fro": str(fro_bin),
        "hyperfine": hyperfine,
        "uutils_coreutils": uutils,
        "branch": branch,
        "git": git_summary,
        "surfaces": surfaces,
        "file_sizes": file_sizes,
        "tree_sizes": tree_sizes,
        "rows": rows,
    }
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2))

    print("mount\tsurface\tsize\tcommand\tmedian_ms\tgbps")
    for row in rows:
        print(
            f"{row['mount_label']}\t{row['surface']}\t{row['size']}\t{row['command']}\t"
            f"{row['median_s'] * 1000.0:.3f}\t"
            f"{format_row_metric(row)}"
        )
    print(f"\nsummary-json\t{output_path}")
    return 0


def build_prepare_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("action")
    parser.add_argument("--paths")
    parser.add_argument("--root")
    parser.add_argument("--source")
    parser.add_argument("--dest-dir")
    parser.add_argument("--dest-name")
    parser.add_argument("--template")
    parser.add_argument("--staged")
    parser.add_argument("--target")
    parser.add_argument("--archive")
    parser.add_argument("--extract-root")
    return parser


def build_sweep_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run reproducible hyperfine-based filesize sweeps for realistic fro coreutils surfaces "
            "against GNU coreutils and locally available uutils subcommands."
        )
    )
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--fro", default="target/release/fro")
    parser.add_argument("--hyperfine", default="hyperfine")
    parser.add_argument("--mount", action="append", required=True, help="Mount or working directory to benchmark on")
    parser.add_argument("--surfaces", nargs="*", default=["all"])
    parser.add_argument("--sizes", nargs="*", default=None, help="Override both file and tree size lists")
    parser.add_argument("--file-sizes", nargs="*", default=DEFAULT_FILE_SIZES)
    parser.add_argument("--tree-sizes", nargs="*", default=DEFAULT_TREE_SIZES)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--output", default="target/coreutils-filesize-sweep-summary.json")
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = build_sweep_parser()
    parser.epilog = "Use 'list-surfaces' to print supported surfaces."
    return parser


def print_supported_surfaces() -> int:
    print("\n".join(DEFAULT_SURFACES))
    return 0


def format_row_metric(row: dict) -> str:
    gbps = row["gbps"]
    return "-" if gbps is None else f"{gbps:.3f}"


def list_surfaces() -> int:
    return print_supported_surfaces()


def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == "__prepare":
        args = build_prepare_parser().parse_args(sys.argv[2:])
        return prepare_entrypoint(args)
    if len(sys.argv) >= 2 and sys.argv[1] == "list-surfaces":
        return list_surfaces()
    parser = build_parser()
    args = parser.parse_args()
    return sweep_entrypoint(args)


if __name__ == "__main__":
    raise SystemExit(main())
