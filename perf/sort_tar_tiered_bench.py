#!/usr/bin/env python3
import argparse
import filecmp
import json
import os
import shutil
import signal
import statistics
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

DEFAULT_SORT_SIZES = ["1K", "4K", "16K"]
SUFFIXES = {"K": 1024, "M": 1024 * 1024, "G": 1024 * 1024 * 1024}
IPC_CLIENT_TEMPLATE = r'''
.intel_syntax noprefix
.equ AF_UNIX, 1
.equ SOCK_STREAM, 1
.equ SOL_SOCKET, 1
.equ SCM_RIGHTS, 1
.equ SYS_SOCKET, 41
.equ SYS_CONNECT, 42
.equ SYS_SENDMSG, 46
.equ SYS_READ, 0
.equ SYS_CLOSE, 3
.equ SYS_EXIT, 60
.equ SYS_OPENAT, 257
.equ AT_FDCWD, -100
.equ O_RDONLY, 0
.equ O_DIRECTORY, 65536
.equ CMSG_LEN_FDS, 32
.equ CMSG_SPACE_FDS, 32
.equ FRAME_SIZE, 32768
.equ CONTROL_OFF, 32000
.equ IOVEC_OFF, 31968
.equ MSGHDR_OFF, 32032
.equ STATUS_OFF, 32128
.equ SOCKADDR_LEN, {sockaddr_len}
.section .rodata
socket_addr:
  .word AF_UNIX
  .ascii "{socket_path}"
  .byte 0
dot_path:
  .ascii "."
  .byte 0
.section .text
.global _start
_start:
  mov rbp, rsp
  mov r12, [rbp]
  test r12, r12
  jz fail
  mov eax, SYS_SOCKET
  mov edi, AF_UNIX
  mov esi, SOCK_STREAM
  xor edx, edx
  syscall
  test rax, rax
  js fail
  mov rbx, rax
  mov r15, -1
  mov eax, SYS_OPENAT
  mov edi, AT_FDCWD
  lea rsi, [rip + dot_path]
  mov edx, O_RDONLY | O_DIRECTORY
  xor r10d, r10d
  syscall
  test rax, rax
  js close_fail
  mov r15, rax
  mov eax, SYS_CONNECT
  mov rdi, rbx
  lea rsi, [rip + socket_addr]
  mov edx, SOCKADDR_LEN
  syscall
  test rax, rax
  js close_fail
  sub rsp, FRAME_SIZE
  mov r13, rsp
  mov qword ptr [r13], r12
  mov r8, 8
  xor r14, r14
copy_arg_loop:
  cmp r14, r12
  je payload_done
  mov rdi, [rbp + 8 + r14 * 8]
  call strlen
  mov r10, rax
  lea r11, [r8 + r10 + 1]
  cmp r11, IOVEC_OFF
  ja close_fail
  mov rcx, r10
  mov rsi, [rbp + 8 + r14 * 8]
  lea rdi, [r13 + r8]
  rep movsb
  mov byte ptr [rdi], 0
  mov r8, r11
  inc r14
  jmp copy_arg_loop
payload_done:
  lea rax, [r13 + CONTROL_OFF]
  mov qword ptr [rax + 0], CMSG_LEN_FDS
  mov dword ptr [rax + 8], SOL_SOCKET
  mov dword ptr [rax + 12], SCM_RIGHTS
  mov dword ptr [rax + 16], 0
  mov dword ptr [rax + 20], 1
  mov dword ptr [rax + 24], 2
  mov dword ptr [rax + 28], r15d
  lea rax, [r13 + IOVEC_OFF]
  mov qword ptr [rax + 0], r13
  mov qword ptr [rax + 8], r8
  lea rax, [r13 + MSGHDR_OFF]
  mov qword ptr [rax + 0], 0
  mov dword ptr [rax + 8], 0
  mov dword ptr [rax + 12], 0
  lea rcx, [r13 + IOVEC_OFF]
  mov qword ptr [rax + 16], rcx
  mov qword ptr [rax + 24], 1
  lea rcx, [r13 + CONTROL_OFF]
  mov qword ptr [rax + 32], rcx
  mov qword ptr [rax + 40], CMSG_SPACE_FDS
  mov dword ptr [rax + 48], 0
  mov dword ptr [rax + 52], 0
  mov eax, SYS_SENDMSG
  mov rdi, rbx
  lea rsi, [r13 + MSGHDR_OFF]
  xor edx, edx
  syscall
  test rax, rax
  js close_fail
  mov eax, SYS_CLOSE
  mov rdi, r15
  syscall
  mov eax, SYS_READ
  mov rdi, rbx
  lea rsi, [r13 + STATUS_OFF]
  mov edx, 1
  syscall
  cmp rax, 1
  jne close_fail
  movzx r10d, byte ptr [r13 + STATUS_OFF]
  mov eax, SYS_CLOSE
  mov rdi, rbx
  syscall
  mov edi, r10d
  mov eax, SYS_EXIT
  syscall
close_fail:
  cmp r15, 0
  jl skip_cwd_close
  mov eax, SYS_CLOSE
  mov rdi, r15
  syscall
skip_cwd_close:
  mov eax, SYS_CLOSE
  mov rdi, rbx
  syscall
fail:
  mov edi, 1
  mov eax, SYS_EXIT
  syscall
strlen:
  xor eax, eax
strlen_loop:
  cmp byte ptr [rdi + rax], 0
  je strlen_done
  inc rax
  jmp strlen_loop
strlen_done:
  ret
'''


def parse_size(token: str) -> int:
    token = token.strip().upper()
    if token[-1] in SUFFIXES:
        return int(token[:-1]) * SUFFIXES[token[-1]]
    return int(token)


def repetition_count(size_bytes: int) -> int:
    if size_bytes <= 4 << 10:
        return 31
    if size_bytes <= 64 << 10:
        return 21
    return 11


def tar_repetition_count() -> int:
    return 15


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


def ensure_symlink(target: Path, link_path: Path):
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if link_path.exists() or link_path.is_symlink():
        link_path.unlink()
    link_path.symlink_to(target)


def build_ipc_client(output_path: Path, socket_path: Path):
    source_path = output_path.with_suffix(".S")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    socket_text = str(socket_path)
    source_path.write_text(
        IPC_CLIENT_TEMPLATE.format(
            sockaddr_len=2 + len(socket_text) + 1,
            socket_path=socket_text,
        )
    )
    run(
        [
            "cc",
            "-x",
            "assembler",
            "-nostdlib",
            "-static",
            "-no-pie",
            "-s",
            str(source_path),
            "-o",
            str(output_path),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def warm_cache_file(path: Path):
    with path.open("rb", buffering=0) as handle:
        while handle.read(1 << 20):
            pass


def warm_tree(root: Path):
    if root.is_file():
        warm_cache_file(root)
        return
    for path in sorted(root.rglob("*")):
        if path.is_file():
            warm_cache_file(path)


def time_command(cmd, *, cwd: Path | None = None, env: dict[str, str] | None = None):
    started = time.perf_counter_ns()
    run(cmd, cwd=cwd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.perf_counter_ns() - started) / 1e6


def tree_snapshot(root: Path):
    rows = []
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if path.is_dir():
            rows.append(("dir", rel, None))
        elif path.is_symlink():
            rows.append(("symlink", rel, os.readlink(path)))
        else:
            rows.append(("file", rel, path.read_bytes()))
    return rows


def check_outputs_equal(commands, *, cwd: Path | None = None, env: dict[str, str] | None = None):
    baseline = None
    for label, cmd in commands:
        completed = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, check=False)
        current = (completed.returncode, completed.stdout, completed.stderr)
        if baseline is None:
            baseline = (label, current)
            continue
        if current != baseline[1]:
            raise RuntimeError(
                f"output mismatch: {label} vs {baseline[0]}\n"
                f"{label} rc={completed.returncode} stdout={completed.stdout!r} stderr={completed.stderr!r}\n"
                f"{baseline[0]} rc={baseline[1][0]} stdout={baseline[1][1]!r} stderr={baseline[1][2]!r}"
            )


def start_ipc_server(server_bin: Path, socket_path: Path):
    if socket_path.exists():
        socket_path.unlink()
    process = subprocess.Popen(
        [str(server_bin), str(socket_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid,
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        if socket_path.exists():
            return process
        if process.poll() is not None:
            raise RuntimeError("fro-ipc-server exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for socket: {socket_path}")


def stop_ipc_server(process: subprocess.Popen):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def make_sort_fixture(path: Path, size_bytes: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    index = 0
    while sum(len(line) for line in lines) < size_bytes:
        value = (size_bytes * 17 - index * 13) % 1000003
        lines.append(f"{value:07d} line-{index:05d}\n")
        index += 1
    data = "".join(lines).encode()[:size_bytes]
    if not data.endswith(b"\n"):
        data = data[: data.rfind(b"\n") + 1]
    path.write_bytes(data)


def make_tar_tree(root: Path):
    shutil.rmtree(root, ignore_errors=True)
    (root / "nested").mkdir(parents=True, exist_ok=True)
    (root / "nested" / "deeper").mkdir(parents=True, exist_ok=True)
    (root / "alpha.txt").write_text("alpha\n" * 8)
    (root / "beta.txt").write_text("beta\n" * 6)
    (root / "nested" / "gamma.txt").write_text("gamma\n" * 5)
    (root / "nested" / "deeper" / "delta.txt").write_text("delta\n" * 4)


def benchmark_sort(size_token: str, fixture: Path, commands: dict[str, list[str]], env: dict[str, str]):
    size_bytes = fixture.stat().st_size
    rows = []
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count(size_bytes)):
            warm_cache_file(fixture)
            samples.append(time_command(cmd, env=env))
        rows.append(
            {
                "family": "sort",
                "size": size_token,
                "size_bytes": size_bytes,
                "command": label,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    return rows


def benchmark_tar_case(
    case_name: str,
    commands: dict[str, tuple[list[str], Path | None, Callable[[], None] | None]],
    warm_target: Callable[[], None],
):
    rows = []
    for label, (cmd, cwd, reset) in commands.items():
        samples = []
        for _ in range(tar_repetition_count()):
            if reset is not None:
                reset()
            warm_target()
            samples.append(time_command(cmd, cwd=cwd))
        rows.append(
            {
                "family": "tar",
                "case": case_name,
                "command": label,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--test-dir", default=Path("/data/fro-test/startup-floor-sort-tar"), type=Path)
    parser.add_argument("--sort-sizes", nargs="*", default=DEFAULT_SORT_SIZES)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    fro_bin = repo_root / "target" / "release" / "fro"
    fro_ipc_server = repo_root / "target" / "release" / "fro-ipc-server"
    if not fro_bin.exists() or not fro_ipc_server.exists():
        raise SystemExit("missing release binaries; run cargo build --release first")
    if shutil.which("cc") is None:
        raise SystemExit("missing compiler: cc")

    test_root = args.test_dir.resolve()
    test_root.mkdir(parents=True, exist_ok=True)
    startup_root = repo_root / "target" / "startup-floor"
    shadow_root = startup_root / "sort-tar-shadow"
    ipc_root = startup_root / "sort-tar-ipc"
    ipc_client = startup_root / "sort_tar_ipc_client"
    socket_path = test_root / "fro-ipc.sock"
    build_ipc_client(ipc_client, socket_path)

    for utility in ("sort", "tar"):
        ensure_symlink(fro_bin, shadow_root / utility)
        ensure_symlink(ipc_client, ipc_root / utility)

    env = dict(os.environ)
    env["LC_ALL"] = "C"
    system_sort = resolve_system_command("sort")
    system_tar = resolve_system_command("tar")

    rows = []
    validations = {"sort": {}, "tar": {}}
    server = start_ipc_server(fro_ipc_server, socket_path)
    try:
        sort_fixture_dir = test_root / "sort-fixtures"
        for size_token in args.sort_sizes:
            size_bytes = parse_size(size_token)
            fixture = sort_fixture_dir / f"{size_token}.txt"
            make_sort_fixture(fixture, size_bytes)
            commands = {
                "system-sort": [system_sort, str(fixture)],
                "fro-sort": [str(fro_bin), "sort", str(fixture)],
                "shadow-sort": [str(shadow_root / "sort"), str(fixture)],
                "ipc-sort": [str(ipc_root / "sort"), str(fixture)],
            }
            check_outputs_equal(list(commands.items()), env=env)
            validations["sort"][size_token] = "ok"
            rows.extend(benchmark_sort(size_token, fixture, commands, env))

        tar_root = test_root / "tar"
        source_tree = tar_root / "source"
        make_tar_tree(source_tree)
        archive = tar_root / "source.tar"
        shutil.rmtree(tar_root / "create", ignore_errors=True)
        (tar_root / "create").mkdir(parents=True, exist_ok=True)
        run(
            [system_tar, "-cf", str(archive), "."],
            cwd=source_tree,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        create_dir = tar_root / "create"
        create_targets = {
            "system-tar": create_dir / "system.tar",
            "fro-tar": create_dir / "fro.tar",
            "shadow-tar": create_dir / "shadow.tar",
            "ipc-tar": create_dir / "ipc.tar",
        }
        create_commands = {
            "system-tar": ([system_tar, "-cf", str(create_targets["system-tar"]), "."], source_tree, lambda: create_targets["system-tar"].unlink(missing_ok=True)),
            "fro-tar": ([str(fro_bin), "tar", "-cf", str(create_targets["fro-tar"]), "."], source_tree, lambda: create_targets["fro-tar"].unlink(missing_ok=True)),
            "shadow-tar": ([str(shadow_root / "tar"), "-cf", str(create_targets["shadow-tar"]), "."], source_tree, lambda: create_targets["shadow-tar"].unlink(missing_ok=True)),
            "ipc-tar": ([str(ipc_root / "tar"), "-cf", str(create_targets["ipc-tar"]), "."], source_tree, lambda: create_targets["ipc-tar"].unlink(missing_ok=True)),
        }
        for label, (cmd, cwd, reset) in create_commands.items():
            reset()
            run(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            listing = subprocess.check_output([system_tar, "-tf", str(create_targets[label])], env=env)
            validations["tar"][f"create-{label}"] = sorted(
                listing.decode("utf-8", errors="replace").splitlines()
            )
        create_baseline = validations["tar"]["create-system-tar"]
        for label in ("create-fro-tar", "create-shadow-tar", "create-ipc-tar"):
            if validations["tar"][label] != create_baseline:
                raise RuntimeError(f"tar create listing mismatch for {label}")
        rows.extend(benchmark_tar_case("create", create_commands, lambda: warm_tree(source_tree)))

        list_commands = {
            "system-tar": ([system_tar, "-tf", str(archive)], None, None),
            "fro-tar": ([str(fro_bin), "tar", "-tf", str(archive)], None, None),
            "shadow-tar": ([str(shadow_root / "tar"), "-tf", str(archive)], None, None),
            "ipc-tar": ([str(ipc_root / "tar"), "-tf", str(archive)], None, None),
        }
        check_outputs_equal([(label, cmd) for label, (cmd, _, _) in list_commands.items()], env=env)
        validations["tar"]["list"] = "ok"
        rows.extend(benchmark_tar_case("list", list_commands, lambda: warm_cache_file(archive)))

        extract_root = tar_root / "extract"
        shutil.rmtree(extract_root, ignore_errors=True)
        extract_root.mkdir(parents=True, exist_ok=True)
        system_out = extract_root / "system"
        system_out.mkdir()
        run([system_tar, "-xf", str(archive), "-C", str(system_out)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        baseline_snapshot = tree_snapshot(system_out)

        def reset_extract(path: Path):
            def _reset():
                shutil.rmtree(path, ignore_errors=True)
                path.mkdir(parents=True, exist_ok=True)
            return _reset

        extract_commands = {
            "system-tar": ([system_tar, "-xf", str(archive), "-C", str(extract_root / "system-bench")], None, reset_extract(extract_root / "system-bench")),
            "fro-tar": ([str(fro_bin), "tar", "-xf", str(archive), "-C", str(extract_root / "fro")], None, reset_extract(extract_root / "fro")),
            "shadow-tar": ([str(shadow_root / "tar"), "-xf", str(archive), "-C", str(extract_root / "shadow")], None, reset_extract(extract_root / "shadow")),
            "ipc-tar": ([str(ipc_root / "tar"), "-xf", str(archive), "-C", str(extract_root / "ipc")], None, reset_extract(extract_root / "ipc")),
        }
        for label, (cmd, cwd, reset) in extract_commands.items():
            reset()
            run(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            out_root = Path(cmd[-1])
            if tree_snapshot(out_root) != baseline_snapshot:
                raise RuntimeError(f"tar extract tree mismatch for {label}")
        validations["tar"]["extract"] = "ok"
        rows.extend(benchmark_tar_case("extract", extract_commands, lambda: warm_cache_file(archive)))
    finally:
        stop_ipc_server(server)

    output = {
        "test_dir": str(test_root),
        "sort_sizes": args.sort_sizes,
        "validations": validations,
        "rows": rows,
    }
    if args.output is None:
        args.output = test_root / "summary.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print("family\tcase\tcommand\tsize\tmedian_ms")
    for row in rows:
        print(
            f"{row['family']}\t{row.get('case', '')}\t{row['command']}\t{row.get('size', '')}\t{row['median_ms']:.3f}"
        )
    print(f"\nsummary-json\t{args.output}")


if __name__ == "__main__":
    main()
