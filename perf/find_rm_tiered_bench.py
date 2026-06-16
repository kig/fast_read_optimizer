#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import signal
import statistics
import subprocess
import time
from pathlib import Path

IPC_CLIENT_TEMPLATE = r"""
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
"""


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


def repetition_count() -> int:
    return 31


def build_binary(source: Path, output_path: Path, fro_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "cc",
            "-x",
            "assembler-with-cpp",
            "-nostdlib",
            "-static",
            "-no-pie",
            "-s",
            f'-DFRO_MULTICALL_PATH="{fro_path}"',
            str(source),
            "-o",
            str(output_path),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


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


def ensure_shadow_link(shadow_dir: Path, name: str, target: Path) -> Path:
    shadow_dir.mkdir(parents=True, exist_ok=True)
    link_path = shadow_dir / name
    if link_path.exists() or link_path.is_symlink():
        link_path.unlink()
    link_path.symlink_to(target)
    return link_path


def wait_for_socket(socket_path: Path, process: subprocess.Popen):
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if socket_path.exists():
            return
        if process.poll() is not None:
            raise RuntimeError("fro-ipc-server exited before creating its socket")
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for socket: {socket_path}")


def start_ipc_server(fro_ipc_server: Path, socket_path: Path):
    if socket_path.exists():
        socket_path.unlink()
    process = subprocess.Popen(
        [str(fro_ipc_server), str(socket_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=os.setsid,
    )
    wait_for_socket(socket_path, process)
    return process


def stop_ipc_server(process: subprocess.Popen):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def ensure_absent(path: Path):
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()


def write_pattern_file(path: Path, size: int, seed: int):
    pattern = bytes(((seed + index) % 251 for index in range(4096)))
    remaining = size
    with path.open("wb") as handle:
        while remaining > 0:
            chunk = pattern[: min(len(pattern), remaining)]
            handle.write(chunk)
            remaining -= len(chunk)


def create_tree(root: Path, dir_count: int = 16, files_per_dir: int = 32, file_size: int = 256):
    ensure_absent(root)
    root.mkdir(parents=True, exist_ok=True)
    for group in range(dir_count):
        parent = root / f"{group:02d}" / f"{group:02d}"
        parent.mkdir(parents=True, exist_ok=True)
        for file_index in range(files_per_dir):
            write_pattern_file(parent / f"file_{file_index:03d}.bin", file_size, group + file_index)


def warm_find_tree(root: Path):
    for current, dirnames, filenames in os.walk(root):
        os.stat(current)
        dirnames.sort()
        filenames.sort()
        for dirname in dirnames:
            os.stat(Path(current) / dirname)
        for filename in filenames:
            os.stat(Path(current) / filename)


def time_command(cmd):
    started = time.perf_counter_ns()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.perf_counter_ns() - started) / 1e6


def run_capture(cmd):
    return subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def normalize_find_stdout(stdout: bytes) -> list[str]:
    return sorted(line for line in stdout.decode("utf-8").splitlines() if line)


def validate_find_case(name: str, left_cmd, right_cmd, normalize: bool = False):
    left = run_capture(left_cmd)
    right = run_capture(right_cmd)
    left_stdout = normalize_find_stdout(left.stdout) if normalize else left.stdout.decode("utf-8", errors="replace")
    right_stdout = normalize_find_stdout(right.stdout) if normalize else right.stdout.decode("utf-8", errors="replace")
    ok = (
        left.returncode == right.returncode
        and left_stdout == right_stdout
        and left.stderr == right.stderr
    )
    return {
        "name": name,
        "ok": ok,
        "left_code": left.returncode,
        "right_code": right.returncode,
        "left_stdout": left_stdout,
        "right_stdout": right_stdout,
        "left_stderr": left.stderr.decode("utf-8", errors="replace"),
        "right_stderr": right.stderr.decode("utf-8", errors="replace"),
    }


def validate_rm_case(name: str, proto_cmd, fro_cmd, root: Path):
    proto_root = root / f"{name}-proto"
    fro_root = root / f"{name}-fro"
    create_tree(proto_root, dir_count=2, files_per_dir=4, file_size=64)
    shutil.copytree(proto_root, fro_root)
    left = run_capture(proto_cmd(proto_root))
    right = run_capture(fro_cmd(fro_root))
    ok = (
        left.returncode == right.returncode
        and not proto_root.exists()
        and not fro_root.exists()
        and left.stderr == right.stderr
    )
    return {
        "name": name,
        "ok": ok,
        "left_code": left.returncode,
        "right_code": right.returncode,
        "left_exists": proto_root.exists(),
        "right_exists": fro_root.exists(),
        "left_stderr": left.stderr.decode("utf-8", errors="replace"),
        "right_stderr": right.stderr.decode("utf-8", errors="replace"),
    }


def bench_find(root: Path, commands: dict[str, list[str]]):
    rows = []
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count()):
            warm_find_tree(root)
            samples.append(time_command(cmd))
        rows.append(
            {
                "case": "find -type f",
                "command": label,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    return rows


def bench_rm(seed_root: Path, work_root: Path, commands: dict[str, callable]):
    rows = []
    for label, builder in commands.items():
        samples = []
        for _ in range(repetition_count()):
            work_path = work_root / label
            ensure_absent(work_path)
            shutil.copytree(seed_root, work_path, copy_function=shutil.copyfile)
            warm_find_tree(work_path)
            samples.append(time_command(builder(work_path)))
            if work_path.exists():
                raise RuntimeError(f"rm benchmark left tree behind for {label}")
        rows.append(
            {
                "case": "rm -rf",
                "command": label,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--test-dir", default=Path("/data/fro-test/find-rm-tiered-bench"), type=Path)
    parser.add_argument("--output", default=None, type=Path)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    fro_bin = repo_root / "target" / "release" / "fro"
    fro_ipc_server = repo_root / "target" / "release" / "fro-ipc-server"
    if not fro_bin.exists() or not fro_ipc_server.exists():
        raise SystemExit("missing release binaries; run cargo build --release first")
    if shutil.which("cc") is None:
        raise SystemExit("missing compiler: cc")

    startup_root = repo_root / "target" / "startup-floor"
    find_tiered = startup_root / "find_tiered_exec"
    rm_tiered = startup_root / "rm_tiered_exec"
    build_binary(repo_root / "examples" / "find_tiered_exec.S", find_tiered, fro_bin)
    build_binary(repo_root / "examples" / "rm_tiered_exec.S", rm_tiered, fro_bin)

    ipc_client = startup_root / "fro_ipc_client_find_rm"
    test_root = args.test_dir.resolve()
    test_root.mkdir(parents=True, exist_ok=True)
    socket_path = test_root / "fro-ipc.sock"
    build_ipc_client(ipc_client, socket_path)
    shadow_dir = startup_root / "find-rm-shadow"
    shadow_find = ensure_shadow_link(shadow_dir, "find", ipc_client)
    shadow_rm = ensure_shadow_link(shadow_dir, "rm", ipc_client)

    validation_root = test_root / "validation"
    validation_root.mkdir(parents=True, exist_ok=True)
    validation_tree = validation_root / "find-tree"
    create_tree(validation_tree, dir_count=3, files_per_dir=3, file_size=64)
    validation_file = validation_root / "single.bin"
    write_pattern_file(validation_file, 128, 7)

    validations = {
        "find-inline-tree": validate_find_case(
            "find-inline-tree",
            [str(find_tiered), str(validation_tree), "-type", "f"],
            [resolve_system_command("find"), str(validation_tree), "-type", "f"],
            normalize=True,
        ),
        "find-inline-file": validate_find_case(
            "find-inline-file",
            [str(find_tiered), str(validation_file), "-type", "f"],
            [resolve_system_command("find"), str(validation_file), "-type", "f"],
        ),
        "find-fallback-maxdepth": validate_find_case(
            "find-fallback-maxdepth",
            [str(find_tiered), str(validation_tree), "-maxdepth", "1"],
            [str(fro_bin), "find", str(validation_tree), "-maxdepth", "1"],
        ),
        "rm-fallback-rf": validate_rm_case(
            "rm-fallback-rf",
            lambda path: [str(rm_tiered), "-rf", str(path)],
            lambda path: [str(fro_bin), "rm", "-rf", str(path)],
            validation_root,
        ),
    }
    failed = [name for name, result in validations.items() if not result["ok"]]
    if failed:
        raise SystemExit(f"validation failed: {', '.join(failed)}")

    find_root = test_root / "find-workload"
    create_tree(find_root, dir_count=4, files_per_dir=8, file_size=256)

    rm_seed = test_root / "rm-seed"
    create_tree(rm_seed, dir_count=4, files_per_dir=8, file_size=256)
    rm_work = test_root / "rm-work"
    rm_work.mkdir(parents=True, exist_ok=True)

    find_commands = {
        "system-find": [resolve_system_command("find"), str(find_root), "-type", "f"],
        "fro-find": [str(fro_bin), "find", str(find_root), "-type", "f"],
        "ipc-find": [str(shadow_find), str(find_root), "-type", "f"],
        "tiered-find": [str(find_tiered), str(find_root), "-type", "f"],
    }
    rm_commands = {
        "system-rm": lambda path: [resolve_system_command("rm"), "-rf", str(path)],
        "fro-rm": lambda path: [str(fro_bin), "rm", "-rf", str(path)],
        "ipc-rm": lambda path: [str(shadow_rm), "-rf", str(path)],
        "tiered-rm": lambda path: [str(rm_tiered), "-rf", str(path)],
    }

    rows = []
    server = start_ipc_server(fro_ipc_server, socket_path)
    try:
        rows.extend(bench_find(find_root, find_commands))
        rows.extend(bench_rm(rm_seed, rm_work, rm_commands))
    finally:
        stop_ipc_server(server)

    output = {
        "test_dir": str(test_root),
        "validations": validations,
        "rows": rows,
    }
    if args.output is None:
        args.output = test_root / "summary.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print("case\tcommand\tmedian_ms")
    for row in rows:
        print(f"{row['case']}\t{row['command']}\t{row['median_ms']:.3f}")
    print(f"\nsummary-json\t{args.output}")


if __name__ == "__main__":
    main()
