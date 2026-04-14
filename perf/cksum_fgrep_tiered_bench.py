#!/usr/bin/env python3
import argparse
import ctypes
import json
import os
import shutil
import signal
import statistics
import subprocess
import time
from pathlib import Path


CKSUM_SIZES = ["512", "1K", "2K", "4K", "8K", "16K", "32K", "64K"]
FGREP_SIZES = ["1K", "4K", "16K", "64K", "128K", "256K"]
SUFFIXES = {
    "K": 1024,
    "M": 1024 * 1024,
    "G": 1024 * 1024 * 1024,
}
LIBC = ctypes.CDLL(None, use_errno=True)
POSIX_FADV_DONTNEED = 4

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


def parse_size(token: str) -> int:
    token = token.strip().upper()
    if token[-1] in SUFFIXES:
        return int(token[:-1]) * SUFFIXES[token[-1]]
    return int(token)


def repetition_count(size_bytes: int) -> int:
    if size_bytes <= 4 << 10:
        return 41
    if size_bytes <= 64 << 10:
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


def build_tiered_binary(source: Path, output_path: Path, inline_max_bytes: int, fro_path: Path):
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
            f"-DINLINE_MAX_BYTES={inline_max_bytes}",
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


def warm_cache(*paths: Path):
    for path in paths:
        with path.open("rb", buffering=0) as handle:
            while handle.read(1 << 20):
                pass


def evict_cache(*paths: Path):
    for path in paths:
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


def prepare_cache(paths, cache_state: str):
    if cache_state == "hot":
        warm_cache(*paths)
    elif cache_state == "cold":
        evict_cache(*paths)
    else:
        raise ValueError(f"unsupported cache state: {cache_state}")


def time_command(cmd) -> float:
    started = time.perf_counter_ns()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.perf_counter_ns() - started) / 1e6


def time_sorted_pipeline(cmd, sort_bin: str) -> float:
    started = time.perf_counter_ns()
    producer = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    assert producer.stdout is not None
    consumer = subprocess.Popen(
        [sort_bin],
        stdin=producer.stdout,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    producer.stdout.close()
    producer_rc = producer.wait()
    consumer_rc = consumer.wait()
    if producer_rc != 0 or consumer_rc != 0:
        raise subprocess.CalledProcessError(producer_rc or consumer_rc, cmd)
    return (time.perf_counter_ns() - started) / 1e6


def compare_results(cmd_a, cmd_b):
    left = subprocess.run(cmd_a, check=False, capture_output=True)
    right = subprocess.run(cmd_b, check=False, capture_output=True)
    return {
        "ok": left.returncode == right.returncode
        and left.stdout == right.stdout
        and left.stderr == right.stderr,
        "left_code": left.returncode,
        "right_code": right.returncode,
        "left_stdout": left.stdout.decode("utf-8", errors="replace"),
        "right_stdout": right.stdout.decode("utf-8", errors="replace"),
        "left_stderr": left.stderr.decode("utf-8", errors="replace"),
        "right_stderr": right.stderr.decode("utf-8", errors="replace"),
    }


def write_cksum_fixture(fro_bin: Path, path: Path, size_token: str):
    run(
        [str(fro_bin), "write", "--create", size_token, str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def write_fgrep_fixture(path: Path, size_bytes: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        line_count, remainder = divmod(size_bytes, 64)
        if remainder != 0:
            raise ValueError(f"fgrep fixture size must be a multiple of 64 bytes: {size_bytes}")
        for line_idx in range(line_count):
            prefix = "ENABLE_" if line_idx % 5 == 0 else "disable_"
            body = f"{prefix}feature_{line_idx:08d} alpha beta gamma delta epsilon"
            handle.write(body.ljust(63, "x")[:63] + "\n")


def bench_rows(kind: str, size_token: str, size_bytes: int, paths, commands, cache_state: str, sort_bin: str | None = None):
    rows = []
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count(size_bytes)):
            prepare_cache(paths, cache_state)
            if kind == "fgrep":
                samples.append(time_sorted_pipeline(cmd, sort_bin))
            else:
                samples.append(time_command(cmd))
        rows.append(
            {
                "utility": kind,
                "size": size_token,
                "size_bytes": size_bytes,
                "cache_state": cache_state,
                "command": label,
                "median_ms": statistics.median(samples),
                "samples_ms": samples,
            }
        )
    return rows


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


def validate_cases(root: Path, cksum_tiered: Path, fgrep_tiered: Path, fro_bin: Path, system_cksum: str, system_fgrep: str):
    validation_root = root / "validation"
    validation_root.mkdir(parents=True, exist_ok=True)

    cksum_small = validation_root / "cksum-small.bin"
    cksum_small.write_bytes(bytes(range(32)) * 32)
    cksum_large = validation_root / "cksum-large.bin"
    cksum_large.write_bytes(bytes(range(64)) * 512)

    fgrep_small = validation_root / "fgrep-small.txt"
    fgrep_small.write_text(
        "alpha\nENABLE_MATCH=1\nomega\nENABLE_SECOND=2\n",
        encoding="utf-8",
    )
    fgrep_large = validation_root / "fgrep-large.txt"
    write_fgrep_fixture(fgrep_large, 96 * 1024)
    fgrep_nomatch = validation_root / "fgrep-nomatch.txt"
    fgrep_nomatch.write_text("alpha\nbeta\ngamma\n", encoding="utf-8")

    checks = {
        "cksum-inline-small": compare_results(
            [str(cksum_tiered), str(cksum_small)],
            [system_cksum, str(cksum_small)],
        ),
        "cksum-large-fallback": compare_results(
            [str(cksum_tiered), str(cksum_large)],
            [str(fro_bin), "cksum", str(cksum_large)],
        ),
        "cksum-flag-fallback": compare_results(
            [str(cksum_tiered), "--direct", str(cksum_small)],
            [str(fro_bin), "cksum", "--direct", str(cksum_small)],
        ),
        "fgrep-inline-match": compare_results(
            [str(fgrep_tiered), "ENABLE_", str(fgrep_small)],
            [system_fgrep, "ENABLE_", str(fgrep_small)],
        ),
        "fgrep-inline-nomatch": compare_results(
            [str(fgrep_tiered), "NOPE", str(fgrep_nomatch)],
            [system_fgrep, "NOPE", str(fgrep_nomatch)],
        ),
        "fgrep-large-fallback": compare_results(
            [str(fgrep_tiered), "ENABLE_", str(fgrep_large)],
            [str(fro_bin), "fgrep", "ENABLE_", str(fgrep_large)],
        ),
        "fgrep-flag-fallback": compare_results(
            [str(fgrep_tiered), "-n", "ENABLE_", str(fgrep_small)],
            [str(fro_bin), "fgrep", "-n", "ENABLE_", str(fgrep_small)],
        ),
    }
    return checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--test-dir", default=Path("/data/fro-test/cksum-fgrep-tiered-bench"), type=Path)
    parser.add_argument("--cache-state", choices=["hot", "cold", "both"], default="hot")
    parser.add_argument("--cksum-sizes", nargs="*", default=CKSUM_SIZES)
    parser.add_argument("--fgrep-sizes", nargs="*", default=FGREP_SIZES)
    parser.add_argument("--cksum-inline-max", default="4K")
    parser.add_argument("--fgrep-inline-max", default="16K")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    fro_bin = repo_root / "target" / "release" / "fro"
    fro_ipc_server = repo_root / "target" / "release" / "fro-ipc-server"
    if not fro_bin.exists() or not fro_ipc_server.exists():
        raise SystemExit("missing release binaries; run cargo build --release first")
    if shutil.which("cc") is None:
        raise SystemExit("missing compiler: cc")

    system_cksum = resolve_system_command("cksum")
    system_fgrep = resolve_system_command("fgrep")
    sort_bin = resolve_system_command("sort")

    run_root = args.test_dir.resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    cache_states = ["hot", "cold"] if args.cache_state == "both" else [args.cache_state]

    startup_root = repo_root / "target" / "startup-floor"
    cksum_tiered = startup_root / "cksum_tiered_exec"
    fgrep_tiered = startup_root / "fgrep_tiered_exec"
    build_tiered_binary(
        repo_root / "examples" / "cksum_tiered_exec.S",
        cksum_tiered,
        parse_size(args.cksum_inline_max),
        fro_bin,
    )
    build_tiered_binary(
        repo_root / "examples" / "fgrep_tiered_exec.S",
        fgrep_tiered,
        parse_size(args.fgrep_inline_max),
        fro_bin,
    )

    ipc_client = startup_root / "fro_ipc_client_cksum_fgrep"
    socket_path = run_root / "fro-ipc.sock"
    build_ipc_client(ipc_client, socket_path)
    shadow_dir = startup_root / "cksum-fgrep-shadow"
    shadow_cksum = ensure_shadow_link(shadow_dir, "cksum", ipc_client)
    shadow_fgrep = ensure_shadow_link(shadow_dir, "fgrep", ipc_client)

    validations = validate_cases(run_root, cksum_tiered, fgrep_tiered, fro_bin, system_cksum, system_fgrep)
    failed = [name for name, result in validations.items() if not result["ok"]]
    if failed:
        raise SystemExit(f"validation failed: {', '.join(failed)}")

    rows = []
    server = start_ipc_server(fro_ipc_server, socket_path)
    try:
        cksum_root = run_root / "cksum"
        cksum_root.mkdir(parents=True, exist_ok=True)
        for size_token in args.cksum_sizes:
            size_bytes = parse_size(size_token)
            fixture = cksum_root / f"{size_token}.bin"
            if not fixture.exists() or fixture.stat().st_size != size_bytes:
                write_cksum_fixture(fro_bin, fixture, size_token)
            commands = {
                "system-cksum": [system_cksum, str(fixture)],
                "fro-cksum-direct": [str(fro_bin), "cksum", "--direct", str(fixture)],
                "ipc-cksum": [str(shadow_cksum), str(fixture)],
                f"tiered-cksum-{args.cksum_inline_max.lower()}": [str(cksum_tiered), str(fixture)],
            }
            for cache_state in cache_states:
                rows.extend(bench_rows("cksum", size_token, size_bytes, [fixture], commands, cache_state))

        fgrep_root = run_root / "fgrep"
        fgrep_root.mkdir(parents=True, exist_ok=True)
        for size_token in args.fgrep_sizes:
            size_bytes = parse_size(size_token)
            fixture = fgrep_root / f"{size_token}.txt"
            if not fixture.exists() or fixture.stat().st_size != size_bytes:
                write_fgrep_fixture(fixture, size_bytes)
            commands = {
                "system-fgrep": [system_fgrep, "ENABLE_", str(fixture)],
                "fro-fgrep-direct": [str(fro_bin), "fgrep", "--direct", "ENABLE_", str(fixture)],
                "ipc-fgrep": [str(shadow_fgrep), "ENABLE_", str(fixture)],
                f"tiered-fgrep-{args.fgrep_inline_max.lower()}": [str(fgrep_tiered), "ENABLE_", str(fixture)],
            }
            for cache_state in cache_states:
                rows.extend(
                    bench_rows(
                        "fgrep",
                        size_token,
                        size_bytes,
                        [fixture],
                        commands,
                        cache_state,
                        sort_bin=sort_bin,
                    )
                )
    finally:
        stop_ipc_server(server)

    output = {
        "test_dir": str(run_root),
        "cache_states": cache_states,
        "cksum_inline_max_bytes": parse_size(args.cksum_inline_max),
        "fgrep_inline_max_bytes": parse_size(args.fgrep_inline_max),
        "cksum_sizes": args.cksum_sizes,
        "fgrep_sizes": args.fgrep_sizes,
        "validations": validations,
        "rows": rows,
    }
    if args.output is None:
        args.output = run_root / "summary.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print("utility\tcache\tcommand\tsize\tmedian_ms")
    for row in rows:
        print(f"{row['utility']}\t{row['cache_state']}\t{row['command']}\t{row['size']}\t{row['median_ms']:.3f}")
    print(f"\nsummary-json\t{args.output}")


if __name__ == "__main__":
    main()
