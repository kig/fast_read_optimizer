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


DEFAULT_SIZES = ["256", "1K", "4K", "16K", "64K", "128K", "256K"]
SUFFIXES = {
    "K": 1024,
    "M": 1024 * 1024,
    "G": 1024 * 1024 * 1024,
}
WC_INLINE_DEFAULT = "128K"
CMP_INLINE_DEFAULT = "128K"

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
        return 31
    if size_bytes <= 64 << 10:
        return 25
    if size_bytes <= 256 << 10:
        return 15
    return 9


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


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


def time_command(cmd):
    started = time.perf_counter_ns()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.perf_counter_ns() - started) / 1e6


def build_tiered_binary(source: Path, output_path: Path, inline_max_bytes: int, fro_path: Path):
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
    run(cmd)


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
        ]
    )


def ensure_shadow_link(shadow_dir: Path, name: str, target: Path):
    shadow_dir.mkdir(parents=True, exist_ok=True)
    link_path = shadow_dir / name
    if link_path.exists() or link_path.is_symlink():
        link_path.unlink()
    link_path.symlink_to(target)
    return link_path


def make_line_fixture(path: Path, size_bytes: int):
    pattern = b"x" * 63 + b"\n"
    repeats, remainder = divmod(size_bytes, len(pattern))
    data = pattern * repeats + pattern[:remainder]
    path.write_bytes(data)


def make_cmp_fixture_pair(path_a: Path, path_b: Path, size_bytes: int):
    pattern = bytes((index % 251 for index in range(4096)))
    repeats, remainder = divmod(size_bytes, len(pattern))
    data = pattern * repeats + pattern[:remainder]
    path_a.write_bytes(data)
    path_b.write_bytes(data)


def mutate_tail_byte(path: Path):
    data = bytearray(path.read_bytes())
    if not data:
        data.append(1)
    else:
        data[-1] ^= 1
    path.write_bytes(data)


def prepare_cache(paths, cache_state: str):
    if cache_state == "hot":
        warm_cache(*paths)
    elif cache_state == "cold":
        evict_cache(*paths)
    else:
        raise ValueError(f"unsupported cache state: {cache_state}")


def bench_case(case_name: str, size_token: str, size_bytes: int, paths, commands, cache_state: str):
    rows = []
    for label, cmd in commands.items():
        samples = []
        for _ in range(repetition_count(size_bytes)):
            prepare_cache(paths, cache_state)
            samples.append(time_command(cmd))
        median_ms = statistics.median(samples)
        rows.append(
            {
                "case": case_name,
                "command": label,
                "cache_state": cache_state,
                "size": size_token,
                "size_bytes": size_bytes,
                "median_ms": median_ms,
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


def check_output_equal(cmd_a, cmd_b):
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


def validate_cases(test_root: Path, wc_tiered: Path, cmp_tiered: Path, fro_bin: Path):
    validation_root = test_root / "validation"
    validation_root.mkdir(parents=True, exist_ok=True)

    wc_small = validation_root / "wc-small.txt"
    wc_large = validation_root / "wc-large.txt"
    make_line_fixture(wc_small, 4096)
    make_line_fixture(wc_large, 256 * 1024)

    cmp_equal_a = validation_root / "cmp-equal-a.bin"
    cmp_equal_b = validation_root / "cmp-equal-b.bin"
    cmp_diff = validation_root / "cmp-diff.bin"
    cmp_large_a = validation_root / "cmp-large-a.bin"
    cmp_large_b = validation_root / "cmp-large-b.bin"
    cmp_size_base = validation_root / "cmp-size-base.bin"
    cmp_size_miss = validation_root / "cmp-size-miss.bin"
    make_cmp_fixture_pair(cmp_equal_a, cmp_equal_b, 4096)
    shutil.copyfile(cmp_equal_b, cmp_diff)
    mutate_tail_byte(cmp_diff)
    make_cmp_fixture_pair(cmp_large_a, cmp_large_b, 256 * 1024)
    make_cmp_fixture_pair(cmp_size_base, cmp_size_miss, 1024)
    cmp_size_miss.write_bytes(cmp_size_miss.read_bytes() + b"x")

    checks = {
        "wc-small-inline": check_output_equal(
            [str(wc_tiered), "-l", str(wc_small)],
            [str(fro_bin), "wc", "-l", str(wc_small)],
        ),
        "wc-large-fallback": check_output_equal(
            [str(wc_tiered), "-l", str(wc_large)],
            [str(fro_bin), "wc", "-l", str(wc_large)],
        ),
        "cmp-equal-inline": check_output_equal(
            [str(cmp_tiered), "-s", str(cmp_equal_a), str(cmp_equal_b)],
            [str(fro_bin), "cmp", "-s", str(cmp_equal_a), str(cmp_equal_b)],
        ),
        "cmp-diff-inline": check_output_equal(
            [str(cmp_tiered), "-s", str(cmp_equal_a), str(cmp_diff)],
            [str(fro_bin), "cmp", "-s", str(cmp_equal_a), str(cmp_diff)],
        ),
        "cmp-size-mismatch-inline": check_output_equal(
            [str(cmp_tiered), "-s", str(cmp_size_base), str(cmp_size_miss)],
            [str(fro_bin), "cmp", "-s", str(cmp_size_base), str(cmp_size_miss)],
        ),
        "cmp-large-fallback": check_output_equal(
            [str(cmp_tiered), "-s", str(cmp_large_a), str(cmp_large_b)],
            [str(fro_bin), "cmp", "-s", str(cmp_large_a), str(cmp_large_b)],
        ),
    }
    return checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--test-dir", default=Path("/data/fro-test/wc-cmp-tiered-bench"), type=Path)
    parser.add_argument("--sizes", nargs="*", default=DEFAULT_SIZES)
    parser.add_argument("--cache-state", choices=["hot", "cold", "both"], default="hot")
    parser.add_argument("--wc-inline-max", default=WC_INLINE_DEFAULT)
    parser.add_argument("--cmp-inline-max", default=CMP_INLINE_DEFAULT)
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
    cache_states = ["hot", "cold"] if args.cache_state == "both" else [args.cache_state]

    wc_tiered = repo_root / "target" / "startup-floor" / "wc_l_tiered_exec"
    cmp_tiered = repo_root / "target" / "startup-floor" / "cmp_tiered_exec"
    build_tiered_binary(
        repo_root / "examples" / "wc_l_tiered_exec.S",
        wc_tiered,
        parse_size(args.wc_inline_max),
        fro_bin,
    )
    build_tiered_binary(
        repo_root / "examples" / "cmp_tiered_exec.S",
        cmp_tiered,
        parse_size(args.cmp_inline_max),
        fro_bin,
    )

    ipc_client = repo_root / "target" / "startup-floor" / "fro_ipc_client_wc_cmp"
    socket_path = test_root / "fro-ipc.sock"
    build_ipc_client(ipc_client, socket_path)
    shadow_dir = repo_root / "target" / "startup-floor" / "wc-cmp-shadow"
    shadow_wc = ensure_shadow_link(shadow_dir, "wc", ipc_client)
    shadow_cmp = ensure_shadow_link(shadow_dir, "cmp", ipc_client)

    validations = validate_cases(test_root, wc_tiered, cmp_tiered, fro_bin)
    failed = [name for name, result in validations.items() if not result["ok"]]
    if failed:
        raise SystemExit(f"validation failed: {', '.join(failed)}")

    rows = []
    server = start_ipc_server(fro_ipc_server, socket_path)
    try:
        system_wc = shutil.which("wc")
        system_cmp = shutil.which("cmp")
        if system_wc is None or system_cmp is None:
            raise SystemExit("missing system wc/cmp")

        for size_token in args.sizes:
            size_bytes = parse_size(size_token)

            wc_fixture = test_root / f"wc-{size_token}.txt"
            if not wc_fixture.exists() or wc_fixture.stat().st_size != size_bytes:
                make_line_fixture(wc_fixture, size_bytes)

            cmp_a = test_root / f"cmp-{size_token}-a.bin"
            cmp_b = test_root / f"cmp-{size_token}-b.bin"
            if (
                not cmp_a.exists()
                or not cmp_b.exists()
                or cmp_a.stat().st_size != size_bytes
                or cmp_b.stat().st_size != size_bytes
            ):
                make_cmp_fixture_pair(cmp_a, cmp_b, size_bytes)

            wc_commands = {
                "system-wc": [system_wc, "-l", str(wc_fixture)],
                "fro-wc": [str(fro_bin), "wc", "-l", str(wc_fixture)],
                "ipc-wc": [str(shadow_wc), "-l", str(wc_fixture)],
                "tiered-wc": [str(wc_tiered), "-l", str(wc_fixture)],
            }
            cmp_commands = {
                "system-cmp": [system_cmp, "-s", str(cmp_a), str(cmp_b)],
                "fro-cmp": [str(fro_bin), "cmp", "-s", str(cmp_a), str(cmp_b)],
                "ipc-cmp": [str(shadow_cmp), "-s", str(cmp_a), str(cmp_b)],
                "tiered-cmp": [str(cmp_tiered), "-s", str(cmp_a), str(cmp_b)],
            }

            for cache_state in cache_states:
                rows.extend(
                    bench_case(
                        "wc -l",
                        size_token,
                        size_bytes,
                        [wc_fixture],
                        wc_commands,
                        cache_state,
                    )
                )
                rows.extend(
                    bench_case(
                        "cmp -s",
                        size_token,
                        size_bytes,
                        [cmp_a, cmp_b],
                        cmp_commands,
                        cache_state,
                    )
                )
    finally:
        stop_ipc_server(server)

    output = {
        "wc_inline_max_bytes": parse_size(args.wc_inline_max),
        "cmp_inline_max_bytes": parse_size(args.cmp_inline_max),
        "cache_states": cache_states,
        "test_dir": str(test_root),
        "sizes": args.sizes,
        "validations": validations,
        "rows": rows,
    }
    if args.output is None:
        args.output = test_root / "summary.json"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))

    print("case\tcache\tcommand\tsize\tmedian_ms")
    for row in rows:
        print(
            f"{row['case']}\t{row['cache_state']}\t{row['command']}\t{row['size']}\t{row['median_ms']:.3f}"
        )
    print(f"\nsummary-json\t{args.output}")


if __name__ == "__main__":
    main()
