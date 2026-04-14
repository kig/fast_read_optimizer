#ifndef INLINE_MAX_BYTES
#define INLINE_MAX_BYTES (256 * 1024)
#endif

#ifndef PATH_BUFFER_BYTES
#define PATH_BUFFER_BYTES 4096
#endif

#ifndef FRO_MULTICALL_PATH
#define FRO_MULTICALL_PATH "/repos/fast_read_optimizer/target/release/fro"
#endif

#ifndef NO_FALLBACK
#define NO_FALLBACK 0
#endif

.intel_syntax noprefix
.equ SYS_READ, 0
.equ SYS_WRITE, 1
.equ SYS_CLOSE, 3
.equ SYS_FSTAT, 5
.equ SYS_EXECVE, 59
.equ SYS_EXIT, 60
.equ SYS_OPENAT, 257
.equ SYS_NEWFSTATAT, 262
.equ AT_FDCWD, -100
.equ O_RDONLY, 0
.equ O_WRONLY, 1
.equ O_CREAT, 64
.equ O_EXCL, 128
.equ PATH_STAT_OFF, PATH_BUFFER_BYTES
.equ SRCSTAT_OFF, PATH_BUFFER_BYTES
.equ DSTSTAT_OFF, PATH_BUFFER_BYTES + 256
.equ SIZE_OFF, PATH_BUFFER_BYTES + 512
.equ SRCFD_OFF, PATH_BUFFER_BYTES + 520
.equ DSTFD_OFF, PATH_BUFFER_BYTES + 528
.equ DATA_OFF, PATH_BUFFER_BYTES + 536
.equ FRAME_BYTES, PATH_BUFFER_BYTES + 536 + INLINE_MAX_BYTES
.equ ST_MODE_OFF, 24
.equ ST_SIZE_OFF, 48
.equ S_IFMT, 0170000
.equ S_IFREG, 0100000
.equ S_IFDIR, 0040000
.equ MODE_MASK, 0777
.equ ENOENT, 2

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
cp_name:
    .asciz "cp"

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    cmp r12, 3
    jne exec_fro

    mov r13, [rbp + 16]
    mov r14, [rbp + 24]
    cmp byte ptr [r13], '-'
    je exec_fro
    cmp byte ptr [r14], '-'
    je exec_fro

    sub rsp, FRAME_BYTES
    mov rbx, rsp
    mov qword ptr [rbx + SRCFD_OFF], -1
    mov qword ptr [rbx + DSTFD_OFF], -1

    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r13
    mov edx, O_RDONLY
    xor r10d, r10d
    syscall
    test rax, rax
    js close_and_exec
    mov [rbx + SRCFD_OFF], rax

    mov eax, SYS_FSTAT
    mov rdi, [rbx + SRCFD_OFF]
    lea rsi, [rbx + SRCSTAT_OFF]
    syscall
    test rax, rax
    js close_and_exec

    mov eax, dword ptr [rbx + SRCSTAT_OFF + ST_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    jne close_and_exec

    mov r11, [rbx + SRCSTAT_OFF + ST_SIZE_OFF]
    mov [rbx + SIZE_OFF], r11
    cmp r11, INLINE_MAX_BYTES
    ja close_and_exec

    mov eax, SYS_NEWFSTATAT
    mov edi, AT_FDCWD
    mov rsi, r14
    lea rdx, [rbx + DSTSTAT_OFF]
    xor r10d, r10d
    syscall
    test rax, rax
    js maybe_missing_dest

    mov eax, dword ptr [rbx + DSTSTAT_OFF + ST_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFDIR
    jne close_and_exec

    lea rdi, [rbx + 0]
    mov rsi, r14
    mov rdx, r13
    call build_target_path
    test rax, rax
    jz close_and_exec
    mov r15, rax
    jmp resolved_dest

maybe_missing_dest:
    cmp rax, -ENOENT
    jne close_and_exec
    mov r15, r14

resolved_dest:
    mov eax, SYS_NEWFSTATAT
    mov edi, AT_FDCWD
    mov rsi, r15
    lea rdx, [rbx + DSTSTAT_OFF]
    xor r10d, r10d
    syscall
    test rax, rax
    js target_missing
    jmp close_and_exec

target_missing:
    cmp rax, -ENOENT
    jne close_and_exec

    mov r10d, dword ptr [rbx + SRCSTAT_OFF + ST_MODE_OFF]
    and r10d, MODE_MASK
    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r15
    mov edx, O_WRONLY | O_CREAT | O_EXCL
    syscall
    test rax, rax
    js close_and_exec
    mov [rbx + DSTFD_OFF], rax

    mov r11, [rbx + SIZE_OFF]
    test r11, r11
    je copy_done

    mov rdi, [rbx + SRCFD_OFF]
    lea rsi, [rbx + DATA_OFF]
    mov rdx, r11
    call read_all
    test rax, rax
    js close_and_fail
    mov r11, rax

    mov rdi, [rbx + DSTFD_OFF]
    lea rsi, [rbx + DATA_OFF]
    mov rdx, r11
    call write_all
    test rax, rax
    js close_and_fail

copy_done:
    mov eax, SYS_CLOSE
    mov rdi, [rbx + DSTFD_OFF]
    syscall
    mov qword ptr [rbx + DSTFD_OFF], -1

    mov eax, SYS_CLOSE
    mov rdi, [rbx + SRCFD_OFF]
    syscall
    mov qword ptr [rbx + SRCFD_OFF], -1

    mov edi, 0
    mov eax, SYS_EXIT
    syscall

close_and_exec:
    mov rax, [rbx + DSTFD_OFF]
    cmp rax, 0
    jl close_src_only_exec
    mov eax, SYS_CLOSE
    mov rdi, [rbx + DSTFD_OFF]
    syscall
    mov qword ptr [rbx + DSTFD_OFF], -1

close_src_only_exec:
    mov rax, [rbx + SRCFD_OFF]
    cmp rax, 0
    jl fallback_or_fail
    mov eax, SYS_CLOSE
    mov rdi, [rbx + SRCFD_OFF]
    syscall
    mov qword ptr [rbx + SRCFD_OFF], -1
    jmp fallback_or_fail

close_and_fail:
    mov rax, [rbx + DSTFD_OFF]
    cmp rax, 0
    jl close_src_only_fail
    mov eax, SYS_CLOSE
    mov rdi, [rbx + DSTFD_OFF]
    syscall
    mov qword ptr [rbx + DSTFD_OFF], -1

close_src_only_fail:
    mov rax, [rbx + SRCFD_OFF]
    cmp rax, 0
    jl fail
    mov eax, SYS_CLOSE
    mov rdi, [rbx + SRCFD_OFF]
    syscall
    mov qword ptr [rbx + SRCFD_OFF], -1
    jmp fail

fallback_or_fail:
#if NO_FALLBACK
    jmp fail
#else
    jmp exec_fro
#endif

exec_fro:
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp
    mov rax, r12
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov rbx, rsp

    lea rax, [rip + cp_name]
    mov [rbx], rax
    mov r13, 1

copy_args:
    cmp r13, r12
    jae finish_exec_argv
    mov rax, [rbp + 8 + r13 * 8]
    mov [rbx + r13 * 8], rax
    inc r13
    jmp copy_args

finish_exec_argv:
    mov qword ptr [rbx + r12 * 8], 0
    mov rax, r12
    add rax, 2
    shl rax, 3
    lea rdx, [rbp + rax]

    mov rdi, r15
    call resolve_fro_exec_path
    mov rdi, rax
    mov eax, SYS_EXECVE
    mov rsi, rbx
    syscall
    jmp fail

build_target_path:
    mov r8, rdi
    mov r9, rsi
    mov r10, rdx

    mov rdi, r9
    call strlen
    mov r11, rax

    mov rdi, r10
    call basename_ptr
    mov r10, rax

    mov rdi, r10
    call strlen
    test rax, rax
    jz build_target_fail
    mov rdx, rax

    mov rax, r11
    add rax, rdx
    test r11, r11
    je build_target_size
    cmp byte ptr [r9 + r11 - 1], '/'
    je build_target_size
    inc rax

build_target_size:
    inc rax
    cmp rax, PATH_BUFFER_BYTES
    ja build_target_fail

    mov rdi, r8
    mov rsi, r9
    mov rcx, r11
    rep movsb

    test r11, r11
    je copy_basename
    cmp byte ptr [r9 + r11 - 1], '/'
    je copy_basename
    mov byte ptr [rdi], '/'
    inc rdi

copy_basename:
    mov rsi, r10
    mov rcx, rdx
    rep movsb
    mov byte ptr [rdi], 0
    mov rax, r8
    ret

build_target_fail:
    xor eax, eax
    ret

basename_ptr:
    mov rax, rdi
    mov rcx, rdi

basename_loop:
    mov dl, byte ptr [rcx]
    test dl, dl
    je basename_done
    cmp dl, '/'
    jne basename_next
    lea rax, [rcx + 1]

basename_next:
    inc rcx
    jmp basename_loop

basename_done:
    ret

strlen:
    xor eax, eax
strlen_loop:
    cmp byte ptr [rdi + rax], 0
    je strlen_done
    inc rax
    jmp strlen_loop
strlen_done:
    ret

read_all:
    mov r8, rdx
    mov r9, rsi
    xor r10, r10

read_loop:
    test r8, r8
    je read_done
    mov eax, SYS_READ
    mov rsi, r9
    mov rdx, r8
    syscall
    test rax, rax
    js io_error
    je read_done
    add r9, rax
    add r10, rax
    sub r8, rax
    jmp read_loop

read_done:
    mov rax, r10
    ret

write_all:
    mov r8, rdx
    mov r9, rsi

write_loop:
    test r8, r8
    je write_done
    mov eax, SYS_WRITE
    mov rsi, r9
    mov rdx, r8
    syscall
    test rax, rax
    jle io_error
    add r9, rax
    sub r8, rax
    jmp write_loop

write_done:
    xor eax, eax
    ret

io_error:
    mov rax, -1
    ret

fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
