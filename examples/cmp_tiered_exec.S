#ifndef INLINE_MAX_BYTES
#define INLINE_MAX_BYTES (128 * 1024)
#endif

#ifndef PATH_BUFFER_BYTES
#define PATH_BUFFER_BYTES 4096
#endif

#ifndef FRO_MULTICALL_PATH
#define FRO_MULTICALL_PATH "/repos/fast_read_optimizer/target/release/fro"
#endif

.intel_syntax noprefix
.equ SYS_READ, 0
.equ SYS_CLOSE, 3
.equ SYS_FSTAT, 5
.equ SYS_EXECVE, 59
.equ SYS_EXIT, 60
.equ SYS_OPENAT, 257
.equ AT_FDCWD, -100
.equ O_RDONLY, 0
.equ S_IFMT, 0xf000
.equ S_IFREG, 0x8000
.equ STAT_MODE_OFF, 24
.equ STAT_SIZE_OFF, 48
.equ TOTAL_BUF_BYTES, (INLINE_MAX_BYTES * 2)

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
cmp_name:
    .asciz "cmp"
arg_short_quiet:
    .asciz "-s"
arg_quiet:
    .asciz "--quiet"
arg_silent:
    .asciz "--silent"

.section .bss
    .align 16
statbuf1:
    .skip 144
statbuf2:
    .skip 144

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    mov r15, -1
    mov rbx, -1

    cmp r12, 4
    jne exec_fro

    mov rdi, [rbp + 16]
    lea rsi, [rip + arg_short_quiet]
    call streq
    test eax, eax
    jnz quiet_ok

    mov rdi, [rbp + 16]
    lea rsi, [rip + arg_quiet]
    call streq
    test eax, eax
    jnz quiet_ok

    mov rdi, [rbp + 16]
    lea rsi, [rip + arg_silent]
    call streq
    test eax, eax
    jz exec_fro

quiet_ok:
    mov r13, [rbp + 24]
    mov r14, [rbp + 32]
    call reject_stdin_paths
    test eax, eax
    jnz exec_fro

    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r13
    mov edx, O_RDONLY
    xor r10d, r10d
    syscall
    test rax, rax
    js exec_fro
    mov r15, rax

    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r14
    mov edx, O_RDONLY
    xor r10d, r10d
    syscall
    test rax, rax
    js close_first_and_exec
    mov rbx, rax

    mov eax, SYS_FSTAT
    mov rdi, r15
    lea rsi, [rip + statbuf1]
    syscall
    test rax, rax
    js close_both_and_exec

    mov eax, SYS_FSTAT
    mov rdi, rbx
    lea rsi, [rip + statbuf2]
    syscall
    test rax, rax
    js close_both_and_exec

    mov eax, dword ptr [rip + statbuf1 + STAT_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    jne close_both_and_exec

    mov eax, dword ptr [rip + statbuf2 + STAT_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    jne close_both_and_exec

    mov r10, qword ptr [rip + statbuf1 + STAT_SIZE_OFF]
    mov r11, qword ptr [rip + statbuf2 + STAT_SIZE_OFF]
    cmp r10, INLINE_MAX_BYTES
    ja close_both_and_exec
    cmp r11, INLINE_MAX_BYTES
    ja close_both_and_exec
    cmp r10, r11
    jne size_mismatch
    mov r12, r10
    test r12, r12
    je compare_equal

    sub rsp, TOTAL_BUF_BYTES
    mov r13, rsp
    lea r14, [r13 + INLINE_MAX_BYTES]

    mov rdi, r15
    mov rsi, r13
    mov rdx, r12
    call read_all
    test rax, rax
    js read_cleanup_exec
    cmp rax, r12
    jne read_cleanup_exec

    mov rdi, rbx
    mov rsi, r14
    mov rdx, r12
    call read_all
    test rax, rax
    js read_cleanup_exec
    cmp rax, r12
    jne read_cleanup_exec

    cld
    mov rcx, r12
    mov rsi, r13
    mov rdi, r14
    repe cmpsb
    je compare_read_equal
    add rsp, TOTAL_BUF_BYTES
    jmp compare_diff

compare_read_equal:
    add rsp, TOTAL_BUF_BYTES

compare_equal:
    call close_open_fds
    mov edi, 0
    mov eax, SYS_EXIT
    syscall

compare_diff:
size_mismatch:
    call close_open_fds
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

read_cleanup_exec:
    add rsp, TOTAL_BUF_BYTES

close_both_and_exec:
    call close_open_fds
    jmp exec_fro

close_first_and_exec:
    cmp r15, 0
    jl exec_fro
    mov eax, SYS_CLOSE
    mov rdi, r15
    syscall
    mov r15, -1
    jmp exec_fro

close_open_fds:
    cmp r15, 0
    jl close_second_if_open
    mov eax, SYS_CLOSE
    mov rdi, r15
    syscall
    mov r15, -1
close_second_if_open:
    cmp rbx, 0
    jl close_done
    mov eax, SYS_CLOSE
    mov rdi, rbx
    syscall
    mov rbx, -1
close_done:
    ret

reject_stdin_paths:
    xor eax, eax
    cmp byte ptr [r13], '-'
    jne check_second_path
    cmp byte ptr [r13 + 1], 0
    jne check_second_path
    mov eax, 1
    ret
check_second_path:
    cmp byte ptr [r14], '-'
    jne reject_done
    cmp byte ptr [r14 + 1], 0
    jne reject_done
    mov eax, 1
reject_done:
    ret

read_all:
    mov r9, rsi
    mov r10, rdx
    xor r8, r8
read_all_loop:
    cmp r8, r10
    jae read_all_done
    mov eax, SYS_READ
    lea rsi, [r9 + r8]
    mov rdx, r10
    sub rdx, r8
    syscall
    test rax, rax
    js read_all_err
    test rax, rax
    je read_all_done
    add r8, rax
    jmp read_all_loop
read_all_done:
    mov rax, r8
    ret
read_all_err:
    mov rax, -1
    ret

streq:
streq_loop:
    mov al, byte ptr [rdi]
    mov dl, byte ptr [rsi]
    cmp al, dl
    jne streq_false
    test al, al
    je streq_true
    inc rdi
    inc rsi
    jmp streq_loop

streq_true:
    mov eax, 1
    ret

streq_false:
    xor eax, eax
    ret

exec_fro:
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp
    mov rax, r12
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov r10, rsp

    lea rax, [rip + cmp_name]
    mov [r10], rax
    mov r11, 1

copy_exec_args:
    cmp r11, r12
    jae finish_exec_argv
    mov rax, [rbp + 8 + r11 * 8]
    mov [r10 + r11 * 8], rax
    inc r11
    jmp copy_exec_args

finish_exec_argv:
    mov qword ptr [r10 + r12 * 8], 0
    mov rax, r12
    add rax, 2
    shl rax, 3
    lea rdx, [rbp + rax]

    mov rdi, r15
    call resolve_fro_exec_path
    mov rdi, rax
    mov eax, SYS_EXECVE
    mov rsi, r10
    syscall
    jmp fail

fail:
    mov edi, 2
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
