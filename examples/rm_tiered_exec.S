#ifndef PATH_BUFFER_BYTES
#define PATH_BUFFER_BYTES 4096
#endif

#ifndef FRO_MULTICALL_PATH
#define FRO_MULTICALL_PATH "/repos/fast_read_optimizer/target/release/fro"
#endif

.intel_syntax noprefix
.equ SYS_EXECVE, 59
.equ SYS_EXIT, 60

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
rm_name:
    .asciz "rm"

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp

    mov rax, r12
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov rbx, rsp

    lea rax, [rip + rm_name]
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

    mov edi, 1
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
