#ifndef INLINE_MAX_BYTES
#define INLINE_MAX_BYTES (512 * 1024)
#endif

#ifndef STDIN_BUFFER_BYTES
#define STDIN_BUFFER_BYTES (32 * 1024)
#endif

#ifndef PATH_BUFFER_BYTES
#define PATH_BUFFER_BYTES 4096
#endif

#ifndef FRO_MULTICALL_PATH
#define FRO_MULTICALL_PATH "/repos/fast_read_optimizer/target/release/fro"
#endif

.intel_syntax noprefix
.equ SYS_READ, 0
.equ SYS_WRITE, 1
.equ SYS_CLOSE, 3
.equ SYS_LSEEK, 8
.equ SYS_EXECVE, 59
.equ SYS_EXIT, 60
.equ SYS_OPENAT, 257
.equ AT_FDCWD, -100
.equ O_RDONLY, 0
.equ SEEK_SET, 0
.equ SEEK_END, 2

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
cat_name:
    .asciz "cat"

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    cmp r12, 1
    je copy_stdin
    cmp r12, 2
    jne exec_fro

    mov r13, [rbp + 16]
    cmp byte ptr [r13], '-'
    jne open_input
    cmp byte ptr [r13 + 1], 0
    je copy_stdin
    jmp exec_fro

open_input:
    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r13
    mov edx, O_RDONLY
    xor r10d, r10d
    syscall
    test rax, rax
    js exec_fro
    mov r14, rax

    mov eax, SYS_LSEEK
    mov rdi, r14
    xor esi, esi
    mov edx, SEEK_END
    syscall
    test rax, rax
    js close_and_exec
    cmp rax, INLINE_MAX_BYTES
    ja close_and_exec

    mov eax, SYS_LSEEK
    mov rdi, r14
    xor esi, esi
    mov edx, SEEK_SET
    syscall
    test rax, rax
    js small_fail

    sub rsp, INLINE_MAX_BYTES
    mov rbx, rsp
    xor r15, r15

read_small:
    mov eax, SYS_READ
    mov rdi, r14
    lea rsi, [rbx + r15]
    mov edx, INLINE_MAX_BYTES
    sub rdx, r15
    syscall
    test rax, rax
    js small_cleanup_fail
    je small_done
    add r15, rax
    cmp r15, INLINE_MAX_BYTES
    jb read_small

small_done:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall

    mov edi, 1
    mov rsi, rbx
    mov rdx, r15
    call write_all
    test rax, rax
    js fail

    mov edi, 0
    mov eax, SYS_EXIT
    syscall

copy_stdin:
    sub rsp, STDIN_BUFFER_BYTES
    mov rbx, rsp

stdin_loop:
    mov eax, SYS_READ
    xor edi, edi
    mov rsi, rbx
    mov edx, STDIN_BUFFER_BYTES
    syscall
    test rax, rax
    js fail
    je success

    mov edi, 1
    mov rsi, rbx
    mov rdx, rax
    call write_all
    test rax, rax
    js fail
    jmp stdin_loop

close_and_exec:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall
    jmp exec_fro

small_cleanup_fail:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall

small_fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

exec_fro:
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp
    mov rax, r12
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov rbx, rsp

    lea rax, [rip + cat_name]
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
    jle write_err
    add r9, rax
    sub r8, rax
    jmp write_loop

write_done:
    xor eax, eax
    ret

write_err:
    mov rax, -1
    ret

success:
    mov edi, 0
    mov eax, SYS_EXIT
    syscall

fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
