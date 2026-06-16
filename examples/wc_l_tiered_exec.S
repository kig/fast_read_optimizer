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
.equ SYS_WRITE, 1
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

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
wc_name:
    .asciz "wc"
arg_l:
    .asciz "-l"
arg_lines:
    .asciz "--lines"
space_byte:
    .byte ' '
newline_byte:
    .byte 10

.section .bss
    .align 16
statbuf:
    .skip 144
numbuf:
    .skip 64

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    cmp r12, 2
    je parse_single_path
    cmp r12, 3
    jne exec_fro

    mov rdi, [rbp + 16]
    lea rsi, [rip + arg_l]
    call streq
    test eax, eax
    jnz parse_flagged_path

    mov rdi, [rbp + 16]
    lea rsi, [rip + arg_lines]
    call streq
    test eax, eax
    jz exec_fro

parse_flagged_path:
    mov r13, [rbp + 24]
    jmp validate_path

parse_single_path:
    mov r13, [rbp + 16]

validate_path:
    cmp byte ptr [r13], '-'
    jne open_input
    cmp byte ptr [r13 + 1], 0
    je exec_fro

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

    mov eax, SYS_FSTAT
    mov rdi, r14
    lea rsi, [rip + statbuf]
    syscall
    test rax, rax
    js close_and_exec

    mov eax, dword ptr [rip + statbuf + STAT_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    jne close_and_exec

    mov r15, qword ptr [rip + statbuf + STAT_SIZE_OFF]
    cmp r15, INLINE_MAX_BYTES
    ja close_and_exec

    sub rsp, INLINE_MAX_BYTES
    mov rbx, rsp
    xor r10, r10

read_small:
    cmp r10, r15
    jae read_done
    mov eax, SYS_READ
    mov rdi, r14
    lea rsi, [rbx + r10]
    mov rdx, r15
    sub rdx, r10
    syscall
    test rax, rax
    js read_cleanup_exec
    test rax, rax
    je read_done
    add r10, rax
    jmp read_small

read_done:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall

    xor r9, r9
    mov rcx, r10
    mov rsi, rbx

count_loop:
    test rcx, rcx
    je count_done
    cmp byte ptr [rsi], 10
    jne count_next
    inc r9
count_next:
    inc rsi
    dec rcx
    jmp count_loop

count_done:
    add rsp, INLINE_MAX_BYTES

    mov rax, r9
    call u64_to_ascii
    mov byte ptr [rsi + rdx], ' '
    inc rdx
    mov edi, 1
    call write_all
    test rax, rax
    js fail

    mov rdi, r13
    call strlen
    mov edi, 1
    mov rsi, r13
    mov rdx, rax
    call write_all
    test rax, rax
    js fail

    mov edi, 1
    lea rsi, [rip + newline_byte]
    mov edx, 1
    call write_all
    test rax, rax
    js fail

    mov edi, 0
    mov eax, SYS_EXIT
    syscall

read_cleanup_exec:
    add rsp, INLINE_MAX_BYTES

close_and_exec:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall
    jmp exec_fro

exec_fro:
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp
    mov rax, r12
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov rbx, rsp

    lea rax, [rip + wc_name]
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

strlen:
    xor eax, eax
strlen_loop:
    cmp byte ptr [rdi + rax], 0
    je strlen_done
    inc rax
    jmp strlen_loop
strlen_done:
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

u64_to_ascii:
    lea rsi, [rip + numbuf + 32]
    mov rcx, 10
    test rax, rax
    jne u64_digits
    dec rsi
    mov byte ptr [rsi], '0'
    mov edx, 1
    ret

u64_digits:
u64_digit_loop:
    xor edx, edx
    div rcx
    dec rsi
    add dl, '0'
    mov byte ptr [rsi], dl
    test rax, rax
    jne u64_digit_loop
    lea rdx, [rip + numbuf + 32]
    sub rdx, rsi
    ret

fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
