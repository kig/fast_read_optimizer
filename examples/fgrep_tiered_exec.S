#ifndef INLINE_MAX_BYTES
#define INLINE_MAX_BYTES (16 * 1024)
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
fgrep_name:
    .asciz "fgrep"

.section .bss
    .align 16
statbuf:
    .skip 144
pattern_len_qword:
    .skip 8
file_size_qword:
    .skip 8
matched_flag_qword:
    .skip 8

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    mov r15, -1

    cmp r12, 3
    jne exec_fro

    mov r13, [rbp + 16]
    mov r14, [rbp + 24]
    cmp byte ptr [r13], '-'
    je exec_fro
    cmp byte ptr [r14], '-'
    je exec_fro

    mov rdi, r13
    call strlen
    mov qword ptr [rip + pattern_len_qword], rax

    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r14
    mov edx, O_RDONLY
    xor r10d, r10d
    syscall
    test rax, rax
    js exec_fro
    mov r15, rax

    mov eax, SYS_FSTAT
    mov rdi, r15
    lea rsi, [rip + statbuf]
    syscall
    test rax, rax
    js close_and_exec

    mov eax, dword ptr [rip + statbuf + STAT_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    jne close_and_exec

    mov r10, qword ptr [rip + statbuf + STAT_SIZE_OFF]
    cmp r10, INLINE_MAX_BYTES
    ja close_and_exec
    mov qword ptr [rip + file_size_qword], r10

    sub rsp, INLINE_MAX_BYTES
    mov rbx, rsp

    mov rdi, r15
    mov rsi, rbx
    mov rdx, r10
    call read_all
    test rax, rax
    js read_cleanup_exec
    cmp rax, qword ptr [rip + file_size_qword]
    jne read_cleanup_exec

    mov eax, SYS_CLOSE
    mov rdi, r15
    syscall
    mov r15, -1

    mov r10, qword ptr [rip + file_size_qword]
    lea r12, [rbx + r10]
    mov r14, rbx
    mov rdx, rbx
    mov qword ptr [rip + matched_flag_qword], 0

scan_loop:
    cmp rdx, r12
    jae scan_tail
    cmp byte ptr [rdx], 10
    jne scan_next_char

    mov rax, rdx
    sub rax, r14
    inc rax
    push rdx
    push rax
    mov rdi, r14
    mov rsi, rax
    mov rdx, r13
    mov rcx, qword ptr [rip + pattern_len_qword]
    call line_matches
    pop rdx
    pop rcx
    mov r15, rcx
    test eax, eax
    jz scan_next_line
    mov edi, 1
    mov rsi, r14
    call write_all
    test rax, rax
    js cleanup_fail
    mov qword ptr [rip + matched_flag_qword], 1

scan_next_line:
    lea r14, [r15 + 1]
    mov rdx, r15

scan_next_char:
    inc rdx
    jmp scan_loop

scan_tail:
    cmp r14, r12
    jae scan_done
    mov rax, r12
    sub rax, r14
    push rax
    mov rdi, r14
    mov rsi, rax
    mov rdx, r13
    mov rcx, qword ptr [rip + pattern_len_qword]
    call line_matches
    pop rdx
    test eax, eax
    jz scan_done
    mov edi, 1
    mov rsi, r14
    call write_all
    test rax, rax
    js cleanup_fail
    mov qword ptr [rip + matched_flag_qword], 1

scan_done:
    add rsp, INLINE_MAX_BYTES
    cmp qword ptr [rip + matched_flag_qword], 0
    jz nomatch_exit
    mov edi, 0
    mov eax, SYS_EXIT
    syscall

nomatch_exit:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

read_cleanup_exec:
    add rsp, INLINE_MAX_BYTES

close_and_exec:
    cmp r15, 0
    jl exec_fro
    mov eax, SYS_CLOSE
    mov rdi, r15
    syscall
    mov r15, -1
    jmp exec_fro

cleanup_fail:
    add rsp, INLINE_MAX_BYTES
    jmp fail

line_matches:
    test rcx, rcx
    je match_true
    cmp rsi, rcx
    jb match_false

    mov r8, rdi
    mov r9, rsi
    sub r9, rcx
    add r9, rdi

search_pos:
    cmp r8, r9
    ja match_false
    xor r10, r10

compare_loop:
    cmp r10, rcx
    jae match_true
    mov al, byte ptr [r8 + r10]
    cmp al, byte ptr [rdx + r10]
    jne advance_search
    inc r10
    jmp compare_loop

advance_search:
    inc r8
    jmp search_pos

match_true:
    mov eax, 1
    ret

match_false:
    xor eax, eax
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

read_all:
    mov r8, rsi
    mov r9, rdx
    xor r10, r10

read_loop:
    cmp r10, r9
    jae read_done
    mov eax, SYS_READ
    lea rsi, [r8 + r10]
    mov rdx, r9
    sub rdx, r10
    syscall
    test rax, rax
    js read_err
    test rax, rax
    je read_done
    add r10, rax
    jmp read_loop

read_done:
    mov rax, r10
    ret

read_err:
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

exec_fro:
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp
    mov rax, r12
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov rbx, rsp

    lea rax, [rip + fgrep_name]
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

fail:
    mov edi, 2
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
