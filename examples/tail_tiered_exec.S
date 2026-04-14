#ifndef INLINE_MAX_BYTES
#define INLINE_MAX_BYTES (256 * 1024)
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
.equ SYS_FSTAT, 5
.equ SYS_EXECVE, 59
.equ SYS_EXIT, 60
.equ SYS_OPENAT, 257
.equ AT_FDCWD, -100
.equ O_RDONLY, 0
.equ SEEK_SET, 0
.equ S_IFMT, 0170000
.equ S_IFREG, 0100000
.equ STAT_SIZE, 144
.equ ST_MODE_OFFSET, 24
.equ ST_SIZE_OFFSET, 48

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
tail_name:
    .asciz "tail"

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    cmp r12, 4
    jne exec_fro

    mov r13, [rbp + 16]
    cmp byte ptr [r13], '-'
    jne exec_fro
    cmp byte ptr [r13 + 1], 'c'
    jne exec_fro
    cmp byte ptr [r13 + 2], 0
    jne exec_fro

    mov rdi, [rbp + 24]
    call parse_count
    cmp rax, -1
    je exec_fro
    mov r15, rax

    mov r14, [rbp + 32]
    cmp byte ptr [r14], '-'
    jne open_input
    cmp byte ptr [r14 + 1], 0
    je exec_fro

open_input:
    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r14
    mov edx, O_RDONLY
    xor r10d, r10d
    syscall
    test rax, rax
    js exec_fro
    mov r14, rax

    sub rsp, STAT_SIZE
    mov eax, SYS_FSTAT
    mov rdi, r14
    mov rsi, rsp
    syscall
    test rax, rax
    js restore_stat_and_exec

    mov eax, dword ptr [rsp + ST_MODE_OFFSET]
    and eax, S_IFMT
    cmp eax, S_IFREG
    jne restore_stat_and_exec

    mov r13, [rsp + ST_SIZE_OFFSET]
    add rsp, STAT_SIZE
    test r13, r13
    js close_and_exec

    xor r10, r10
    mov r8, r13
    cmp r13, r15
    jbe have_window
    mov r10, r13
    sub r10, r15
    mov r8, r15

have_window:
    mov eax, SYS_LSEEK
    mov rdi, r14
    mov rsi, r10
    mov edx, SEEK_SET
    syscall
    cmp rax, r10
    jne close_and_exec

    sub rsp, INLINE_MAX_BYTES
    mov rbx, rsp
    xor r13, r13

    cmp r8, 0
    je tail_done

read_loop:
    mov eax, SYS_READ
    mov rdi, r14
    lea rsi, [rbx + r13]
    mov rdx, r8
    sub rdx, r13
    syscall
    test rax, rax
    js tail_fail
    je tail_done
    add r13, rax
    cmp r13, r8
    jb read_loop

tail_done:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall

    mov edi, 1
    mov rsi, rbx
    mov rdx, r13
    call write_all
    test rax, rax
    js fail

    mov edi, 0
    mov eax, SYS_EXIT
    syscall

restore_stat_and_exec:
    add rsp, STAT_SIZE

close_and_exec:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall
    jmp exec_fro

tail_fail:
    mov eax, SYS_CLOSE
    mov rdi, r14
    syscall
    jmp fail

exec_fro:
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp
    mov rax, r12
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov rbx, rsp

    lea rax, [rip + tail_name]
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

parse_count:
    xor eax, eax
    xor ecx, ecx

parse_digits:
    movzx edx, byte ptr [rdi]
    cmp dl, '0'
    jb digits_done
    cmp dl, '9'
    ja digits_done
    imul rax, rax, 10
    sub dl, '0'
    add rax, rdx
    cmp rax, INLINE_MAX_BYTES
    ja parse_invalid
    inc rdi
    inc ecx
    jmp parse_digits

digits_done:
    test ecx, ecx
    jz parse_invalid
    movzx edx, byte ptr [rdi]
    test dl, dl
    je parse_success
    cmp dl, 'K'
    je suffix_k
    cmp dl, 'k'
    je suffix_k
    cmp dl, 'M'
    je suffix_m
    cmp dl, 'm'
    je suffix_m
    cmp dl, 'G'
    je suffix_g
    cmp dl, 'g'
    je suffix_g
    jmp parse_invalid

suffix_k:
    shl rax, 10
    inc rdi
    jmp suffix_done

suffix_m:
    imul rax, rax, 1048576
    inc rdi
    jmp suffix_done

suffix_g:
    imul rax, rax, 1073741824
    inc rdi

suffix_done:
    cmp byte ptr [rdi], 0
    jne parse_invalid
    cmp rax, INLINE_MAX_BYTES
    ja parse_invalid

parse_success:
    ret

parse_invalid:
    mov rax, -1
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

fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
