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
.equ SYS_RENAME, 82
.equ SYS_EXECVE, 59
.equ SYS_EXIT, 60
.equ SYS_NEWFSTATAT, 262
.equ AT_FDCWD, -100
.equ ST_MODE_OFF, 24
.equ S_IFMT, 0170000
.equ S_IFREG, 0100000
.equ S_IFDIR, 0040000
.equ ENOENT, 2
.equ SRCSTAT_OFF, PATH_BUFFER_BYTES
.equ DSTSTAT_OFF, PATH_BUFFER_BYTES + 256
.equ FRAME_BYTES, PATH_BUFFER_BYTES + 512

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
mv_name:
    .asciz "mv"

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

    mov eax, SYS_NEWFSTATAT
    mov edi, AT_FDCWD
    mov rsi, r13
    lea rdx, [rbx + SRCSTAT_OFF]
    xor r10d, r10d
    syscall
    test rax, rax
    js fallback_or_fail

    mov eax, dword ptr [rbx + SRCSTAT_OFF + ST_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    jne fallback_or_fail

    mov eax, SYS_NEWFSTATAT
    mov edi, AT_FDCWD
    mov rsi, r14
    lea rdx, [rbx + DSTSTAT_OFF]
    xor r10d, r10d
    syscall
    test rax, rax
    js maybe_plain_dest

    mov eax, dword ptr [rbx + DSTSTAT_OFF + ST_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFDIR
    jne plain_dest

    lea rdi, [rbx + 0]
    mov rsi, r14
    mov rdx, r13
    call build_target_path
    test rax, rax
    jz fallback_or_fail
    mov r15, rax
    jmp attempt_rename

maybe_plain_dest:
    cmp rax, -ENOENT
    jne fallback_or_fail

plain_dest:
    mov r15, r14

attempt_rename:
    mov eax, SYS_RENAME
    mov rdi, r13
    mov rsi, r15
    syscall
    test rax, rax
    js fallback_or_fail

    mov edi, 0
    mov eax, SYS_EXIT
    syscall

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

    lea rax, [rip + mv_name]
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

fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
