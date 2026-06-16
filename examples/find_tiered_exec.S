#ifndef PATH_BUFFER_BYTES
#define PATH_BUFFER_BYTES 4096
#endif

#ifndef DENTS_BUFFER_BYTES
#define DENTS_BUFFER_BYTES 16384
#endif

#ifndef OUTPUT_BUFFER_BYTES
#define OUTPUT_BUFFER_BYTES 16384
#endif

#ifndef FRO_MULTICALL_PATH
#define FRO_MULTICALL_PATH "/repos/fast_read_optimizer/target/release/fro"
#endif

.intel_syntax noprefix
.equ SYS_WRITE, 1
.equ SYS_CLOSE, 3
.equ SYS_GETDENTS64, 217
.equ SYS_EXECVE, 59
.equ SYS_EXIT, 60
.equ SYS_OPENAT, 257
.equ SYS_NEWFSTATAT, 262
.equ AT_FDCWD, -100
.equ AT_SYMLINK_NOFOLLOW, 256
.equ O_RDONLY, 0
.equ O_DIRECTORY, 65536
.equ DT_UNKNOWN, 0
.equ DT_DIR, 4
.equ DT_REG, 8
.equ S_IFMT, 0170000
.equ S_IFREG, 0100000
.equ S_IFDIR, 0040000
.equ STAT_MODE_OFF, 24
.equ STACK_SAVED_BYTES, 48
.equ STACK_FRAME_BYTES, STACK_SAVED_BYTES + DENTS_BUFFER_BYTES

.section .rodata
fro_path:
    .asciz FRO_MULTICALL_PATH
find_name:
    .asciz "find"
arg_type:
    .asciz "-type"
arg_f:
    .asciz "f"
newline_byte:
    .byte 10

.section .bss
    .align 16
pathbuf:
    .skip PATH_BUFFER_BYTES
outbuf:
    .skip OUTPUT_BUFFER_BYTES
outlen:
    .quad 0
statbuf:
    .skip 144

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]
    cmp r12, 4
    jne exec_fro

    mov r13, [rbp + 16]
    mov r14, [rbp + 24]
    mov r15, [rbp + 32]

    mov rdi, r14
    lea rsi, [rip + arg_type]
    call streq
    test eax, eax
    jz exec_fro

    mov rdi, r15
    lea rsi, [rip + arg_f]
    call streq
    test eax, eax
    jz exec_fro

    mov rdi, r13
    call strlen
    test rax, rax
    jz exec_fro
    cmp rax, PATH_BUFFER_BYTES - 1
    jae exec_fro
    mov r12, rax

    lea rdi, [rip + pathbuf]
    mov rsi, r13
    mov rcx, r12
    rep movsb
    mov byte ptr [rdi], 0

    mov eax, SYS_NEWFSTATAT
    mov edi, AT_FDCWD
    mov rsi, r13
    lea rdx, [rip + statbuf]
    mov r10d, AT_SYMLINK_NOFOLLOW
    syscall
    test rax, rax
    js exec_fro

    mov eax, dword ptr [rip + statbuf + STAT_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    je emit_root_file
    cmp eax, S_IFDIR
    jne exec_fro

    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    mov rsi, r13
    mov edx, O_RDONLY | O_DIRECTORY
    xor r10d, r10d
    syscall
    test rax, rax
    js exec_fro
    mov r13, rax

    mov rdi, r13
    mov rsi, r12
    call walk_dir
    mov r12, rax

    mov eax, SYS_CLOSE
    mov rdi, r13
    syscall

    test r12, r12
    js fail

    call flush_output
    test rax, rax
    js fail
    jmp success

emit_root_file:
    mov rdi, r12
    call emit_path
    test rax, rax
    js fail
    call flush_output
    test rax, rax
    js fail
    jmp success

walk_dir:
    push rbx
    push r12
    push r13
    push r14
    push r15
    push rbp
    sub rsp, DENTS_BUFFER_BYTES

    mov r12, rdi
    mov r13, rsi

walk_read:
    mov eax, SYS_GETDENTS64
    mov rdi, r12
    mov rsi, rsp
    mov edx, DENTS_BUFFER_BYTES
    syscall
    test rax, rax
    js walk_error
    je walk_done

    mov r14, rax
    xor r15, r15

walk_entry:
    cmp r15, r14
    jae walk_read

    lea rbx, [rsp + r15]
    lea rdx, [rbx + 19]

    cmp byte ptr [rdx], '.'
    jne walk_handle
    cmp byte ptr [rdx + 1], 0
    je walk_next
    cmp byte ptr [rdx + 1], '.'
    jne walk_handle
    cmp byte ptr [rdx + 2], 0
    je walk_next

walk_handle:
    mov rdi, rdx
    call strlen
    mov rbp, rax

    mov rdi, r13
    mov rsi, rdx
    mov rdx, rbp
    call append_child_path
    test rax, rax
    jz walk_error
    mov rbp, rax

    movzx eax, byte ptr [rbx + 18]
    cmp eax, DT_REG
    je walk_emit
    cmp eax, DT_DIR
    je walk_descend
    cmp eax, DT_UNKNOWN
    jne walk_next

    mov eax, SYS_NEWFSTATAT
    mov rdi, r12
    lea rsi, [rbx + 19]
    lea rdx, [rip + statbuf]
    mov r10d, AT_SYMLINK_NOFOLLOW
    syscall
    test rax, rax
    js walk_error

    mov eax, dword ptr [rip + statbuf + STAT_MODE_OFF]
    and eax, S_IFMT
    cmp eax, S_IFREG
    je walk_emit
    cmp eax, S_IFDIR
    jne walk_next

walk_descend:
    mov eax, SYS_OPENAT
    mov rdi, r12
    lea rsi, [rbx + 19]
    mov edx, O_RDONLY | O_DIRECTORY
    xor r10d, r10d
    syscall
    test rax, rax
    js walk_error

    push rax
    mov rdi, rax
    mov rsi, rbp
    call walk_dir
    mov r10, rax

    pop rdi
    mov eax, SYS_CLOSE
    syscall

    test r10, r10
    js walk_error
    jmp walk_next

walk_emit:
    mov rdi, rbp
    call emit_path
    test rax, rax
    js walk_error

walk_next:
    movzx eax, word ptr [rbx + 16]
    add r15, rax
    jmp walk_entry

walk_done:
    xor eax, eax
    jmp walk_return

walk_error:
    mov rax, -1

walk_return:
    add rsp, DENTS_BUFFER_BYTES
    pop rbp
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    ret

append_child_path:
    lea r8, [rip + pathbuf]
    mov r9, rdi
    mov r10, rdi
    cmp byte ptr [r8 + r9 - 1], '/'
    je append_no_slash
    inc r10
append_no_slash:
    mov rax, r10
    add rax, rdx
    cmp rax, PATH_BUFFER_BYTES - 1
    ja append_fail

    cmp r10, r9
    je append_copy
    mov byte ptr [r8 + r9], '/'

append_copy:
    lea rdi, [r8 + r10]
    mov rcx, rdx
    rep movsb
    mov byte ptr [r8 + rax], 0
    ret

append_fail:
    xor eax, eax
    ret

emit_path:
    mov r8, rdi
    mov rax, qword ptr [rip + outlen]
    mov r9, rax
    add rax, r8
    inc rax
    cmp rax, OUTPUT_BUFFER_BYTES
    jbe emit_buffered

    call flush_output
    test rax, rax
    js emit_fail
    xor r9d, r9d

emit_buffered:
    lea rdi, [rip + outbuf]
    add rdi, r9
    lea rsi, [rip + pathbuf]
    mov rcx, r8
    rep movsb
    mov byte ptr [rdi], 10
    inc r9
    add r9, r8
    mov qword ptr [rip + outlen], r9
    xor eax, eax
    ret

emit_fail:
    mov rax, -1
    ret

flush_output:
    mov rdx, qword ptr [rip + outlen]
    test rdx, rdx
    je flush_done
    mov edi, 1
    lea rsi, [rip + outbuf]
    call write_all
    test rax, rax
    js flush_fail
    mov qword ptr [rip + outlen], 0
flush_done:
    xor eax, eax
    ret
flush_fail:
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

exec_fro:
    sub rsp, PATH_BUFFER_BYTES
    mov r15, rsp
    mov rax, [rbp]
    mov r12, rax
    add rax, 1
    shl rax, 3
    sub rsp, rax
    mov rbx, rsp

    lea rax, [rip + find_name]
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

success:
    mov edi, 0
    mov eax, SYS_EXIT
    syscall

fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

#include "fro_exec_path.inc"
