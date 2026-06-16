.intel_syntax noprefix
.equ AF_UNIX, 1
.equ SOCK_STREAM, 1
.equ SYS_SOCKET, 41
.equ SYS_CONNECT, 42
.equ SYS_READ, 0
.equ SYS_WRITE, 1
.equ SYS_CLOSE, 3
.equ SYS_SHUTDOWN, 48
.equ SYS_EXIT, 60
.equ SHUT_WR, 1
.equ SOCKADDR_LEN, 19
.equ MAX_PATH_LEN, 2047

.section .rodata
socket_addr:
    .word AF_UNIX
    .ascii "/tmp/fro-mv.sock"
    .byte 0

.section .text
.global _start

_start:
    mov rax, [rsp]
    cmp rax, 3
    jne fail

    mov r12, [rsp + 16]
    mov r13, [rsp + 24]

    mov rdi, r12
    call strlen
    mov r14, rax
    cmp r14, MAX_PATH_LEN
    ja fail

    mov rdi, r13
    call strlen
    mov r15, rax
    cmp r15, MAX_PATH_LEN
    ja fail

    mov eax, SYS_SOCKET
    mov edi, AF_UNIX
    mov esi, SOCK_STREAM
    xor edx, edx
    syscall
    test rax, rax
    js fail
    mov rbx, rax

    mov eax, SYS_CONNECT
    mov rdi, rbx
    lea rsi, [rip + socket_addr]
    mov edx, SOCKADDR_LEN
    syscall
    test rax, rax
    js close_fail

    mov rax, r14
    add rax, r15
    add rax, 2
    sub rsp, rax
    mov r8, rsp
    mov rcx, r14
    mov rsi, r12
    mov rdi, r8
    rep movsb
    mov byte ptr [rdi], 0
    inc rdi
    mov rcx, r15
    mov rsi, r13
    rep movsb
    mov byte ptr [rdi], 0

    mov rdx, r14
    add rdx, r15
    add rdx, 2
    mov rdi, rbx
    mov rsi, r8
    call write_all
    test rax, rax
    js close_fail

    mov eax, SYS_SHUTDOWN
    mov rdi, rbx
    mov esi, SHUT_WR
    syscall
    test rax, rax
    js close_fail

    sub rsp, 8
    mov eax, SYS_READ
    mov rdi, rbx
    mov rsi, rsp
    mov edx, 1
    syscall
    cmp rax, 1
    jne close_fail
    movzx edi, byte ptr [rsp]
    mov r10d, edi

    mov eax, SYS_CLOSE
    mov rdi, rbx
    syscall

    mov edi, r10d
    mov eax, SYS_EXIT
    syscall

close_fail:
    mov eax, SYS_CLOSE
    mov rdi, rbx
    syscall
fail:
    mov edi, 1
    mov eax, SYS_EXIT
    syscall

strlen:
    xor eax, eax
.strlen_loop:
    cmp byte ptr [rdi + rax], 0
    je .strlen_done
    inc rax
    jmp .strlen_loop
.strlen_done:
    ret

write_all:
    mov r9, rdx
    mov r10, rsi
.write_loop:
    test r9, r9
    je .write_done
    mov eax, SYS_WRITE
    syscall
    test rax, rax
    jle .write_err
    add r10, rax
    mov rsi, r10
    sub r9, rax
    mov rdx, r9
    jmp .write_loop
.write_done:
    xor eax, eax
    ret
.write_err:
    mov rax, -1
    ret
