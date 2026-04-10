.intel_syntax noprefix
.equ AF_UNIX, 1
.equ SOCK_STREAM, 1
.equ SOL_SOCKET, 1
.equ SCM_RIGHTS, 1
.equ SYS_SOCKET, 41
.equ SYS_CONNECT, 42
.equ SYS_SENDMSG, 46
.equ SYS_READ, 0
.equ SYS_CLOSE, 3
.equ SYS_EXIT, 60
.equ SOCKADDR_LEN, 20
.equ CMSG_LEN_FDS, 28
.equ CMSG_SPACE_FDS, 32

.section .rodata
socket_addr:
    .word AF_UNIX
    .ascii "/tmp/fro-cat.sock"
    .byte 0

.section .text
.global _start

_start:
    mov rax, [rsp]
    cmp rax, 2
    ja fail

    mov r12, rax
    dec r12                    # operand count: 0 or 1
    cmp r12, 1
    ja fail

    xor r14, r14
    cmp r12, 0
    je .have_arg_len
    mov rdi, [rsp + 16]
    call strlen
    mov r14, rax
.have_arg_len:
    mov r15, 8
    cmp r12, 0
    je .payload_size_done
    add r15, r14
    inc r15
.payload_size_done:

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

    sub rsp, 768
    mov r13, rsp               # request frame base

    mov qword ptr [r13], r12   # argc
    cmp r12, 0
    je .payload_ready
    mov rcx, r14
    mov rsi, [rsp + 784]       # original argv[1]: rsp moved by 768 bytes
    lea rdi, [r13 + 8]
    rep movsb
    mov byte ptr [rdi], 0
.payload_ready:

    lea rax, [r13 + 512]       # control buffer
    mov qword ptr [rax + 0], CMSG_LEN_FDS
    mov dword ptr [rax + 8], SOL_SOCKET
    mov dword ptr [rax + 12], SCM_RIGHTS
    mov dword ptr [rax + 16], 0
    mov dword ptr [rax + 20], 1
    mov dword ptr [rax + 24], 2

    lea rax, [r13 + 480]       # iovec
    mov qword ptr [rax + 0], r13
    mov qword ptr [rax + 8], r15

    lea rax, [r13 + 544]       # msghdr
    mov qword ptr [rax + 0], 0
    mov dword ptr [rax + 8], 0
    mov dword ptr [rax + 12], 0
    lea rcx, [r13 + 480]
    mov qword ptr [rax + 16], rcx
    mov qword ptr [rax + 24], 1
    lea rcx, [r13 + 512]
    mov qword ptr [rax + 32], rcx
    mov qword ptr [rax + 40], CMSG_SPACE_FDS
    mov dword ptr [rax + 48], 0
    mov dword ptr [rax + 52], 0

    mov eax, SYS_SENDMSG
    mov rdi, rbx
    lea rsi, [r13 + 544]
    xor edx, edx
    syscall
    test rax, rax
    js close_fail

    mov eax, SYS_READ
    mov rdi, rbx
    lea rsi, [r13 + 640]
    mov edx, 1
    syscall
    cmp rax, 1
    jne close_fail
    movzx r10d, byte ptr [r13 + 200]

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
