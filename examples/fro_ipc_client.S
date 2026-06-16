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
.equ SYS_OPENAT, 257
.equ AT_FDCWD, -100
.equ O_RDONLY, 0
.equ O_DIRECTORY, 65536
.equ CMSG_LEN_FDS, 32
.equ CMSG_SPACE_FDS, 32
.equ FRAME_SIZE, 32768
.equ CONTROL_OFF, 32000
.equ IOVEC_OFF, 31968
.equ MSGHDR_OFF, 32032
.equ STATUS_OFF, 32128

.section .rodata
socket_addr:
    .word AF_UNIX
    .ascii "/tmp/fro-ipc.sock"
    .byte 0

.section .text
.global _start

_start:
    mov rbp, rsp
    mov r12, [rbp]             # argc
    test r12, r12
    jz fail

    mov eax, SYS_SOCKET
    mov edi, AF_UNIX
    mov esi, SOCK_STREAM
    xor edx, edx
    syscall
    test rax, rax
    js fail
    mov rbx, rax
    mov r15, -1

    mov eax, SYS_OPENAT
    mov edi, AT_FDCWD
    lea rsi, [rip + dot_path]
    mov edx, O_RDONLY | O_DIRECTORY
    xor r10d, r10d
    syscall
    test rax, rax
    js close_fail
    mov r15, rax

    mov eax, SYS_CONNECT
    mov rdi, rbx
    lea rsi, [rip + socket_addr]
    mov edx, SOCKADDR_LEN
    syscall
    test rax, rax
    js close_fail

    sub rsp, FRAME_SIZE
    mov r13, rsp               # payload base
    mov qword ptr [r13], r12
    mov r8, 8                  # payload length
    xor r14, r14               # argv index

.copy_arg_loop:
    cmp r14, r12
    je .payload_done
    mov rdi, [rbp + 8 + r14 * 8]
    call strlen
    mov r10, rax
    lea r11, [r8 + r10 + 1]
    cmp r11, IOVEC_OFF
    ja close_fail
    mov rcx, r10
    mov rsi, [rbp + 8 + r14 * 8]
    lea rdi, [r13 + r8]
    rep movsb
    mov byte ptr [rdi], 0
    mov r8, r11
    inc r14
    jmp .copy_arg_loop

.payload_done:
    lea rax, [r13 + CONTROL_OFF]
    mov qword ptr [rax + 0], CMSG_LEN_FDS
    mov dword ptr [rax + 8], SOL_SOCKET
    mov dword ptr [rax + 12], SCM_RIGHTS
    mov dword ptr [rax + 16], 0
    mov dword ptr [rax + 20], 1
    mov dword ptr [rax + 24], 2
    mov dword ptr [rax + 28], r15d

    lea rax, [r13 + IOVEC_OFF]
    mov qword ptr [rax + 0], r13
    mov qword ptr [rax + 8], r8

    lea rax, [r13 + MSGHDR_OFF]
    mov qword ptr [rax + 0], 0
    mov dword ptr [rax + 8], 0
    mov dword ptr [rax + 12], 0
    lea rcx, [r13 + IOVEC_OFF]
    mov qword ptr [rax + 16], rcx
    mov qword ptr [rax + 24], 1
    lea rcx, [r13 + CONTROL_OFF]
    mov qword ptr [rax + 32], rcx
    mov qword ptr [rax + 40], CMSG_SPACE_FDS
    mov dword ptr [rax + 48], 0
    mov dword ptr [rax + 52], 0

    mov eax, SYS_SENDMSG
    mov rdi, rbx
    lea rsi, [r13 + MSGHDR_OFF]
    xor edx, edx
    syscall
    test rax, rax
    js close_fail

    mov eax, SYS_CLOSE
    mov rdi, r15
    syscall

    mov eax, SYS_READ
    mov rdi, rbx
    lea rsi, [r13 + STATUS_OFF]
    mov edx, 1
    syscall
    cmp rax, 1
    jne close_fail
    movzx r10d, byte ptr [r13 + STATUS_OFF]

    mov eax, SYS_CLOSE
    mov rdi, rbx
    syscall

    mov edi, r10d
    mov eax, SYS_EXIT
    syscall

close_fail:
    cmp r15, 0
    jl .skip_cwd_close
    mov eax, SYS_CLOSE
    mov rdi, r15
    syscall
.skip_cwd_close:
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

.section .rodata
dot_path:
    .ascii "."
    .byte 0
