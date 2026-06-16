.intel_syntax noprefix
.global _start
.text

_start:
    mov rax, [rsp]
    cmp rax, 3
    jne exit_fail

    mov rdi, [rsp + 16]
    mov rsi, [rsp + 24]
    mov eax, 82
    syscall
    test rax, rax
    js exit_fail

    xor edi, edi
    mov eax, 60
    syscall

exit_fail:
    mov edi, 1
    mov eax, 60
    syscall
