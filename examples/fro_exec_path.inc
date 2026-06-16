#ifndef PATH_BUFFER_BYTES
#define PATH_BUFFER_BYTES 4096
#endif

.equ SYS_READLINK, 89

.section .rodata
proc_self_exe_path:
    .asciz "/proc/self/exe"
fro_basename:
    .asciz "fro"

.section .text
resolve_fro_exec_path:
    push rdx
    mov r8, rdi
    mov eax, SYS_READLINK
    lea rdi, [rip + proc_self_exe_path]
    mov rsi, r8
    mov edx, PATH_BUFFER_BYTES - 1
    syscall
    test rax, rax
    jle resolve_fro_exec_path_fallback
    cmp rax, PATH_BUFFER_BYTES - 1
    jae resolve_fro_exec_path_fallback

    mov byte ptr [r8 + rax], 0
    lea rcx, [r8 + rax]

resolve_fro_exec_path_scan:
    cmp rcx, r8
    je resolve_fro_exec_path_fallback
    dec rcx
    cmp byte ptr [rcx], '/'
    jne resolve_fro_exec_path_scan

    inc rcx
    lea rsi, [rip + fro_basename]

resolve_fro_exec_path_copy:
    mov al, byte ptr [rsi]
    mov byte ptr [rcx], al
    inc rcx
    inc rsi
    test al, al
    jne resolve_fro_exec_path_copy

    mov rax, r8
    pop rdx
    ret

resolve_fro_exec_path_fallback:
    lea rax, [rip + fro_path]
    pop rdx
    ret
