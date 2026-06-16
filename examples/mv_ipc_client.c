#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define SOCKET_PATH "/tmp/fro-mv.sock"

static int write_all(int fd, const void *buf, size_t len) {
    const char *ptr = (const char *)buf;
    while (len > 0) {
        ssize_t written = write(fd, ptr, len);
        if (written <= 0) {
            return -1;
        }
        ptr += written;
        len -= (size_t)written;
    }
    return 0;
}

int main(int argc, char **argv) {
    struct sockaddr_un addr;
    int fd;
    unsigned char status = 1;

    if (argc != 3) {
        fprintf(stderr, "USAGE: %s <source> <target>\n", argv[0]);
        return 1;
    }

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    memcpy(addr.sun_path, SOCKET_PATH, sizeof(SOCKET_PATH));
    if (connect(fd, (struct sockaddr *)&addr, sizeof(sa_family_t) + sizeof(SOCKET_PATH)) != 0) {
        close(fd);
        return 1;
    }

    if (write_all(fd, argv[1], strlen(argv[1])) != 0 ||
        write_all(fd, "\0", 1) != 0 ||
        write_all(fd, argv[2], strlen(argv[2])) != 0 ||
        write_all(fd, "\0", 1) != 0 ||
        shutdown(fd, SHUT_WR) != 0 ||
        read(fd, &status, 1) != 1) {
        close(fd);
        return 1;
    }

    close(fd);
    return status;
}
