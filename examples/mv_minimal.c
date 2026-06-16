#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "USAGE: %s <source> <target>\n", argv[0]);
        return 1;
    }

    if (rename(argv[1], argv[2]) != 0) {
        perror(argv[0]);
        return 1;
    }

    return 0;
}
