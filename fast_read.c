#define _GNU_SOURCE
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <fcntl.h>
#include <string.h>
#include <math.h>

#define LOG2(X) ((unsigned) (8*sizeof (unsigned long long) - __builtin_clzll((X)) - 1))

int main(int argc, char **argv) {
    if (argc < 2 || strncmp(argv[1], "--help", 7) == 0) {
        printf("USAGE: fast_read [--direct] FILENAME\n");
        return -1;
    }

    int num_threads = 16;
    int block_size = 1024*1024;
    int direct_io = 0;

    direct_io = strncmp(argv[1], "--direct", 9) == 0 ? 1 : 0;

    if (direct_io) {
        block_size *= 3;
    }

    int fastest_found_num_threads = num_threads;
    int fastest_found_block_size = block_size;

    // Read /data/iotest into memory using num_threads threads.
    char *fn = argv[argc-1];

    printf("Opening file %s for reading\n", fn);

    FILE *fp = fopen(fn, "rb");
    if (fp == NULL) {
        printf("Error opening file %s\n", fn);
    }
    // Get file size
    fseek(fp, 0, SEEK_END);
    size_t fsize = ftell(fp);
    fclose(fp);
    printf("Reading %lu bytes\n", fsize);

    int iterations_since_last_fastest_found = 0;

    double fastest_time = 1e9;
    double fastest_time_decayed = fastest_time;

    for (int i = 0; i < 1000; i++) {
        double start = omp_get_wtime();

        size_t read_count = 0;

        iterations_since_last_fastest_found++;

        fastest_time_decayed *= 1.0005;

        int jump_multiplier = pow(((double)rand() / (double)RAND_MAX), 2.0) * LOG2(iterations_since_last_fastest_found/4) + 1;

        int r = rand();
        if (r < RAND_MAX/3) {
            num_threads += jump_multiplier;
        } else if (r < RAND_MAX/3 * 2) {
            num_threads -= jump_multiplier;
            if (num_threads < 1) num_threads = 1;
        } else {
            num_threads = fastest_found_num_threads;
        }

        jump_multiplier = pow(((double)rand() / (double)RAND_MAX), 2.0) * LOG2(iterations_since_last_fastest_found/4) + 1;

        r = rand();
        if (r < RAND_MAX/3) {
            block_size += jump_multiplier*256*1024;
        } else if (r < RAND_MAX/3 * 2) {
            block_size -= jump_multiplier*256*1024;
            if (block_size < 1024) block_size = 128*1024;
        } else {
            block_size = fastest_found_block_size;
        }

        // Read the entire file into memory
        // Using num_threads OMP threads
        // Read the file in 1MB chunks
        #pragma omp parallel for num_threads(num_threads)
        for (int t = 0; t < num_threads; t++) {
            int tfp = open(fn, O_RDONLY | (direct_io == 0 ? 0 : O_DIRECT), 0);
            // Allocate memory
            char *data;
            posix_memalign((void *)&data, 1024*1024, block_size);
            size_t rb = 0;
            for (off_t i = t*block_size; i < fsize; i += num_threads*block_size) {
                lseek(tfp, i, SEEK_SET);
                rb += read(tfp, data, block_size);
            }
            close(tfp);
            read_count += rb;
        }

        double cpu_time_used = omp_get_wtime() - start;

        if (cpu_time_used < fastest_time_decayed) {
            fastest_time_decayed = cpu_time_used;
            fastest_found_num_threads += (num_threads - fastest_found_num_threads);
            fastest_found_block_size += (block_size - fastest_found_block_size);
            iterations_since_last_fastest_found = 0;
        }

        if (cpu_time_used < fastest_time) {
            fastest_time = cpu_time_used;
            printf("Read %lu bytes in %f s, %.1f GB/s, t=%d bs=%d\n", read_count, cpu_time_used, read_count / cpu_time_used / 1e9, num_threads, block_size/1024);
        }
    }
    return 0;
}

