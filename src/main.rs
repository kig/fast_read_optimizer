use std::env;

mod config;
mod common;
mod optimizer;
mod reader;
mod writer;
mod differ;
mod mincore;

use config::AppConfig;
use optimizer::run_optimizer;
use reader::read_file;
use writer::write_file;
use differ::{diff_files, bench_diff_memory};
use std::fs::{File};
use std::io::{Seek, SeekFrom};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args[1] == "--help" {
        println!("USAGE: {} grep [--no-direct] [--direct] [-v] [-n iterations] <pattern> <filename>", args[0]);
        println!("USAGE: {} read [--no-direct] [--direct] [-n iterations] <filename>", args[0]);
        println!("USAGE: {} write [--no-direct] [--direct] [-n iterations] <filename>", args[0]);
        println!("USAGE: {} copy [--no-direct] [--direct] [-v] [-n iterations] <source> <target>", args[0]);
        println!("USAGE: {} diff [--no-direct] [--direct] [-v] [-n iterations] <file1> <file2>", args[0]);
        println!("USAGE: {} dual-read-bench [--no-direct] [--direct] [-v] [-n iterations] <file1> <file2>", args[0]);
        println!("USAGE: {} bench-diff", args[0]);
        println!("USAGE: {} bench-mmap-write <filename>", args[0]);
        println!("USAGE: {} bench-write <filename>", args[0]);
        return;
    }
    let mode = args[1].as_str();
    let mut io_mode = common::IOMode::Auto;
    let mut io_mode_write = common::IOMode::Auto;
    let mut verbose = false;
    let mut source = None;
    let mut pattern = "";
    let mut filename = "";
    let mut iterations = if mode == "read" { 1000 } else { 1 };
    let mut save_config = false;
    
    let mut i = 2;
    while i < args.len() {
        if args[i] == "--direct" {
            io_mode = common::IOMode::Direct;
            io_mode_write = common::IOMode::Direct;
        } else if args[i] == "--no-direct" {
            io_mode = common::IOMode::PageCache;
            io_mode_write = common::IOMode::PageCache;
        } else if args[i] == "--auto" {
            io_mode = common::IOMode::Auto;
            io_mode_write = common::IOMode::Auto;
        } else if args[i] == "--direct-write" {
            io_mode_write = common::IOMode::Direct;
        } else if args[i] == "--no-direct-write" {
            io_mode_write = common::IOMode::PageCache;
        } else if args[i] == "--auto-write" {
            io_mode_write = common::IOMode::Auto;
        } else if args[i] == "-v" || args[i] == "--verbose" { verbose = true; }
        else if args[i] == "-s" || args[i] == "--save" { save_config = true; } 
        else if args[i] == "-n" {
            i += 1;
            if i < args.len() { iterations = args[i].parse().expect("Invalid number of iterations"); }
        } else if mode == "copy" || mode == "diff" || mode == "dual-read-bench" {
            if source.is_none() { source = Some(args[i].as_str()); }
            else if filename == "" { filename = args[i].as_str(); }
        } else if mode == "grep" {
            if pattern == "" { pattern = args[i].as_str(); }
            else { filename = args[i].as_str(); }
        } else { filename = args[i].as_str(); }
        i += 1;
    }
    if mode == "bench-diff" { bench_diff_memory(16, 1024 * 1024); return; }
    if mode == "bench-mmap-write" {
        if filename == "" { println!("Filename missing"); return; }
        writer::bench_mmap_write(filename);
        return;
    }
    if mode == "bench-write" {
        if filename == "" { println!("Filename missing"); return; }
        writer::bench_write(filename);
        return;
    }

    if filename == "" { println!("Filename missing"); return; }

    let mut config = AppConfig::load("fast_read_optimizer.json");

    let params_page_cache = config.get_params(mode, false);
    let params_direct = config.get_params(mode, true);
    
    let num_threads_pc = params_page_cache.num_threads;
    let qd_pc = params_page_cache.qd;

    let num_threads_direct = params_direct.num_threads;
    let qd_direct = params_direct.qd;

    // We reverse the scaling factor logic here since run_optimizer multiplies by bsf*1024
    let base_block_size_pc = params_page_cache.block_size / (4 * 1024);
    let base_block_size_direct = params_direct.block_size / (256 * 1024);

    let start_params = vec![num_threads_pc, base_block_size_pc, qd_pc as u64, num_threads_direct, base_block_size_direct, qd_direct as u64];
    let params_steps = vec![1, 4 * 1024, 1, 1, 256 * 1024, 1];

    let mode_name = mode;
    let verbose = verbose || mode == "read" || mode == "write";
    if verbose { eprintln!("Opening file {} for {}", filename, mode); }    

    let mut exit_code = 0;
    
    let mode_callback = |p: &[u64]| {
        if mode == "read" || mode == "grep" {
            read_file(pattern, filename, p[0], p[1], p[2] as usize, p[3], p[4], p[5] as usize, io_mode)
        } else if mode == "write" {
            write_file(
                source, filename,
                p[0], p[1], p[2] as usize, 
                p[3], p[4], p[5] as usize, 
                io_mode, io_mode_write
            )
        } else if mode == "copy" {
            write_file(
                source, filename,
                p[0], p[1], p[2] as usize, 
                p[3], p[4], p[5] as usize, 
                io_mode, io_mode_write
            )
        } else if mode == "diff" || mode == "dual-read-bench" {
            let mut f1_check = File::open(source.unwrap()).unwrap();
            let s1 = f1_check.seek(SeekFrom::End(0)).unwrap();
            let mut f2_check = File::open(filename).unwrap();
            let s2 = f2_check.seek(SeekFrom::End(0)).unwrap();
            if s1 != s2 {
                if verbose { eprintln!("Files have different sizes: {} != {}", s1, s2); }
                if mode == "diff" { exit_code = 1; }
            }

            if exit_code == 0 {
                let bench_only = mode == "dual-read-bench";
                let size = std::fs::File::open(filename).unwrap().metadata().unwrap().len();
                let res = diff_files(
                    source.unwrap(), filename,
                    p[0], p[1], p[2] as usize,
                    p[3], p[4], p[5] as usize, 
                    io_mode,
                    bench_only
                );
                if res != 0 && mode == "diff" { exit_code = 1; }
                size * 2
            } else { 0 }
        } else {
            panic!("Invalid mode {}", mode)
        }
    };

    let best_params = run_optimizer(
        mode_name,
        start_params,
        params_steps,
        iterations,
        verbose,
        mode_callback
    );

    if save_config && io_mode != common::IOMode::Auto {
        let direct = io_mode == common::IOMode::Direct;
        config.update_params(mode, direct, config::IOParams { num_threads: best_params[0], block_size: best_params[1], qd: best_params[2] as usize });
        config.save("fast_read_optimizer.json");
    }
}
