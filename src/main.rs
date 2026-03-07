use std::env;

mod common;
mod optimizer;
mod reader;
mod writer;
mod differ;

use optimizer::run_optimizer;
use reader::read_file;
use writer::write_file;
use differ::{diff_files, bench_diff_memory};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args[1] == "--help" {
        println!("USAGE: {} grep [--direct] [-v] [-n iterations] <pattern> <filename>", args[0]);
        println!("USAGE: {} read [--direct] [-n iterations] <filename>", args[0]);
        println!("USAGE: {} write [--direct] [-n iterations] <filename>", args[0]);
        println!("USAGE: {} copy [--no-direct-io] [--force-direct] [-v] [-n iterations] <source> <target>", args[0]);
        println!("USAGE: {} diff [--no-direct-io] [--force-direct] [-v] <file1> <file2>", args[0]);
        println!("USAGE: {} dual-read-bench [--no-direct-io] [--force-direct] [-v] <file1> <file2>", args[0]);
        println!("USAGE: {} bench-diff", args[0]);
        println!("USAGE: {} bench-mmap-write <filename>", args[0]);
        println!("USAGE: {} bench-write <filename>", args[0]);
        return;
    }
    let mode = args[1].as_str();
    let mut direct_io = if mode == "copy" || mode == "diff" || mode == "dual-read-bench" { true } else { false };
    let mut force_direct = false;
    let mut no_direct = false;
    let mut verbose = false;
    let mut source = None;
    let mut pattern = "";
    let mut filename = "";
    let mut _filename2 = "";
    let mut iterations = if mode == "read" || mode == "grep" { 1000 } else { 1 };
    if mode == "grep" || mode == "copy" || mode == "diff" || mode == "dual-read-bench" { iterations = 1; }
    
    let mut i = 2;
    while i < args.len() {
        if args[i] == "--direct" { direct_io = true; }
        else if args[i] == "--force-direct" { force_direct = true; }
        else if args[i] == "--no-direct-io" { no_direct = true; }
        else if args[i] == "-v" || args[i] == "--verbose" { verbose = true; }
        else if args[i] == "-n" {
            i += 1;
            if i < args.len() { iterations = args[i].parse().expect("Invalid number of iterations"); }
        } else if mode == "copy" || mode == "diff" || mode == "dual-read-bench" {
            if source.is_none() { source = Some(args[i].as_str()); }
            else if filename == "" { filename = args[i].as_str(); }
            else { _filename2 = args[i].as_str(); }
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
    let mut num_threads = 32;
    let mut block_size = 384 * 1024;
    let mut qd: usize = 1;
    let mut bsf = 4;
    if direct_io {
        num_threads = 16;
        block_size = 3 * 1024 * 1024;
        qd = 2;
        bsf = 256;
    } else if mode == "diff" || mode == "dual-read-bench" {
        num_threads = 4; // tuned for page cache diff
    }
    
    if mode == "grep" || mode == "read" {
        if verbose || mode == "read" { eprintln!("Opening file {} for {}", filename, mode); }
        run_optimizer(if mode == "grep" { "Grep" } else { "Read" }, vec![num_threads, block_size / bsf / 1024, qd as u64], vec![1, bsf * 1024, 1], iterations, verbose || mode == "read", |p| {
            read_file(pattern, filename, p[0], p[1], p[2] as usize, direct_io)
        });
    } else if mode == "write" || mode == "copy" {
        if verbose || mode == "write" { eprintln!("Opening file {} for {}", filename, mode); }
        run_optimizer(if mode == "copy" { "Copy" } else { "Write" }, vec![num_threads, block_size / bsf / 1024, qd as u64], vec![1, bsf * 1024, 1], iterations, verbose || mode == "write", |p| {
            write_file(source, filename, p[0], p[1], p[2] as usize, force_direct, no_direct)
        });
    } else if mode == "diff" || mode == "dual-read-bench" {
        if verbose { eprintln!("Opening files for {}", mode); }
        let res = diff_files(source.unwrap(), filename, num_threads, block_size, qd, force_direct, no_direct, verbose, mode == "dual-read-bench");
        if res == 0 && mode == "diff" { if verbose { eprintln!("Files are identical"); } }
        else if mode == "diff" { std::process::exit(1); }
    }
}
