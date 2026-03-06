use rand::Rng;

// Use a stochastic hill climber to find the best-performing parameters for the given IO function.
// Takes in start_params and scaling factors to convert them to use with the IO function.
// The optimizer nudges the parameters by a small integer amount, and the scaling factors are
// used to go from block_size = 3 -> block_size = 3 * 1024 * 1024
pub fn run_optimizer<F>(
    name: &str,
    start_params: Vec<u64>,
    param_scaling_factors: Vec<u64>,
    num_iterations: usize,
    verbose: bool,
    mut op: F
) where F: FnMut(&[u64]) -> u64 {
    let mut rng = rand::thread_rng();
    let mut fastest_time = 1e9;
    let mut fastest_time_decayed = fastest_time;
    let mut optimize_params = start_params.clone();
    let mut best_params = optimize_params.clone();
    let mut iterations_since_last_fastest_found = 0;
    for _i in 0..num_iterations {
        let start = std::time::Instant::now();
        iterations_since_last_fastest_found += 1;
        fastest_time_decayed *= 1.0005;
        let mut scaled_params = vec![0u64; optimize_params.len()];
        for j in 0..optimize_params.len() {
            if num_iterations > 1 {
                let jump_multiplier = (rng.gen::<f64>().powf(2.0) * (iterations_since_last_fastest_found as f64 / 4.0).log2() + 1.0) as u64;
                let r = rng.gen::<u64>();
                if r < u64::MAX / 3 { optimize_params[j] += jump_multiplier; }
                else if r < u64::MAX / 3 * 2 { optimize_params[j] = optimize_params[j].saturating_sub(jump_multiplier).max(1); }
                else { optimize_params[j] = best_params[j]; }
            }
            scaled_params[j] = optimize_params[j] * param_scaling_factors[j];
        }
        let count = op(&scaled_params);
        let cpu_time_used = start.elapsed().as_secs_f64();
        if cpu_time_used < fastest_time_decayed {
            fastest_time_decayed = cpu_time_used;
            best_params = optimize_params.clone();
            iterations_since_last_fastest_found = 0;
        }
        if cpu_time_used < fastest_time || num_iterations == 1 {
            if cpu_time_used < fastest_time { fastest_time = cpu_time_used; }
            if verbose { eprintln!("{} {} bytes in {:.4} s, {:.1} GB/s, t={} bs={}, qd={}", name, count, cpu_time_used, count as f64 / cpu_time_used / 1e9, scaled_params[0], scaled_params[1] / 1024, scaled_params[2]); }
            else if num_iterations > 1 { println!("{} {} bytes in {:.4} s, {:.1} GB/s, t={} bs={}, qd={}", name, count, cpu_time_used, count as f64 / cpu_time_used / 1e9, scaled_params[0], scaled_params[1] / 1024, scaled_params[2]); }
        }
    }
}
