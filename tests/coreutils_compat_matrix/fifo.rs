use super::*;

#[test]
fn fifo_text_inputs_match_system_output() {
    let _lock = FIFO_TEST_LOCK.lock().unwrap();
    let tmp = unique_temp_dir("fro-coreutils-fifo-matrix");
    let text = b"alpha\nneedle beta\nomega\n".to_vec();
    let text_fifo = tmp.join("text-default.fifo");

    let fro = with_fifo_input(&text_fifo, &text, |fifo_path| run_fro("cat", &[fifo_path]));
    let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
        run_system("cat", &[fifo_path])
    });
    assert_same_result(fro, sys, "cat fifo");

    let fro = with_fifo_input(&text_fifo, &text, |fifo_path| run_fro("tac", &[fifo_path]));
    let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
        run_system("tac", &[fifo_path])
    });
    assert_same_result(fro, sys, "tac fifo");

    for head_args in [vec!["-c", "5"], vec!["-n", "2"], vec!["-n", "4096"]] {
        let fro = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = head_args.clone();
            args.push(fifo_path);
            run_fro("head", &args)
        });
        let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = head_args.clone();
            args.push(fifo_path);
            run_system("head", &args)
        });
        let mut label_args = head_args.clone();
        label_args.push("FIFO");
        assert_same_result(fro, sys, &format!("head fifo {:?}", label_args));
    }

    for tail_args in [vec!["-c", "5"], vec!["-n", "2"]] {
        let fro = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = tail_args.clone();
            args.push(fifo_path);
            run_fro("tail", &args)
        });
        let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = tail_args.clone();
            args.push(fifo_path);
            run_system("tail", &args)
        });
        let mut label_args = tail_args.clone();
        label_args.push("FIFO");
        assert_same_result(fro, sys, &format!("tail fifo {:?}", label_args));
    }

    for wc_flags in wc_flag_sets() {
        let fro = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = wc_flags.clone();
            args.push(fifo_path);
            run_fro("wc", &args)
        });
        let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = wc_flags.clone();
            args.push(fifo_path);
            run_system("wc", &args)
        });
        let mut label_args = wc_flags.clone();
        label_args.push("FIFO");
        assert_same_wc(fro, sys, &format!("wc fifo {:?}", label_args));
    }

    for grep_prefix in [vec!["needle"], vec!["-n", "needle"]] {
        let fro = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut args = grep_prefix.clone();
            args.push(fifo_path);
            run_fro("fgrep", &args)
        });
        let sys = with_fifo_input(&text_fifo, &text, |fifo_path| {
            let mut grep_args = grep_prefix.clone();
            grep_args.push(fifo_path);
            let mut args = vec!["-F"];
            args.extend(grep_args.iter().copied());
            run_system("grep", &args)
        });
        let mut label_args = grep_prefix.clone();
        label_args.push("FIFO");
        assert_same_result(fro, sys, &format!("fgrep fifo {:?}", label_args));
    }
}

#[test]
fn fifo_tail_large_byte_windows_match_system_output() {
    let _lock = FIFO_TEST_LOCK.lock().unwrap();
    let tmp = unique_temp_dir("fro-coreutils-fifo-tail-large");
    let binary = (0..(2 * 1024 * 1024 + 12345))
        .map(|i| ((i * 29) % 251) as u8)
        .collect::<Vec<_>>();
    let binary_fifo = tmp.join("tail-large.fifo");

    for tail_args in [vec!["-c", "65536"], vec!["-c", "1572865"]] {
        let fro = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
            let mut args = tail_args.clone();
            args.push(fifo_path);
            run_fro("tail", &args)
        });
        let sys = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
            let mut args = tail_args.clone();
            args.push(fifo_path);
            run_system("tail", &args)
        });
        let mut label_args = tail_args.clone();
        label_args.push("FIFO");
        assert_same_result(fro, sys, &format!("tail fifo {:?}", label_args));
    }
}

#[test]
fn fifo_hash_inputs_match_system_output() {
    let _lock = FIFO_TEST_LOCK.lock().unwrap();
    let tmp = unique_temp_dir("fro-coreutils-fifo-hash");
    let binary = (0..65557)
        .map(|i| ((i * 17) % 251) as u8)
        .collect::<Vec<_>>();
    let binary_fifo = tmp.join("binary-default.fifo");

    let fro = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_fro("cksum", &[fifo_path])
    });
    let sys = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_system("cksum", &[fifo_path])
    });
    assert_same_result(fro, sys, "cksum fifo");

    let fro = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_fro("sha256sum", &[fifo_path])
    });
    let sys = with_fifo_input(&binary_fifo, &binary, |fifo_path| {
        run_system("sha256sum", &[fifo_path])
    });
    assert_same_result(fro, sys, "sha256sum fifo");
}
