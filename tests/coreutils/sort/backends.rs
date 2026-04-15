use super::*;

#[test]
fn sort_month_and_version_merge_check_and_spill_match_system() {
    let tmp = unique_temp_dir("fro-coreutils-sort-month-version-backends");
    let month_left = tmp.join("month-left.txt");
    let month_right = tmp.join("month-right.txt");
    let month_unsorted = tmp.join("month-unsorted.txt");
    let version_left = tmp.join("version-left.txt");
    let version_right = tmp.join("version-right.txt");
    let version_unsorted = tmp.join("version-unsorted.txt");
    let spill_input = tmp.join("spill-input.txt");
    let fro_output = tmp.join("fro-spill.txt");
    let sys_output = tmp.join("sys-spill.txt");

    fs::write(&month_left, b"Jan\nMarx\n").unwrap();
    fs::write(&month_right, b"Apr\nDec\n").unwrap();
    fs::write(&month_unsorted, b"Feb\nJan\n").unwrap();
    fs::write(&version_left, b"v01\nv1\nv1.0\n").unwrap();
    fs::write(&version_right, b"v1.0.2\nv1.0.10\n").unwrap();
    fs::write(&version_unsorted, b"v1.0.10\nv1.0.2\n").unwrap();

    let mut spill_bytes = Vec::new();
    for idx in 0..120000 {
        spill_bytes.extend_from_slice(format!("pkg-{}.tar\n", 120000 - idx).as_bytes());
    }
    fs::write(&spill_input, &spill_bytes).unwrap();

    assert_same_result(
        run_fro(
            "sort",
            &[
                "-mM",
                month_left.to_str().unwrap(),
                month_right.to_str().unwrap(),
            ],
        ),
        run_system_sort(&[
            "-mM",
            month_left.to_str().unwrap(),
            month_right.to_str().unwrap(),
        ]),
        "sort merge month",
    );
    assert_same_result(
        run_fro("sort", &["-cM", month_unsorted.to_str().unwrap()]),
        run_system_sort(&["-cM", month_unsorted.to_str().unwrap()]),
        "sort check month",
    );
    assert_same_result(
        run_fro(
            "sort",
            &[
                "-mV",
                version_left.to_str().unwrap(),
                version_right.to_str().unwrap(),
            ],
        ),
        run_system_sort(&[
            "-mV",
            version_left.to_str().unwrap(),
            version_right.to_str().unwrap(),
        ]),
        "sort merge version",
    );
    assert_same_result(
        run_fro("sort", &["-cV", version_unsorted.to_str().unwrap()]),
        run_system_sort(&["-cV", version_unsorted.to_str().unwrap()]),
        "sort check version",
    );

    let fro = run_fro_env(
        "sort",
        &[
            "-V",
            "--no-direct",
            "-o",
            fro_output.to_str().unwrap(),
            spill_input.to_str().unwrap(),
        ],
        &[("FRO_SORT_MAX_IN_MEMORY_BYTES", "65536")],
    );
    let system = run_system_sort(&[
        "-V",
        "-o",
        sys_output.to_str().unwrap(),
        spill_input.to_str().unwrap(),
    ]);
    assert_eq!(fro.status.code(), system.status.code());
    assert!(fro.stdout.is_empty());
    assert_eq!(fro.stderr, system.stderr);
    assert_eq!(
        fs::read(&fro_output).unwrap(),
        fs::read(&sys_output).unwrap()
    );
}
