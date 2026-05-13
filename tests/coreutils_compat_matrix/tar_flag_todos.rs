use std::collections::BTreeSet;

#[path = "../../src/help_compat.rs"]
mod help_compat;

const TAR_REMAINING_SYSTEM_TOKENS: &str = "--absolute-names, --acls, --add-file, --after-date, --after-date=DATE-OR-FILE, --anchored, --append, --atime-preserve, --auto-compress, --backup, --block-number, --blocking-factor, --bzip2, --catenate, --check-device, --check-links, --checkpoint, --checkpoint-action, --clamp-mtime, --compare, --compress, --concatenate, --confirmation, --delay-directory-restore, --delete, --dereference, --diff, --exclude, --exclude-backups, --exclude-caches, --exclude-caches-all, --exclude-caches-under, --exclude-from, --exclude-ignore, --exclude-ignore-recursive, --exclude-tag, --exclude-tag-all, --exclude-tag-under, --exclude-vcs, --exclude-vcs-ignores, --files-from, --force-local, --format, --format=gnu, --format=posix, --format=v7, --full-time, --get, --group, --group-map, --hard-dereference, --hole-detection, --ignore-case, --ignore-command-error, --ignore-failed-read, --ignore-zeros, --incremental, --index-file, --info-script, --interactive, --keep-directory-symlink, --keep-newer-files, --keep-old-files, --label, --level, --listed-incremental, --lzip, --lzma, --lzop, --mode, --mtime, --mtime=DATE-OR-FILE, --multi-volume, --new-volume-script, --newer, --newer-mtime, --newer=DATE-OR-FILE, --no-acls, --no-anchored, --no-auto-compress, --no-check-device, --no-delay-directory-restore, --no-ignore-case, --no-ignore-command-error, --no-null, --no-overwrite-dir, --no-quote-chars, --no-recursion, --no-same-owner, --no-same-permissions, --no-seek, --no-selinux, --no-unquote, --no-verbatim-files-from, --no-wildcards, --no-wildcards-match-slash, --no-xattrs, --null, --numeric-owner, --occurrence, --old-archive, --one-file-system, --one-top-level, --overwrite, --overwrite-dir, --owner, --owner-map, --pax-option, --pax-option=keyword, --portability, --posix, --preserve-order, --preserve-permissions, --quote-chars, --quoting-style, --quoting-style=escape, --read-full-records, --record-size, --recursion, --recursive-unlink, --remove-files, --restrict, --rmt-command, --rmt-command=, --rsh-command, --rsh-command=, --same-order, --same-owner, --same-permissions, --seek, --selinux, --show-defaults, --show-omitted-dirs, --show-snapshot-field-ranges, --show-stored-names, --show-transformed-names, --skip-old-files, --sort, --sparse, --sparse-version, --starting-file, --starting-file=MEMBER-NAME, --strip-components, --suffix, --tape-length, --test-label, --to-command, --to-stdout, --totals, --touch, --transform, --uncompress, --unlink-first, --unquote, --update, --usage, --use-compress-program, --utc, --verbatim-files-from, --verify, --volno-file, --warning, --wildcards, --wildcards-match-slash, --xattrs, --xattrs-exclude, --xattrs-include, --xform, --xz, -A, -B, -F, -G, -H, -I, -K, -L, -M, -N, -O, -P, -R, -S, -T, -U, -V, -W, -X, -Z, -a, -b, -d, -f-, -g, -h, -i, -j, -k, -l, -m, -n, -o, -p, -r, -s, -u, -w";

fn expected_tar_remaining_tokens() -> BTreeSet<String> {
    TAR_REMAINING_SYSTEM_TOKENS
        .split(", ")
        .map(str::to_string)
        .collect()
}

#[test]
fn tar_remaining_system_flag_inventory_matches_actual_help_gap() {
    let coverage = help_compat::help_token_coverage(env!("CARGO_BIN_EXE_fro"), "tar");
    let actual = coverage
        .missing_tokens()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected = expected_tar_remaining_tokens();
    assert_eq!(
        actual, expected,
        "tar remaining-system-token inventory drifted; update the TODO inventory or add real per-flag scenarios"
    );
}

#[test]
#[ignore = "TODO: tar still has a large remaining system-only behavioral flag matrix; add real per-flag scenarios and shrink TAR_REMAINING_SYSTEM_TOKENS as they land"]
fn tar_remaining_system_flags_real_behavior_matrix_todo() {
    assert!(
        !expected_tar_remaining_tokens().is_empty(),
        "remove this TODO test once every remaining tar system token has a real behavioral scenario"
    );
}
