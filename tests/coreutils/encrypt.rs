use super::*;

const OPENSSL_HEADER_LEN: usize = 16;
const ENCRYPT_INPUT_SIZE_FOR_TEST: usize = 2 * 512 * 1024;

fn openssl_header(ciphertext: &[u8]) -> &[u8] {
    &ciphertext[..OPENSSL_HEADER_LEN]
}

#[test]
fn encrypt_decrypt_roundtrip_matches_plaintext() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-roundtrip");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let decrypted = tmp.join("plain.out");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(1024 * 1024 + 137))
        .map(|i| ((i * 29 + 7) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"roundtrip-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(
        encrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&encrypt.stdout),
        String::from_utf8_lossy(&encrypt.stderr)
    );
    assert!(encrypt.stdout.is_empty());
    assert!(encrypt.stderr.is_empty());

    let ciphertext_bytes = fs::read(&ciphertext).unwrap();
    assert_eq!(&openssl_header(&ciphertext_bytes)[..8], b"Salted__");
    assert_ne!(ciphertext_bytes, bytes);

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            decrypted.to_str().unwrap(),
            ciphertext.to_str().unwrap(),
        ],
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert!(decrypt.stdout.is_empty());
    assert!(decrypt.stderr.is_empty());
    assert_eq!(fs::read(&decrypted).unwrap(), bytes);
}

#[test]
fn encrypt_decrypt_small_regular_file_roundtrip_matches_plaintext() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-small-roundtrip");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let decrypted = tmp.join("plain.out");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(700 * 1024 + 19))
        .map(|i| ((i * 47 + 5) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"small-roundtrip-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(encrypt.status.success());
    assert_eq!(
        &openssl_header(&fs::read(&ciphertext).unwrap())[..8],
        b"Salted__"
    );

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            decrypted.to_str().unwrap(),
            ciphertext.to_str().unwrap(),
        ],
    );
    assert!(decrypt.status.success());
    assert_eq!(fs::read(&decrypted).unwrap(), bytes);
}

#[test]
fn encrypt_and_decrypt_support_stdio() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-stdio");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..8193)
        .map(|i| ((i * 17 + 13) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&passphrase, b"stdio-secret\n").unwrap();

    let encrypted = run_fro_with_stdin(
        "encrypt",
        &["--passphrase-file", passphrase.to_str().unwrap()],
        &bytes,
    );
    assert!(
        encrypted.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&encrypted.stdout),
        String::from_utf8_lossy(&encrypted.stderr)
    );
    assert!(encrypted.stderr.is_empty());
    assert_eq!(&encrypted.stdout[..8], b"Salted__");

    let decrypted = run_fro_with_stdin(
        "decrypt",
        &["--passphrase-file", passphrase.to_str().unwrap()],
        &encrypted.stdout,
    );
    assert!(
        decrypted.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypted.stdout),
        String::from_utf8_lossy(&decrypted.stderr)
    );
    assert!(decrypted.stderr.is_empty());
    assert_eq!(decrypted.stdout, bytes);
}

#[test]
fn encrypt_file_to_stdout_and_decrypt_stdin_to_file_roundtrip() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-auto-pairing");
    let plaintext = tmp.join("plain.bin");
    let decrypted = tmp.join("plain.out");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(700 * 1024 + 19))
        .map(|i| ((i * 23 + 3) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"pairing-secret\n").unwrap();

    let encrypted = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(
        encrypted.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&encrypted.stdout),
        String::from_utf8_lossy(&encrypted.stderr)
    );
    assert!(encrypted.stderr.is_empty());
    assert_eq!(&encrypted.stdout[..8], b"Salted__");

    let decrypt = run_fro_with_stdin(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            decrypted.to_str().unwrap(),
        ],
        &encrypted.stdout,
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert!(decrypt.stdout.is_empty());
    assert!(decrypt.stderr.is_empty());
    assert_eq!(fs::read(&decrypted).unwrap(), bytes);
}

#[test]
fn encrypt_and_decrypt_empty_file_roundtrip() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-empty");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let decrypted = tmp.join("plain.out");
    let passphrase = tmp.join("pass.txt");
    fs::write(&plaintext, []).unwrap();
    fs::write(&passphrase, b"empty-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(
        encrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&encrypt.stdout),
        String::from_utf8_lossy(&encrypt.stderr)
    );
    assert!(encrypt.stdout.is_empty());
    assert!(encrypt.stderr.is_empty());

    let ciphertext_bytes = fs::read(&ciphertext).unwrap();
    assert_eq!(ciphertext_bytes.len(), OPENSSL_HEADER_LEN);
    assert_eq!(&openssl_header(&ciphertext_bytes)[..8], b"Salted__");

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            decrypted.to_str().unwrap(),
            ciphertext.to_str().unwrap(),
        ],
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert!(decrypt.stdout.is_empty());
    assert!(decrypt.stderr.is_empty());
    assert!(fs::read(&decrypted).unwrap().is_empty());
}

#[test]
fn encrypt_uses_random_salt_with_openssl_header() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-salt");
    let plaintext = tmp.join("plain.bin");
    let cipher_one = tmp.join("cipher1.bin");
    let cipher_two = tmp.join("cipher2.bin");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(ENCRYPT_INPUT_SIZE_FOR_TEST + 777))
        .map(|i| ((i * 11 + 5) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"salt-secret\n").unwrap();

    for output in [&cipher_one, &cipher_two] {
        let result = run_fro(
            "encrypt",
            &[
                "--passphrase-file",
                passphrase.to_str().unwrap(),
                "-o",
                output.to_str().unwrap(),
                plaintext.to_str().unwrap(),
            ],
        );
        assert!(result.status.success());
    }

    let first = fs::read(&cipher_one).unwrap();
    let second = fs::read(&cipher_two).unwrap();
    assert_ne!(first, second, "ciphertext should change across salts");
    assert_eq!(&openssl_header(&first)[..8], b"Salted__");
    assert_eq!(&openssl_header(&second)[..8], b"Salted__");
    assert_ne!(
        &openssl_header(&first)[8..16],
        &openssl_header(&second)[8..16],
        "salt bytes should differ"
    );
}

#[test]
fn fro_encrypt_output_decrypts_with_openssl_cli() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-openssl-decrypt");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let decrypted = tmp.join("plain.out");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(256 * 1024 + 73))
        .map(|i| ((i * 41 + 9) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"openssl-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(encrypt.status.success());

    let openssl = run_system(
        "openssl",
        &[
            "enc",
            "-d",
            "-aes-256-ctr",
            "-pbkdf2",
            "-pass",
            &format!("file:{}", passphrase.display()),
            "-in",
            ciphertext.to_str().unwrap(),
            "-out",
            decrypted.to_str().unwrap(),
        ],
    );
    assert!(
        openssl.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&openssl.stdout),
        String::from_utf8_lossy(&openssl.stderr)
    );
    assert_eq!(fs::read(&decrypted).unwrap(), bytes);
}

#[test]
fn fro_decrypt_accepts_openssl_cli_ciphertext() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-openssl-encrypt");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(768 * 1024 + 11))
        .map(|i| ((i * 13 + 21) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"interop-secret\n").unwrap();

    let openssl = run_system(
        "openssl",
        &[
            "enc",
            "-aes-256-ctr",
            "-salt",
            "-pbkdf2",
            "-pass",
            &format!("file:{}", passphrase.display()),
            "-in",
            plaintext.to_str().unwrap(),
            "-out",
            ciphertext.to_str().unwrap(),
        ],
    );
    assert!(openssl.status.success());

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            ciphertext.to_str().unwrap(),
        ],
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert!(decrypt.stderr.is_empty());
    assert_eq!(decrypt.stdout, bytes);
}

#[test]
fn decrypt_rejects_non_openssl_header() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-bad-header");
    let ciphertext = tmp.join("cipher.bin");
    let passphrase = tmp.join("pass.txt");
    fs::write(&ciphertext, b"FROENC01not-openssl").unwrap();
    fs::write(&passphrase, b"mismatch-secret\n").unwrap();

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            ciphertext.to_str().unwrap(),
        ],
    );
    assert!(!decrypt.status.success());
    assert!(decrypt.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&decrypt.stderr);
    assert!(stderr.contains("unsupported ciphertext format"));
}

#[test]
fn decrypt_rejects_empty_and_truncated_ciphertext_before_header() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-short");
    let passphrase = tmp.join("pass.txt");
    fs::write(&passphrase, b"short-secret\n").unwrap();

    for (name, bytes) in [
        ("empty", Vec::new()),
        ("truncated", b"Salted__abc".to_vec()),
    ] {
        let ciphertext = tmp.join(format!("{name}.bin"));
        fs::write(&ciphertext, bytes).unwrap();

        let decrypt = run_fro(
            "decrypt",
            &[
                "--passphrase-file",
                passphrase.to_str().unwrap(),
                ciphertext.to_str().unwrap(),
            ],
        );
        assert!(
            !decrypt.status.success(),
            "{name} ciphertext unexpectedly decrypted"
        );
        assert!(
            decrypt.stdout.is_empty(),
            "{name} stderr should stay on stderr"
        );
        let stderr = String::from_utf8_lossy(&decrypt.stderr);
        assert!(
            stderr.contains("failed to fill whole buffer"),
            "{name} stderr:\n{stderr}"
        );
    }
}

#[test]
fn decrypt_truncated_payload_returns_plaintext_prefix_without_error() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-truncated-payload");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let truncated = tmp.join("cipher.truncated.bin");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(131_072 + 29))
        .map(|i| ((i * 37 + 19) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"truncate-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(encrypt.status.success());

    let ciphertext_bytes = fs::read(&ciphertext).unwrap();
    let truncated_payload_len = 8193;
    fs::write(
        &truncated,
        &ciphertext_bytes[..OPENSSL_HEADER_LEN + truncated_payload_len],
    )
    .unwrap();

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            truncated.to_str().unwrap(),
        ],
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert!(decrypt.stderr.is_empty());
    assert_eq!(decrypt.stdout, bytes[..truncated_payload_len]);
}

#[test]
fn decrypt_corrupted_payload_returns_corrupted_plaintext_without_error() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-corrupt-payload");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let corrupted = tmp.join("cipher.corrupted.bin");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(65_536 + 17))
        .map(|i| ((i * 31 + 1) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"corrupt-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(encrypt.status.success());

    let mut ciphertext_bytes = fs::read(&ciphertext).unwrap();
    let corrupted_index = OPENSSL_HEADER_LEN + 4097;
    ciphertext_bytes[corrupted_index] ^= 0x5a;
    fs::write(&corrupted, &ciphertext_bytes).unwrap();

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            corrupted.to_str().unwrap(),
        ],
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert!(decrypt.stderr.is_empty());
    let plaintext_index = corrupted_index - OPENSSL_HEADER_LEN;
    assert_eq!(
        &decrypt.stdout[..plaintext_index],
        &bytes[..plaintext_index],
        "payload corruption should not affect earlier bytes"
    );
    assert_ne!(decrypt.stdout[plaintext_index], bytes[plaintext_index]);
    assert_eq!(
        &decrypt.stdout[plaintext_index + 1..],
        &bytes[plaintext_index + 1..],
        "aes-256-ctr corruption should stay localized to the changed byte"
    );
}

#[test]
fn decrypt_corrupted_salt_returns_nonmatching_plaintext_without_error() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-corrupt-salt");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let corrupted = tmp.join("cipher.corrupted.bin");
    let passphrase = tmp.join("pass.txt");
    let bytes = (0..(32 * 1024 + 9))
        .map(|i| ((i * 43 + 15) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"salt-corrupt-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(encrypt.status.success());

    let mut ciphertext_bytes = fs::read(&ciphertext).unwrap();
    ciphertext_bytes[b"Salted__".len()] ^= 0x01;
    fs::write(&corrupted, &ciphertext_bytes).unwrap();

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            corrupted.to_str().unwrap(),
        ],
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert!(decrypt.stderr.is_empty());
    assert_eq!(decrypt.stdout.len(), bytes.len());
    assert_ne!(decrypt.stdout, bytes);
}

#[test]
fn decrypt_with_wrong_passphrase_produces_nonmatching_plaintext() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-wrong-pass");
    let plaintext = tmp.join("plain.bin");
    let ciphertext = tmp.join("cipher.bin");
    let passphrase = tmp.join("pass.txt");
    let wrong_passphrase = tmp.join("wrong-pass.txt");
    let bytes = (0..65539)
        .map(|i| ((i * 7 + 3) % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&plaintext, &bytes).unwrap();
    fs::write(&passphrase, b"good-secret\n").unwrap();
    fs::write(&wrong_passphrase, b"bad-secret\n").unwrap();

    let encrypt = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "-o",
            ciphertext.to_str().unwrap(),
            plaintext.to_str().unwrap(),
        ],
    );
    assert!(encrypt.status.success());

    let decrypt = run_fro(
        "decrypt",
        &[
            "--passphrase-file",
            wrong_passphrase.to_str().unwrap(),
            ciphertext.to_str().unwrap(),
        ],
    );
    assert!(
        decrypt.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&decrypt.stdout),
        String::from_utf8_lossy(&decrypt.stderr)
    );
    assert_ne!(decrypt.stdout, bytes);
}

#[test]
fn encrypt_requires_passphrase_file() {
    let output = run_fro("encrypt", &[]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("missing required option '--passphrase-file'"));
}

#[test]
fn encrypt_rejects_unsupported_cipher() {
    let tmp = unique_temp_dir("fro-coreutils-encrypt-bad-cipher");
    let passphrase = tmp.join("pass.txt");
    fs::write(&passphrase, b"cipher-secret\n").unwrap();

    let output = run_fro(
        "encrypt",
        &[
            "--passphrase-file",
            passphrase.to_str().unwrap(),
            "--cipher",
            "aes-256-cbc",
        ],
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("supported: aes-256-ctr"));
}

#[test]
fn encrypt_help_mentions_openssl_compatibility_and_no_b3sum() {
    let output = run_fro("encrypt", &["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("OpenSSL-compatible"));
    assert!(stdout.contains("aes-256-ctr"));
    assert!(stdout.contains("No trailing b3sum"));
    assert!(stdout.contains("unauthenticated"));
}
