use super::*;

use std::fs;
use std::path::PathBuf;

fn unique_temp_file(prefix: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{}-{}-{}.bin", prefix, pid, nanos))
}

fn fixture_bytes(size: usize) -> Vec<u8> {
        (0..size).map(|i| ((i * 131) % 251) as u8).collect()
}

fn digest_u64(value: u64) -> BlockDigest {
        BlockDigest::from_prefix(&value.to_le_bytes())
}

fn digest_sha256(byte: u8) -> BlockDigest {
        BlockHashAlgorithm::Sha256.hash_block(&[byte; 17])
}

fn expected_hashes(data: &[u8], block_size: usize) -> Vec<BlockDigest> {
        data.chunks(block_size)
            .map(|chunk| BlockHashAlgorithm::Xxh3.hash_block(chunk))
            .collect()
}

#[test]
fn hash_file_blocks_matches_sequential_hashes() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes((block_size * 3) + 12345);
        let path = unique_temp_file("fro-block-hash");
        fs::write(&path, &data).unwrap();

        let manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            3,
            BLOCK_HASH_SIZE,
            2,
            3,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(manifest.file_size, data.len() as u64);
        assert_eq!(manifest.block_size, block_size as u64);
        assert_eq!(manifest.bytes_hashed, data.len() as u64);
        assert_eq!(manifest.block_hashes, expected_hashes(&data, block_size));
        assert!(manifest.verify_integrity());

        let _ = fs::remove_file(path);
}

#[test]
fn verify_integrity_rejects_corrupted_block_hash() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes((block_size * 2) + 17);
        let path = unique_temp_file("fro-block-hash-corrupt");
        fs::write(&path, &data).unwrap();

        let mut manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            2,
            BLOCK_HASH_SIZE,
            2,
            2,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();
        manifest.block_hashes[1] = digest_u64(1);

        assert!(!manifest.verify_integrity());

        let _ = fs::remove_file(path);
}

#[test]
fn verify_integrity_accepts_single_bad_hash_of_hash_copy() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes(block_size + 99);
        let path = unique_temp_file("fro-block-hash-majority");
        fs::write(&path, &data).unwrap();

        let mut manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Xxh3,
            2,
            BLOCK_HASH_SIZE,
            2,
            2,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert!(manifest.verify_integrity());

        manifest.hash_of_hashes = digest_u64(1);
        assert!(!manifest.verify_integrity());

        let _ = fs::remove_file(path);
}

#[test]
fn sha256_manifest_uses_variable_digest_size() {
        let block_size = BLOCK_HASH_SIZE as usize;
        let data = fixture_bytes(block_size + 31);
        let path = unique_temp_file("fro-block-hash-sha256");
        fs::write(&path, &data).unwrap();

        let manifest = hash_file_blocks(
            path.to_str().unwrap(),
            BlockHashAlgorithm::Sha256,
            2,
            BLOCK_HASH_SIZE,
            2,
            2,
            BLOCK_HASH_SIZE,
            2,
            IOMode::PageCache,
        )
        .unwrap();

        assert_eq!(manifest.hash_type, BlockHashAlgorithm::Sha256);
        assert!(manifest
            .block_hashes
            .iter()
            .all(|digest| digest.len as usize == BlockHashAlgorithm::Sha256.digest_len()));
        assert_eq!(
            manifest.hash_of_hashes.len as usize,
            BlockHashAlgorithm::Sha256.digest_len()
        );
        assert!(manifest.verify_integrity());

        let _ = fs::remove_file(path);
}

#[test]
fn verify_integrity_rejects_wrong_digest_lengths_and_partial_hashes() {
        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Sha256,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE - 1,
            block_hashes: vec![digest_u64(99)],
            hash_of_hashes: digest_sha256(7),
        };
        assert!(!manifest.verify_integrity());

        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Sha256,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_sha256(9)],
            hash_of_hashes: digest_u64(7),
        };
        assert!(!manifest.verify_integrity());
}

#[test]
fn recover_block_hash_prefers_intact_hash_manifest() {
        let manifest_a = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(222)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(222)],
            ),
        };
        let manifest_b = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(999)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(999)],
            ),
        };

        let decision = recover_block_hash(
            0,
            &[Some(digest_u64(111)), Some(digest_u64(333))],
            &[Some(&manifest_a), Some(&manifest_b), None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(111)));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(decision.basis, Some(BlockRecoveryBasis::IntactHash));
        assert_eq!(decision.failure, None);
        assert_eq!(
            decision.status_message(),
            "recovered block based on intact hash"
        );
        assert_eq!(decision.votes[0].hash, digest_u64(111));
}

#[test]
fn recover_block_hash_reports_manifest_only_agreement() {
        let manifest_a = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(111)],
            hash_of_hashes: digest_u64(0),
        };
        let manifest_b = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(111)],
            hash_of_hashes: digest_u64(0),
        };

        let decision = recover_block_hash(
            0,
            &[Some(digest_u64(222)), Some(digest_u64(333))],
            &[Some(&manifest_a), Some(&manifest_b), None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(111)));
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(decision.basis, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::ManifestOnlyAgreement)
        );
        assert_eq!(
            decision.status_message(),
            "failed to recover corrupt block [manifest+manifest hashes agree]"
        );
}

#[test]
fn recover_block_hash_can_use_file_and_manifest_agreement() {
        let good_manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(555)],
            hash_of_hashes: digest_u64(0),
        };
        let bad_manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(777)],
            hash_of_hashes: digest_u64(0),
        };

        let decision = recover_block_hash(
            0,
            &[Some(digest_u64(555)), Some(digest_u64(999))],
            &[Some(&good_manifest), Some(&bad_manifest), None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(555)));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(
            decision.basis,
            Some(BlockRecoveryBasis::FileAndManifestAgreement)
        );
        assert_eq!(decision.failure, None);
        assert_eq!(
            decision.status_message(),
            "recovered block based on file+manifest agreement"
        );
}

#[test]
fn recover_block_hash_can_use_file_and_file_agreement() {
        let decision = recover_block_hash(
            0,
            &[
                Some(digest_u64(444)),
                Some(digest_u64(444)),
                Some(digest_u64(999)),
            ],
            &[None, None, None],
        );

        assert_eq!(decision.elected_hash, Some(digest_u64(444)));
        assert_eq!(decision.repair_source_index, Some(0));
        assert_eq!(
            decision.basis,
            Some(BlockRecoveryBasis::FileAndFileAgreement)
        );
        assert_eq!(decision.failure, None);
        assert_eq!(
            decision.status_message(),
            "recovered block based on file+file agreement"
        );
}

#[test]
fn recover_block_hash_rejects_intact_manifest_that_disagrees_with_file_majority() {
        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(999)],
            hash_of_hashes: hash_hashes(BlockHashAlgorithm::Xxh3, &[digest_u64(999)]),
        };

        let decision = recover_block_hash(
            0,
            &[
                Some(digest_u64(111)),
                Some(digest_u64(111)),
                Some(digest_u64(222)),
            ],
            &[Some(&manifest), None, None],
        );

        assert_eq!(decision.elected_hash, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::ConflictingIntactHashes)
        );
}

#[test]
fn recover_block_hash_rejects_intact_manifest_when_one_file_matches_but_majority_disagrees() {
        let manifest = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE,
            block_hashes: vec![digest_u64(111)],
            hash_of_hashes: hash_hashes(BlockHashAlgorithm::Xxh3, &[digest_u64(111)]),
        };

        let decision = recover_block_hash(
            0,
            &[
                Some(digest_u64(111)),
                Some(digest_u64(999)),
                Some(digest_u64(999)),
            ],
            &[Some(&manifest), None, None],
        );

        assert_eq!(decision.elected_hash, None);
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::ConflictingIntactHashes)
        );
}

#[test]
fn recover_block_hash_reports_no_hash_found() {
        let decision = recover_block_hash(0, &[None, None], &[None, None, None]);

        assert_eq!(decision.elected_hash, None);
        assert_eq!(decision.repair_source_index, None);
        assert_eq!(decision.basis, None);
        assert_eq!(
            decision.failure,
            Some(BlockRecoveryFailure::NoBlockHashFound)
        );
        assert_eq!(
            decision.status_message(),
            "failed to recover corrupt block [no block hash found either]"
        );
}

#[test]
fn consistent_intact_manifest_requires_full_manifest_agreement() {
        let manifest_a = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(222)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(222)],
            ),
        };
        let manifest_b = BlockHashManifest {
            hash_type: BlockHashAlgorithm::Xxh3,
            file_size: BLOCK_HASH_SIZE * 2,
            block_size: BLOCK_HASH_SIZE,
            bytes_hashed: BLOCK_HASH_SIZE * 2,
            block_hashes: vec![digest_u64(111), digest_u64(999)],
            hash_of_hashes: hash_hashes(
                BlockHashAlgorithm::Xxh3,
                &[digest_u64(111), digest_u64(999)],
            ),
        };

        assert!(consistent_intact_manifest(&[Some(manifest_a), Some(manifest_b)]).is_none());
}

#[test]
fn document_block_recovery_decision_table() {
        struct Case {
            scenario: &'static str,
            file_copy_hashes: Vec<Option<BlockDigest>>,
            manifests: Vec<Option<BlockHashManifest>>,
            expected_elected_hash: Option<BlockDigest>,
            expected_repair_source_index: Option<usize>,
            expected_basis: Option<BlockRecoveryBasis>,
            expected_failure: Option<BlockRecoveryFailure>,
            expected_status: &'static str,
        }

        fn intact_manifest(block_hash: u64) -> BlockHashManifest {
            BlockHashManifest {
                hash_type: BlockHashAlgorithm::Xxh3,
                file_size: BLOCK_HASH_SIZE,
                block_size: BLOCK_HASH_SIZE,
                bytes_hashed: BLOCK_HASH_SIZE,
                block_hashes: vec![digest_u64(block_hash)],
                hash_of_hashes: hash_hashes(BlockHashAlgorithm::Xxh3, &[digest_u64(block_hash)]),
            }
        }

        fn corrupted_manifest(block_hash: u64) -> BlockHashManifest {
            BlockHashManifest {
                hash_type: BlockHashAlgorithm::Xxh3,
                file_size: BLOCK_HASH_SIZE,
                block_size: BLOCK_HASH_SIZE,
                bytes_hashed: BLOCK_HASH_SIZE,
                block_hashes: vec![digest_u64(block_hash)],
                hash_of_hashes: digest_u64(0),
            }
        }

        let cases = vec![
            Case {
                scenario: "target block matches an intact manifest",
                file_copy_hashes: vec![Some(digest_u64(111)), Some(digest_u64(333))],
                manifests: vec![
                    Some(intact_manifest(111)),
                    Some(corrupted_manifest(999)),
                    None,
                ],
                expected_elected_hash: Some(digest_u64(111)),
                expected_repair_source_index: Some(0),
                expected_basis: Some(BlockRecoveryBasis::IntactHash),
                expected_failure: None,
                expected_status: "recovered block based on intact hash",
            },
            Case {
                scenario: "intact manifests agree, but no available file copy matches them",
                file_copy_hashes: vec![Some(digest_u64(222)), Some(digest_u64(333))],
                manifests: vec![Some(intact_manifest(111)), Some(intact_manifest(111)), None],
                expected_elected_hash: Some(digest_u64(111)),
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::IntactHashWithoutMatchingBlock),
                expected_status:
                    "failed to recover corrupt block [intact hash found but no matching file block]",
            },
            Case {
                scenario: "intact manifests disagree with each other",
                file_copy_hashes: vec![Some(digest_u64(111)), Some(digest_u64(222))],
                manifests: vec![Some(intact_manifest(111)), Some(intact_manifest(222)), None],
                expected_elected_hash: None,
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::ConflictingIntactHashes),
                expected_status: "failed to recover corrupt block [conflicting intact hashes]",
            },
            Case {
                scenario: "two file copies agree and no intact manifest overrides them",
                file_copy_hashes: vec![
                    Some(digest_u64(444)),
                    Some(digest_u64(444)),
                    Some(digest_u64(999)),
                ],
                manifests: vec![None, None, None],
                expected_elected_hash: Some(digest_u64(444)),
                expected_repair_source_index: Some(0),
                expected_basis: Some(BlockRecoveryBasis::FileAndFileAgreement),
                expected_failure: None,
                expected_status: "recovered block based on file+file agreement",
            },
            Case {
                scenario: "one file copy agrees with a manifest witness",
                file_copy_hashes: vec![Some(digest_u64(555)), Some(digest_u64(999))],
                manifests: vec![
                    Some(corrupted_manifest(555)),
                    Some(corrupted_manifest(777)),
                    None,
                ],
                expected_elected_hash: Some(digest_u64(555)),
                expected_repair_source_index: Some(0),
                expected_basis: Some(BlockRecoveryBasis::FileAndManifestAgreement),
                expected_failure: None,
                expected_status: "recovered block based on file+manifest agreement",
            },
            Case {
                scenario: "two manifest witnesses agree, but no file copy matches them",
                file_copy_hashes: vec![Some(digest_u64(222)), Some(digest_u64(333))],
                manifests: vec![
                    Some(corrupted_manifest(111)),
                    Some(corrupted_manifest(111)),
                    None,
                ],
                expected_elected_hash: Some(digest_u64(111)),
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::ManifestOnlyAgreement),
                expected_status: "failed to recover corrupt block [manifest+manifest hashes agree]",
            },
            Case {
                scenario: "all witnesses are missing",
                file_copy_hashes: vec![None, None],
                manifests: vec![None, None, None],
                expected_elected_hash: None,
                expected_repair_source_index: None,
                expected_basis: None,
                expected_failure: Some(BlockRecoveryFailure::NoBlockHashFound),
                expected_status: "failed to recover corrupt block [no block hash found either]",
            },
        ];

        println!("| Scenario | Elected hash | Repair source | Basis | Failure | Status message |");
        println!("| --- | --- | --- | --- | --- | --- |");

        for case in cases {
            let manifest_refs = case
                .manifests
                .iter()
                .map(|m| m.as_ref())
                .collect::<Vec<_>>();
            let decision = recover_block_hash(0, &case.file_copy_hashes, &manifest_refs);

            assert_eq!(decision.elected_hash, case.expected_elected_hash);
            assert_eq!(
                decision.repair_source_index,
                case.expected_repair_source_index
            );
            assert_eq!(decision.basis, case.expected_basis);
            assert_eq!(decision.failure, case.expected_failure);
            assert_eq!(decision.status_message(), case.expected_status);

            let basis = decision
                .basis
                .as_ref()
                .map(|basis| format!("{basis:?}"))
                .unwrap_or_else(|| "-".to_string());
            let failure = decision
                .failure
                .as_ref()
                .map(|failure| format!("{failure:?}"))
                .unwrap_or_else(|| "-".to_string());
            let elected_hash = decision
                .elected_hash
                .map(|hash| hash.to_string())
                .unwrap_or_else(|| "-".to_string());
            let repair_source_index = decision
                .repair_source_index
                .map(|index| index.to_string())
                .unwrap_or_else(|| "-".to_string());

            println!(
                "| {} | {} | {} | {} | {} | {} |",
                case.scenario,
                elected_hash,
                repair_source_index,
                basis,
                failure,
                decision.status_message()
            );
        }
}

#[test]
fn block_offset_rejects_overflow() {
        let err = block_offset(u64::MAX - 3, 4, 8, 1024).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
