use super::*;

fn manifest_geometry(
    manifests: &[Option<BlockHashManifest>],
    label: &str,
) -> std::io::Result<Option<ManifestGeometry>> {
    let mut expected: Option<ManifestGeometry> = None;

    for manifest in manifests.iter().flatten() {
        let block_count = block_count_for_size(manifest.file_size, manifest.block_size)?;
        if manifest.block_hashes.len() != block_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{} has {} block hashes but file_size={} and block_size={} imply {}",
                    label,
                    manifest.block_hashes.len(),
                    manifest.file_size,
                    manifest.block_size,
                    block_count
                ),
            ));
        }

        let current = ManifestGeometry {
            file_size: manifest.file_size,
            block_size: manifest.block_size,
            block_count,
            hash_type: manifest.hash_type,
        };

        if let Some(previous) = expected {
            if previous.file_size != current.file_size
                || previous.block_size != current.block_size
                || previous.block_count != current.block_count
                || previous.hash_type != current.hash_type
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{} disagree on geometry; saw file_size={}, block_size={}, blocks={}, hash_type={:?} and file_size={}, block_size={}, blocks={}, hash_type={:?}",
                        label,
                        previous.file_size,
                        previous.block_size,
                        previous.block_count,
                        previous.hash_type,
                        current.file_size,
                        current.block_size,
                        current.block_count,
                        current.hash_type
                    ),
                ));
            }
        } else {
            expected = Some(current);
        }
    }

    Ok(expected)
}

pub(super) fn consistent_intact_manifest(
    manifests: &[Option<BlockHashManifest>],
) -> Option<&BlockHashManifest> {
    let mut intact = manifests
        .iter()
        .flatten()
        .filter(|manifest| manifest.verify_integrity());
    let first = intact.next()?;
    if intact.all(|manifest| {
        manifest.file_size == first.file_size
            && manifest.block_size == first.block_size
            && manifest.hash_type == first.hash_type
            && manifest.block_hashes == first.block_hashes
    }) {
        Some(first)
    } else {
        None
    }
}

fn sidecars_are_fully_healthy_for_current(
    current: &BlockHashManifest,
    manifests: &[Option<BlockHashManifest>],
) -> bool {
    manifests.len() == 3
        && manifests.iter().all(|manifest| {
            manifest.as_ref().is_some_and(|manifest| {
                manifest.verify_integrity()
                    && manifest.hash_type == current.hash_type
                    && manifest.file_size == current.file_size
                    && manifest.block_size == current.block_size
                    && manifest.block_hashes == current.block_hashes
            })
        })
}

fn verify_report_from_current(
    current: &BlockHashManifest,
    manifests: &[Option<BlockHashManifest>],
) -> VerifyReport {
    let intact_manifest = consistent_intact_manifest(manifests);
    let loaded_manifests = manifests.iter().filter(|m| m.is_some()).count();
    let manifest_refs = manifests.iter().map(|m| m.as_ref()).collect::<Vec<_>>();

    let mut ok_blocks = 0;
    let mut bad_blocks = Vec::new();

    for (block_index, current_hash) in current.block_hashes.iter().copied().enumerate() {
        if intact_manifest
            .and_then(|manifest| manifest.block_hashes.get(block_index))
            .is_some_and(|expected_hash| *expected_hash == current_hash)
        {
            ok_blocks += 1;
            continue;
        }

        let decision = recover_block_hash(block_index, &[Some(current_hash)], &manifest_refs);
        let block_ok =
            decision.elected_hash == Some(current_hash) && decision.repair_source_index == Some(0);
        if block_ok {
            ok_blocks += 1;
        } else {
            bad_blocks.push(BlockVerifyIssue {
                block_index,
                current_hash,
                decision,
            });
        }
    }

    VerifyReport {
        bytes_hashed: current.bytes_hashed,
        total_blocks: current.block_hashes.len(),
        loaded_manifests,
        ok_blocks,
        bad_blocks,
    }
}

fn tally_hash_votes(values: &[(BlockDigest, BlockHashWitnessKind)]) -> Vec<BlockHashVote> {
    let mut votes = Vec::<BlockHashVote>::new();
    for (hash, kind) in values {
        if let Some(vote) = votes.iter_mut().find(|vote| vote.hash == *hash) {
            vote.total_votes += 1;
            match kind {
                BlockHashWitnessKind::FileCopy => vote.file_copy_votes += 1,
                BlockHashWitnessKind::HashReplica => vote.hash_replica_votes += 1,
            }
            continue;
        }

        votes.push(BlockHashVote {
            hash: *hash,
            total_votes: 1,
            file_copy_votes: usize::from(*kind == BlockHashWitnessKind::FileCopy),
            hash_replica_votes: usize::from(*kind == BlockHashWitnessKind::HashReplica),
        });
    }
    votes.sort_by(|a, b| {
        b.total_votes
            .cmp(&a.total_votes)
            .then_with(|| b.file_copy_votes.cmp(&a.file_copy_votes))
            .then_with(|| b.hash_replica_votes.cmp(&a.hash_replica_votes))
            .then_with(|| a.hash.cmp(&b.hash))
    });
    votes
}

fn first_matching_file_copy(
    file_copy_hashes: &[Option<BlockDigest>],
    expected_hash: BlockDigest,
) -> Option<usize> {
    file_copy_hashes
        .iter()
        .position(|hash| hash.is_some_and(|value| value == expected_hash))
}

fn unique_top_vote<F>(votes: &[BlockHashVote], predicate: F) -> Option<&BlockHashVote>
where
    F: Fn(&BlockHashVote) -> bool,
{
    let mut matching = votes.iter().filter(|vote| predicate(vote));
    let top = matching.next()?;
    let next = matching.next();
    if next
        .map(|other| other.total_votes < top.total_votes)
        .unwrap_or(true)
    {
        Some(top)
    } else {
        None
    }
}

pub fn recover_block_hash(
    block_index: usize,
    file_copy_hashes: &[Option<BlockDigest>],
    hash_manifests: &[Option<&BlockHashManifest>],
) -> BlockRecoveryDecision {
    let mut witnesses = Vec::<(BlockDigest, BlockHashWitnessKind)>::new();
    let mut intact_manifest_hashes = Vec::<BlockDigest>::new();

    for hash in file_copy_hashes.iter().flatten() {
        witnesses.push((*hash, BlockHashWitnessKind::FileCopy));
    }

    for manifest in hash_manifests.iter().flatten() {
        if let Some(hash) = manifest.block_hashes.get(block_index) {
            if manifest.verify_integrity() {
                intact_manifest_hashes.push(*hash);
            }
            witnesses.push((*hash, BlockHashWitnessKind::HashReplica));
        }
    }

    let votes = tally_hash_votes(&witnesses);

    if let Some(first) = intact_manifest_hashes.first().copied() {
        if intact_manifest_hashes.iter().any(|hash| *hash != first) {
            return BlockRecoveryDecision {
                block_index,
                elected_hash: None,
                repair_source_index: None,
                basis: None,
                failure: Some(BlockRecoveryFailure::ConflictingIntactHashes),
                votes,
            };
        }

        if let Some(top) = unique_top_vote(&votes, |vote| vote.file_copy_votes >= 2) {
            if top.hash != first {
                return BlockRecoveryDecision {
                    block_index,
                    elected_hash: None,
                    repair_source_index: None,
                    basis: None,
                    failure: Some(BlockRecoveryFailure::ConflictingIntactHashes),
                    votes,
                };
            }
        }

        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(first),
            repair_source_index: first_matching_file_copy(file_copy_hashes, first),
            basis: first_matching_file_copy(file_copy_hashes, first)
                .map(|_| BlockRecoveryBasis::IntactHash),
            failure: first_matching_file_copy(file_copy_hashes, first)
                .is_none()
                .then_some(BlockRecoveryFailure::IntactHashWithoutMatchingBlock),
            votes,
        };
    }

    if let Some(top) = unique_top_vote(&votes, |vote| vote.file_copy_votes >= 2) {
        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(top.hash),
            repair_source_index: first_matching_file_copy(file_copy_hashes, top.hash),
            basis: Some(BlockRecoveryBasis::FileAndFileAgreement),
            failure: None,
            votes,
        };
    }

    if let Some(top) = unique_top_vote(&votes, |vote| {
        vote.file_copy_votes >= 1 && vote.hash_replica_votes >= 1
    }) {
        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(top.hash),
            repair_source_index: first_matching_file_copy(file_copy_hashes, top.hash),
            basis: Some(BlockRecoveryBasis::FileAndManifestAgreement),
            failure: None,
            votes,
        };
    }

    if let Some(top) = unique_top_vote(&votes, |vote| vote.hash_replica_votes >= 2) {
        return BlockRecoveryDecision {
            block_index,
            elected_hash: Some(top.hash),
            repair_source_index: None,
            basis: None,
            failure: Some(BlockRecoveryFailure::ManifestOnlyAgreement),
            votes,
        };
    }

    BlockRecoveryDecision {
        block_index,
        elected_hash: None,
        repair_source_index: None,
        basis: None,
        failure: Some(BlockRecoveryFailure::NoBlockHashFound),
        votes,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn block_offset(
    thread_base: u64,
    block_id: u64,
    num_threads: u64,
    block_size: u64,
) -> std::io::Result<u64> {
    let stride = block_id
        .checked_mul(num_threads)
        .and_then(|value| value.checked_mul(block_size))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "hash offset calculation overflowed",
            )
        })?;
    thread_base.checked_add(stride).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hash offset calculation overflowed",
        )
    })
}

fn resolve_block_hash_params(
    filename: &str,
    page_cache: &IOParams,
    direct: &IOParams,
    io_mode: IOMode,
) -> std::io::Result<ResolvedReadParams> {
    let params = resolve_reader_params(filename, page_cache, direct, io_mode)?;
    validate_block_size(params.block_size)?;
    Ok(params)
}

fn open_hash_file(filename: &str, use_direct: bool) -> std::io::Result<ParallelFile> {
    let config = LoadedConfig::Legacy {
        path: std::path::PathBuf::from("hash-file-blocks"),
        config: crate::config::AppConfig::default(),
    };
    ParallelFile::open(
        &config,
        "hash",
        filename,
        if use_direct {
            IOMode::Direct
        } else {
            IOMode::PageCache
        },
    )
}

fn hash_file_blocks_inner(
    filename: &str,
    hash_type: BlockHashAlgorithm,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<BlockHashManifest> {
    let params = resolve_block_hash_params(
        filename,
        &IOParams {
            num_threads: num_threads_p,
            block_size: block_size_p,
            qd: qd_p,
        },
        &IOParams {
            num_threads: num_threads_d,
            block_size: block_size_d,
            qd: qd_d,
        },
        io_mode,
    )?;
    let file = open_hash_file(filename, params.use_direct)?;
    file.map_reduce_blocks_with_params(
        params,
        move |_, data| Ok(hash_type.hash_block(data)),
        move |block_hashes: Vec<BlockDigest>, report| {
            Ok(BlockHashManifest {
                hash_type,
                file_size: report.file_size,
                block_size: report.params.block_size,
                bytes_hashed: report.bytes_read,
                hash_of_hashes: hash_hashes(hash_type, &block_hashes),
                block_hashes,
            })
        },
    )
}

pub fn hash_file_blocks(
    filename: &str,
    hash_type: BlockHashAlgorithm,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<BlockHashManifest> {
    hash_file_blocks_inner(
        filename,
        hash_type,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )
}

pub fn hash_file_to_replicas(
    filename: &str,
    hash_base: Option<&str>,
    hash_type: BlockHashAlgorithm,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<BlockHashManifest> {
    let manifest = hash_file_blocks(
        filename,
        hash_type,
        num_threads_p,
        block_size_p,
        qd_p,
        num_threads_d,
        block_size_d,
        qd_d,
        io_mode,
    )?;
    let base = hash_base
        .map(str::to_string)
        .unwrap_or_else(|| default_hash_base(filename));
    save_manifest_replicas(&base, &manifest)?;
    Ok(manifest)
}

pub fn verify_file_with_replicas(
    filename: &str,
    hash_base: Option<&str>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
) -> std::io::Result<VerifyReport> {
    let base = hash_base_for_file(0, filename, hash_base);
    let manifests = load_manifest_replicas(&base);
    let manifest_geometry =
        manifest_geometry(&manifests, &format!("hash replicas for {}", filename))?;
    let current = hash_file_blocks_inner(
        filename,
        manifest_geometry
            .map(|g| g.hash_type)
            .unwrap_or(BlockHashAlgorithm::Xxh3),
        num_threads_p,
        manifest_geometry
            .map(|g| g.block_size)
            .unwrap_or(block_size_p),
        qd_p,
        num_threads_d,
        manifest_geometry
            .map(|g| g.block_size)
            .unwrap_or(block_size_d),
        qd_d,
        io_mode,
    )?;
    Ok(verify_report_from_current(&current, &manifests))
}

fn read_block(
    path: &str,
    block_index: usize,
    file_size: u64,
    block_size: u64,
) -> std::io::Result<Vec<u8>> {
    let offset = block_index as u64 * block_size;
    let len = std::cmp::min(block_size, file_size.saturating_sub(offset)) as usize;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_block(
    path: &str,
    block_index: usize,
    block_size: u64,
    data: &[u8],
) -> std::io::Result<()> {
    let offset = block_index as u64 * block_size;
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}

fn load_manifest_sets(
    files: &[String],
    hash_base: Option<&str>,
) -> Vec<Vec<Option<BlockHashManifest>>> {
    files
        .iter()
        .enumerate()
        .map(|(index, path)| load_manifest_replicas(&hash_base_for_file(index, path, hash_base)))
        .collect()
}

fn ensure_matching_file_geometry(
    files: &[String],
    manifests: &[BlockHashManifest],
) -> std::io::Result<()> {
    let Some(first) = manifests.first() else {
        return Ok(());
    };

    for (path, manifest) in files.iter().zip(manifests.iter()).skip(1) {
        if manifest.file_size != first.file_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical file sizes; {} has {} bytes but target has {} bytes",
                    path, manifest.file_size, first.file_size
                ),
            ));
        }
        if manifest.block_size != first.block_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical block sizes; {} uses {} bytes but target uses {} bytes",
                    path, manifest.block_size, first.block_size
                ),
            ));
        }
        if manifest.block_hashes.len() != first.block_hashes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical block layouts; {} has {} hash blocks but target has {}",
                    path,
                    manifest.block_hashes.len(),
                    first.block_hashes.len()
                ),
            ));
        }
        if manifest.hash_type != first.hash_type {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "recover expects identical hash types; {} uses {:?} but target uses {:?}",
                    path, manifest.hash_type, first.hash_type
                ),
            ));
        }
    }

    Ok(())
}

fn common_manifest_geometry_for_recovery(
    files: &[String],
    stored_replicas: &[Vec<Option<BlockHashManifest>>],
) -> std::io::Result<Option<ManifestGeometry>> {
    let mut expected: Option<ManifestGeometry> = None;

    for (path, replicas) in files.iter().zip(stored_replicas.iter()) {
        let Some(current) = manifest_geometry(replicas, &format!("hash replicas for {}", path))?
        else {
            continue;
        };
        if let Some(previous) = expected {
            if previous.file_size != current.file_size
                || previous.block_size != current.block_size
                || previous.block_count != current.block_count
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "recover expects all sidecars to agree on geometry; {} uses file_size={}, block_size={}, blocks={} but another sidecar set uses file_size={}, block_size={}, blocks={}",
                        path,
                        current.file_size,
                        current.block_size,
                        current.block_count,
                        previous.file_size,
                        previous.block_size,
                        previous.block_count
                    ),
                ));
            }
        } else {
            expected = Some(current);
        }
    }

    Ok(expected)
}

fn repair_file_set(
    files: &[String],
    manifests_now: &mut [BlockHashManifest],
    stored_replicas: &[Vec<Option<BlockHashManifest>>],
    repair_targets: &[usize],
    hash_base: Option<&str>,
    refresh_all_sidecars: bool,
) -> std::io::Result<(usize, usize, usize, Vec<BlockRecoverIssue>)> {
    let stored_refs = stored_replicas
        .iter()
        .flat_map(|replicas| replicas.iter())
        .map(|m| m.as_ref())
        .collect::<Vec<_>>();
    let mut repaired_blocks = 0;
    let mut repaired_file_flags = vec![false; files.len()];
    let mut failed_blocks = Vec::new();

    for block_index in 0..manifests_now[0].block_hashes.len() {
        for &target_index in repair_targets {
            let file_copy_hashes = manifests_now
                .iter()
                .map(|manifest| manifest.block_hashes.get(block_index).copied())
                .collect::<Vec<_>>();
            let decision = recover_block_hash(block_index, &file_copy_hashes, &stored_refs);
            let target_hash = file_copy_hashes[target_index].unwrap();

            if decision.elected_hash == Some(target_hash) {
                continue;
            }

            if let Some(source_index) = decision.repair_source_index {
                if source_index != target_index {
                    let block = read_block(
                        &files[source_index],
                        block_index,
                        manifests_now[source_index].file_size,
                        manifests_now[source_index].block_size,
                    )?;
                    write_block(
                        &files[target_index],
                        block_index,
                        manifests_now[target_index].block_size,
                        &block,
                    )?;
                    manifests_now[target_index].block_hashes[block_index] =
                        decision.elected_hash.unwrap();
                    repaired_blocks += 1;
                    repaired_file_flags[target_index] = true;
                    continue;
                }
            }

            failed_blocks.push(BlockRecoverIssue {
                file_index: target_index,
                file_path: files[target_index].clone(),
                block_index,
                current_hash: target_hash,
                decision,
            });
        }
    }

    let mut sidecars_refreshed = 0;
    for (index, manifest) in manifests_now.iter_mut().enumerate() {
        let should_consider_file = refresh_all_sidecars || index == 0;
        if !should_consider_file {
            continue;
        }

        let file_has_failures = failed_blocks.iter().any(|issue| issue.file_index == index);
        if file_has_failures {
            continue;
        }

        manifest.hash_of_hashes = hash_hashes(manifest.hash_type, &manifest.block_hashes);
        if !sidecars_are_fully_healthy_for_current(manifest, &stored_replicas[index]) {
            let base = hash_base_for_file(index, &files[index], hash_base);
            save_manifest_replicas(&base, manifest)?;
            sidecars_refreshed += 1;
        }
    }

    let repaired_files = repaired_file_flags.into_iter().filter(|flag| *flag).count();
    Ok((
        repaired_blocks,
        repaired_files,
        sidecars_refreshed,
        failed_blocks,
    ))
}

pub fn recover_file_with_copies(
    target: &str,
    copies: &[String],
    hash_base: Option<&str>,
    num_threads_p: u64,
    block_size_p: u64,
    qd_p: usize,
    num_threads_d: u64,
    block_size_d: u64,
    qd_d: usize,
    io_mode: IOMode,
    recover_mode: RecoverMode,
) -> std::io::Result<RecoverReport> {
    let mut files = Vec::with_capacity(1 + copies.len());
    files.push(target.to_string());
    files.extend(copies.iter().cloned());

    let stored_replicas = load_manifest_sets(&files, hash_base);
    let stored_geometry = common_manifest_geometry_for_recovery(&files, &stored_replicas)?;
    let mut used_fast_path = false;
    let mut fell_back_to_full_scan = false;

    let mut manifests_now = if recover_mode == RecoverMode::Fast {
        let mut current = hash_file_blocks_inner(
            target,
            stored_geometry
                .map(|g| g.hash_type)
                .unwrap_or(BlockHashAlgorithm::Xxh3),
            num_threads_p,
            stored_geometry
                .map(|g| g.block_size)
                .unwrap_or(block_size_p),
            qd_p,
            num_threads_d,
            stored_geometry
                .map(|g| g.block_size)
                .unwrap_or(block_size_d),
            qd_d,
            io_mode,
        )?;
        let verify_report = verify_report_from_current(&current, &stored_replicas[0]);
        if consistent_intact_manifest(&stored_replicas[0]).is_some()
            && verify_report.bad_blocks.is_empty()
        {
            used_fast_path = true;
            let mut sidecars_refreshed = 0;
            current.hash_of_hashes = hash_hashes(current.hash_type, &current.block_hashes);
            if !sidecars_are_fully_healthy_for_current(&current, &stored_replicas[0]) {
                let base = hash_base_for_file(0, target, hash_base);
                save_manifest_replicas(&base, &current)?;
                sidecars_refreshed = 1;
            }
            return Ok(RecoverReport {
                bytes_hashed: current.bytes_hashed,
                repaired_blocks: 0,
                repaired_files: 0,
                sidecars_refreshed,
                used_fast_path,
                fell_back_to_full_scan,
                failed_blocks: Vec::new(),
            });
        }

        fell_back_to_full_scan = true;
        let mut manifests = Vec::with_capacity(files.len());
        manifests.push(current);
        for path in files.iter().skip(1) {
            manifests.push(hash_file_blocks_inner(
                path,
                stored_geometry
                    .map(|g| g.hash_type)
                    .unwrap_or(BlockHashAlgorithm::Xxh3),
                num_threads_p,
                stored_geometry
                    .map(|g| g.block_size)
                    .unwrap_or(block_size_p),
                qd_p,
                num_threads_d,
                stored_geometry
                    .map(|g| g.block_size)
                    .unwrap_or(block_size_d),
                qd_d,
                io_mode,
            )?);
        }
        manifests
    } else {
        files
            .iter()
            .map(|path| {
                hash_file_blocks_inner(
                    path,
                    stored_geometry
                        .map(|g| g.hash_type)
                        .unwrap_or(BlockHashAlgorithm::Xxh3),
                    num_threads_p,
                    stored_geometry
                        .map(|g| g.block_size)
                        .unwrap_or(block_size_p),
                    qd_p,
                    num_threads_d,
                    stored_geometry
                        .map(|g| g.block_size)
                        .unwrap_or(block_size_d),
                    qd_d,
                    io_mode,
                )
            })
            .collect::<std::io::Result<Vec<_>>>()?
    };

    ensure_matching_file_geometry(&files, &manifests_now)?;
    let bytes_hashed = manifests_now
        .iter()
        .map(|manifest| manifest.bytes_hashed)
        .sum();
    let repair_targets = if recover_mode == RecoverMode::InPlaceAll {
        (0..files.len()).collect::<Vec<_>>()
    } else {
        vec![0]
    };
    let refresh_all_sidecars = recover_mode == RecoverMode::InPlaceAll;

    let (repaired_blocks, repaired_files, sidecars_refreshed, failed_blocks) = repair_file_set(
        &files,
        &mut manifests_now,
        &stored_replicas,
        &repair_targets,
        hash_base,
        refresh_all_sidecars,
    )?;

    Ok(RecoverReport {
        bytes_hashed,
        repaired_blocks,
        repaired_files,
        sidecars_refreshed,
        used_fast_path,
        fell_back_to_full_scan,
        failed_blocks,
    })
}
