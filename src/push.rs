use std::{
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, bail, Context, Result};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use rayon::prelude::*;
use xxhash_rust::xxh3::xxh3_64;

#[cfg(target_os = "linux")]
use crate::rdma::RdmaMode;
use crate::{
    hashing::hash_file,
    remote::{
        DestinationEntry, DestinationMetadata, EntryKind, LocalFsRemote, RemoteClient, RemoteEntry,
        RemoteSpec, SshDestination,
    },
    sync::{
        check_interrupted, clear_interrupt_flag, install_signal_handlers, is_interrupted,
        RunSummary, SyncOptions,
    },
};

const SMALL_PUSH_BYTES: u64 = 1024 * 1024;
const SMALL_PUSH_JOBS: usize = 4;
const LARGE_PUSH_JOBS: usize = 8;

#[derive(Debug, Clone)]
struct FilePlan {
    entry: RemoteEntry,
    local_path: PathBuf,
    transfer: bool,
    apply_metadata: bool,
}

pub(crate) fn run_push(
    source: &LocalFsRemote,
    remote_spec: RemoteSpec,
    options: &SyncOptions,
) -> Result<RunSummary> {
    validate_push_options(options)?;
    install_signal_handlers()?;
    clear_interrupt_flag();

    let listing_started = Instant::now();
    status(options, "stage=listing: listing local entries...");
    let mut entries = source.list_entries(options.recursive)?;
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    for entry in &entries {
        validate_relative_path(&entry.relative_path)?;
    }
    let listing_ms = listing_started.elapsed().as_millis() as u64;

    let file_count = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .count();
    let total_source_bytes = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .map(|entry| entry.size)
        .sum();
    let jobs = effective_push_jobs(options, file_count, total_source_bytes);
    status(
        options,
        format!(
            "stage=connecting: establishing {} ssh push worker{}...",
            jobs,
            if jobs == 1 { "" } else { "s" }
        ),
    );
    let destination = SshDestination::connect(remote_spec, jobs, !options.dry_run)?;
    status(
        options,
        format!(
            "stage=connecting: connected to {}",
            destination.display_host()
        ),
    );

    let planning_started = Instant::now();
    let mut directories: Vec<RemoteEntry> = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Dir)
        .cloned()
        .collect();
    directories.sort_by_key(|entry| entry.relative_path.components().count());

    if !options.dry_run {
        for directory in &directories {
            check_interrupted()?;
            destination
                .create_dir(&directory.relative_path)
                .with_context(|| {
                    format!(
                        "create remote directory: {}",
                        directory.relative_path.display()
                    )
                })?;
        }
    }

    let mut skipped_symlinks = 0_u64;
    for entry in entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Symlink)
    {
        check_interrupted()?;
        if !options.links {
            skipped_symlinks += 1;
            continue;
        }
        let target = entry
            .link_target
            .as_deref()
            .ok_or_else(|| anyhow!("local symlink is missing a target"))?;
        let already_matches = destination
            .stat(&entry.relative_path)?
            .is_some_and(|existing| {
                existing.kind == EntryKind::Symlink
                    && existing.link_target.as_deref() == Some(target)
            });
        if !already_matches && !options.dry_run {
            destination
                .create_or_replace_symlink(&entry.relative_path, target)
                .with_context(|| {
                    format!("create remote symlink: {}", entry.relative_path.display())
                })?;
        }
    }

    let rayon_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(jobs)
        .build()
        .context("build push thread pool")?;
    let file_entries: Vec<RemoteEntry> = entries
        .into_iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .collect();
    let file_plans: Vec<FilePlan> = rayon_pool.install(|| {
        file_entries
            .par_iter()
            .map(|entry| plan_file(source, &destination, entry, options))
            .collect::<Result<Vec<_>>>()
    })?;
    let planning_ms = planning_started.elapsed().as_millis() as u64;

    let queued_files = file_plans.iter().filter(|plan| plan.transfer).count();
    let queued_bytes: u64 = file_plans
        .iter()
        .filter(|plan| plan.transfer)
        .map(|plan| plan.entry.size)
        .sum();
    let skipped_files = (file_plans.len() - queued_files) as u64;
    status(
        options,
        format!(
            "plan: files={} queued={} skipped={} bytes={} jobs={}",
            file_plans.len(),
            queued_files,
            skipped_files,
            queued_bytes,
            jobs
        ),
    );

    if !options.dry_run {
        rayon_pool.install(|| {
            file_plans
                .par_iter()
                .filter(|plan| !plan.transfer && plan.apply_metadata)
                .try_for_each(|plan| {
                    destination.set_metadata(
                        &plan.entry.relative_path,
                        destination_metadata(&plan.entry, options)?,
                    )
                })
        })?;
    }

    let transfer_started = Instant::now();
    let ui = Arc::new(PushUi::new(queued_bytes, options.progress));
    let transferred_files = AtomicU64::new(0);
    let transferred_bytes = AtomicU64::new(0);

    let transfer_result = rayon_pool.install(|| {
        file_plans
            .par_iter()
            .filter(|plan| plan.transfer)
            .try_for_each(|plan| -> Result<()> {
                check_interrupted()?;
                if options.dry_run {
                    transferred_files.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }

                let partial_name = partial_name(&plan.entry);
                let backup_name = backup_name(&plan.entry);
                let metadata = destination_metadata(&plan.entry, options)?;
                let file_progress = AtomicU64::new(0);
                let on_bytes = |absolute: u64| {
                    ui.set_file_progress(&file_progress, absolute);
                };
                let cancelled = is_interrupted;
                let mut last_error = None;

                for attempt in 0..options.retries {
                    check_interrupted()?;
                    let latest = source.stat_file(&plan.entry.relative_path)?;
                    if latest.size != plan.entry.size || latest.mtime_secs != plan.entry.mtime_secs
                    {
                        bail!(
                            "local source changed before transfer: {}",
                            plan.entry.relative_path.display()
                        );
                    }

                    match destination.upload_file(
                        &plan.local_path,
                        &plan.entry.relative_path,
                        &partial_name,
                        &backup_name,
                        plan.entry.size,
                        plan.entry.mtime_secs,
                        options.resume || attempt > 0,
                        options.strict_durability,
                        push_buffer_size(options),
                        metadata,
                        &on_bytes,
                        &cancelled,
                    ) {
                        Ok(_) => {
                            transferred_files.fetch_add(1, Ordering::Relaxed);
                            transferred_bytes.fetch_add(plan.entry.size, Ordering::Relaxed);
                            return Ok(());
                        }
                        Err(error) => {
                            last_error = Some(error);
                            if is_interrupted() {
                                break;
                            }
                        }
                    }
                }

                Err(last_error
                    .unwrap_or_else(|| anyhow!("remote upload failed"))
                    .context(format!(
                        "failed upload after {} attempt{}: {}",
                        options.retries,
                        if options.retries == 1 { "" } else { "s" },
                        plan.entry.relative_path.display()
                    )))
            })
    });
    ui.finish();
    transfer_result?;
    let transfer_write_ms = transfer_started.elapsed().as_millis() as u64;

    let metadata_started = Instant::now();
    if !options.dry_run {
        for directory in directories.iter().rev() {
            check_interrupted()?;
            destination.set_metadata(
                &directory.relative_path,
                destination_metadata(directory, options)?,
            )?;
        }
    }
    let metadata_ms = metadata_started.elapsed().as_millis() as u64;

    Ok(RunSummary {
        transferred_files: transferred_files.load(Ordering::Relaxed),
        skipped_files,
        transferred_bytes: transferred_bytes.load(Ordering::Relaxed),
        transfer_elapsed_ms: transfer_write_ms,
        verbose: options.verbose,
        listing_ms,
        planning_ms,
        transfer_write_ms,
        metadata_ms,
        skipped_symlinks,
        ..RunSummary::default()
    })
}

fn validate_push_options(options: &SyncOptions) -> Result<()> {
    if options.delta_enabled {
        bail!("--delta is not supported for local-to-SSH transfers yet")
    }
    #[cfg(target_os = "linux")]
    if options.rdma_mode == RdmaMode::Require {
        bail!("--rdma=require is not supported for local-to-SSH transfers yet")
    }
    if options.preserve_acls {
        bail!("-A/--acls is not supported for SSH destinations yet")
    }
    if options.preserve_xattrs {
        bail!("-X/--xattrs is not supported for SSH destinations yet")
    }
    if options.strict_durability {
        bail!(
            "--strict-durability is not supported for SSH destinations because SFTP cannot fsync the destination directory"
        )
    }
    Ok(())
}

fn plan_file(
    source: &LocalFsRemote,
    destination: &SshDestination,
    entry: &RemoteEntry,
    options: &SyncOptions,
) -> Result<FilePlan> {
    let local_path = source.local_path_for(&entry.relative_path);
    let Some(existing) = destination.stat(&entry.relative_path)? else {
        return Ok(FilePlan {
            entry: entry.clone(),
            local_path,
            transfer: true,
            apply_metadata: false,
        });
    };

    let (transfer, apply_metadata) =
        should_transfer(&local_path, destination, entry, &existing, options)?;
    Ok(FilePlan {
        entry: entry.clone(),
        local_path,
        transfer,
        apply_metadata,
    })
}

fn should_transfer(
    local_path: &Path,
    destination: &SshDestination,
    source: &RemoteEntry,
    existing: &DestinationEntry,
    options: &SyncOptions,
) -> Result<(bool, bool)> {
    match existing.kind {
        EntryKind::Dir => bail!(
            "remote destination path is a directory, expected a file: {}",
            source.relative_path.display()
        ),
        EntryKind::Symlink => return Ok((true, false)),
        EntryKind::File => {}
    }

    if options.update && existing.mtime_secs > source.mtime_secs {
        return Ok((false, false));
    }
    if existing.size != source.size || existing.mtime_secs != source.mtime_secs {
        return Ok((true, false));
    }
    if options.verify_existing
        && hash_file(local_path)? != destination.hash_file(&source.relative_path)?
    {
        return Ok((true, false));
    }

    Ok((false, should_apply_metadata(options)))
}

fn destination_metadata(entry: &RemoteEntry, options: &SyncOptions) -> Result<DestinationMetadata> {
    let uid = if options.preserve_owner {
        Some(entry.uid.ok_or_else(|| {
            anyhow!(
                "source owner is unavailable for {}",
                entry.relative_path.display()
            )
        })?)
    } else {
        None
    };
    let gid = if options.preserve_group {
        Some(entry.gid.ok_or_else(|| {
            anyhow!(
                "source group is unavailable for {}",
                entry.relative_path.display()
            )
        })?)
    } else {
        None
    };

    Ok(DestinationMetadata {
        mtime_secs: entry.mtime_secs,
        mode: options.preserve_perms.then_some(entry.mode & 0o7777),
        uid,
        gid,
    })
}

fn should_apply_metadata(options: &SyncOptions) -> bool {
    options.preserve_perms || options.preserve_owner || options.preserve_group
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.is_absolute() {
        bail!("local entry has absolute path: {}", path.display());
    }
    for component in path.components() {
        match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("unsafe local path component in {}", path.display())
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }
    Ok(())
}

fn effective_push_jobs(options: &SyncOptions, file_count: usize, total_bytes: u64) -> usize {
    let requested = options.jobs.max(1).min(file_count.max(1));
    if options.jobs_explicit {
        return requested;
    }
    let default_limit = if total_bytes < SMALL_PUSH_BYTES && file_count <= 16 {
        SMALL_PUSH_JOBS
    } else {
        LARGE_PUSH_JOBS
    };
    requested.min(default_limit)
}

fn push_buffer_size(options: &SyncOptions) -> usize {
    options.chunk_size.clamp(64 * 1024, 1024 * 1024) as usize
}

fn partial_name(entry: &RemoteEntry) -> String {
    let path_hash = xxh3_64(entry.relative_path.as_os_str().as_encoded_bytes());
    let identity = format!("{}:{}", entry.size, entry.mtime_secs);
    let identity_hash = xxh3_64(identity.as_bytes());
    format!(".parsync-part-{path_hash:016x}-{identity_hash:016x}")
}

fn backup_name(entry: &RemoteEntry) -> String {
    let path_hash = xxh3_64(entry.relative_path.as_os_str().as_encoded_bytes());
    format!(".parsync-backup-{path_hash:016x}")
}

fn status(options: &SyncOptions, message: impl AsRef<str>) {
    if options.verbose {
        eprintln!("{}", message.as_ref());
    }
}

struct PushUi {
    bytes: ProgressBar,
}

impl PushUi {
    fn new(total_bytes: u64, enabled: bool) -> Self {
        let bytes = ProgressBar::new(total_bytes);
        if enabled {
            bytes.set_draw_target(ProgressDrawTarget::stderr());
            bytes.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar()),
            );
        } else {
            bytes.set_draw_target(ProgressDrawTarget::hidden());
        }
        Self { bytes }
    }

    fn set_file_progress(&self, file_progress: &AtomicU64, absolute: u64) {
        let previous = file_progress.swap(absolute, Ordering::Relaxed);
        if absolute >= previous {
            self.bytes.inc(absolute - previous);
        } else {
            self.bytes.dec(previous - absolute);
        }
    }

    fn finish(&self) {
        self.bytes.finish_and_clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(jobs: usize, jobs_explicit: bool) -> SyncOptions {
        SyncOptions {
            verbose: false,
            debug: false,
            progress: false,
            recursive: true,
            links: false,
            update: false,
            preserve_perms: false,
            preserve_owner: false,
            preserve_group: false,
            preserve_acls: false,
            preserve_xattrs: false,
            jobs,
            jobs_explicit,
            chunk_size: 8 * 1024 * 1024,
            chunk_threshold: 64 * 1024 * 1024,
            retries: 5,
            resume: true,
            dry_run: false,
            state_root: None,
            delta_enabled: false,
            delta_min_size: 8 * 1024 * 1024,
            delta_block_size: None,
            delta_max_literals: 64 * 1024 * 1024,
            delta_helper: "parsync --internal-remote-helper".to_string(),
            delta_fallback: true,
            strict_durability: false,
            verify_existing: false,
            sftp_read_concurrency: 4,
            sftp_read_chunk_size: 4 * 1024 * 1024,
            #[cfg(target_os = "linux")]
            rdma_mode: RdmaMode::Auto,
            #[cfg(target_os = "linux")]
            rdma_bind: None,
            #[cfg(target_os = "linux")]
            rdma_min_size: crate::rdma::DEFAULT_RDMA_MIN_SIZE,
            #[cfg(target_os = "linux")]
            rdma_helper: "parsync --internal-rdma-send".to_string(),
            strict_windows_metadata: false,
        }
    }

    #[test]
    fn small_push_defaults_to_four_workers() {
        assert_eq!(effective_push_jobs(&options(32, false), 11, 318_000), 4);
    }

    #[test]
    fn larger_push_defaults_to_eight_workers() {
        assert_eq!(
            effective_push_jobs(&options(32, false), 16, 40 * 1024 * 1024),
            8
        );
    }

    #[test]
    fn explicit_worker_count_is_respected() {
        assert_eq!(effective_push_jobs(&options(16, true), 20, 100), 16);
    }

    #[test]
    fn partial_name_changes_with_source_identity() {
        let mut entry = RemoteEntry {
            relative_path: PathBuf::from("site/index.html"),
            kind: EntryKind::File,
            size: 10,
            mtime_secs: 100,
            mode: 0o644,
            uid: None,
            gid: None,
            link_target: None,
        };
        let first = partial_name(&entry);
        entry.mtime_secs += 1;
        assert_ne!(first, partial_name(&entry));
    }
}
