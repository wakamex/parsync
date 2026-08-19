use std::{
    collections::HashSet,
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::remote::{ConnectionPool, RemoteSpec};

#[derive(Debug, Clone)]
struct PushFile {
    local_path: PathBuf,
    relative_path: PathBuf,
}

#[derive(Debug, Clone)]
struct PushOptions {
    jobs_list: Vec<usize>,
    runs: usize,
    fsync: bool,
    selected_files: Option<Vec<String>>,
    json_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PushJobResult {
    jobs: usize,
    runs_seconds: Vec<f64>,
    median_seconds: f64,
    mean_seconds: f64,
    remote_run_roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PushBenchmarkReport {
    source: String,
    remote: String,
    files: usize,
    total_bytes: u64,
    fsync: bool,
    results: Vec<PushJobResult>,
}

fn inventory_source(
    source: &Path,
    selected_files: Option<&[String]>,
) -> Result<(Vec<PathBuf>, Vec<PushFile>, u64)> {
    let root = source
        .canonicalize()
        .with_context(|| format!("source path not found: {}", source.display()))?;
    if !root.is_dir() {
        bail!("source is not a directory: {}", root.display());
    }

    if let Some(selected) = selected_files {
        if selected.is_empty() {
            bail!("selected file manifest is empty");
        }
        let mut files = Vec::new();
        let mut directory_set = HashSet::new();
        let mut total_bytes = 0u64;
        let mut seen = HashSet::new();

        for entry in selected {
            let rel = Path::new(entry);
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| matches!(c, Component::ParentDir | Component::RootDir))
            {
                bail!("selected file must be a safe relative path: {entry}");
            }
            let rel_str = rel.to_string_lossy().to_string();
            if !seen.insert(rel_str.clone()) {
                bail!("duplicate selected file in manifest: {entry}");
            }

            let full = root.join(rel);
            let meta = fs::symlink_metadata(&full)
                .with_context(|| format!("selected file not found: {}", full.display()))?;
            if meta.file_type().is_symlink() {
                bail!("prototype does not support symlinks: {}", full.display());
            }
            if !meta.file_type().is_file() {
                bail!(
                    "prototype only supports regular files: {}",
                    full.display()
                );
            }

            let mut current = rel.parent();
            while let Some(parent) = current {
                if parent.as_os_str().is_empty() {
                    break;
                }
                directory_set.insert(parent.to_path_buf());
                current = parent.parent();
            }

            let size = meta.len();
            total_bytes += size;
            files.push(PushFile {
                local_path: full,
                relative_path: rel.to_path_buf(),
            });
        }

        let mut directories: Vec<PathBuf> = directory_set.into_iter().collect();
        directories.sort_by_key(|p| (p.components().count(), p.clone()));
        return Ok((directories, files, total_bytes));
    }

    let mut directories = Vec::new();
    let mut files = Vec::new();
    let mut total_bytes = 0u64;

    fn walk_dir(
        current_dir: &Path,
        root: &Path,
        directories: &mut Vec<PathBuf>,
        files: &mut Vec<PushFile>,
        total_bytes: &mut u64,
    ) -> Result<()> {
        let mut entries: Vec<_> = fs::read_dir(current_dir)
            .with_context(|| format!("read directory: {}", current_dir.display()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("read dir entry: {}", current_dir.display()))?;
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let meta = fs::symlink_metadata(&path)
                .with_context(|| format!("stat path: {}", path.display()))?;
            let rel = path
                .strip_prefix(root)
                .with_context(|| format!("prefix strip: {}", path.display()))?
                .to_path_buf();

            if meta.file_type().is_symlink() {
                bail!("prototype does not support symlinks: {}", path.display());
            }

            if meta.file_type().is_dir() {
                directories.push(rel.clone());
                walk_dir(&path, root, directories, files, total_bytes)?;
            } else if meta.file_type().is_file() {
                let size = meta.len();
                *total_bytes += size;
                files.push(PushFile {
                    local_path: path,
                    relative_path: rel,
                });
            } else {
                bail!(
                    "prototype only supports regular files and directories: {}",
                    path.display()
                );
            }
        }
        Ok(())
    }

    walk_dir(&root, &root, &mut directories, &mut files, &mut total_bytes)?;
    directories.sort_by_key(|p| (p.components().count(), p.clone()));

    if files.is_empty() {
        bail!("source has no regular files to transfer");
    }

    Ok((directories, files, total_bytes))
}

fn run_push_benchmark(
    source: &Path,
    remote_spec: &RemoteSpec,
    options: &PushOptions,
) -> Result<PushBenchmarkReport> {
    let (directories, files, total_bytes) =
        inventory_source(source, options.selected_files.as_deref())?;

    let mut job_results = Vec::new();

    let session_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let session_token = format!("{:x}", session_timestamp & 0xffffff);

    for &jobs in &options.jobs_list {
        let jobs = jobs.max(1);
        let mut elapsed_times = Vec::new();
        let mut remote_run_roots = Vec::new();

        for run_idx in 1..=options.runs {
            let run_name = format!(
                ".parsync-push-bench-rust-j{}-{}-{:03}",
                jobs, session_token, run_idx
            );
            let remote_run_root = if remote_spec.path.is_empty() || remote_spec.path == "." {
                PathBuf::from(&run_name)
            } else {
                PathBuf::from(&remote_spec.path).join(&run_name)
            };
            let remote_run_root_str = remote_run_root.to_string_lossy().to_string();
            remote_run_roots.push(remote_run_root_str);

            let started = Instant::now();

            let pool = Arc::new(ConnectionPool::connect(remote_spec, jobs)?);

            {
                let mut conn = pool.checkout()?;
                conn.create_dir_all(&remote_run_root)?;
                for dir in &directories {
                    conn.create_dir_all(&remote_run_root.join(dir))?;
                }
            }

            let rayon_pool = rayon::ThreadPoolBuilder::new()
                .num_threads(jobs)
                .build()
                .context("failed to build rayon thread pool")?;

            rayon_pool.install(|| {
                files
                    .par_iter()
                    .enumerate()
                    .try_for_each(|(idx, file)| -> Result<()> {
                        let mut conn = pool.checkout()?;
                        let temp_name = format!(
                            ".parsync-part-{}-{:06}",
                            session_token,
                            idx + 1
                        );
                        let final_path = remote_run_root.join(&file.relative_path);
                        let temp_path = match final_path.parent() {
                            Some(parent) => parent.join(&temp_name),
                            None => PathBuf::from(&temp_name),
                        };

                        conn.upload_file(&file.local_path, &temp_path, &final_path, options.fsync)?;
                        Ok(())
                    })
            })?;

            let elapsed = started.elapsed().as_secs_f64();
            elapsed_times.push(elapsed);
        }

        let mut sorted = elapsed_times.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_seconds = if sorted.is_empty() {
            0.0
        } else if sorted.len() % 2 == 1 {
            sorted[sorted.len() / 2]
        } else {
            (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
        };
        let mean_seconds = if elapsed_times.is_empty() {
            0.0
        } else {
            elapsed_times.iter().sum::<f64>() / elapsed_times.len() as f64
        };

        job_results.push(PushJobResult {
            jobs,
            runs_seconds: elapsed_times,
            median_seconds,
            mean_seconds,
            remote_run_roots,
        });
    }

    let report = PushBenchmarkReport {
        source: source.display().to_string(),
        remote: format!(
            "{}{}:{}",
            remote_spec
                .user
                .as_ref()
                .map(|u| format!("{u}@"))
                .unwrap_or_default(),
            remote_spec.host,
            remote_spec.path
        ),
        files: files.len(),
        total_bytes,
        fsync: options.fsync,
        results: job_results,
    };

    if let Some(json_path) = &options.json_path {
        let json = serde_json::to_string_pretty(&report)?;
        fs::write(json_path, format!("{json}\n"))
            .with_context(|| format!("write json to {}", json_path.display()))?;
    }

    Ok(report)
}

fn print_markdown_report(report: &PushBenchmarkReport) {
    println!("# Concurrent Rust SFTP push benchmark");
    println!();
    println!("- Source: `{}`", report.source);
    println!("- Remote destination: `{}`", report.remote);
    println!("- Files: {}", report.files);
    println!("- Total size: {} bytes", report.total_bytes);
    println!(
        "- Transfer: parsync ConnectionPool + ssh2::Sftp with Rayon concurrency"
    );
    println!("- SFTP fsync requested: {}", if report.fsync { "yes" } else { "no" });
    println!();
    println!("| Strategy | Workers (--jobs) | Runs | Times | Median | Mean |");
    println!("|---|---:|---:|---|---:|---:|");
    for res in &report.results {
        let times_str = res
            .runs_seconds
            .iter()
            .map(|t| format!("{t:.3}s"))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "| Parsync Rust push | {} | {} | {} | {:.3}s | {:.3}s |",
            res.jobs,
            res.runs_seconds.len(),
            times_str,
            res.median_seconds,
            res.mean_seconds
        );
    }
    println!();
    println!("Remote benchmark directories left in place for inspection and manual cleanup:");
    for res in &report.results {
        for root in &res.remote_run_roots {
            println!("- `{root}`");
        }
    }
}

pub fn run_cli(args: &[String]) -> Result<i32> {
    let mut source: Option<PathBuf> = None;
    let mut remote: Option<String> = None;
    let mut jobs_list = vec![1];
    let mut runs: usize = 3;
    let mut fsync = false;
    let mut json_path: Option<PathBuf> = None;
    let mut selected_files: Option<Vec<String>> = None;

    let parse_jobs = |s: &str| -> Result<Vec<usize>> {
        let mut list = Vec::new();
        for item in s.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            list.push(item.parse().context("invalid worker count in --jobs")?);
        }
        if list.is_empty() {
            bail!("--jobs cannot be empty");
        }
        Ok(list)
    };

    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--prototype-push" || arg == "--internal-push-prototype" {
            i += 1;
            continue;
        }
        if arg == "--jobs" || arg == "-j" {
            i += 1;
            if i < args.len() {
                jobs_list = parse_jobs(&args[i])?;
            }
        } else if let Some(val) = arg.strip_prefix("--jobs=") {
            jobs_list = parse_jobs(val)?;
        } else if arg == "--runs" {
            i += 1;
            if i < args.len() {
                runs = args[i].parse().context("invalid --runs value")?;
            }
        } else if let Some(val) = arg.strip_prefix("--runs=") {
            runs = val.parse().context("invalid --runs value")?;
        } else if arg == "--fsync" {
            fsync = true;
        } else if arg == "--json" {
            i += 1;
            if i < args.len() {
                json_path = Some(PathBuf::from(&args[i]));
            }
        } else if let Some(val) = arg.strip_prefix("--json=") {
            json_path = Some(PathBuf::from(val));
        } else if arg == "--selected-files" {
            i += 1;
            if i < args.len() {
                selected_files = Some(
                    args[i]
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect(),
                );
            }
        } else if let Some(val) = arg.strip_prefix("--selected-files=") {
            selected_files = Some(
                val.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        } else if !arg.starts_with('-') {
            if source.is_none() {
                source = Some(PathBuf::from(arg));
            } else if remote.is_none() {
                remote = Some(arg.clone());
            } else {
                bail!("unexpected positional argument: {arg}");
            }
        }
        i += 1;
    }

    let source = source.ok_or_else(|| anyhow!("missing source directory"))?;
    let remote = remote.ok_or_else(|| anyhow!("missing remote destination ([user@]host:path)"))?;
    let remote_spec = RemoteSpec::parse(&remote)?;

    let options = PushOptions {
        jobs_list,
        runs,
        fsync,
        selected_files,
        json_path,
    };

    let report = run_push_benchmark(&source, &remote_spec, &options)?;
    print_markdown_report(&report);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_inventory_source_full() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("sub")).unwrap();
        fs::write(root.join("a.txt"), "hello").unwrap();
        fs::write(root.join("sub").join("b.txt"), "world!").unwrap();

        let (dirs, files, total_bytes) = inventory_source(root, None).unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0], PathBuf::from("sub"));
        assert_eq!(files.len(), 2);
        assert_eq!(total_bytes, 11);
    }

    #[test]
    fn test_inventory_source_selected_manifest() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("a.txt"), "12345").unwrap();
        fs::write(root.join("b.txt"), "ignored").unwrap();
        fs::write(root.join("nested").join("c.txt"), "67890").unwrap();

        let manifest = vec!["a.txt".to_string(), "nested/c.txt".to_string()];
        let (dirs, files, total_bytes) = inventory_source(root, Some(&manifest)).unwrap();
        assert_eq!(dirs, vec![PathBuf::from("nested")]);
        assert_eq!(files.len(), 2);
        assert_eq!(total_bytes, 10);
    }

    #[test]
    fn test_inventory_rejects_parent_traversal() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join("a.txt"), "12345").unwrap();

        let manifest = vec!["../a.txt".to_string()];
        assert!(inventory_source(root, Some(&manifest)).is_err());
    }
}
