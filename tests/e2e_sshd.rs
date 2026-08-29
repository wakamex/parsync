use std::{
    fs,
    path::Path,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use tempfile::TempDir;

struct DockerContainer {
    id: String,
}

impl Drop for DockerContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).status();
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn docker_run(args: &[&str]) -> Result<String> {
    let output = Command::new("docker").args(args).output()?;
    if !output.status.success() {
        return Err(anyhow!(
            "docker command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn parse_mapped_port(port_output: &str) -> Result<u16> {
    let mapped = port_output
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow!("missing docker port output"))?;
    let port = mapped
        .rsplit(':')
        .next()
        .ok_or_else(|| anyhow!("invalid docker port mapping"))?
        .parse::<u16>()
        .context("parse mapped port")?;
    Ok(port)
}

fn run_parsync(remote: &str, destination: &Path) -> Result<()> {
    let args = vec![
        "-vrPlu".to_string(),
        remote.to_string(),
        destination.display().to_string(),
    ];
    let output = run_parsync_args(&args)?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "parsync failed: {}",
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn run_parsync_args(args: &[String]) -> Result<Output> {
    let home = tempfile::tempdir().context("temp home")?;
    Command::new(assert_cmd::cargo::cargo_bin!("parsync"))
        .args(args)
        .env("PARSYNC_SSH_PASSWORD", "pass")
        .env("PARSYNC_ACCEPT_NEW_HOST_KEYS", "1")
        .env("HOME", home.path())
        .output()
        .context("run parsync")
}

fn docker_exec(container: &str, args: &[&str]) -> Result<Output> {
    Command::new("docker")
        .arg("exec")
        .arg(container)
        .args(args)
        .output()
        .context("docker exec")
}

#[test]
#[ignore = "requires docker"]
fn e2e_pull_over_sftp_with_resume_state() -> Result<()> {
    if !docker_available() {
        return Ok(());
    }

    let fixture = TempDir::new()?;
    fs::create_dir_all(fixture.path().join("sub"))?;
    fs::write(fixture.path().join("hello.txt"), b"hello world")?;
    fs::write(fixture.path().join("sub/nested.txt"), b"nested")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(fixture.path(), fs::Permissions::from_mode(0o755))?;
        fs::set_permissions(
            fixture.path().join("sub"),
            fs::Permissions::from_mode(0o755),
        )?;
        fs::set_permissions(
            fixture.path().join("hello.txt"),
            fs::Permissions::from_mode(0o644),
        )?;
        fs::set_permissions(
            fixture.path().join("sub/nested.txt"),
            fs::Permissions::from_mode(0o644),
        )?;
    }

    let cid = docker_run(&[
        "run",
        "-d",
        "-P",
        "-v",
        &format!("{}:/home/foo/upload:Z", fixture.path().display()),
        "docker.io/atmoz/sftp",
        "foo:pass:::upload",
    ])?;
    let _container = DockerContainer { id: cid.clone() };

    let port_out = docker_run(&["port", &cid, "22/tcp"])?;
    let port = parse_mapped_port(&port_out)?;
    let remote = format!("foo@127.0.0.1:{port}:/upload");

    let destination = TempDir::new()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_err: Option<anyhow::Error> = None;
    while Instant::now() < deadline {
        match run_parsync(&remote, destination.path()) {
            Ok(()) => {
                last_err = None;
                break;
            }
            Err(err) => {
                last_err = Some(err);
                thread::sleep(Duration::from_millis(500));
            }
        }
    }
    if let Some(err) = last_err {
        return Err(err);
    }

    assert_eq!(
        fs::read(destination.path().join("hello.txt"))?,
        b"hello world"
    );
    assert_eq!(
        fs::read(destination.path().join("sub/nested.txt"))?,
        b"nested"
    );
    assert!(!destination.path().join(".parsync").exists());

    Ok(())
}

#[cfg(unix)]
#[test]
#[ignore = "requires docker"]
fn e2e_push_over_sftp_replaces_and_skips_files() -> Result<()> {
    if !docker_available() {
        return Ok(());
    }

    let cid = docker_run(&[
        "run",
        "-d",
        "-P",
        "docker.io/atmoz/sftp",
        "foo:pass:::upload",
    ])?;
    let _container = DockerContainer { id: cid.clone() };
    let port_out = docker_run(&["port", &cid, "22/tcp"])?;
    let port = parse_mapped_port(&port_out)?;
    let remote = format!("foo@127.0.0.1:{port}:/upload");

    let source = TempDir::new()?;
    fs::create_dir_all(source.path().join("nested"))?;
    fs::write(source.path().join("hello.txt"), b"hello world")?;
    fs::write(source.path().join("nested/data.txt"), b"nested")?;
    std::os::unix::fs::symlink("hello.txt", source.path().join("hello-link"))?;
    let source_children = format!("{}/*", source.path().display());
    let args = vec![
        "-vrPlu".to_string(),
        "--jobs".to_string(),
        "4".to_string(),
        source_children.clone(),
        remote.clone(),
    ];

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_error = None;
    while Instant::now() < deadline {
        let output = run_parsync_args(&args)?;
        if output.status.success() {
            last_error = None;
            break;
        }
        last_error = Some(String::from_utf8_lossy(&output.stderr).to_string());
        thread::sleep(Duration::from_millis(500));
    }
    if let Some(error) = last_error {
        bail!("push did not become ready: {error}");
    }

    let hello = docker_exec(&cid, &["cat", "/home/foo/upload/hello.txt"])?;
    assert!(hello.status.success());
    assert_eq!(hello.stdout, b"hello world");
    let nested = docker_exec(&cid, &["cat", "/home/foo/upload/nested/data.txt"])?;
    assert!(nested.status.success());
    assert_eq!(nested.stdout, b"nested");
    let link = docker_exec(&cid, &["readlink", "/home/foo/upload/hello-link"])?;
    assert!(link.status.success());
    assert_eq!(String::from_utf8_lossy(&link.stdout).trim(), "hello.txt");

    let replace_with_symlink = docker_exec(
        &cid,
        &[
            "sh",
            "-c",
            "rm /home/foo/upload/hello.txt && ln -s nested/data.txt /home/foo/upload/hello.txt",
        ],
    )?;
    assert!(replace_with_symlink.status.success());
    fs::write(source.path().join("hello.txt"), b"replacement content")?;
    let replacement = run_parsync_args(&args)?;
    assert!(
        replacement.status.success(),
        "{}",
        String::from_utf8_lossy(&replacement.stderr)
    );
    assert!(String::from_utf8_lossy(&replacement.stderr).contains("/s aggregate"));
    let hello = docker_exec(&cid, &["cat", "/home/foo/upload/hello.txt"])?;
    assert_eq!(hello.stdout, b"replacement content");

    let warm_args = vec![
        "-vrPlu".to_string(),
        "--debug".to_string(),
        "--jobs".to_string(),
        "4".to_string(),
        source_children,
        remote,
    ];
    let warm = run_parsync_args(&warm_args)?;
    assert!(
        warm.status.success(),
        "{}",
        String::from_utf8_lossy(&warm.stderr)
    );
    let stderr = String::from_utf8_lossy(&warm.stderr);
    assert!(stderr.contains("transferred=0, skipped=2"), "{stderr}");

    let internal_files = docker_exec(
        &cid,
        &["find", "/home/foo/upload", "-name", ".parsync-*", "-print"],
    )?;
    assert!(internal_files.status.success());
    assert!(internal_files.stdout.is_empty());

    fs::create_dir(source.path().join("escape"))?;
    fs::write(
        source.path().join("escape/parsync-escape-test"),
        b"must stay contained",
    )?;
    let remote_symlink = docker_exec(&cid, &["ln", "-s", "/tmp", "/home/foo/upload/escape"])?;
    assert!(remote_symlink.status.success());
    let escaped_push = run_parsync_args(&args)?;
    assert!(!escaped_push.status.success());
    assert!(String::from_utf8_lossy(&escaped_push.stderr)
        .contains("remote destination path component is not a directory"));
    let escaped_file = docker_exec(&cid, &["test", "!", "-e", "/tmp/parsync-escape-test"])?;
    assert!(escaped_file.status.success());

    Ok(())
}
