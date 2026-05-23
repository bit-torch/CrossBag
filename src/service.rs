//! 系统服务管理
//!
//! 将 CrossBag 注册为系统服务，支持开机自启和后台运行。
//!
//! - Windows: 使用 `sc.exe` 创建/管理 Windows 服务
//! - Linux: 生成 systemd unit 文件
//! - macOS: 生成 launchd plist 文件

use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing::info;

/// 获取 CrossBag 可执行文件路径
fn current_exe_path() -> Result<PathBuf> {
    std::env::current_exe().context("Failed to get current executable path")
}

/// 获取配置文件路径
fn config_path_for_service() -> PathBuf {
    crate::config::CrossBagConfig::default_path()
}

/// 安装系统服务
pub fn install(binary_path: Option<PathBuf>, config_path: Option<PathBuf>) -> Result<()> {
    let binary = binary_path.unwrap_or_else(|| current_exe_path().unwrap_or_default());
    let config = config_path.unwrap_or_else(config_path_for_service);

    if !binary.exists() {
        anyhow::bail!("CrossBag binary not found: {:?}", binary);
    }

    info!("Installing CrossBag service...");
    info!("  Binary: {:?}", binary);
    info!("  Config: {:?}", config);

    #[cfg(target_os = "windows")]
    install_windows(&binary, &config)?;

    #[cfg(target_os = "linux")]
    install_linux(&binary, &config)?;

    #[cfg(target_os = "macos")]
    install_macos(&binary, &config)?;

    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    anyhow::bail!("Service installation not supported on this platform");

    info!("Service installed successfully");
    Ok(())
}

/// 卸载系统服务
pub fn uninstall() -> Result<()> {
    info!("Uninstalling CrossBag service...");

    #[cfg(target_os = "windows")]
    uninstall_windows()?;

    #[cfg(target_os = "linux")]
    uninstall_linux()?;

    #[cfg(target_os = "macos")]
    uninstall_macos()?;

    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    anyhow::bail!("Service uninstallation not supported on this platform");

    info!("Service uninstalled successfully");
    Ok(())
}

/// 启动系统服务
pub fn start_service() -> Result<()> {
    info!("Starting CrossBag service...");

    #[cfg(target_os = "windows")]
    {
        let status = std::process::Command::new("sc")
            .args(["start", "CrossBag"])
            .status()
            .context("Failed to start CrossBag service")?;

        if !status.success() {
            anyhow::bail!("Failed to start service (exit code: {:?})", status.code());
        }
    }

    #[cfg(target_os = "linux")]
    {
        let status = std::process::Command::new("systemctl")
            .args(["start", "crossbag"])
            .status()
            .context("Failed to start crossbag service")?;

        if !status.success() {
            anyhow::bail!("Failed to start service");
        }
    }

    info!("Service started successfully");
    Ok(())
}

/// 停止系统服务
pub fn stop_service() -> Result<()> {
    info!("Stopping CrossBag service...");

    #[cfg(target_os = "windows")]
    {
        let status = std::process::Command::new("sc")
            .args(["stop", "CrossBag"])
            .status()
            .context("Failed to stop CrossBag service")?;
        let _ = status;
    }

    #[cfg(target_os = "linux")]
    {
        let status = std::process::Command::new("systemctl")
            .args(["stop", "crossbag"])
            .status()
            .context("Failed to stop crossbag service")?;
        let _ = status;
    }

    info!("Service stopped");
    Ok(())
}

/// 查询服务状态
pub fn query_status() -> Result<String> {
    #[cfg(target_os = "windows")]
    {
        let output = std::process::Command::new("sc")
            .args(["query", "CrossBag"])
            .output()
            .context("Failed to query CrossBag service")?;

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    #[cfg(target_os = "linux")]
    {
        let output = std::process::Command::new("systemctl")
            .args(["status", "crossbag", "--no-pager"])
            .output()
            .context("Failed to query crossbag service")?;

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        Ok("Service status query not supported on this platform".into())
    }
}

// ==================== Windows 实现 ====================

#[cfg(target_os = "windows")]
fn install_windows(binary: &std::path::Path, config: &std::path::Path) -> Result<()> {
    let bin_str = binary.to_string_lossy();

    // 先删除旧服务 (忽略错误)
    let _ = std::process::Command::new("sc")
        .args(["delete", "CrossBag"])
        .status();

    // 创建服务
    let status = std::process::Command::new("sc")
        .args([
            "create",
            "CrossBag",
            "binPath=",
            &format!("\"{}\" serve -c \"{}\"", bin_str, config.to_string_lossy()),
            "start=",
            "auto",
            "DisplayName=",
            "CrossBag File Sync",
            "Description=",
            "CrossBag - Cross-machine file sync over Easytier",
        ])
        .status()
        .context("Failed to create Windows service")?;

    if !status.success() {
        anyhow::bail!("sc create failed with exit code: {:?}", status.code());
    }

    // 设置恢复选项: 崩溃后重启
    let _ = std::process::Command::new("sc")
        .args([
            "failure",
            "CrossBag",
            "reset=",
            "86400",
            "actions=",
            "restart/5000/restart/10000/restart/30000",
        ])
        .status();

    Ok(())
}

#[cfg(target_os = "windows")]
fn uninstall_windows() -> Result<()> {
    // 先停止服务
    let _ = std::process::Command::new("sc")
        .args(["stop", "CrossBag"])
        .status();

    // 删除服务
    let status = std::process::Command::new("sc")
        .args(["delete", "CrossBag"])
        .status()
        .context("Failed to delete Windows service")?;

    if !status.success() {
        anyhow::bail!("sc delete failed");
    }

    Ok(())
}

// ==================== Linux (systemd) 实现 ====================

#[cfg(target_os = "linux")]
fn install_linux(binary: &std::path::Path, config: &std::path::Path) -> Result<()> {
    let bin_str = binary.to_string_lossy();
    let config_str = config.to_string_lossy();

    let unit_file = format!(
        r#"[Unit]
Description=CrossBag File Sync Service
After=network.target easytier.service
Wants=network.target

[Service]
Type=simple
ExecStart={} serve -c {}
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=crossbag

[Install]
WantedBy=multi-user.target
"#,
        bin_str, config_str
    );

    let unit_path = std::path::PathBuf::from("/etc/systemd/system/crossbag.service");

    // 需要 root 权限
    std::fs::write(&unit_path, &unit_file)
        .with_context(|| format!("Failed to write unit file to {:?}", unit_path))?;

    // 重新加载 systemd
    let _ = std::process::Command::new("systemctl")
        .args(["daemon-reload"])
        .status();

    // 启用服务
    let _ = std::process::Command::new("systemctl")
        .args(["enable", "crossbag"])
        .status();

    Ok(())
}

#[cfg(target_os = "linux")]
fn uninstall_linux() -> Result<()> {
    let _ = std::process::Command::new("systemctl")
        .args(["stop", "crossbag"])
        .status();

    let _ = std::process::Command::new("systemctl")
        .args(["disable", "crossbag"])
        .status();

    let _ = std::fs::remove_file("/etc/systemd/system/crossbag.service");

    let _ = std::process::Command::new("systemctl")
        .args(["daemon-reload"])
        .status();

    Ok(())
}

// ==================== macOS (launchd) 实现 ====================

#[cfg(target_os = "macos")]
fn install_macos(binary: &std::path::Path, config: &std::path::Path) -> Result<()> {
    let bin_str = binary.to_string_lossy();
    let config_str = config.to_string_lossy();

    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.crossbag.sync</string>
    <key>ProgramArguments</key>
    <array>
        <string>{}</string>
        <string>serve</string>
        <string>-c</string>
        <string>{}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/crossbag.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/crossbag.err.log</string>
</dict>
</plist>"#,
        bin_str, config_str
    );

    let plist_path = dirs_next_plist().join("com.crossbag.sync.plist");
    if let Some(parent) = plist_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(&plist_path, &plist)
        .with_context(|| format!("Failed to write plist to {:?}", plist_path))?;

    // 加载服务
    let _ = std::process::Command::new("launchctl")
        .args(["load", "-w"])
        .arg(&plist_path)
        .status();

    Ok(())
}

#[cfg(target_os = "macos")]
fn uninstall_macos() -> Result<()> {
    let plist_path = dirs_next_plist().join("com.crossbag.sync.plist");

    let _ = std::process::Command::new("launchctl")
        .args(["unload"])
        .arg(&plist_path)
        .status();

    let _ = std::fs::remove_file(&plist_path);

    Ok(())
}

#[cfg(target_os = "macos")]
fn dirs_next_plist() -> PathBuf {
    std::env::var("HOME")
        .map(|h| PathBuf::from(h).join("Library").join("LaunchAgents"))
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}
