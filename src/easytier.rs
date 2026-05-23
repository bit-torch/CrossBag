//! Easytier 子进程管理
//!
//! 将 Easytier 作为独立子进程运行，通过进程间通信实现生命周期管理。
//! 提供启动、停止、健康检查、崩溃自动重启等功能。
//!
//! # 架构
//! ```text
//! CrossBag ──spawn──> easytier-core (子进程)
//!    │                    │
//!    │──stdin──>          │ (发送命令)
//!    │<──stdout──         │ (读取状态)
//!    │<──stderr──         │ (错误日志)
//!    │                    │
//!    │──SIGTERM──>        │ (优雅关闭)
//! ```

use crate::config::EasytierConfig;
use anyhow::{Context, Result};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

/// Easytier 子进程管理器
pub struct EasytierManager {
    /// Easytier 配置
    config: EasytierConfig,
    /// 子进程句柄 (Arc<Mutex<>> 保证线程安全)
    child: Option<Child>,
    /// 当前重启次数
    restart_count: u32,
    /// 输出缓冲区 (用于状态检测)
    last_output: String,
}

/// Easytier 运行状态
#[derive(Debug, Clone, PartialEq)]
pub enum EasytierState {
    /// 未启动
    Stopped,
    /// 启动中
    Starting,
    /// 运行中
    Running,
    /// 已崩溃
    Crashed(String),
    /// 已达最大重启次数
    Failed(String),
}

impl EasytierManager {
    /// 创建新的 Easytier 管理器
    pub fn new(config: EasytierConfig) -> Self {
        EasytierManager {
            config,
            child: None,
            restart_count: 0,
            last_output: String::new(),
        }
    }

    /// 检查 Easytier 二进制文件是否存在
    pub fn check_binary(&self) -> Result<String> {
        let path = &self.config.binary_path;

        // 如果路径包含路径分隔符，直接检查
        if path.contains('/') || path.contains('\\') {
            if std::path::Path::new(path).exists() {
                return Ok(path.clone());
            }
            anyhow::bail!("Easytier binary not found at: {}", path);
        }

        // 否则在 PATH 中搜索
        let which_cmd = if cfg!(target_os = "windows") {
            format!("where {}", path)
        } else {
            format!("which {}", path)
        };

        let output = std::process::Command::new(if cfg!(target_os = "windows") {
            "cmd"
        } else {
            "sh"
        })
        .arg(if cfg!(target_os = "windows") {
            "/c"
        } else {
            "-c"
        })
        .arg(&which_cmd)
        .output()
        .context("Failed to search for easytier binary")?;

        if output.status.success() {
            let found = String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .unwrap_or(path)
                .trim()
                .to_string();
            debug!("Found easytier binary: {}", found);
            Ok(found)
        } else {
            anyhow::bail!(
                "Easytier binary '{}' not found in PATH. Please install easytier or specify full path in config.",
                path
            )
        }
    }

    /// 构建 Easytier 命令行参数
    fn build_args(&self, _binary: &str) -> Vec<String> {
        let mut args = Vec::new();

        args.push("--instance-name".to_string());
        args.push(self.config.instance_name.clone());

        args.push("--network-name".to_string());
        args.push(self.config.network_name.clone());

        if !self.config.network_secret.is_empty() {
            args.push("--network-secret".to_string());
            args.push(self.config.network_secret.clone());
        }

        // 添加监听地址
        if !self.config.listeners.is_empty() {
            args.push("--listeners".to_string());
            args.push(self.config.listeners.join(" "));
        }

        // 禁用 DHCP (我们手动管理 IP)
        args.push("--disable-dhcp".to_string());

        args
    }

    /// 启动 Easytier 子进程
    pub async fn start(&mut self) -> Result<()> {
        if self.child.is_some() {
            warn!("Easytier is already running");
            return Ok(());
        }

        let binary = self.check_binary()?;
        let args = self.build_args(&binary);

        info!("Starting Easytier: {} {}", binary, args.join(" "));

        let mut cmd = Command::new(&binary);
        cmd.args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true); // 确保 CrossBag 退出时子进程也被终止

        // 平台特定设置
        #[cfg(windows)]
        {
            #[allow(unused_imports)]
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("Failed to start Easytier process: {}", binary))?;

        info!("Easytier started with PID: {}", child.id().unwrap_or(0));

        self.child = Some(child);

        // 启动输出监控
        self.spawn_output_monitor();

        // 等待启动就绪
        self.wait_for_ready().await?;

        Ok(())
    }

    /// 等待 Easytier 准备就绪
    async fn wait_for_ready(&mut self) -> Result<()> {
        let startup_timeout = Duration::from_secs(self.config.startup_timeout);

        info!(
            "Waiting for Easytier to be ready (timeout: {}s)...",
            startup_timeout.as_secs()
        );

        let result = timeout(startup_timeout, async {
            loop {
                // 检查子进程是否仍在运行
                if let Some(ref mut child) = self.child {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            let msg = format!("Easytier exited early with status: {}", status);
                            error!("{}", msg);
                            // 收集 stderr
                            if let Some(ref mut stderr) = child.stderr {
                                let mut buf = String::new();
                                let mut reader = BufReader::new(stderr);
                                use tokio::io::AsyncReadExt;
                                let _ = reader.read_to_string(&mut buf).await;
                                if !buf.is_empty() {
                                    error!("Easytier stderr: {}", buf);
                                }
                            }
                            anyhow::bail!(msg);
                        }
                        Ok(None) => {
                            // 进程仍在运行
                            // 尝试通过查询 easytier-cli 验证网络是否就绪
                            if self.verify_network_ready().await.is_ok() {
                                info!("Easytier network is ready");
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            anyhow::bail!("Failed to check Easytier status: {}", e);
                        }
                    }
                }
                sleep(Duration::from_millis(500)).await;
            }
        })
        .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                // 超时不算致命错误，Easytier 可能仍在建立连接
                warn!("Easytier startup timeout, continuing anyway (network may still be establishing)");
                Ok(())
            }
        }
    }

    /// 验证 Easytier 网络是否就绪
    async fn verify_network_ready(&self) -> Result<()> {
        // 尝试运行 easytier-cli 检查节点列表
        let output = tokio::process::Command::new("easytier-cli")
            .arg("node")
            .arg("list")
            .output()
            .await;

        match output {
            Ok(out) if out.status.success() => {
                let stdout = String::from_utf8_lossy(&out.stdout);
                debug!("easytier-cli output: {}", stdout);
                Ok(())
            }
            _ => {
                // easytier-cli 可能不可用或网络尚未就绪
                anyhow::bail!("Easytier network not ready yet");
            }
        }
    }

    /// 启动输出监控 (独立 task)
    fn spawn_output_monitor(&mut self) {
        if let Some(ref mut child) = self.child {
            // 取出 stdout/stderr (take 后 child 不再持有)
            if let Some(stdout) = child.stdout.take() {
                let reader = BufReader::new(stdout);
                tokio::spawn(async move {
                    let mut lines = reader.lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        debug!("[easytier] {}", line);
                    }
                    debug!("[easytier] stdout closed");
                });
            }

            if let Some(stderr) = child.stderr.take() {
                let reader = BufReader::new(stderr);
                tokio::spawn(async move {
                    let mut lines = reader.lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        if line.contains("ERROR") || line.contains("error") {
                            error!("[easytier:err] {}", line);
                        } else {
                            warn!("[easytier:err] {}", line);
                        }
                    }
                    debug!("[easytier] stderr closed");
                });
            }
        }
    }

    /// 停止 Easytier 子进程
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            info!("Stopping Easytier (PID: {})...", child.id().unwrap_or(0));

            // 先尝试优雅关闭
            #[cfg(unix)]
            {
                use tokio::signal::unix::{kill, SignalKind};
                let pid = child.id().unwrap_or(0) as i32;
                let _ = kill(pid, SignalKind::terminate());
            }

            #[cfg(windows)]
            {
                // Windows 上用 CTRL_BREAK 或直接 kill
                let _ = child.start_kill();
            }

            // 等待进程结束
            match timeout(Duration::from_secs(10), child.wait()).await {
                Ok(Ok(status)) => {
                    info!("Easytier stopped with status: {}", status);
                }
                Ok(Err(e)) => {
                    error!("Failed to wait for Easytier: {}", e);
                    let _ = child.start_kill();
                }
                Err(_) => {
                    warn!("Easytier stop timeout, force killing");
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
            }

            self.restart_count = 0;
        }

        Ok(())
    }

    /// 检查 Easytier 是否正在运行
    pub fn is_running(&self) -> bool {
        self.child.is_some()
    }

    /// 执行健康检查 (自动重启已崩溃进程)
    pub async fn health_check(&mut self) -> EasytierState {
        match &mut self.child {
            None => {
                if self.config.auto_start && self.restart_count < self.config.max_restarts {
                    warn!(
                        "Easytier not running, attempting restart ({}/{})",
                        self.restart_count + 1,
                        self.config.max_restarts
                    );
                    self.restart_count += 1;
                    match self.start().await {
                        Ok(()) => EasytierState::Running,
                        Err(e) => {
                            error!("Easytier restart failed: {}", e);
                            EasytierState::Failed(e.to_string())
                        }
                    }
                } else if self.restart_count >= self.config.max_restarts {
                    EasytierState::Failed(format!(
                        "Max restarts ({}) reached",
                        self.config.max_restarts
                    ))
                } else {
                    EasytierState::Stopped
                }
            }
            Some(ref mut child) => {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        let err_msg =
                            format!("Easytier exited unexpectedly with status: {}", status);
                        error!("{}", err_msg);
                        self.child = None;

                        // 自动重启
                        if self.config.auto_start && self.restart_count < self.config.max_restarts {
                            self.restart_count += 1;
                            warn!(
                                "Auto-restarting Easytier ({}/{})...",
                                self.restart_count, self.config.max_restarts
                            );
                            match self.start().await {
                                Ok(()) => EasytierState::Running,
                                Err(e) => EasytierState::Failed(e.to_string()),
                            }
                        } else {
                            EasytierState::Crashed(err_msg)
                        }
                    }
                    Ok(None) => {
                        // 进程正常运行
                        EasytierState::Running
                    }
                    Err(e) => {
                        error!("Failed to check Easytier status: {}", e);
                        EasytierState::Crashed(e.to_string())
                    }
                }
            }
        }
    }

    /// 获取当前状态 (不触发重启)
    pub async fn status(&mut self) -> EasytierState {
        match &mut self.child {
            None => EasytierState::Stopped,
            Some(ref mut child) => match child.try_wait() {
                Ok(Some(_)) => {
                    self.child = None;
                    EasytierState::Crashed("Process exited".into())
                }
                Ok(None) => EasytierState::Running,
                Err(e) => EasytierState::Crashed(e.to_string()),
            },
        }
    }
}

impl Drop for EasytierManager {
    fn drop(&mut self) {
        // 确保子进程被清理
        if let Some(mut child) = self.child.take() {
            info!("Cleaning up Easytier subprocess on drop");
            let _ = child.start_kill();
        }
    }
}
