//! 文件系统监控模块
//!
//! 基于 notify crate 实现跨平台的文件变更监控，
//! 支持递归目录监听、事件去抖和变更队列。

use anyhow::{Context, Result};
use notify::event::{CreateKind, ModifyKind, RemoveKind};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// 文件变更事件
#[derive(Debug, Clone)]
pub enum FileChange {
    /// 文件/目录被创建
    Created(PathBuf),
    /// 文件内容被修改
    Modified(PathBuf),
    /// 文件/目录被删除
    Removed(PathBuf),
    /// 文件/目录被重命名
    Renamed { from: PathBuf, to: PathBuf },
}

/// 文件监控器
pub struct FileWatcher {
    /// 变更事件发送通道
    tx: mpsc::UnboundedSender<FileChange>,
    /// 接收变更事件 (供外部使用)
    rx: Option<mpsc::UnboundedReceiver<FileChange>>,
    /// 监控的根目录列表
    watched_paths: Vec<PathBuf>,
    /// 去抖时间
    debounce_duration: Duration,
}

impl FileWatcher {
    /// 创建新的文件监控器
    pub fn new(debounce_ms: u64) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        FileWatcher {
            tx,
            rx: Some(rx),
            watched_paths: Vec::new(),
            debounce_duration: Duration::from_millis(debounce_ms),
        }
    }

    /// 获取事件接收器
    pub fn receiver(&mut self) -> mpsc::UnboundedReceiver<FileChange> {
        self.rx.take().expect("Receiver already taken")
    }

    /// 添加监控路径
    pub fn watch_path(&mut self, path: &Path) -> Result<()> {
        if !path.exists() {
            warn!("Path does not exist, cannot watch: {:?}", path);
            return Ok(());
        }

        if !self.watched_paths.contains(&path.to_path_buf()) {
            self.watched_paths.push(path.to_path_buf());
            info!("Added watch path: {:?}", path);
        }
        Ok(())
    }

    /// 启动监控 (此方法会阻塞，应在独立线程中运行)
    pub fn start(self) -> Result<()> {
        if self.watched_paths.is_empty() {
            anyhow::bail!("No paths configured for watching");
        }

        let tx = self.tx.clone();

        // 使用 recommended_watcher 自动选择最佳后端
        let mut watcher = notify::recommended_watcher(
            move |event_result: std::result::Result<Event, notify::Error>| {
                match event_result {
                    Ok(event) => {
                        // 过滤非文件系统事件
                        let change = match event.kind {
                            EventKind::Create(CreateKind::File) => {
                                event.paths.first().map(|p| FileChange::Created(p.clone()))
                            }
                            EventKind::Create(CreateKind::Folder) => {
                                // 新目录也需要跟踪
                                event.paths.first().map(|p| FileChange::Created(p.clone()))
                            }
                            EventKind::Modify(ModifyKind::Data(_)) => {
                                event.paths.first().map(|p| FileChange::Modified(p.clone()))
                            }
                            EventKind::Modify(ModifyKind::Metadata(_)) => {
                                // 元数据变更也视为修改
                                event.paths.first().map(|p| FileChange::Modified(p.clone()))
                            }
                            EventKind::Remove(RemoveKind::File)
                            | EventKind::Remove(RemoveKind::Folder) => {
                                event.paths.first().map(|p| FileChange::Removed(p.clone()))
                            }
                            EventKind::Modify(ModifyKind::Name(_)) => {
                                // 重命名: 需要两个路径
                                if event.paths.len() >= 2 {
                                    Some(FileChange::Renamed {
                                        from: event.paths[0].clone(),
                                        to: event.paths[1].clone(),
                                    })
                                } else {
                                    None
                                }
                            }
                            _ => {
                                // 忽略其他事件类型
                                None
                            }
                        };

                        if let Some(change) = change {
                            debug!("File change detected: {:?}", change);
                            if tx.send(change).is_err() {
                                error!("File change receiver dropped");
                            }
                        }
                    }
                    Err(e) => {
                        error!("File watch error: {}", e);
                    }
                }
            },
        )
        .context("Failed to create file watcher")?;

        // 配置 watcher
        watcher.configure(
            notify::Config::default()
                .with_poll_interval(Duration::from_secs(2))
                .with_compare_contents(false),
        )?;

        // 添加所有监控路径
        for path in &self.watched_paths {
            info!("Watching: {:?}", path);
            watcher
                .watch(path, RecursiveMode::Recursive)
                .with_context(|| format!("Failed to watch path: {:?}", path))?;
        }

        // 阻塞等待 (实际使用中会在独立线程运行)
        info!(
            "File watcher started, monitoring {} path(s)",
            self.watched_paths.len()
        );

        // 保持 watcher 存活
        std::thread::park();

        Ok(())
    }

    /// 在独立线程中启动监控
    pub fn spawn(self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            if let Err(e) = self.start() {
                error!("File watcher error: {}", e);
            }
        })
    }
}

/// 带有去抖的变更收集器
pub struct ChangeCollector {
    rx: mpsc::UnboundedReceiver<FileChange>,
    buffer: Vec<FileChange>,
    debounce_interval: Duration,
}

impl ChangeCollector {
    pub fn new(rx: mpsc::UnboundedReceiver<FileChange>, debounce_ms: u64) -> Self {
        ChangeCollector {
            rx,
            buffer: Vec::new(),
            debounce_interval: Duration::from_millis(debounce_ms),
        }
    }

    /// 收集变更事件，返回一批去抖后的变更
    pub async fn collect_batch(&mut self) -> Vec<FileChange> {
        self.buffer.clear();

        // 接收第一个事件
        match self.rx.recv().await {
            Some(event) => self.buffer.push(event),
            None => return Vec::new(),
        }

        // 在去抖窗口内收集更多事件
        let deadline = tokio::time::Instant::now() + self.debounce_interval;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, self.rx.recv()).await {
                Ok(Some(event)) => {
                    // 去重: 同一个文件的多个事件合并
                    if !self.contains_same_path(&event) {
                        self.buffer.push(event);
                    }
                }
                Ok(None) => break,
                Err(_) => break, // 超时
            }
        }

        std::mem::take(&mut self.buffer)
    }

    fn contains_same_path(&self, event: &FileChange) -> bool {
        self.buffer.iter().any(|e| match (e, event) {
            (FileChange::Created(a), FileChange::Created(b)) => a == b,
            (FileChange::Modified(a), FileChange::Modified(b)) => a == b,
            (FileChange::Removed(a), FileChange::Removed(b)) => a == b,
            _ => false,
        })
    }
}
