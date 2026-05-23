//! 事件驱动同步守护进程
//!
//! 将 FileWatcher、SyncEngine、NetworkManager 串联为统一的同步事件循环。
//!
//! ```text
//! Watcher ──FileChange──> Daemon ──> SyncEngine
//! Network ──Message─────> Daemon
//! ```

use crate::config::{CrossBagConfig, SyncPair};
use crate::protocol::{FileEntry, FileIndex, Message};
use crate::state::SyncState;
use crate::sync::SyncEngine;
use crate::watcher::{FileChange, FileWatcher};
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// 同步操作请求
#[derive(Debug)]
pub enum SyncAction {
    /// 本地文件变更触发同步
    LocalChange {
        pair_id: String,
        changes: Vec<FileChange>,
    },
    /// 收到远程文件索引
    RemoteIndex {
        pair_id: String,
        peer_id: String,
        index: FileIndex,
    },
    /// 收到远程文件请求
    RemoteFileRequest {
        pair_id: String,
        peer_id: String,
        files: Vec<String>,
    },
    /// 定时全量同步
    PeriodicFullSync { pair_id: String },
}

/// 事件驱动同步守护进程
pub struct SyncDaemon {
    config: Arc<CrossBagConfig>,
    engine: SyncEngine,
    /// 每对同步对的状态缓存
    states: HashMap<String, SyncState>,
    action_tx: mpsc::UnboundedSender<SyncAction>,
    action_rx: mpsc::UnboundedReceiver<SyncAction>,
}

impl SyncDaemon {
    pub fn new(config: Arc<CrossBagConfig>) -> Self {
        let (action_tx, action_rx) = mpsc::unbounded_channel();

        // 启动时加载所有持久化状态
        let mut states = HashMap::new();
        for pair in &config.sync_pairs {
            let state_path = SyncState::default_path(&pair.id);
            match SyncState::load(&state_path) {
                Ok(Some(state)) => {
                    info!(
                        "Loaded persisted state for '{}': {} files",
                        pair.id,
                        state.files.len()
                    );
                    states.insert(pair.id.clone(), state);
                }
                Ok(None) => {
                    debug!("No persisted state for '{}', starting fresh", pair.id);
                    states.insert(pair.id.clone(), SyncState::new(&pair.id));
                }
                Err(e) => {
                    warn!("Failed to load state for '{}': {}", pair.id, e);
                    states.insert(pair.id.clone(), SyncState::new(&pair.id));
                }
            }
        }

        SyncDaemon {
            engine: SyncEngine::new(config.clone()),
            config,
            states,
            action_tx,
            action_rx,
        }
    }

    /// 获取用于提交同步操作的事件发送端 (外部注入到 Watcher / Network)
    pub fn action_sender(&self) -> mpsc::UnboundedSender<SyncAction> {
        self.action_tx.clone()
    }

    /// 启动同步事件循环
    ///
    /// 持续消费 SyncAction 事件并驱动同步引擎。
    /// 此方法是异步的，应在 tokio::spawn 中运行。
    pub async fn run(mut self) {
        info!("Sync daemon event loop started");

        while let Some(action) = self.action_rx.recv().await {
            match action {
                SyncAction::LocalChange { pair_id, changes } => {
                    debug!(
                        "Processing {} local changes for pair '{}'",
                        changes.len(),
                        pair_id
                    );
                    if let Err(e) = self.handle_local_changes(&pair_id, &changes).await {
                        error!("Failed to process local changes for '{}': {}", pair_id, e);
                    }
                }

                SyncAction::RemoteIndex {
                    pair_id,
                    peer_id,
                    index,
                } => {
                    debug!(
                        "Received remote index from '{}' for pair '{}': {} files",
                        peer_id,
                        pair_id,
                        index.files.len()
                    );
                    if let Err(e) = self.handle_remote_index(&pair_id, &peer_id, &index).await {
                        error!("Failed to process remote index from '{}': {}", peer_id, e);
                    }
                }

                SyncAction::RemoteFileRequest {
                    pair_id,
                    peer_id,
                    files,
                } => {
                    debug!(
                        "Peer '{}' requests {} files for pair '{}'",
                        peer_id,
                        files.len(),
                        pair_id
                    );
                    if let Err(e) = self.handle_file_request(&pair_id, &peer_id, &files).await {
                        error!("Failed to handle file request from '{}': {}", peer_id, e);
                    }
                }

                SyncAction::PeriodicFullSync { pair_id } => {
                    debug!("Running periodic full sync for pair '{}'", pair_id);
                    if let Err(e) = self.run_full_sync(&pair_id).await {
                        error!("Full sync failed for '{}': {}", pair_id, e);
                    }
                }
            }
        }

        info!("Sync daemon event loop stopped");
    }

    /// 处理本地文件变更 (使用增量状态)
    async fn handle_local_changes(&mut self, pair_id: &str, changes: &[FileChange]) -> Result<()> {
        let pair = self.find_pair(pair_id)?.clone();
        let local_path = pair.local_path.clone();
        let exclude = pair.exclude_patterns.clone();

        let state = self
            .states
            .get_mut(pair_id)
            .ok_or_else(|| anyhow::anyhow!("No state for pair '{}'", pair_id))?;

        // 增量更新 (仅重哈希变更的文件)
        let entries = state.incremental_update(&local_path, &exclude)?;

        // 保存状态
        let state_path = SyncState::default_path(pair_id);
        if let Err(e) = state.save(&state_path) {
            warn!("Failed to save state for '{}': {}", pair_id, e);
        }

        // 通知远程节点
        let index_msg = Message::FileIndex(FileIndex {
            pair_id: pair_id.to_string(),
            files: entries,
            timestamp: chrono::Utc::now(),
        });
        let _ = index_msg;
        let _ = changes;

        debug!(
            "Incremental update for '{}': {} entries",
            pair_id,
            state.files.len()
        );
        Ok(())
    }

    /// 处理收到的远程文件索引
    async fn handle_remote_index(
        &mut self,
        pair_id: &str,
        _peer_id: &str,
        remote_index: &FileIndex,
    ) -> Result<()> {
        let pair = self.find_pair(pair_id)?;

        // 构建本地索引
        let local_index = SyncEngine::build_file_index(&pair.local_path, &pair.exclude_patterns)?;

        // 将远程 FileEntry 列表转为 HashMap
        let remote_map: HashMap<PathBuf, FileEntry> = remote_index
            .files
            .iter()
            .map(|e| (PathBuf::from(&e.relative_path), e.clone()))
            .collect();

        // 计算差异
        let (local_only, remote_only) = SyncEngine::diff_indexes(&local_index, &remote_map);

        info!(
            "Sync '{}': {} local-only, {} remote-only files",
            pair_id,
            local_only.len(),
            remote_only.len()
        );

        // 发送本地独有的文件 (对方需要)
        if !local_only.is_empty() {
            let request = Message::FileRequest(crate::protocol::FileRequest {
                pair_id: pair_id.to_string(),
                files: local_only.iter().map(|e| e.relative_path.clone()).collect(),
            });
            // TODO: 发送给 peer
            debug!("Sending file request: {} files", local_only.len());
            let _ = request;
        }

        // 请求远程独有的文件 (本地需要)
        if !remote_only.is_empty() {
            let request = Message::FileRequest(crate::protocol::FileRequest {
                pair_id: pair_id.to_string(),
                files: remote_only
                    .iter()
                    .map(|e| e.relative_path.clone())
                    .collect(),
            });
            // TODO: 发送给 peer
            debug!("Requesting files from peer: {} files", remote_only.len());
            let _ = request;
        }

        Ok(())
    }

    /// 执行一次完整同步
    async fn run_full_sync(&mut self, pair_id: &str) -> Result<()> {
        let pair = self.find_pair(pair_id)?.clone();
        let result = self.engine.full_sync(&pair).await?;

        info!(
            "Full sync '{}' complete: {} files, {} bytes, {} errors",
            pair_id,
            result.files_synced,
            result.bytes_transferred,
            result.errors.len()
        );

        for err in &result.errors {
            warn!("  Sync error in '{}': {}", pair_id, err);
        }

        Ok(())
    }

    /// 处理远程文件请求 (对方需要本节点的文件)
    async fn handle_file_request(
        &mut self,
        pair_id: &str,
        _peer_id: &str,
        files: &[String],
    ) -> Result<()> {
        let pair = self.find_pair(pair_id)?;

        for file_path in files {
            let full_path = pair.local_path.join(file_path);
            if !full_path.exists() {
                warn!("Requested file not found: {:?}", full_path);
                continue;
            }

            // 读取文件并准备分块传输
            let chunks = SyncEngine::read_file_chunks(&full_path, self.config.advanced.chunk_size)?;

            debug!(
                "Prepared {} chunks for file '{}' (to peer {})",
                chunks.len(),
                file_path,
                _peer_id
            );

            // TODO: 通过 NetworkManager 发送 FileChunk 消息给 peer
            let _ = chunks;
        }

        Ok(())
    }

    /// 根据 ID 查找同步对配置
    fn find_pair(&self, pair_id: &str) -> Result<&SyncPair> {
        self.config
            .sync_pairs
            .iter()
            .find(|p| p.id == pair_id)
            .ok_or_else(|| anyhow::anyhow!("Sync pair '{}' not found", pair_id))
    }
}

/// 启动文件监控桥接 (将 FileWatcher 事件转换为 SyncAction)
pub fn spawn_watcher_bridge(
    mut watcher: FileWatcher,
    pair_ids: Vec<String>,
    action_tx: mpsc::UnboundedSender<SyncAction>,
    debounce_ms: u64,
) {
    tokio::spawn(async move {
        let mut rx = watcher.receiver();
        let mut batch: Vec<FileChange> = Vec::new();
        let mut last_flush = tokio::time::Instant::now();
        let debounce = tokio::time::Duration::from_millis(debounce_ms);

        loop {
            match tokio::time::timeout(debounce, rx.recv()).await {
                Ok(Some(change)) => {
                    batch.push(change);
                    // 如果上次 flush 已经过了 debounce 时间，立即提交
                    if last_flush.elapsed() >= debounce && !batch.is_empty() {
                        flush_batch(&batch, &pair_ids, &action_tx).await;
                        batch.clear();
                        last_flush = tokio::time::Instant::now();
                    }
                }
                Ok(None) => break, // channel closed
                Err(_) => {
                    // 超时，flush 累积的变更
                    if !batch.is_empty() {
                        flush_batch(&batch, &pair_ids, &action_tx).await;
                        batch.clear();
                        last_flush = tokio::time::Instant::now();
                    }
                }
            }
        }
    });
}

async fn flush_batch(
    batch: &[FileChange],
    pair_ids: &[String],
    action_tx: &mpsc::UnboundedSender<SyncAction>,
) {
    for pair_id in pair_ids {
        let _ = action_tx.send(SyncAction::LocalChange {
            pair_id: pair_id.clone(),
            changes: batch.to_vec(),
        });
    }
}
