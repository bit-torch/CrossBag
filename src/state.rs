//! 同步状态持久化
//!
//! 将文件索引和同步元数据持久化到磁盘 (JSON)，实现重启时增量恢复。
//!
//! # 工作原理
//! 1. 保存: 每次同步后存储 (路径 → 哈希 + 修改时间)
//! 2. 恢复: 启动时加载上次状态 → 比较 mtime → 仅重哈希变更文件
//! 3. 清理: 删除已不存在的文件记录

use crate::protocol::FileEntry;
use anyhow::{Context, Result};
use blake3::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// 持久化的同步状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    /// 同步对 ID
    pub pair_id: String,
    /// 上次同步时间
    pub last_sync: DateTime<Utc>,
    /// 文件状态映射 (相对路径 → 状态条目)
    pub files: HashMap<String, FileStateEntry>,
}

/// 单个文件的状态快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStateEntry {
    pub size: u64,
    pub modified: DateTime<Utc>,
    pub hash: Hash,
}

impl SyncState {
    /// 创建空状态
    pub fn new(pair_id: &str) -> Self {
        SyncState {
            pair_id: pair_id.to_string(),
            last_sync: Utc::now(),
            files: HashMap::new(),
        }
    }

    /// 从磁盘加载状态
    pub fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }

        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read state file: {:?}", path))?;

        let state: SyncState = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse state file: {:?}", path))?;

        info!(
            "Loaded state for '{}': {} files (last sync: {})",
            state.pair_id,
            state.files.len(),
            state.last_sync
        );

        Ok(Some(state))
    }

    /// 保存状态到磁盘
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let content =
            serde_json::to_string_pretty(self).context("Failed to serialize sync state")?;

        std::fs::write(path, &content)
            .with_context(|| format!("Failed to write state file: {:?}", path))?;

        debug!(
            "Saved state for '{}': {} files",
            self.pair_id,
            self.files.len()
        );
        Ok(())
    }

    /// 从当前文件系统增量更新状态
    ///
    /// 对每个文件:
    /// - 如果 mtime 未变 → 复用旧哈希 (快速路径)
    /// - 如果 mtime 已变 → 重新计算哈希
    /// - 如果是新文件 → 计算哈希并加入
    pub fn incremental_update(
        &mut self,
        root: &Path,
        exclude_patterns: &[String],
    ) -> Result<Vec<FileEntry>> {
        use crate::sync::SyncEngine;
        use walkdir::WalkDir;

        let mut entries = Vec::new();
        let root = root.canonicalize()?;
        let mut seen_paths = std::collections::HashSet::new();

        for entry in WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                let path = e.path();
                let file_name = path.file_name().and_then(|n| n.to_str());
                for pattern in exclude_patterns {
                    if let Ok(glob) = glob::Pattern::new(pattern) {
                        if glob.matches_path(path) || file_name.is_some_and(|n| glob.matches(n)) {
                            return false;
                        }
                    }
                }
                if let Some(name) = file_name {
                    if name.starts_with('.') && name != "." {
                        return false;
                    }
                }
                true
            })
        {
            let entry = entry?;
            let path = entry.path();

            if path == root {
                continue;
            }

            let relative_path = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .to_string();

            seen_paths.insert(relative_path.clone());

            let metadata = entry.metadata()?;
            let is_dir = metadata.is_dir();

            if is_dir {
                entries.push(FileEntry {
                    relative_path,
                    size: 0,
                    modified: Utc::now(),
                    hash: Hash::from([0; 32]),
                    is_dir: true,
                    mode: 0,
                });
                continue;
            }

            let current_mtime = metadata
                .modified()
                .ok()
                .and_then(|t| {
                    chrono::DateTime::from_timestamp(
                        t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64,
                        0,
                    )
                })
                .unwrap_or_else(Utc::now);

            let current_size = metadata.len();

            // 检查是否可以复用旧哈希 (快速路径)
            let hash = if let Some(old_entry) = self.files.get(&relative_path) {
                if old_entry.size == current_size && old_entry.modified == current_mtime {
                    // mtime 和 size 都没变 → 复用旧哈希
                    old_entry.hash
                } else {
                    // 文件已修改 → 重新哈希
                    SyncEngine::compute_file_hash(path).unwrap_or(Hash::from([0; 32]))
                }
            } else {
                // 新文件 → 计算哈希
                SyncEngine::compute_file_hash(path).unwrap_or(Hash::from([0; 32]))
            };

            // 更新状态
            self.files.insert(
                relative_path.clone(),
                FileStateEntry {
                    size: current_size,
                    modified: current_mtime,
                    hash,
                },
            );

            entries.push(FileEntry {
                relative_path,
                size: current_size,
                modified: current_mtime,
                hash,
                is_dir: false,
                mode: 0,
            });
        }

        // 清理已删除的文件
        self.files.retain(|k, _| seen_paths.contains(k));
        self.last_sync = Utc::now();

        Ok(entries)
    }

    /// 获取默认状态文件路径
    pub fn default_path(pair_id: &str) -> PathBuf {
        let base = crate::config::CrossBagConfig::default_path();
        base.parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".crossbag")
            .join(format!("state_{}.json", pair_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("crossbag_state_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_file(dir: &Path, name: &str, content: &[u8]) {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content).unwrap();
    }

    #[test]
    fn test_save_and_load() {
        let dir = temp_dir();
        let state_file = dir.join("state.json");

        let mut state = SyncState::new("test-pair");
        state.files.insert(
            "test.txt".into(),
            FileStateEntry {
                size: 100,
                modified: Utc::now(),
                hash: blake3::hash(b"hello"),
            },
        );

        state.save(&state_file).unwrap();
        assert!(state_file.exists());

        let loaded = SyncState::load(&state_file).unwrap().unwrap();
        assert_eq!(loaded.pair_id, "test-pair");
        assert_eq!(loaded.files.len(), 1);
        assert!(loaded.files.contains_key("test.txt"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_nonexistent() {
        let result = SyncState::load(Path::new("/nonexistent/state.json")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_incremental_update_new_files() {
        let dir = temp_dir();
        write_file(&dir, "a.txt", b"hello");
        write_file(&dir, "b.txt", b"world");

        let mut state = SyncState::new("incr-test");
        let entries = state.incremental_update(&dir, &[]).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(state.files.len(), 2);
        assert_ne!(state.files.get("a.txt").unwrap().hash, Hash::from([0; 32]));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_incremental_reuse_hash() {
        let dir = temp_dir();
        write_file(&dir, "unchanged.txt", b"static content");

        // 第一次扫描
        let mut state = SyncState::new("reuse-test");
        state.incremental_update(&dir, &[]).unwrap();
        let first_hash = state.files.get("unchanged.txt").unwrap().hash;

        // 第二次扫描 (无修改)
        state.incremental_update(&dir, &[]).unwrap();
        let second_hash = state.files.get("unchanged.txt").unwrap().hash;

        assert_eq!(
            first_hash, second_hash,
            "Hash should be reused when file unchanged"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cleanup_deleted_files() {
        let dir = temp_dir();
        write_file(&dir, "keep.txt", b"keep");
        write_file(&dir, "remove.txt", b"remove");

        let mut state = SyncState::new("cleanup-test");
        state.incremental_update(&dir, &[]).unwrap();
        assert_eq!(state.files.len(), 2);

        // 删除 remove.txt
        std::fs::remove_file(dir.join("remove.txt")).unwrap();

        state.incremental_update(&dir, &[]).unwrap();
        assert_eq!(state.files.len(), 1);
        assert!(state.files.contains_key("keep.txt"));
        assert!(!state.files.contains_key("remove.txt"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
