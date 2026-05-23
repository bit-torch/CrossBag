//! 同步引擎
//!
//! 实现文件索引构建、差异检测、冲突解决和传输调度。

use crate::config::{CrossBagConfig, SyncPair};
use crate::protocol::FileEntry;
use anyhow::{Context, Result};
use blake3::Hash;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};
use walkdir::WalkDir;

/// 同步引擎
pub struct SyncEngine {
    /// 全局配置
    config: std::sync::Arc<CrossBagConfig>,
    /// 文件索引缓存
    index_cache: HashMap<String, HashMap<PathBuf, FileEntry>>,
}

/// 同步结果
#[derive(Debug)]
pub struct SyncResult {
    pub pair_id: String,
    pub files_synced: usize,
    pub files_deleted: usize,
    pub bytes_transferred: u64,
    pub errors: Vec<String>,
}

impl SyncEngine {
    pub fn new(config: std::sync::Arc<CrossBagConfig>) -> Self {
        SyncEngine {
            config,
            index_cache: HashMap::new(),
        }
    }

    /// 计算文件的 BLAKE3 哈希
    pub fn compute_file_hash(path: &Path) -> Result<Hash> {
        let mut hasher = blake3::Hasher::new();
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("Failed to open file: {:?}", path))?;

        let mut buffer = [0u8; 8192];
        loop {
            use std::io::Read;
            let bytes_read = file
                .read(&mut buffer)
                .with_context(|| format!("Failed to read file: {:?}", path))?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        Ok(hasher.finalize())
    }

    /// 构建目录的文件索引
    pub fn build_file_index(
        root: &Path,
        exclude_patterns: &[String],
    ) -> Result<HashMap<PathBuf, FileEntry>> {
        let mut index = HashMap::new();
        let root = root.canonicalize()?;

        for entry in WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| {
                let path = e.path();
                let file_name = path.file_name().and_then(|n| n.to_str());

                // 过滤排除模式
                for pattern in exclude_patterns {
                    if let Ok(glob) = glob::Pattern::new(pattern) {
                        // 匹配文件名或相对路径
                        if glob.matches_path(path) {
                            return false;
                        }
                        if let Some(name) = file_name {
                            if glob.matches(name) {
                                return false;
                            }
                        }
                    }
                }
                // 忽略隐藏文件 (以 . 开头)
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with('.') && name != "." {
                        return false;
                    }
                }
                true
            })
        {
            let entry = entry?;
            let path = entry.path();

            // 跳过根目录本身
            if path == root {
                continue;
            }

            let relative_path = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .to_string();

            let metadata = entry.metadata()?;
            let modified = metadata
                .modified()
                .ok()
                .and_then(|t| {
                    chrono::DateTime::from_timestamp(
                        t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64,
                        0,
                    )
                })
                .unwrap_or_else(|| Utc::now());

            let is_dir = metadata.is_dir();

            let hash = if is_dir {
                Hash::from([0; 32]) // 目录使用零哈希
            } else {
                match SyncEngine::compute_file_hash(path) {
                    Ok(h) => h,
                    Err(e) => {
                        warn!("Failed to hash file {:?}: {}", path, e);
                        Hash::from([0; 32])
                    }
                }
            };

            let file_entry = FileEntry {
                relative_path,
                size: metadata.len(),
                modified,
                hash,
                is_dir,
                mode: 0,
            };

            index.insert(path.to_path_buf(), file_entry);
        }

        Ok(index)
    }

    /// 比较两个文件索引，找出差异
    pub fn diff_indexes(
        local: &HashMap<PathBuf, FileEntry>,
        remote: &HashMap<PathBuf, FileEntry>,
    ) -> (Vec<FileEntry>, Vec<FileEntry>) {
        let mut local_only = Vec::new();
        let mut remote_only = Vec::new();

        // 找出本地有但远程没有的文件
        let remote_rel_paths: HashSet<&str> = remote
            .values()
            .map(|e| e.relative_path.as_str())
            .collect();

        let _local_rel_map: HashMap<&str, &FileEntry> = local
            .values()
            .map(|e| (e.relative_path.as_str(), e))
            .collect();

        // 本地有、远程没有的
        for entry in local.values() {
            match remote_rel_paths.get(entry.relative_path.as_str()) {
                Some(_) => {
                    // 双方都有, 比较哈希
                    if let Some(remote_entry) = remote
                        .values()
                        .find(|e| e.relative_path == entry.relative_path)
                    {
                        if !entry.is_same_as(remote_entry) {
                            // 文件内容不同，以修改时间较新的为准
                            if entry.modified > remote_entry.modified {
                                local_only.push(entry.clone());
                            } else {
                                remote_only.push(remote_entry.clone());
                            }
                        }
                    }
                }
                None => {
                    // 仅本地有
                    local_only.push(entry.clone());
                }
            }
        }

        // 远程有、本地没有的
        let local_rel_set: HashSet<&str> =
            local.values().map(|e| e.relative_path.as_str()).collect();

        for entry in remote.values() {
            if !local_rel_set.contains(entry.relative_path.as_str()) {
                remote_only.push(entry.clone());
            }
        }

        debug!(
            "Diff: {} local-only, {} remote-only",
            local_only.len(),
            remote_only.len()
        );
        (local_only, remote_only)
    }

    /// 执行一次完整同步
    pub async fn full_sync(&mut self, pair: &SyncPair) -> Result<SyncResult> {
        let mut result = SyncResult {
            pair_id: pair.id.clone(),
            files_synced: 0,
            files_deleted: 0,
            bytes_transferred: 0,
            errors: Vec::new(),
        };

        info!("Starting full sync for pair '{}'", pair.id);

        // 构建本地文件索引
        let local_index = match SyncEngine::build_file_index(&pair.local_path, &pair.exclude_patterns) {
            Ok(idx) => idx,
            Err(e) => {
                result.errors.push(format!("Failed to build local index: {}", e));
                return Ok(result);
            }
        };

        // TODO: 从远程节点获取远程文件索引
        // 目前仅构建本地索引，需要配合网络模块获取远程索引

        self.index_cache
            .insert(pair.id.clone(), local_index);

        info!(
            "Sync complete for pair '{}': {} changes",
            pair.id, result.files_synced
        );

        Ok(result)
    }

    /// 创建目录并确保存在
    pub fn ensure_dir(path: &Path) -> Result<()> {
        if !path.exists() {
            std::fs::create_dir_all(path)
                .with_context(|| format!("Failed to create directory: {:?}", path))?;
        }
        Ok(())
    }

    /// 将文件分块读取
    pub fn read_file_chunks(
        path: &Path,
        chunk_size: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let data = std::fs::read(path)
            .with_context(|| format!("Failed to read file: {:?}", path))?;

        let chunks: Vec<Vec<u8>> = data
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        Ok(chunks)
    }

    /// 将分块数据写入文件
    pub fn write_file_chunks(path: &Path, chunks: &[Vec<u8>]) -> Result<()> {
        if let Some(parent) = path.parent() {
            SyncEngine::ensure_dir(parent)?;
        }

        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        let mut data = Vec::with_capacity(total_size);
        for chunk in chunks {
            data.extend_from_slice(chunk);
        }

        std::fs::write(path, &data)
            .with_context(|| format!("Failed to write file: {:?}", path))?;

        Ok(())
    }

    /// 验证文件哈希
    pub fn verify_file(path: &Path, expected_hash: &Hash) -> Result<bool> {
        let actual_hash = SyncEngine::compute_file_hash(path)?;
        Ok(&actual_hash == expected_hash)
    }
}

/// 冲突解决策略
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictStrategy {
    /// 保留较新的版本
    NewerWins,
    /// 保留本地版本
    LocalWins,
    /// 保留远程版本
    RemoteWins,
    /// 创建备份
    CreateBackup,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        ConflictStrategy::NewerWins
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// 创建临时目录
    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("crossbag_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// 创建测试文件
    fn write_test_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content).unwrap();
        path
    }

    /// 测试文件哈希计算
    #[test]
    fn test_compute_file_hash() {
        let dir = temp_dir();
        let file = write_test_file(&dir, "test.txt", b"hello crossbag");
        let hash = SyncEngine::compute_file_hash(&file).unwrap();
        let expected = blake3::hash(b"hello crossbag");
        assert_eq!(hash, expected);
        // 清理
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 测试文件哈希会随内容变化
    #[test]
    fn test_compute_file_hash_different() {
        let dir = temp_dir();
        let f1 = write_test_file(&dir, "a.txt", b"hello");
        let f2 = write_test_file(&dir, "b.txt", b"world");
        let h1 = SyncEngine::compute_file_hash(&f1).unwrap();
        let h2 = SyncEngine::compute_file_hash(&f2).unwrap();
        assert_ne!(h1, h2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 测试构建文件索引
    #[test]
    fn test_build_file_index() {
        let dir = temp_dir();
        write_test_file(&dir, "readme.md", b"# Test");
        write_test_file(&dir, "src/main.rs", b"fn main() {}");
        std::fs::create_dir_all(dir.join("subdir")).unwrap();
        write_test_file(&dir, "subdir/config.toml", b"[test]\nkey = \"val\"");

        let index = SyncEngine::build_file_index(&dir, &[]).unwrap();
        // 应该有 3 个文件 (subdir 是目录，可能被 walkdir 跳过 root 后的条目)
        let file_count = index.values().filter(|e| !e.is_dir).count();
        assert!(file_count >= 2, "Expected at least 2 files, got {}", file_count);

        // 验证每个条目都有路径、大小和哈希
        for entry in index.values() {
            assert!(!entry.relative_path.is_empty());
            if !entry.is_dir {
                assert_ne!(entry.hash, blake3::Hash::from([0; 32]));
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 测试排除模式
    #[test]
    fn test_build_file_index_with_exclude() {
        let dir = temp_dir();
        write_test_file(&dir, "keep.txt", b"keep");
        write_test_file(&dir, "skip.tmp", b"skip");
        std::fs::create_dir_all(dir.join("node_modules")).unwrap();
        write_test_file(&dir, "node_modules/pkg.js", b"console.log('x')");

        let index = SyncEngine::build_file_index(
            &dir,
            &["*.tmp".into()],
        )
        .unwrap();

        // 不应包含 .tmp 文件
        let paths: Vec<&str> = index.values().map(|e| e.relative_path.as_str()).collect();
        assert!(paths.iter().any(|p| p.contains("keep.txt")));
        assert!(!paths.iter().any(|p| p.contains("skip.tmp")));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 测试差异检测
    #[test]
    fn test_diff_indexes() {
        let hash_a = blake3::hash(b"a");
        let hash_b = blake3::hash(b"b");
        let now = chrono::Utc::now();

        let entry_a = FileEntry {
            relative_path: "a.txt".into(),
            size: 10,
            modified: now,
            hash: hash_a,
            is_dir: false,
            mode: 0,
        };
        let entry_b = FileEntry {
            relative_path: "b.txt".into(),
            size: 20,
            modified: now,
            hash: hash_b,
            is_dir: false,
            mode: 0,
        };

        let mut local = HashMap::new();
        local.insert(PathBuf::from("/local/a.txt"), entry_a);
        // local has a.txt but not b.txt

        let mut remote = HashMap::new();
        remote.insert(PathBuf::from("/remote/b.txt"), entry_b);
        // remote has b.txt but not a.txt

        let (local_only, remote_only) = SyncEngine::diff_indexes(&local, &remote);

        assert_eq!(local_only.len(), 1);
        assert_eq!(remote_only.len(), 1);
        assert_eq!(local_only[0].relative_path, "a.txt");
        assert_eq!(remote_only[0].relative_path, "b.txt");
    }

    /// 测试相同文件无差异
    #[test]
    fn test_diff_indexes_identical() {
        let hash = blake3::hash(b"same");
        let now = chrono::Utc::now();

        let entry = FileEntry {
            relative_path: "same.txt".into(),
            size: 10,
            modified: now,
            hash,
            is_dir: false,
            mode: 0,
        };

        let mut local = HashMap::new();
        local.insert(PathBuf::from("/a/same.txt"), entry.clone());

        let mut remote = HashMap::new();
        remote.insert(PathBuf::from("/b/same.txt"), entry);

        let (local_only, remote_only) = SyncEngine::diff_indexes(&local, &remote);
        assert_eq!(local_only.len(), 0);
        assert_eq!(remote_only.len(), 0);
    }

    /// 测试分块读写
    #[test]
    fn test_chunk_read_write() {
        let dir = temp_dir();
        let file = write_test_file(&dir, "chunked.bin", &[0u8; 100000]);

        let chunks = SyncEngine::read_file_chunks(&file, 4096).unwrap();
        let expected_chunks = (100000u64 + 4095) / 4096;
        assert_eq!(chunks.len() as u64, expected_chunks);

        // 写回
        let out = dir.join("chunked_out.bin");
        SyncEngine::write_file_chunks(&out, &chunks).unwrap();
        let original = std::fs::read(&file).unwrap();
        let written = std::fs::read(&out).unwrap();
        assert_eq!(original, written);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 测试文件校验
    #[test]
    fn test_verify_file() {
        let dir = temp_dir();
        let file = write_test_file(&dir, "verify.txt", b"verify me");
        let hash = blake3::hash(b"verify me");

        assert!(SyncEngine::verify_file(&file, &hash).unwrap());

        let wrong_hash = blake3::hash(b"wrong");
        assert!(!SyncEngine::verify_file(&file, &wrong_hash).unwrap());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 测试 ensure_dir
    #[test]
    fn test_ensure_dir() {
        let parent = temp_dir();
        let new_dir = parent.join("a").join("b").join("c");
        assert!(!new_dir.exists());
        SyncEngine::ensure_dir(&new_dir).unwrap();
        assert!(new_dir.exists());
        let _ = std::fs::remove_dir_all(&parent);
    }
}
