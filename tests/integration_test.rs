//! CrossBag 集成测试
//!
//! 模拟双机同步全流程，使用临时目录代替真实节点。
//! 覆盖场景: 初始同步、增量同步、冲突检测、文件删除。

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// 测试辅助: 创建临时目录
fn temp_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("crossbag_it_{}_{}", prefix, uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 测试辅助: 写文件
fn write_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(content).unwrap();
    path
}

/// 测试辅助: 递归比较两个目录的文件内容是否一致
fn dirs_are_identical(a: &Path, b: &Path, exclude: &[&str]) -> bool {
    let index_a = crossbag::sync::SyncEngine::build_file_index(a, &[]).unwrap_or_default();
    let index_b = crossbag::sync::SyncEngine::build_file_index(b, &[]).unwrap_or_default();

    let a_paths: HashMap<String, _> = index_a
        .values()
        .filter(|e| !e.is_dir)
        .map(|e| (e.relative_path.clone(), e.hash))
        .collect();

    let b_paths: HashMap<String, _> = index_b
        .values()
        .filter(|e| !e.is_dir)
        .map(|e| (e.relative_path.clone(), e.hash))
        .collect();

    for (path, hash) in &a_paths {
        if exclude.iter().any(|p| path.contains(p)) {
            continue;
        }
        match b_paths.get(path) {
            Some(b_hash) if b_hash == hash => {}
            _ => return false,
        }
    }

    for (path, hash) in &b_paths {
        if exclude.iter().any(|p| path.contains(p)) {
            continue;
        }
        match a_paths.get(path) {
            Some(a_hash) if a_hash == hash => {}
            _ => return false,
        }
    }

    true
}

// ============================================================
// 场景 1: 初始全量同步
// ============================================================
#[test]
fn test_initial_full_sync() {
    let node_a = temp_dir("node_a");
    let node_b = temp_dir("node_b");

    // 在 node_a 创建文件
    write_file(&node_a, "readme.md", b"# CrossBag Docs");
    write_file(&node_a, "config.toml", b"[core]\nversion = \"1.0\"");
    write_file(
        &node_a,
        "src/main.rs",
        b"fn main() { println!(\"hello\"); }",
    );

    // 构建两边的文件索引
    let index_a = crossbag::sync::SyncEngine::build_file_index(&node_a, &[]).unwrap();
    let index_b = crossbag::sync::SyncEngine::build_file_index(&node_b, &[]).unwrap();

    // 差异检测: node_a 有的 node_b 都应该没有
    let (local_only, remote_only) = crossbag::sync::SyncEngine::diff_indexes(&index_a, &index_b);

    assert!(!local_only.is_empty(), "node_a should have new files");
    assert!(remote_only.is_empty(), "node_b should be empty");

    // 模拟同步: 把 local_only 文件 "传输" 到 node_b
    for entry in &local_only {
        let src = node_a.join(&entry.relative_path);
        let dst = node_b.join(&entry.relative_path);

        if entry.is_dir {
            std::fs::create_dir_all(&dst).unwrap();
        } else {
            // 读取源文件
            let content = std::fs::read(&src).unwrap();
            // 写入目标
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&dst, &content).unwrap();
        }
    }

    // 验证: node_b 现在应该包含 node_a 的所有文件
    assert!(
        dirs_are_identical(&node_a, &node_b, &[]),
        "After sync, dirs should be identical"
    );

    // 清理
    let _ = std::fs::remove_dir_all(&node_a);
    let _ = std::fs::remove_dir_all(&node_b);
}

// ============================================================
// 场景 2: 增量同步 - 文件修改
// ============================================================
#[test]
fn test_incremental_sync_modified_file() {
    let node_a = temp_dir("node_a_mod");
    let node_b = temp_dir("node_b_mod");

    // 初始状态: 两边都有相同文件
    write_file(&node_a, "data.txt", b"original content");
    write_file(&node_b, "data.txt", b"original content");

    // node_a 修改文件 (确保时间戳足够不同)
    std::thread::sleep(std::time::Duration::from_secs(2));
    write_file(&node_a, "data.txt", b"modified content on node A");

    // 构建索引 + 差异检测
    let index_a = crossbag::sync::SyncEngine::build_file_index(&node_a, &[]).unwrap();
    let index_b = crossbag::sync::SyncEngine::build_file_index(&node_b, &[]).unwrap();
    let (local_only, _remote_only) = crossbag::sync::SyncEngine::diff_indexes(&index_a, &index_b);

    // node_a 的 data.txt 应该出现在 local_only (因为修改时间更新)
    assert!(
        local_only.iter().any(|e| e.relative_path == "data.txt"),
        "Modified file should appear in local_only"
    );

    // 模拟增量同步
    for entry in &local_only {
        let src = node_a.join(&entry.relative_path);
        let dst = node_b.join(&entry.relative_path);
        let content = std::fs::read(&src).unwrap();
        std::fs::write(&dst, &content).unwrap();
    }

    // 验证一致
    assert!(dirs_are_identical(&node_a, &node_b, &[]));

    let _ = std::fs::remove_dir_all(&node_a);
    let _ = std::fs::remove_dir_all(&node_b);
}

// ============================================================
// 场景 3: 新增文件 + 嵌套目录
// ============================================================
#[test]
fn test_sync_nested_directories() {
    let node_a = temp_dir("node_a_nest");
    let node_b = temp_dir("node_b_nest");

    write_file(&node_a, "src/main.rs", b"fn main() {}");
    write_file(
        &node_a,
        "src/lib.rs",
        b"pub fn add(a: i32, b: i32) -> i32 { a + b }",
    );
    write_file(&node_a, "src/utils/helpers.rs", b"pub fn greet() {}");
    write_file(&node_a, "tests/test_main.rs", b"#[test]\nfn test() {}");
    write_file(&node_a, "Cargo.toml", b"[package]\nname = \"test\"");
    write_file(&node_a, ".gitignore", b"target/");

    let index_a = crossbag::sync::SyncEngine::build_file_index(&node_a, &[]).unwrap();
    let index_b = crossbag::sync::SyncEngine::build_file_index(&node_b, &[]).unwrap();
    let (local_only, _remote_only) = crossbag::sync::SyncEngine::diff_indexes(&index_a, &index_b);

    // 同步所有文件
    for entry in &local_only {
        let src = node_a.join(&entry.relative_path);
        let dst = node_b.join(&entry.relative_path);

        if entry.is_dir {
            std::fs::create_dir_all(&dst).unwrap();
        } else if src.exists() {
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::copy(&src, &dst).unwrap();
        }
    }

    // 验证所有文件都存在且相同
    let index_b_after = crossbag::sync::SyncEngine::build_file_index(&node_b, &[]).unwrap();
    let b_files: Vec<&str> = index_b_after
        .values()
        .filter(|e| !e.is_dir)
        .map(|e| e.relative_path.as_str())
        .collect();

    assert!(b_files
        .iter()
        .any(|p| p.contains("main.rs") && p.contains("src")));
    assert!(b_files
        .iter()
        .any(|p| p.contains("lib.rs") && p.contains("src")));
    assert!(b_files.iter().any(|p| p.contains("helpers.rs")));
    assert!(b_files.iter().any(|p| p.contains("test_main.rs")));
    assert!(b_files.iter().any(|p| p.contains("Cargo.toml")));

    assert!(dirs_are_identical(&node_a, &node_b, &[]));

    let _ = std::fs::remove_dir_all(&node_a);
    let _ = std::fs::remove_dir_all(&node_b);
}

// ============================================================
// 场景 4: 文件删除检测
// ============================================================
#[test]
fn test_delete_detection() {
    let node_a = temp_dir("node_a_del");
    let node_b = temp_dir("node_b_del");

    // 初始: 两边都有 a.txt 和 b.txt
    write_file(&node_a, "a.txt", b"file a");
    write_file(&node_a, "b.txt", b"file b");
    write_file(&node_b, "a.txt", b"file a");
    write_file(&node_b, "b.txt", b"file b");

    // node_a 删除 b.txt
    std::fs::remove_file(node_a.join("b.txt")).unwrap();

    let index_a = crossbag::sync::SyncEngine::build_file_index(&node_a, &[]).unwrap();
    let index_b = crossbag::sync::SyncEngine::build_file_index(&node_b, &[]).unwrap();
    let (_local_only, remote_only) = crossbag::sync::SyncEngine::diff_indexes(&index_a, &index_b);

    // node_b 仍然有 b.txt → 应在 remote_only 中 (对方有、本地没有)
    assert!(
        remote_only.iter().any(|e| e.relative_path == "b.txt"),
        "Deleted file should appear in remote_only (remote still has it)"
    );

    let _ = std::fs::remove_dir_all(&node_a);
    let _ = std::fs::remove_dir_all(&node_b);
}

// ============================================================
// 场景 5: 大文件分块传输模拟
// ============================================================
#[test]
fn test_large_file_chunk_transfer() {
    let dir = temp_dir("chunk");

    // 创建 2MB 的文件
    let data = vec![0xABu8; 2 * 1024 * 1024];
    let file = write_file(&dir, "large.bin", &data);

    // 分块读取
    let chunks = crossbag::sync::SyncEngine::read_file_chunks(&file, 65536).unwrap();
    let expected_chunks = (data.len() as f64 / 65536.0).ceil() as usize;
    assert_eq!(chunks.len(), expected_chunks);

    // 分块写入
    let out = dir.join("large_out.bin");
    crossbag::sync::SyncEngine::write_file_chunks(&out, &chunks).unwrap();

    // 验证完整性
    let original_hash = crossbag::sync::SyncEngine::compute_file_hash(&file).unwrap();
    let restored_hash = crossbag::sync::SyncEngine::compute_file_hash(&out).unwrap();
    assert_eq!(original_hash, restored_hash);

    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================
// 场景 6: 双向同步冲突检测
// ============================================================
#[test]
fn test_bidirectional_conflict_detection() {
    let node_a = temp_dir("node_a_conflict");
    let node_b = temp_dir("node_b_conflict");

    // 初始相同
    write_file(&node_a, "shared.txt", b"version 1");
    write_file(&node_b, "shared.txt", b"version 1");

    std::thread::sleep(std::time::Duration::from_millis(100));

    // 两边同时修改 (不同的内容)
    write_file(&node_a, "shared.txt", b"modified by A");
    std::thread::sleep(std::time::Duration::from_millis(100));
    write_file(&node_b, "shared.txt", b"modified by B");

    let index_a = crossbag::sync::SyncEngine::build_file_index(&node_a, &[]).unwrap();
    let index_b = crossbag::sync::SyncEngine::build_file_index(&node_b, &[]).unwrap();
    let (local_only, remote_only) = crossbag::sync::SyncEngine::diff_indexes(&index_a, &index_b);

    // 两边都认为自己的 shared.txt 需要同步
    // 至少有一边检测到了差异
    assert!(
        local_only.iter().any(|e| e.relative_path == "shared.txt")
            || remote_only.iter().any(|e| e.relative_path == "shared.txt"),
        "At least one side should detect the modification"
    );

    let _ = std::fs::remove_dir_all(&node_a);
    let _ = std::fs::remove_dir_all(&node_b);
}

// ============================================================
// 场景 7: 哈希校验完整流程
// ============================================================
#[test]
fn test_hash_verification_pipeline() {
    let dir = temp_dir("hashverify");

    // 创建 → 哈希 → 修改 → 重新哈希 → 验证差异
    let file = write_file(&dir, "verify.txt", b"original");

    let hash1 = crossbag::sync::SyncEngine::compute_file_hash(&file).unwrap();
    assert!(crossbag::sync::SyncEngine::verify_file(&file, &hash1).unwrap());

    // 修改文件
    std::thread::sleep(std::time::Duration::from_millis(50));
    std::fs::write(&file, b"modified").unwrap();

    // 旧哈希应该验证失败
    assert!(!crossbag::sync::SyncEngine::verify_file(&file, &hash1).unwrap());

    // 新哈希应该匹配
    let hash2 = crossbag::sync::SyncEngine::compute_file_hash(&file).unwrap();
    assert_ne!(hash1, hash2);
    assert!(crossbag::sync::SyncEngine::verify_file(&file, &hash2).unwrap());

    let _ = std::fs::remove_dir_all(&dir);
}

// ============================================================
// 场景 8: 协议消息编解码往返 (集成层面)
// ============================================================
#[test]
fn test_protocol_message_pipeline() {
    use crossbag::protocol::*;

    // 构建完整的 FileIndex 消息
    let files: Vec<FileEntry> = (0..100)
        .map(|i| FileEntry {
            relative_path: format!("dir/file_{}.txt", i),
            size: (i * 1024) as u64,
            modified: chrono::Utc::now(),
            hash: blake3::hash(format!("content_{}", i).as_bytes()),
            is_dir: false,
            mode: 0,
        })
        .collect();

    let msg = Message::FileIndex(FileIndex {
        pair_id: "integration-test".into(),
        files,
        timestamp: chrono::Utc::now(),
    });

    // 编码
    let encoded = msg.to_bytes().unwrap();
    assert!(encoded.len() > 100, "Encoded message should be substantial");

    // 解码
    let decoded = Message::from_bytes(&encoded).unwrap();
    match decoded {
        Message::FileIndex(index) => {
            assert_eq!(index.pair_id, "integration-test");
            assert_eq!(index.files.len(), 100);
        }
        _ => panic!("Wrong message type"),
    }
}
