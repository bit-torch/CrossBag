//! CrossBag 同步协议定义
//!
//! 定义了节点间通信的二进制协议，包括握手、文件列表交换、
//! 文件请求和分块传输等消息类型。

use blake3::Hash;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 协议版本
pub const PROTOCOL_VERSION: u32 = 1;

/// 默认端口
pub const DEFAULT_PORT: u16 = 9527;

/// 文件块大小 (64KB)
pub const CHUNK_SIZE: usize = 64 * 1024;

/// 协议消息类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// 握手消息 - 节点首次连接时交换
    Handshake(Handshake),
    /// 握手确认
    HandshakeAck(HandshakeAck),
    /// 文件索引 - 发送本节点文件列表
    FileIndex(FileIndex),
    /// 文件索引确认 - 返回差异文件列表
    FileIndexAck(FileIndexAck),
    /// 文件请求 - 请求传输指定文件
    FileRequest(FileRequest),
    /// 文件响应 - 文件元数据确认
    FileResponse(FileResponse),
    /// 文件数据块
    FileChunk(FileChunk),
    /// 传输完成确认
    TransferComplete(TransferComplete),
    /// 心跳
    Heartbeat(Heartbeat),
    /// 心跳响应
    HeartbeatAck(HeartbeatAck),
    /// 错误消息
    Error(ErrorMessage),
}

/// 握手消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handshake {
    pub protocol_version: u32,
    pub node_id: Uuid,
    pub node_name: String,
    pub hostname: String,
}

/// 握手确认
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeAck {
    pub accepted: bool,
    pub node_id: Uuid,
    pub node_name: String,
    pub message: Option<String>,
}

/// 文件条目信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// 相对于同步根目录的路径
    pub relative_path: String,
    /// 文件大小 (字节)
    pub size: u64,
    /// 修改时间
    pub modified: DateTime<Utc>,
    /// BLAKE3 哈希值
    pub hash: Hash,
    /// 是否为目录
    pub is_dir: bool,
    /// 文件权限模式 (非 Unix 平台始终为 0)
    #[serde(default)]
    pub mode: u32,
}

/// 文件索引 - 发送本节点管理的文件列表
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileIndex {
    /// 同步对标识
    pub pair_id: String,
    /// 文件列表
    pub files: Vec<FileEntry>,
    /// 索引时间戳
    pub timestamp: DateTime<Utc>,
}

/// 文件索引确认 - 返回需要同步的文件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileIndexAck {
    pub pair_id: String,
    /// 本节点缺失或需要更新的文件路径列表
    pub needed_files: Vec<String>,
    /// 对方节点缺失的文件列表 (本节点可以提供)
    pub offered_files: Vec<String>,
}

/// 文件请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRequest {
    pub pair_id: String,
    /// 请求的文件路径
    pub files: Vec<String>,
}

/// 文件响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileResponse {
    pub relative_path: String,
    pub size: u64,
    pub hash: Hash,
    pub chunk_count: u32,
    /// 是否接受传输
    pub accepted: bool,
    pub error: Option<String>,
}

/// 文件数据块
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChunk {
    pub relative_path: String,
    /// 块序号 (从 0 开始)
    pub chunk_index: u32,
    /// 总块数
    pub total_chunks: u32,
    /// 块数据
    pub data: Vec<u8>,
    /// 块哈希 (用于校验)
    pub chunk_hash: Hash,
}

/// 传输完成
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferComplete {
    pub relative_path: String,
    pub success: bool,
    pub error: Option<String>,
}

/// 心跳消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub node_id: Uuid,
    pub timestamp: DateTime<Utc>,
}

/// 心跳响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatAck {
    pub node_id: Uuid,
    pub timestamp: DateTime<Utc>,
}

/// 错误消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub code: u32,
    pub message: String,
}

impl Message {
    /// 序列化消息为字节
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// 从字节反序列化消息
    pub fn from_bytes(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

impl FileEntry {
    /// 判断两个文件条目是否相同 (相同哈希即视为相同)
    pub fn is_same_as(&self, other: &FileEntry) -> bool {
        self.hash == other.hash
    }
}

/// 错误码定义
pub mod error_codes {
    pub const VERSION_MISMATCH: u32 = 1001;
    pub const AUTH_FAILED: u32 = 1002;
    pub const FILE_NOT_FOUND: u32 = 2001;
    pub const PERMISSION_DENIED: u32 = 2002;
    pub const TRANSFER_FAILED: u32 = 3001;
    pub const CHECKSUM_MISMATCH: u32 = 3002;
    pub const NODE_UNREACHABLE: u32 = 4001;
    pub const TIMEOUT: u32 = 4002;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// 测试 Handshake 序列化往返
    #[test]
    fn test_handshake_roundtrip() {
        let original = Message::Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            node_id: Uuid::new_v4(),
            node_name: "test-node".into(),
            hostname: "test-host".into(),
        });

        let bytes = original.to_bytes().expect("serialize");
        let decoded = Message::from_bytes(&bytes).expect("deserialize");

        match decoded {
            Message::Handshake(h) => {
                assert_eq!(h.protocol_version, PROTOCOL_VERSION);
                assert_eq!(h.node_name, "test-node");
                assert_eq!(h.hostname, "test-host");
            }
            _ => panic!("Expected Handshake"),
        }
    }

    /// 测试所有消息类型的序列化往返
    #[test]
    fn test_all_message_types_roundtrip() {
        let messages = vec![
            Message::Handshake(Handshake {
                protocol_version: 1,
                node_id: Uuid::new_v4(),
                node_name: "n1".into(),
                hostname: "h1".into(),
            }),
            Message::HandshakeAck(HandshakeAck {
                accepted: true,
                node_id: Uuid::new_v4(),
                node_name: "n2".into(),
                message: None,
            }),
            Message::Heartbeat(Heartbeat {
                node_id: Uuid::new_v4(),
                timestamp: Utc::now(),
            }),
            Message::HeartbeatAck(HeartbeatAck {
                node_id: Uuid::new_v4(),
                timestamp: Utc::now(),
            }),
            Message::Error(ErrorMessage {
                code: error_codes::TIMEOUT,
                message: "test error".into(),
            }),
        ];

        for msg in messages {
            let bytes = msg.to_bytes().expect("serialize");
            let decoded = Message::from_bytes(&bytes).expect("deserialize");
            assert_eq!(
                std::mem::discriminant(&msg),
                std::mem::discriminant(&decoded),
                "Message type mismatch after roundtrip"
            );
        }
    }

    /// 测试 FileEntry 相等比较
    #[test]
    fn test_file_entry_equality() {
        let hash1 = blake3::hash(b"hello");
        let hash2 = blake3::hash(b"world");

        let entry1 = FileEntry {
            relative_path: "/test/file.txt".into(),
            size: 100,
            modified: Utc::now(),
            hash: hash1,
            is_dir: false,
            mode: 0,
        };

        let entry2 = FileEntry {
            relative_path: "/test/file.txt".into(),
            size: 100,
            modified: Utc::now(),
            hash: hash1,
            is_dir: false,
            mode: 0,
        };

        let entry3 = FileEntry {
            relative_path: "/test/file.txt".into(),
            size: 200,
            modified: Utc::now(),
            hash: hash2,
            is_dir: false,
            mode: 0,
        };

        assert!(entry1.is_same_as(&entry2));
        assert!(!entry1.is_same_as(&entry3));
    }

    /// 测试 FileIndex 消息序列化
    #[test]
    fn test_file_index_serialization() {
        let entry = FileEntry {
            relative_path: "docs/readme.md".into(),
            size: 1024,
            modified: Utc::now(),
            hash: blake3::hash(b"content"),
            is_dir: false,
            mode: 0,
        };

        let index = Message::FileIndex(FileIndex {
            pair_id: "sync-1".into(),
            files: vec![entry],
            timestamp: Utc::now(),
        });

        let bytes = index.to_bytes().expect("serialize");
        let decoded = Message::from_bytes(&bytes).expect("deserialize");

        match decoded {
            Message::FileIndex(idx) => {
                assert_eq!(idx.pair_id, "sync-1");
                assert_eq!(idx.files.len(), 1);
                assert_eq!(idx.files[0].relative_path, "docs/readme.md");
                assert_eq!(idx.files[0].size, 1024);
            }
            _ => panic!("Expected FileIndex"),
        }
    }

    /// 测试 FileChunk 消息
    #[test]
    fn test_file_chunk_roundtrip() {
        let data = vec![0u8; CHUNK_SIZE];
        let chunk = Message::FileChunk(FileChunk {
            relative_path: "bigfile.bin".into(),
            chunk_index: 0,
            total_chunks: 4,
            data: data.clone(),
            chunk_hash: blake3::hash(&data),
        });

        let bytes = chunk.to_bytes().expect("serialize");
        let decoded = Message::from_bytes(&bytes).expect("deserialize");

        match decoded {
            Message::FileChunk(c) => {
                assert_eq!(c.relative_path, "bigfile.bin");
                assert_eq!(c.chunk_index, 0);
                assert_eq!(c.total_chunks, 4);
                assert_eq!(c.data.len(), CHUNK_SIZE);
            }
            _ => panic!("Expected FileChunk"),
        }
    }

    /// 测试大消息序列化
    #[test]
    fn test_large_message() {
        let mut files = Vec::new();
        for i in 0..500 {
            files.push(FileEntry {
                relative_path: format!("dir/file_{}.txt", i),
                size: (i * 100) as u64,
                modified: Utc::now(),
                hash: blake3::hash(format!("file{}", i).as_bytes()),
                is_dir: false,
                mode: 0,
            });
        }

        let index = Message::FileIndex(FileIndex {
            pair_id: "large-sync".into(),
            files,
            timestamp: Utc::now(),
        });

        let bytes = index.to_bytes().expect("serialize large index");
        let decoded = Message::from_bytes(&bytes).expect("deserialize large index");

        match decoded {
            Message::FileIndex(idx) => {
                assert_eq!(idx.files.len(), 500);
            }
            _ => panic!("Expected FileIndex"),
        }
    }
}
