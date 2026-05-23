//! CrossBag 配置管理
//!
//! 支持 TOML 格式的配置文件，管理同步对、网络设置和节点信息。

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// 默认配置文件名
pub const DEFAULT_CONFIG_FILE: &str = "crossbag.toml";

/// 顶层配置结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossBagConfig {
    /// 节点信息
    pub node: NodeConfig,
    /// 网络设置
    pub network: NetworkConfig,
    /// Easytier 子进程配置
    #[serde(default)]
    pub easytier: EasytierConfig,
    /// 同步对列表
    pub sync_pairs: Vec<SyncPair>,
    /// 高级设置
    #[serde(default)]
    pub advanced: AdvancedConfig,
}

/// 节点配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// 节点唯一标识 (首次运行时自动生成)
    #[serde(default = "Uuid::new_v4")]
    pub node_id: Uuid,
    /// 节点名称
    #[serde(default = "default_node_name")]
    pub name: String,
    /// 监听地址
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    /// 监听端口
    #[serde(default = "default_port")]
    pub port: u16,
}

/// 网络配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// 对等节点列表 (Easytier 虚拟 IP 地址)
    #[serde(default)]
    pub peers: HashMap<String, PeerConfig>,
    /// 连接超时 (秒)
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
    /// 心跳间隔 (秒)
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
    /// 传输超时 (秒)
    #[serde(default = "default_transfer_timeout")]
    pub transfer_timeout: u64,
}

/// 对等节点配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    /// 节点名称 (可读标识)
    pub name: String,
    /// Easytier 虚拟 IP + 端口
    pub address: String,
}

/// Easytier 子进程配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EasytierConfig {
    /// Easytier 二进制文件路径 (为空则自动搜索 PATH)
    #[serde(default = "default_easytier_binary")]
    pub binary_path: String,
    /// 网络名称 (所有节点必须一致)
    #[serde(default = "default_easytier_network_name")]
    pub network_name: String,
    /// 网络密钥 (所有节点必须一致)
    #[serde(default)]
    pub network_secret: String,
    /// 实例名称
    #[serde(default = "default_node_name")]
    pub instance_name: String,
    /// 监听地址列表
    #[serde(default = "default_easytier_listeners")]
    pub listeners: Vec<String>,
    /// 是否随 CrossBag 自动启动
    #[serde(default = "default_enabled")]
    pub auto_start: bool,
    /// 健康检查间隔 (秒)
    #[serde(default = "default_easytier_health_interval")]
    pub health_check_interval: u64,
    /// 崩溃后最大重启次数 (0 表示不限制)
    #[serde(default)]
    pub max_restarts: u32,
    /// 启动等待超时 (秒)
    #[serde(default = "default_easytier_startup_timeout")]
    pub startup_timeout: u64,
}

impl Default for EasytierConfig {
    fn default() -> Self {
        EasytierConfig {
            binary_path: default_easytier_binary(),
            network_name: default_easytier_network_name(),
            network_secret: String::new(),
            instance_name: default_node_name(),
            listeners: default_easytier_listeners(),
            auto_start: true,
            health_check_interval: default_easytier_health_interval(),
            max_restarts: 5,
            startup_timeout: default_easytier_startup_timeout(),
        }
    }
}

fn default_easytier_binary() -> String {
    "easytier-core".to_string()
}

fn default_easytier_network_name() -> String {
    "crossbag-network".to_string()
}

fn default_easytier_listeners() -> Vec<String> {
    vec![
        "tcp://0.0.0.0:11010".to_string(),
        "udp://0.0.0.0:11010".to_string(),
    ]
}

fn default_easytier_health_interval() -> u64 {
    10
}

fn default_easytier_startup_timeout() -> u64 {
    30
}

/// 同步对 - 定义一组需要同步的目录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPair {
    /// 同步对唯一标识
    pub id: String,
    /// 本地目录路径 (绝对路径)
    pub local_path: PathBuf,
    /// 远程节点 ID (对应 NetworkConfig.peers 的 key)
    pub remote_node: String,
    /// 远程目录路径 (绝对路径)
    pub remote_path: PathBuf,
    /// 同步方向
    #[serde(default)]
    pub direction: SyncDirection,
    /// 排除模式 (glob 格式)
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    /// 是否启用
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// 是否监控实时变更
    #[serde(default = "default_enabled")]
    pub watch: bool,
    /// 定期全量同步间隔 (秒, 0 表示不启用)
    #[serde(default)]
    pub full_sync_interval: u64,
}

/// 同步方向
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum SyncDirection {
    /// 双向同步
    #[default]
    Bidirectional,
    /// 仅从本地推送到远程
    PushOnly,
    /// 仅从远程拉取到本地
    PullOnly,
}

/// 高级配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedConfig {
    /// 文件哈希算法 (固定为 blake3)
    #[serde(default = "default_hash_algo")]
    pub hash_algorithm: String,
    /// 传输块大小 (字节)
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    /// 最大并发传输数
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_transfers: usize,
    /// 日志级别
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// 元数据存储路径
    #[serde(default)]
    pub metadata_dir: Option<PathBuf>,
}

// === 默认值函数 ===

fn default_node_name() -> String {
    hostname().unwrap_or_else(|_| "crossbag-node".to_string())
}

fn default_listen_addr() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    crate::protocol::DEFAULT_PORT
}

fn default_connect_timeout() -> u64 {
    30
}

fn default_heartbeat_interval() -> u64 {
    10
}

fn default_transfer_timeout() -> u64 {
    300
}

fn default_enabled() -> bool {
    true
}

fn default_hash_algo() -> String {
    "blake3".to_string()
}

fn default_chunk_size() -> usize {
    crate::protocol::CHUNK_SIZE
}

fn default_max_concurrent() -> usize {
    4
}

fn default_log_level() -> String {
    "info".to_string()
}

fn hostname() -> Result<String> {
    Ok(hostname::get()
        .context("Failed to get hostname")?
        .to_string_lossy()
        .to_string())
}

impl Default for AdvancedConfig {
    fn default() -> Self {
        AdvancedConfig {
            hash_algorithm: default_hash_algo(),
            chunk_size: default_chunk_size(),
            max_concurrent_transfers: default_max_concurrent(),
            log_level: default_log_level(),
            metadata_dir: None,
        }
    }
}

impl CrossBagConfig {
    /// 生成默认配置
    pub fn default_config() -> Self {
        CrossBagConfig {
            node: NodeConfig {
                node_id: Uuid::new_v4(),
                name: default_node_name(),
                listen_addr: default_listen_addr(),
                port: default_port(),
            },
            network: NetworkConfig {
                peers: HashMap::new(),
                connect_timeout: default_connect_timeout(),
                heartbeat_interval: default_heartbeat_interval(),
                transfer_timeout: default_transfer_timeout(),
            },
            sync_pairs: Vec::new(),
            easytier: EasytierConfig::default(),
            advanced: AdvancedConfig::default(),
        }
    }

    /// 从文件加载配置
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: CrossBagConfig =
            toml::from_str(&content).with_context(|| "Failed to parse config file")?;

        Ok(config)
    }

    /// 保存配置到文件
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self).context("Failed to serialize config")?;

        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(path.as_ref(), content)
            .with_context(|| format!("Failed to write config file: {:?}", path.as_ref()))?;

        Ok(())
    }

    /// 获取默认配置文件路径
    pub fn default_path() -> PathBuf {
        dirs_next()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(DEFAULT_CONFIG_FILE)
    }

    /// 验证配置有效性
    pub fn validate(&self) -> Result<()> {
        if self.network.peers.is_empty() {
            anyhow::bail!("At least one peer must be configured");
        }

        for pair in &self.sync_pairs {
            if !pair.local_path.exists() {
                anyhow::bail!(
                    "Local path does not exist for sync pair '{}': {:?}",
                    pair.id,
                    pair.local_path
                );
            }
            if !self.network.peers.contains_key(&pair.remote_node) {
                anyhow::bail!(
                    "Remote node '{}' not found in peers for sync pair '{}'",
                    pair.remote_node,
                    pair.id
                );
            }
        }

        Ok(())
    }
}

/// 获取跨平台的配置目录
fn dirs_next() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        std::env::var("XDG_CONFIG_HOME")
            .ok()
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var("HOME")
                    .ok()
                    .map(|h| PathBuf::from(h).join(".config").join("crossbag"))
            })
    }

    #[cfg(target_os = "macos")]
    {
        std::env::var("HOME").ok().map(|h| {
            PathBuf::from(h)
                .join("Library")
                .join("Application Support")
                .join("crossbag")
        })
    }

    #[cfg(target_os = "windows")]
    {
        std::env::var("APPDATA")
            .ok()
            .map(|h| PathBuf::from(h).join("crossbag"))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试默认配置生成和序列化
    #[test]
    fn test_default_config_serialization() {
        let config = CrossBagConfig::default_config();
        let toml_str = toml::to_string_pretty(&config).expect("serialize");
        assert!(toml_str.contains("[node]"));
        assert!(toml_str.contains("[network]"));
        assert!(toml_str.contains("[easytier]"));

        // 往返: 序列化 → 反序列化
        let parsed: CrossBagConfig = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(parsed.node.name, config.node.name);
        assert_eq!(parsed.easytier.network_name, "crossbag-network");
    }

    /// 测试 EasytierConfig 默认值
    #[test]
    fn test_easytier_config_defaults() {
        let config = EasytierConfig::default();
        assert_eq!(config.binary_path, "easytier-core");
        assert_eq!(config.network_name, "crossbag-network");
        assert_eq!(config.listeners.len(), 2);
        assert!(config.auto_start);
        assert_eq!(config.health_check_interval, 10);
        assert_eq!(config.max_restarts, 5);
        assert_eq!(config.startup_timeout, 30);
    }

    /// 测试 SyncDirection 默认值
    #[test]
    fn test_sync_direction_default() {
        assert_eq!(SyncDirection::default(), SyncDirection::Bidirectional);
    }

    /// 测试配置验证: 空的 peers 应报错
    #[test]
    fn test_validate_empty_peers() {
        let mut config = CrossBagConfig::default_config();
        config.network.peers.clear();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("peer"));
    }

    /// 测试配置验证: 有效的配置
    #[test]
    fn test_validate_valid_config() {
        let mut config = CrossBagConfig::default_config();
        config.network.peers.insert(
            "test-peer".into(),
            PeerConfig {
                name: "Test Peer".into(),
                address: "10.0.0.1:9527".into(),
            },
        );
        // 没有 sync_pairs 应该也是有效的 (validate 不检查空 sync_pairs)
        let result = config.validate();
        assert!(
            result.is_ok(),
            "Expected valid config, got: {:?}",
            result.err()
        );
    }

    /// 测试 SyncDirection 序列化
    #[test]
    fn test_sync_direction_serialization() {
        #[derive(Debug, Serialize, Deserialize)]
        struct TestPair {
            direction: SyncDirection,
        }

        let bidir = TestPair {
            direction: SyncDirection::Bidirectional,
        };
        let push = TestPair {
            direction: SyncDirection::PushOnly,
        };
        let pull = TestPair {
            direction: SyncDirection::PullOnly,
        };

        assert_eq!(
            toml::to_string_pretty(&bidir).unwrap().trim(),
            "direction = \"Bidirectional\""
        );
        assert_eq!(
            toml::to_string_pretty(&push).unwrap().trim(),
            "direction = \"PushOnly\""
        );
        assert_eq!(
            toml::to_string_pretty(&pull).unwrap().trim(),
            "direction = \"PullOnly\""
        );
    }
}
