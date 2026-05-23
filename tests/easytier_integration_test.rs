//! Easytier 子进程管理器联调测试
//!
//! 测试 EasytierManager 的完整生命周期：配置验证、参数构建、状态机。

use crossbag::config::EasytierConfig;
use crossbag::easytier::{EasytierManager, EasytierState};

/// 测试默认配置有效性
#[test]
fn test_easytier_config_defaults() {
    let config = EasytierConfig::default();
    assert_eq!(config.binary_path, "easytier-core");
    assert_eq!(config.network_name, "crossbag-network");
    assert!(config.listeners.len() >= 2);
    assert!(config.auto_start);
    assert_eq!(config.max_restarts, 5);
    assert_eq!(config.startup_timeout, 30);
    assert_eq!(config.health_check_interval, 10);
}

/// 测试自定义配置
#[test]
fn test_easytier_custom_config() {
    let config = EasytierConfig {
        binary_path: "/usr/local/bin/easytier-core".into(),
        network_name: "my-network".into(),
        network_secret: "secret123".into(),
        instance_name: "node-a".into(),
        listeners: vec!["tcp://0.0.0.0:22020".into()],
        auto_start: false,
        health_check_interval: 30,
        max_restarts: 3,
        startup_timeout: 60,
    };

    assert_eq!(config.binary_path, "/usr/local/bin/easytier-core");
    assert_eq!(config.network_name, "my-network");
    assert!(!config.auto_start);
    assert_eq!(config.max_restarts, 3);
}

/// 测试 manager 创建和初始状态
#[test]
fn test_manager_initial_state() {
    let config = EasytierConfig::default();
    let mgr = EasytierManager::new(config);
    assert!(!mgr.is_running());
}

/// 测试二进制检测逻辑 (无 easytier 环境时返回错误)
#[test]
fn test_binary_detection() {
    let config = EasytierConfig {
        binary_path: "nonexistent-easytier-binary-xyz".into(),
        ..EasytierConfig::default()
    };
    let mgr = EasytierManager::new(config);
    let result = mgr.check_binary();
    assert!(result.is_err(), "Should fail for nonexistent binary");
}

/// 测试二进制检测 (用已知存在的命令，如 cargo)
#[test]
fn test_binary_detection_existing() {
    let config = EasytierConfig {
        binary_path: "cargo".into(), // cargo 应该在 PATH 中
        ..EasytierConfig::default()
    };
    let mgr = EasytierManager::new(config);
    let result = mgr.check_binary();
    assert!(result.is_ok(), "Should find cargo: {:?}", result.err());
}

/// 测试 EasytierState 的状态转换逻辑
#[test]
fn test_state_transitions() {
    // 初始状态
    let config = EasytierConfig::default();
    let mut mgr = EasytierManager::new(config);

    // 未启动时状态
    let state = tokio_test::block_on(mgr.status());
    assert_eq!(state, EasytierState::Stopped);
}

/// 测试带密钥的配置序列化
#[test]
fn test_config_with_secret_serialization() {
    let config = EasytierConfig {
        network_secret: "super-secret-key".into(),
        ..EasytierConfig::default()
    };

    // 验证配置可以序列化为 TOML
    let toml_str = toml::to_string_pretty(&config).unwrap();
    assert!(toml_str.contains("super-secret-key"));
    assert!(toml_str.contains("crossbag-network"));

    // 往返
    let parsed: EasytierConfig = toml::from_str(&toml_str).unwrap();
    assert_eq!(parsed.network_secret, "super-secret-key");
    assert_eq!(parsed.max_restarts, 5);
}

/// 测试版本兼容性检查 (protocol version)
#[test]
fn test_protocol_version_compatibility() {
    use crossbag::protocol::PROTOCOL_VERSION;

    // 协议版本不应为 0
    assert!(PROTOCOL_VERSION > 0);

    // 当前版本为 1
    assert_eq!(PROTOCOL_VERSION, 1);
}

/// 测试 CrossBag 版本号是有效的语义化版本
#[test]
fn test_crossbag_semver() {
    let version = env!("CARGO_PKG_VERSION");

    // 验证是有效的 semver (x.y.z)
    let parts: Vec<&str> = version.split('.').collect();
    assert_eq!(parts.len(), 3, "Version should be semver: {}", version);
    assert!(
        parts[0].parse::<u32>().is_ok(),
        "Major version should be number"
    );
    assert!(
        parts[1].parse::<u32>().is_ok(),
        "Minor version should be number"
    );
    assert!(
        parts[2].parse::<u32>().is_ok(),
        "Patch version should be number"
    );
}
