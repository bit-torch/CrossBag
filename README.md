# CrossBag

> **跨机器文件同步工具** — 基于 Easytier 虚拟网络的高性能 P2P 文件同步

[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-29%20passed-green.svg)]()

## 简介

CrossBag 是一个使用 Rust 编写的高性能跨机器文件同步工具。它运行在 [Easytier](https://github.com/EasyTier/EasyTier) 虚拟网络之上，实现两台电脑之间文件夹（或整个磁盘）的实时双向同步。

### 核心特性

- **实时监控**: 基于文件系统事件实现毫秒级变更感知
- **增量同步**: 仅传输变更的文件块，节省带宽
- **BLAKE3 哈希**: 超快速的文件内容校验
- **P2P 加密**: 通过 Easytier 建立加密的直连隧道
- **双向同步**: 支持 Push/Pull/Bidirectional 三种模式
- **冲突解决**: 可配置的冲突处理策略
- **灵活过滤**: 支持 glob 模式排除文件

## 架构

```
┌─────────────────────────────────────┐
│           CrossBag Node A            │
│  ┌──────────┐  ┌──────────────────┐ │
│  │  Watcher  │──▶   Sync Engine    │ │
│  └──────────┘  └────────┬─────────┘ │
│                          │           │
│                   ┌──────▼──────┐   │
│                   │   Network    │   │
│                   └──────┬──────┘   │
└──────────────────────────┼──────────┘
                           │
                    ┌──────▼──────┐
                    │   Easytier   │
                    │ Overlay Net  │
                    └──────┬──────┘
                           │
┌──────────────────────────┼──────────┐
│                   ┌──────▼──────┐   │
│                   │   Network    │   │
│                   └──────┬──────┘   │
│                          │           │
│  ┌──────────┐  ┌────────▼─────────┐ │
│  │  Watcher  │◀──   Sync Engine    │ │
│  └──────────┘  └──────────────────┘ │
│           CrossBag Node B            │
└─────────────────────────────────────┘
```

## 快速开始

### 1. 安装 Easytier

参见 [Easytier 安装指南](easytier-setup.md)

### 2. 安装 CrossBag

```bash
# 从源码编译
git clone https://github.com/your-org/crossbag.git
cd crossbag
cargo build --release

# 安装到系统路径 (可选)
cargo install --path .
```

### 3. 配置

```bash
# 生成默认配置文件
crossbag init

# 编辑 crossbag.toml
crossbag add --id my-sync --local /path/to/local --remote-node office-pc --remote /path/to/remote
```

### 4. 运行

```bash
# 启动同步守护进程
crossbag serve

# 或执行单次同步
crossbag sync

# 查看状态
crossbag status
```

## 配置文件示例

```toml
[node]
node_id = "550e8400-e29b-41d4-a716-446655440000"
name = "home-pc"
listen_addr = "0.0.0.0"
port = 9527

[network.peers.office-pc]
name = "Office PC"
address = "10.10.10.2:9527"  # Easytier 虚拟 IP

[[sync_pairs]]
id = "work-documents"
local_path = "/home/user/Documents"
remote_node = "office-pc"
remote_path = "/home/user/Documents"
direction = "Bidirectional"
exclude_patterns = ["*.tmp", "node_modules/**", ".git/**"]
enabled = true
watch = true
full_sync_interval = 300
```

## 命令参考

| 命令 | 说明 |
|------|------|
| `crossbag init` | 生成默认配置文件 |
| `crossbag serve` | 启动同步守护进程 |
| `crossbag sync` | 手动触发全量同步 |
| `crossbag status` | 查看同步状态 |
| `crossbag add` | 添加同步对 |
| `crossbag list` | 列出同步对 |

## 技术栈

- **语言**: Rust
- **异步运行时**: Tokio
- **CLI 框架**: Clap
- **文件监控**: Notify
- **网络层**: Easytier (虚拟组网)
- **哈希算法**: BLAKE3
- **序列化**: Bincode
- **配置格式**: TOML

## 开发计划

- [x] 项目架构设计
- [x] 核心协议定义
- [x] 配置管理模块
- [x] 文件监控模块
- [x] 网络通信模块
- [x] 同步引擎
- [x] CLI 命令行界面
- [x] 单元测试 (21 tests)
- [x] 集成测试 (8 scenarios)
- [x] Windows/Linux/macOS 服务管理
- [ ] GUI 管理界面
- [ ] 移动端支持

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

Copyright 2026 bit-torch
