//! 命令行界面
//!
//! 基于 clap 实现的命令行参数解析和子命令定义。

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// CrossBag - 跨机器文件同步工具
///
/// 基于 Easytier 虚拟网络的高性能文件同步工具。
/// 支持双向实时同步、增量传输和冲突解决。
#[derive(Parser, Debug)]
#[command(
    name = "crossbag",
    version,
    about = "CrossBag - Cross-machine file sync over Easytier",
    long_about = "A high-performance file synchronization tool that operates over Easytier virtual network.
Supports bidirectional real-time sync, incremental transfers, and conflict resolution."
)]
pub struct Cli {
    /// 配置文件路径 (默认: ~/.config/crossbag/crossbag.toml)
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,

    /// 日志级别
    #[arg(short, long, global = true, default_value = "info")]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 启动同步守护进程
    ///
    /// 启动 CrossBag 服务，开始文件监控和自动同步
    Serve(ServeArgs),

    /// 手动触发一次全量同步
    ///
    /// 对指定同步对执行一次完整同步
    Sync(SyncArgs),

    /// 查看同步状态
    ///
    /// 显示所有同步对的状态信息
    Status(StatusArgs),

    /// 生成默认配置文件
    ///
    /// 在指定位置生成默认配置
    Init(InitArgs),

    /// 添加同步对
    ///
    /// 添加一组新的同步目录配置
    Add(AddArgs),

    /// 列出所有同步对
    List,

    /// 管理系统服务 (安装/卸载/启动/停止)
    ///
    /// 将 CrossBag 注册为系统服务，支持开机自启
    Service(ServiceArgs),

    /// 生成配对码并等待连接
    ///
    /// 启动 Easytier 虚拟网络，生成一次性配对码，等待另一台机器使用该码连接
    StartConnect(StartConnectArgs),

    /// 使用配对码连接到远程节点
    ///
    /// 输入对方生成的配对码，自动加入虚拟网络并建立连接
    Connect(ConnectArgs),

    /// 显示版本信息
    Version,
}

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// 是否前台运行
    #[arg(short, long)]
    pub foreground: bool,

    /// 仅监控指定同步对
    #[arg(short, long)]
    pub pair: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct SyncArgs {
    /// 同步对 ID (不指定则同步全部)
    #[arg(short, long)]
    pub pair: Option<String>,

    /// 强制执行 (忽略时间戳比较)
    #[arg(short, long)]
    pub force: bool,
}

#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// 仅显示指定同步对
    #[arg(short, long)]
    pub pair: Option<String>,

    /// 详细输出
    #[arg(short, long)]
    pub verbose: bool,
}

#[derive(clap::Args, Debug)]
pub struct InitArgs {
    /// 配置文件输出路径
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// 强制覆盖已存在的配置
    #[arg(short, long)]
    pub force: bool,

    /// 节点名称
    #[arg(short, long)]
    pub name: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct AddArgs {
    /// 同步对 ID (唯一标识)
    #[arg(short, long)]
    pub id: String,

    /// 本地路径
    #[arg(short, long)]
    pub local: PathBuf,

    /// 远程节点 ID
    #[arg(short, long)]
    pub remote_node: String,

    /// 远程路径
    #[arg(short, long)]
    pub remote: PathBuf,
}

#[derive(clap::Subcommand, Debug)]
pub enum ServiceAction {
    /// 安装系统服务
    Install,
    /// 卸载系统服务
    Uninstall,
    /// 启动系统服务
    Start,
    /// 停止系统服务
    Stop,
    /// 查询服务状态
    Status,
}

#[derive(clap::Args, Debug)]
pub struct ServiceArgs {
    #[command(subcommand)]
    pub action: ServiceAction,

    /// 手动指定可执行文件路径
    #[arg(short, long)]
    pub binary: Option<PathBuf>,
}

#[derive(clap::Args, Debug)]
pub struct StartConnectArgs {
    /// 配对超时（秒），0 表示无限等待
    #[arg(short, long, default_value = "300")]
    pub timeout: u64,
}

#[derive(clap::Args, Debug)]
pub struct ConnectArgs {
    /// 配对码 (格式: XXXXX-XXXXX-XXXXX-XXXXX-XXXXX)
    pub code: String,

    /// 连接超时（秒）
    #[arg(short, long, default_value = "30")]
    pub timeout: u64,
}
