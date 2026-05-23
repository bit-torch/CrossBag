//! CrossBag - 跨机器文件同步工具
//!
//! 基于 Easytier 虚拟网络的高性能文件同步工具。
//!
//! # 功能特性
//! - 实时文件变更监控
//! - 双向增量同步
//! - BLAKE3 快速哈希校验
//! - 基于 Easytier 的 P2P 加密连接
//! - 冲突检测与解决
//! - 灵活的文件过滤规则
//!
//! # 快速开始
//!
//! ```bash
//! # 生成默认配置
//! crossbag init
//!
//! # 编辑配置文件，添加同步对和节点信息
//! # 然后启动服务
//! crossbag serve
//!
//! # 或执行一次手动同步
//! crossbag sync
//! ```

use anyhow::Result;
use clap::Parser;
use crossbag::{cli, config, daemon, easytier, network, service, sync, watcher};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// 查找配置文件路径
fn find_config_path(cli_config: &Option<PathBuf>) -> PathBuf {
    // 1. 命令行指定的路径
    if let Some(path) = cli_config {
        if path.exists() {
            return path.clone();
        }
        warn!("Config file not found at {:?}, using default", path);
    }

    // 2. 当前目录
    let cwd_config = PathBuf::from(config::DEFAULT_CONFIG_FILE);
    if cwd_config.exists() {
        return cwd_config;
    }

    // 3. 默认配置目录
    config::CrossBagConfig::default_path()
}

/// 设置日志系统
fn setup_logging(level: &str) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("crossbag={}", level)));

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(env_filter)
        .init();
}

/// 处理 serve 命令
async fn handle_serve(_args: cli::ServeArgs, config_path: PathBuf) -> Result<()> {
    let config = config::CrossBagConfig::load(&config_path)?;
    config.validate()?;

    info!("CrossBag daemon starting...");
    info!("Node ID: {}", config.node.node_id);
    info!("Node Name: {}", config.node.name);
    info!("Sync Pairs: {}", config.sync_pairs.len());

    let config = Arc::new(config);

    // ========== 1. Easytier 子进程 ==========
    let easytier_mgr: Option<Arc<tokio::sync::Mutex<easytier::EasytierManager>>> =
        if config.easytier.auto_start {
            let mut mgr = easytier::EasytierManager::new(config.easytier.clone());
            match mgr.start().await {
                Ok(()) => info!("Easytier started"),
                Err(e) => warn!("Easytier start failed: {}", e),
            }
            Some(Arc::new(tokio::sync::Mutex::new(mgr)))
        } else {
            None
        };

    // ========== 2. 网络 (先创建，后与 Daemon 绑定) ==========
    let mut network = network::NetworkManager::new(config.clone());
    network.start().await?;

    // ========== 3. 同步守护进程 ==========
    let daemon = daemon::SyncDaemon::new(config.clone());
    let action_tx = daemon.action_sender();

    // 将 Daemon 的消息通道注入 Network (入站消息 → Daemon)
    network.set_action_sender(action_tx.clone());

    // 启动事件循环
    tokio::spawn(daemon.run());

    // 启动文件监控 → 注入到 daemon
    let mut watcher = watcher::FileWatcher::new(500);
    let pair_ids: Vec<String> = config
        .sync_pairs
        .iter()
        .filter(|p| p.watch && p.enabled)
        .map(|p| p.id.clone())
        .collect();

    for pair in &config.sync_pairs {
        if pair.watch && pair.enabled {
            watcher.watch_path(&pair.local_path)?;
        }
    }

    if !pair_ids.is_empty() {
        daemon::spawn_watcher_bridge(watcher, pair_ids.clone(), action_tx.clone(), 500);
    } else {
        let _ = watcher.spawn();
    }

    // 定期全量同步
    for pair in &config.sync_pairs {
        if pair.enabled && pair.full_sync_interval > 0 {
            let tx = action_tx.clone();
            let pair_id = pair.id.clone();
            let interval = pair.full_sync_interval;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval));
                loop {
                    ticker.tick().await;
                    let _ = tx.send(daemon::SyncAction::PeriodicFullSync {
                        pair_id: pair_id.clone(),
                    });
                }
            });
        }
    }

    // ========== 4. Easytier 健康检查 ==========
    if let Some(ref mgr) = easytier_mgr {
        let mgr = mgr.clone();
        let interval = config.easytier.health_check_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval));
            loop {
                ticker.tick().await;
                if let easytier::EasytierState::Failed(msg) = mgr.lock().await.health_check().await {
                    error!("Easytier failed: {}", msg);
                    break;
                }
            }
        });
    }

    info!("CrossBag daemon running. Press Ctrl+C to stop.");

    // ========== 5. 等待退出 ==========
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    network.stop().await;

    if let Some(mgr) = easytier_mgr {
        match mgr.lock().await.stop().await {
            Ok(()) => info!("Easytier stopped"),
            Err(e) => error!("Easytier stop error: {}", e),
        }
    }

    info!("Shutdown complete");
    Ok(())
}

/// 处理 sync 命令
async fn handle_sync(args: cli::SyncArgs, config_path: PathBuf) -> Result<()> {
    let config = config::CrossBagConfig::load(&config_path)?;
    let config = Arc::new(config);

    let mut engine = sync::SyncEngine::new(config.clone());

    let pairs: Vec<&config::SyncPair> = if let Some(ref pair_id) = args.pair {
        config
            .sync_pairs
            .iter()
            .filter(|p| p.id == *pair_id)
            .collect()
    } else {
        config.sync_pairs.iter().collect()
    };

    if pairs.is_empty() {
        warn!("No sync pairs found");
        return Ok(());
    }

    for pair in pairs {
        info!(
            "Syncing pair '{}': {:?} -> {}",
            pair.id, pair.local_path, pair.remote_node
        );
        match engine.full_sync(pair).await {
            Ok(result) => {
                println!(
                    "Pair '{}': {} files synced, {} bytes transferred",
                    pair.id, result.files_synced, result.bytes_transferred
                );
                if !result.errors.is_empty() {
                    for err in &result.errors {
                        eprintln!("  Error: {}", err);
                    }
                }
            }
            Err(e) => {
                error!("Sync failed for pair '{}': {}", pair.id, e);
            }
        }
    }

    Ok(())
}

/// 处理 status 命令
async fn handle_status(args: cli::StatusArgs, config_path: PathBuf) -> Result<()> {
    let config = config::CrossBagConfig::load(&config_path)?;

    println!("=== CrossBag Status ===");
    println!("Node ID:   {}", config.node.node_id);
    println!("Node Name: {}", config.node.name);
    println!(
        "Listen:    {}:{}",
        config.node.listen_addr, config.node.port
    );
    println!();

    let pairs: Vec<&config::SyncPair> = if let Some(ref pair_id) = args.pair {
        config
            .sync_pairs
            .iter()
            .filter(|p| p.id == *pair_id)
            .collect()
    } else {
        config.sync_pairs.iter().collect()
    };

    println!("Sync Pairs: {}", pairs.len());
    for pair in &pairs {
        println!("  [{}] {}", if pair.enabled { "✓" } else { "✗" }, pair.id);
        println!("    Local:  {:?}", pair.local_path);
        println!("    Remote: {} -> {:?}", pair.remote_node, pair.remote_path);
        println!("    Direction: {:?}", pair.direction);
        if args.verbose {
            println!("    Watch: {}", pair.watch);
            println!("    Full Sync Interval: {}s", pair.full_sync_interval);
            if !pair.exclude_patterns.is_empty() {
                println!("    Exclude: {:?}", pair.exclude_patterns);
            }
        }
        println!();
    }

    println!();
    println!("Peers: {}", config.network.peers.len());
    for (id, peer) in &config.network.peers {
        println!("  {}: {} ({})", id, peer.name, peer.address);
    }

    Ok(())
}

/// 处理 init 命令
async fn handle_init(args: cli::InitArgs) -> Result<()> {
    let output_path = args
        .output
        .unwrap_or_else(|| PathBuf::from(config::DEFAULT_CONFIG_FILE));

    if output_path.exists() && !args.force {
        anyhow::bail!(
            "Config file already exists at {:?}. Use --force to overwrite.",
            output_path
        );
    }

    let mut config = config::CrossBagConfig::default_config();

    if let Some(name) = args.name {
        config.node.name = name;
    }

    config.save(&output_path)?;
    println!("Config file created at {:?}", output_path);
    println!();
    println!("Next steps:");
    println!("1. Edit the config file to add your peers and sync pairs");
    println!("2. Set up Easytier network between your machines");
    println!("3. Run 'crossbag serve' to start syncing");

    Ok(())
}

/// 处理 add 命令
async fn handle_add(args: cli::AddArgs, config_path: PathBuf) -> Result<()> {
    let mut config = config::CrossBagConfig::load(&config_path)?;

    // 检查同步对 ID 是否重复
    if config.sync_pairs.iter().any(|p| p.id == args.id) {
        anyhow::bail!("Sync pair '{}' already exists", args.id);
    }

    // 检查远程节点是否存在
    if !config.network.peers.contains_key(&args.remote_node) {
        anyhow::bail!(
            "Remote node '{}' not found in peers. Add it first.",
            args.remote_node
        );
    }

    let pair = config::SyncPair {
        id: args.id,
        local_path: args.local,
        remote_node: args.remote_node,
        remote_path: args.remote,
        direction: config::SyncDirection::Bidirectional,
        exclude_patterns: Vec::new(),
        enabled: true,
        watch: true,
        full_sync_interval: 300, // 5 minutes default
    };

    config.sync_pairs.push(pair);
    config.save(&config_path)?;

    println!("Sync pair added successfully");
    Ok(())
}

/// 处理 list 命令
async fn handle_list(config_path: PathBuf) -> Result<()> {
    let config = config::CrossBagConfig::load(&config_path)?;

    if config.sync_pairs.is_empty() {
        println!("No sync pairs configured.");
        return Ok(());
    }

    println!(
        "{:<20} {:<40} {:<20} {:<40}",
        "ID", "Local Path", "Remote Node", "Remote Path"
    );
    println!("{}", "-".repeat(120));

    for pair in &config.sync_pairs {
        println!(
            "{:<20} {:<40} {:<20} {:<40}",
            pair.id,
            pair.local_path.display(),
            pair.remote_node,
            pair.remote_path.display()
        );
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    // 设置日志
    setup_logging(&cli.log_level);

    // 查找配置文件
    let config_path = find_config_path(&cli.config);

    // 处理命令
    match cli.command {
        cli::Commands::Serve(args) => {
            handle_serve(args, config_path).await?;
        }
        cli::Commands::Sync(args) => {
            handle_sync(args, config_path).await?;
        }
        cli::Commands::Status(args) => {
            handle_status(args, config_path).await?;
        }
        cli::Commands::Init(args) => {
            handle_init(args).await?;
        }
        cli::Commands::Add(args) => {
            handle_add(args, config_path).await?;
        }
        cli::Commands::List => {
            handle_list(config_path).await?;
        }
        cli::Commands::Service(args) => {
            handle_service(args)?;
        }
        cli::Commands::Version => {
            print_version();
        }
    }

    Ok(())
}

/// 处理 service 命令
fn handle_service(args: cli::ServiceArgs) -> Result<()> {
    use cli::ServiceAction;

    match args.action {
        ServiceAction::Install => {
            service::install(args.binary, None)?;
            println!("CrossBag service installed. Run 'crossbag service start' to launch.");
        }
        ServiceAction::Uninstall => {
            service::uninstall()?;
            println!("CrossBag service uninstalled.");
        }
        ServiceAction::Start => {
            service::start_service()?;
            println!("CrossBag service started.");
        }
        ServiceAction::Stop => {
            service::stop_service()?;
            println!("CrossBag service stopped.");
        }
        ServiceAction::Status => {
            let status = service::query_status()?;
            println!("{}", status);
        }
    }

    Ok(())
}

/// 打印版本信息
fn print_version() {
    println!("CrossBag {}", env!("CARGO_PKG_VERSION"));
    println!("Protocol version: {}", crossbag::protocol::PROTOCOL_VERSION);
    println!("License: {}", env!("CARGO_PKG_LICENSE"));
    println!("Repository: https://github.com/bit-torch/CrossBag");
}
