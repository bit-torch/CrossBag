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

use anyhow::{Context, Result};
use clap::Parser;
use crossbag::{cli, config, daemon, easytier, network, pairing, service, sync, watcher};
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

    // 创建 Daemon → Network 的命令通道
    let (network_cmd_tx, network_cmd_rx) = tokio::sync::mpsc::unbounded_channel();

    // ========== 3. 同步守护进程 ==========
    let mut daemon = daemon::SyncDaemon::new(config.clone());
    let action_tx = daemon.action_sender();

    // 双向绑定:
    //   Daemon → Network: network_cmd_tx
    //   Network → Daemon: action_tx
    daemon.set_network_sender(network_cmd_tx);
    network.set_action_sender(action_tx.clone());
    network.set_command_receiver(network_cmd_rx);

    network.start().await?;

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
                if let easytier::EasytierState::Failed(msg) = mgr.lock().await.health_check().await
                {
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
        cli::Commands::StartConnect(args) => {
            handle_start_connect(args, config_path).await?;
        }
        cli::Commands::Connect(args) => {
            handle_connect(args, config_path).await?;
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

/// 处理 start-connect 命令
async fn handle_start_connect(args: cli::StartConnectArgs, config_path: PathBuf) -> Result<()> {
    let config = config::CrossBagConfig::load(&config_path)?;
    let config = Arc::new(config);

    println!("CrossBag Pairing Mode");
    println!("=====================");
    println!();

    // 1. 启动 Easytier 作为监听方
    let mut easytier_mgr = easytier::EasytierManager::new(config.easytier.clone());
    match easytier_mgr.start_as_listener().await {
        Ok(()) => info!("Easytier started in listener mode"),
        Err(e) => {
            // 回退到普通模式
            warn!("Easytier listener mode failed: {}, trying standard mode", e);
            easytier_mgr.start().await?;
        }
    }

    // 2. 获取物理 IP
    let physical_ip = if let Some(ref ip_str) = config.node.physical_ip {
        ip_str
            .parse::<std::net::Ipv4Addr>()
            .context("Invalid physical_ip in config")?
    } else {
        match pairing::get_physical_ip() {
            Ok(ip) => ip,
            Err(_) => {
                warn!("Could not detect physical IP, pairing code will not include direct address");
                std::net::Ipv4Addr::new(0, 0, 0, 0)
            }
        }
    };

    let ip_bytes = physical_ip.octets();

    // 3. 生成配对码
    let mut listener = pairing::PairingListener::new(config.clone());
    let code = listener.generate_code(ip_bytes)?;

    println!("Pairing Code: {}", code);
    println!();
    println!("Share this code with the other machine.");
    println!("Waiting for connection...");

    // 4. 等待配对
    let timeout_duration = if args.timeout == 0 {
        tokio::time::Duration::from_secs(86400) // 24h as "infinite"
    } else {
        tokio::time::Duration::from_secs(args.timeout)
    };

    match listener.wait_for_pairing(timeout_duration).await {
        Ok(peer_info) => {
            println!();
            println!(
                "[Connected] Paired with '{}' (host: {})",
                peer_info.node_name, peer_info.hostname
            );

            // 5. 保存 peer 到配置
            if let Err(e) = pairing::save_peer_to_config(&config_path, &peer_info) {
                error!("Failed to save peer to config: {}", e);
            } else {
                println!("Peer saved to configuration.");
            }

            println!();
            println!("Use 'crossbag serve' to start syncing.");
        }
        Err(e) => {
            println!();
            println!("Pairing failed: {}", e);
        }
    }

    // 停止 Easytier
    easytier_mgr.stop().await?;

    Ok(())
}

/// 处理 connect 命令
async fn handle_connect(args: cli::ConnectArgs, config_path: PathBuf) -> Result<()> {
    let config = config::CrossBagConfig::load(&config_path)?;
    let config = Arc::new(config);

    // 1. 解码配对码
    let code = pairing::PairingCode::decode(&args.code)?;
    println!("Decoded pairing code");
    println!(
        "  Target: {}:{}",
        if code.has_physical_ip() {
            code.physical_ip_str()
        } else {
            "discover via shared node".to_string()
        },
        code.easytier_port()
    );

    // 2. 验证网络参数
    if !code.verify_network(
        &config.easytier.network_name,
        &config.easytier.network_secret,
    ) {
        anyhow::bail!(
            "Network name/secret mismatch! The pairing code was generated for a different network. \
             Check your crossbag.toml [easytier] section."
        );
    }

    // 3. 启动 Easytier 加入网络
    let mut easytier_mgr = easytier::EasytierManager::new(config.easytier.clone());
    if code.has_physical_ip() {
        let peer_url = code.peer_url();
        println!("Connecting to {}...", peer_url);
        easytier_mgr.start_with_peer(&peer_url).await?;
    } else if let Some(ref external_node) = config.easytier.external_node {
        println!("Connecting via shared node: {}...", external_node);
        easytier_mgr.start_with_external_node(external_node).await?;
    } else {
        anyhow::bail!(
            "Pairing code has no direct address and no external_node configured. \
             Either set physical_ip on the other machine or add external_node to your config."
        );
    }

    println!("Easytier network established.");

    // 4. 使用 PairingConnector 连接
    let connector = pairing::PairingConnector::new(config.clone());
    let connect_timeout = tokio::time::Duration::from_secs(args.timeout);

    match connector.connect(&code, connect_timeout).await {
        Ok(peer_info) => {
            println!(
                "Paired with '{}' (host: {})... OK",
                peer_info.node_name, peer_info.hostname
            );

            // 5. 保存 peer
            if let Err(e) = pairing::save_peer_to_config(&config_path, &peer_info) {
                error!("Failed to save peer to config: {}", e);
            } else {
                println!("Peer saved to configuration.");
            }

            println!();
            println!("Use 'crossbag serve' to start syncing.");
        }
        Err(e) => {
            println!("Connection failed: {}", e);
        }
    }

    // 停止 Easytier
    easytier_mgr.stop().await?;

    Ok(())
}

/// 打印版本信息
fn print_version() {
    println!("CrossBag {}", env!("CARGO_PKG_VERSION"));
    println!("Protocol version: {}", crossbag::protocol::PROTOCOL_VERSION);
    println!("License: {}", env!("CARGO_PKG_LICENSE"));
    println!("Repository: https://github.com/bit-torch/CrossBag");
}
