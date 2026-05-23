//! 网络通信模块
//!
//! 基于 TCP 实现节点间的 P2P 通信，运行在 Easytier 虚拟网络之上。
//! 提供连接管理、消息收发和心跳维持功能。

use crate::config::{CrossBagConfig, PeerConfig};
use crate::daemon::SyncAction;
use crate::protocol::Message;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{interval, timeout, Duration};
use tracing::{debug, error, info, warn};

/// Daemon → NetworkManager 的发送指令
#[derive(Debug)]
pub enum NetworkCommand {
    /// 向指定 peer 发送协议消息
    SendToPeer { peer_id: String, message: Message },
    /// 向所有已连接的 peer 广播消息
    Broadcast(Message),
}

/// 连接状态
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Failed(String),
}

/// 对等连接
pub struct PeerConnection {
    pub peer_id: String,
    pub peer_name: String,
    pub address: String,
    pub state: ConnectionState,
    /// 写通道 (向 peer 发送消息)
    write_tx: Option<mpsc::Sender<Vec<u8>>>,
}

/// 网络管理器
pub struct NetworkManager {
    /// 本节点配置
    config: Arc<CrossBagConfig>,
    /// 活跃连接
    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    /// 是否正在运行
    running: Arc<Mutex<bool>>,
    /// 消息转发到 Daemon 的通道
    action_tx: Option<mpsc::UnboundedSender<SyncAction>>,
    /// 接收 Daemon 发送指令的通道
    command_rx: Option<mpsc::UnboundedReceiver<NetworkCommand>>,
}

impl NetworkManager {
    /// 创建网络管理器
    pub fn new(config: Arc<CrossBagConfig>) -> Self {
        NetworkManager {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
            action_tx: None,
            command_rx: None,
        }
    }

    /// 设置 Daemon 消息通道
    pub fn set_action_sender(&mut self, tx: mpsc::UnboundedSender<SyncAction>) {
        self.action_tx = Some(tx);
    }

    /// 设置接收 Daemon 发送指令的通道
    pub fn set_command_receiver(&mut self, rx: mpsc::UnboundedReceiver<NetworkCommand>) {
        self.command_rx = Some(rx);
    }

    /// 启动网络服务 (监听 + 连接)
    pub async fn start(&mut self) -> Result<()> {
        *self.running.lock().await = true;

        // 初始化所有对等节点的连接状态
        {
            let mut conns = self.connections.write().await;
            for (peer_id, peer_config) in &self.config.network.peers {
                conns.insert(
                    peer_id.clone(),
                    PeerConnection {
                        peer_id: peer_id.clone(),
                        peer_name: peer_config.name.clone(),
                        address: peer_config.address.clone(),
                        state: ConnectionState::Disconnected,
                        write_tx: None,
                    },
                );
            }
        }

        // 启动 TCP 监听
        let listen_addr = format!("{}:{}", self.config.node.listen_addr, self.config.node.port);

        let listener = TcpListener::bind(&listen_addr)
            .await
            .with_context(|| format!("Failed to bind to {}", listen_addr))?;

        info!("CrossBag listening on {}", listen_addr);

        // 接受连接循环
        let connections = self.connections.clone();
        let node_config = self.config.clone();
        let action_tx = self.action_tx.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("Incoming connection from {}", addr);
                        tokio::spawn(handle_incoming_connection(
                            stream,
                            connections.clone(),
                            node_config.clone(),
                            action_tx.clone(),
                        ));
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }
        });

        // 启动主动连接任务
        self.connect_to_peers().await;

        // 启动心跳任务
        self.start_heartbeat().await;

        // 启动命令处理任务 (Daemon → Network)
        self.start_command_handler().await;

        Ok(())
    }

    /// 连接到所有配置的对等节点
    async fn connect_to_peers(&self) {
        let connections = self.connections.clone();
        let peers = self.config.network.peers.clone();
        let connect_timeout = self.config.network.connect_timeout;
        let node_config = self.config.clone();
        let action_tx = self.action_tx.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            loop {
                let peers_to_connect: Vec<(String, PeerConfig)> = {
                    let conns = connections.read().await;
                    peers
                        .iter()
                        .filter(|(id, _)| {
                            conns
                                .get(*id)
                                .map(|c| c.state != ConnectionState::Connected)
                                .unwrap_or(true)
                        })
                        .map(|(id, cfg)| (id.clone(), cfg.clone()))
                        .collect()
                };

                for (peer_id, peer_config) in peers_to_connect {
                    if !*running.lock().await {
                        return;
                    }

                    info!(
                        "Connecting to peer '{}' at {}",
                        peer_id, peer_config.address
                    );

                    {
                        let mut conns = connections.write().await;
                        if let Some(conn) = conns.get_mut(&peer_id) {
                            conn.state = ConnectionState::Connecting;
                        }
                    }

                    let addr = peer_config.address.clone();
                    match connect_and_handshake(&addr, connect_timeout, &node_config).await {
                        Ok(stream) => {
                            info!("Connected + handshake complete with '{}'", peer_id);

                            // Split stream: 读端给 reader，写端存入 connection map
                            let (read_half, write_half) = tokio::io::split(stream);
                            let (write_tx, write_rx) = mpsc::channel::<Vec<u8>>(256);

                            {
                                let mut conns = connections.write().await;
                                if let Some(conn) = conns.get_mut(&peer_id) {
                                    conn.state = ConnectionState::Connected;
                                    conn.write_tx = Some(write_tx);
                                }
                            }

                            // 写任务: 消费 write_rx 中的消息并写入 TCP
                            let peer = peer_id.clone();
                            let conns_w = connections.clone();
                            tokio::spawn(async move {
                                peer_writer(write_half, write_rx).await;
                                let mut conns = conns_w.write().await;
                                if let Some(conn) = conns.get_mut(&peer) {
                                    conn.state = ConnectionState::Disconnected;
                                    conn.write_tx = None;
                                }
                            });

                            // 读任务
                            let conns_r = connections.clone();
                            let peer_r = peer_id.clone();
                            let tx = action_tx.clone();
                            tokio::spawn(async move {
                                peer_reader(read_half, &addr, tx).await;
                                let mut conns = conns_r.write().await;
                                if let Some(conn) = conns.get_mut(&peer_r) {
                                    conn.state = ConnectionState::Disconnected;
                                }
                                warn!("Peer '{}' disconnected", peer_r);
                            });
                        }
                        Err(e) => {
                            warn!("Failed to connect '{}': {}", peer_id, e);
                            let mut conns = connections.write().await;
                            if let Some(conn) = conns.get_mut(&peer_id) {
                                conn.state = ConnectionState::Failed(e.to_string());
                            }
                        }
                    }
                }

                // 等待重试间隔
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    /// 启动心跳日志 (每周期报告连接状态)
    async fn start_heartbeat(&self) {
        let connections = self.connections.clone();
        let interval_secs = self.config.network.heartbeat_interval;

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(interval_secs));
            loop {
                ticker.tick().await;
                let conns = connections.read().await;
                let connected: Vec<&str> = conns
                    .iter()
                    .filter(|(_, c)| c.state == ConnectionState::Connected)
                    .map(|(id, _)| id.as_str())
                    .collect();
                let total = conns.len();
                debug!(
                    "Network state: {}/{} peers connected{}",
                    connected.len(),
                    total,
                    if connected.is_empty() { "" } else { ": " }
                );
                for peer in &connected {
                    debug!("  ✓ {}", peer);
                }
            }
        });
    }

    /// 启动 Daemon → Network 命令处理循环
    async fn start_command_handler(&mut self) {
        // 取出 command_rx（只能取一次）
        let command_rx = match self.command_rx.take() {
            Some(rx) => rx,
            None => return, // 没有 channel 则跳过
        };

        let connections = self.connections.clone();
        tokio::spawn(async move {
            let mut rx = command_rx;
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    NetworkCommand::SendToPeer { peer_id, message } => {
                        let data = match message.to_bytes() {
                            Ok(d) => d,
                            Err(e) => {
                                error!("Failed to serialize message for {}: {}", peer_id, e);
                                continue;
                            }
                        };

                        let len = data.len() as u32;
                        let mut framed = Vec::with_capacity(4 + data.len());
                        framed.extend_from_slice(&len.to_be_bytes());
                        framed.extend_from_slice(&data);

                        let conns = connections.read().await;
                        if let Some(conn) = conns.get(&peer_id) {
                            if let Some(ref tx) = conn.write_tx {
                                if let Err(e) = tx.send(framed).await {
                                    error!("Failed to send to peer {}: {}", peer_id, e);
                                }
                            } else {
                                warn!("Peer {} connected but write channel unavailable", peer_id);
                            }
                        } else {
                            warn!("Peer {} not found in connections", peer_id);
                        }
                    }
                    NetworkCommand::Broadcast(message) => {
                        let data = match message.to_bytes() {
                            Ok(d) => d,
                            Err(e) => {
                                error!("Failed to serialize broadcast message: {}", e);
                                continue;
                            }
                        };

                        let len = data.len() as u32;
                        let mut framed = Vec::with_capacity(4 + data.len());
                        framed.extend_from_slice(&len.to_be_bytes());
                        framed.extend_from_slice(&data);

                        let conns = connections.read().await;
                        let connected_peers: Vec<_> =
                            conns.iter().filter(|(_, c)| c.write_tx.is_some()).collect();

                        for (peer_id, conn) in connected_peers {
                            if let Some(ref tx) = conn.write_tx {
                                if let Err(e) = tx.send(framed.clone()).await {
                                    error!("Broadcast failed for {}: {}", peer_id, e);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// 发送消息到指定对等节点
    pub async fn send_to_peer(&self, peer_id: &str, message: &Message) -> Result<()> {
        let data = message.to_bytes().context("Failed to serialize message")?;

        let len = data.len() as u32;
        let mut framed = Vec::with_capacity(4 + data.len());
        framed.extend_from_slice(&len.to_be_bytes());
        framed.extend_from_slice(&data);

        let conns = self.connections.read().await;
        if let Some(conn) = conns.get(peer_id) {
            if let Some(ref tx) = conn.write_tx {
                tx.send(framed)
                    .await
                    .map_err(|_| anyhow::anyhow!("Write channel closed for peer {}", peer_id))?;
                return Ok(());
            }
        }

        anyhow::bail!("Peer {} not connected", peer_id)
    }

    /// 添加一个已建立的外部连接（来自配对流程）
    pub async fn add_peer_connection(
        &self,
        peer_id: String,
        peer_name: String,
        address: String,
        stream: TcpStream,
    ) -> Result<()> {
        let (read_half, write_half) = tokio::io::split(stream);
        let (write_tx, write_rx) = mpsc::channel::<Vec<u8>>(256);

        {
            let mut conns = self.connections.write().await;
            conns.insert(
                peer_id.clone(),
                PeerConnection {
                    peer_id: peer_id.clone(),
                    peer_name: peer_name.clone(),
                    address: address.clone(),
                    state: ConnectionState::Connected,
                    write_tx: Some(write_tx),
                },
            );
        }

        // 启动写任务
        let peer_w = peer_id.clone();
        let conns_w = self.connections.clone();
        tokio::spawn(async move {
            peer_writer(write_half, write_rx).await;
            let mut conns = conns_w.write().await;
            if let Some(conn) = conns.get_mut(&peer_w) {
                conn.state = ConnectionState::Disconnected;
                conn.write_tx = None;
            }
        });

        // 启动读任务
        let conns_r = self.connections.clone();
        let peer_r = peer_id.clone();
        let tx = self.action_tx.clone();
        let reader_addr = address.clone();
        tokio::spawn(async move {
            peer_reader(read_half, &reader_addr, tx).await;
            let mut conns = conns_r.write().await;
            if let Some(conn) = conns.get_mut(&peer_r) {
                conn.state = ConnectionState::Disconnected;
            }
            warn!("Peer '{}' disconnected", peer_r);
        });

        info!("Added peer connection: '{}' at {}", peer_name, address);
        Ok(())
    }

    /// 获取所有连接状态
    pub async fn get_connection_states(&self) -> HashMap<String, ConnectionState> {
        let conns = self.connections.read().await;
        conns
            .iter()
            .map(|(id, conn)| (id.clone(), conn.state.clone()))
            .collect()
    }

    /// 停止网络服务
    pub async fn stop(&self) {
        *self.running.lock().await = false;
        let mut conns = self.connections.write().await;
        for (_, conn) in conns.iter_mut() {
            conn.state = ConnectionState::Disconnected;
            conn.write_tx = None; // 关闭写通道
        }
        info!("Network manager stopped");
    }
}

/// 持续从 TCP 流读取消息并转发给 Daemon
async fn read_message_loop(
    stream: &mut TcpStream,
    action_tx: &Option<mpsc::UnboundedSender<SyncAction>>,
    peer_addr: &str,
) {
    loop {
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 100 * 1024 * 1024 {
            break;
        }
        let mut msg_buf = vec![0u8; msg_len];
        if stream.read_exact(&mut msg_buf).await.is_err() {
            break;
        }
        if let Ok(message) = Message::from_bytes(&msg_buf) {
            if let Some(ref tx) = action_tx {
                forward_to_daemon(tx, &message, peer_addr);
            }
        }
    }
}

/// 处理传入连接
async fn handle_incoming_connection(
    mut stream: TcpStream,
    _connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    config: Arc<CrossBagConfig>,
    action_tx: Option<mpsc::UnboundedSender<SyncAction>>,
) {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    info!("Connection from {}", peer_addr);

    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return;
    }

    let msg_len = u32::from_be_bytes(len_buf) as usize;
    if msg_len > 100 * 1024 * 1024 {
        error!("Message too large from {}: {}", peer_addr, msg_len);
        return;
    }

    let mut msg_buf = vec![0u8; msg_len];
    if stream.read_exact(&mut msg_buf).await.is_err() {
        return;
    }

    match Message::from_bytes(&msg_buf) {
        Ok(Message::Handshake(handshake)) => {
            info!(
                "Handshake from {} ({}) protocol v{}",
                handshake.hostname, handshake.node_name, handshake.protocol_version
            );

            // 检查协议版本
            let accepted = handshake.protocol_version == crate::protocol::PROTOCOL_VERSION;
            let ack = Message::HandshakeAck(crate::protocol::HandshakeAck {
                accepted,
                node_id: config.node.node_id,
                node_name: config.node.name.clone(),
                message: if accepted {
                    None
                } else {
                    Some("Protocol version mismatch".into())
                },
            });

            if let Ok(ack_bytes) = ack.to_bytes() {
                let len = ack_bytes.len() as u32;
                let mut framed = Vec::with_capacity(4 + ack_bytes.len());
                framed.extend_from_slice(&len.to_be_bytes());
                framed.extend_from_slice(&ack_bytes);
                let _ = stream.write_all(&framed).await;
            }

            if accepted {
                // 入站连接读取循环
                read_message_loop(&mut stream, &action_tx, &peer_addr).await;
            }
        }
        Ok(other) => {
            if let Some(ref tx) = action_tx {
                forward_to_daemon(tx, &other, &peer_addr);
            }
            // 继续读取后续消息
            read_message_loop(&mut stream, &action_tx, &peer_addr).await;
        }
        Err(e) => {
            error!("Failed to decode from {}: {}", peer_addr, e);
        }
    }
}

/// 将网络消息转发给 Daemon
fn forward_to_daemon(tx: &mpsc::UnboundedSender<SyncAction>, msg: &Message, peer_id: &str) {
    match msg {
        Message::FileIndex(index) => {
            let _ = tx.send(SyncAction::RemoteIndex {
                pair_id: index.pair_id.clone(),
                peer_id: peer_id.to_string(),
                index: index.clone(),
            });
            debug!("Forwarded FileIndex from {} to daemon", peer_id);
        }
        Message::FileRequest(req) => {
            let _ = tx.send(SyncAction::RemoteFileRequest {
                pair_id: req.pair_id.clone(),
                peer_id: peer_id.to_string(),
                files: req.files.clone(),
            });
            debug!("Forwarded FileRequest from {} to daemon", peer_id);
        }
        _ => {
            // 其他消息类型暂不处理
        }
    }
}

/// TCP 连接 + 发送握手 + 等待 HandshakeAck
async fn connect_and_handshake(
    addr: &str,
    connect_timeout: u64,
    config: &CrossBagConfig,
) -> Result<TcpStream> {
    let stream = timeout(
        Duration::from_secs(connect_timeout),
        TcpStream::connect(addr),
    )
    .await
    .context("Connection timed out")?
    .with_context(|| format!("Failed to connect to {}", addr))?;

    // 构建握手消息
    let handshake = Message::Handshake(crate::protocol::Handshake {
        protocol_version: crate::protocol::PROTOCOL_VERSION,
        node_id: config.node.node_id,
        node_name: config.node.name.clone(),
        hostname: hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_default(),
    });

    // 发送握手帧 (4字节长度前缀 + 消息体)
    let payload = handshake
        .to_bytes()
        .context("Failed to serialize handshake")?;
    let len = payload.len() as u32;
    let (mut reader, mut writer) = stream.into_split();

    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&payload);
    writer
        .write_all(&framed)
        .await
        .context("Failed to send handshake")?;

    // 读取 HandshakeAck
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .context("Failed to read handshake ack length")?;

    let ack_len = u32::from_be_bytes(len_buf) as usize;
    if ack_len > 10 * 1024 * 1024 {
        anyhow::bail!("Handshake ack too large: {} bytes", ack_len);
    }
    let mut ack_buf = vec![0u8; ack_len];
    reader
        .read_exact(&mut ack_buf)
        .await
        .context("Failed to read handshake ack body")?;

    let ack = Message::from_bytes(&ack_buf).context("Failed to decode handshake ack")?;
    match ack {
        Message::HandshakeAck(h) if h.accepted => {
            info!("Handshake accepted by peer at {}", addr);
        }
        Message::HandshakeAck(h) => {
            anyhow::bail!("Handshake rejected: {:?}", h.message);
        }
        other => {
            anyhow::bail!(
                "Expected HandshakeAck, got {:?}",
                std::mem::discriminant(&other)
            );
        }
    }

    // 将 split 的读写端重新合并为 stream
    let stream = writer
        .reunite(reader)
        .map_err(|_| anyhow::anyhow!("Failed to reunite stream"))?;
    Ok(stream)
}

/// 从 stream 读取端持续读取消息
async fn peer_reader(
    mut reader: tokio::io::ReadHalf<TcpStream>,
    peer_addr: &str,
    action_tx: Option<mpsc::UnboundedSender<SyncAction>>,
) {
    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() {
            break;
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 100 * 1024 * 1024 {
            error!("Message too large from {}", peer_addr);
            break;
        }

        let mut msg_buf = vec![0u8; msg_len];
        if reader.read_exact(&mut msg_buf).await.is_err() {
            break;
        }

        match Message::from_bytes(&msg_buf) {
            Ok(message) => {
                debug!(
                    "Received from {}: {:?}",
                    peer_addr,
                    std::mem::discriminant(&message)
                );
                if let Some(ref tx) = action_tx {
                    forward_to_daemon(tx, &message, peer_addr);
                }
            }
            Err(e) => {
                error!("Failed to decode from {}: {}", peer_addr, e);
            }
        }
    }
}

/// 从写通道消费消息并写入 TCP
async fn peer_writer(mut writer: tokio::io::WriteHalf<TcpStream>, mut rx: mpsc::Receiver<Vec<u8>>) {
    while let Some(data) = rx.recv().await {
        if writer.write_all(&data).await.is_err() {
            break;
        }
    }
}
