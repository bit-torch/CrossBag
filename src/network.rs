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
use uuid::Uuid;

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
    stream: Option<TcpStream>,
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
}

impl NetworkManager {
    /// 创建网络管理器
    pub fn new(config: Arc<CrossBagConfig>) -> Self {
        NetworkManager {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
            action_tx: None,
        }
    }

    /// 设置 Daemon 消息通道
    pub fn set_action_sender(&mut self, tx: mpsc::UnboundedSender<SyncAction>) {
        self.action_tx = Some(tx);
    }

    /// 启动网络服务 (监听 + 连接)
    pub async fn start(&self) -> Result<()> {
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
                        stream: None,
                    },
                );
            }
        }

        // 启动 TCP 监听
        let listen_addr = format!(
            "{}:{}",
            self.config.node.listen_addr, self.config.node.port
        );

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

        Ok(())
    }

    /// 连接到所有配置的对等节点
    async fn connect_to_peers(&self) {
        let connections = self.connections.clone();
        let peers = self.config.network.peers.clone();
        let connect_timeout = self.config.network.connect_timeout;

        tokio::spawn(async move {
            loop {
                let peers_to_connect: Vec<(String, PeerConfig)> = {
                    let conns = connections.read().await;
                    peers
                        .iter()
                        .filter(|(id, _)| {
                            conns.get(*id)
                                .map(|c| c.state != ConnectionState::Connected)
                                .unwrap_or(true)
                        })
                        .map(|(id, cfg)| (id.clone(), cfg.clone()))
                        .collect()
                };

                for (peer_id, peer_config) in peers_to_connect {
                    info!("Connecting to peer {} at {}", peer_id, peer_config.address);

                    // 更新状态为连接中
                    {
                        let mut conns = connections.write().await;
                        if let Some(conn) = conns.get_mut(&peer_id) {
                            conn.state = ConnectionState::Connecting;
                        }
                    }

                    match timeout(
                        Duration::from_secs(connect_timeout),
                        TcpStream::connect(&peer_config.address),
                    )
                    .await
                    {
                        Ok(Ok(stream)) => {
                            info!("Connected to peer {}", peer_id);

                            // 发送握手
                            let handshake = Message::Handshake(
                                crate::protocol::Handshake {
                                    protocol_version: crate::protocol::PROTOCOL_VERSION,
                                    node_id: Uuid::new_v4(), // TODO: use actual node_id
                                    node_name: "crossbag-node".to_string(),
                                    hostname: hostname::get()
                                        .map(|h| h.to_string_lossy().to_string())
                                        .unwrap_or_default(),
                                },
                            );

                            let mut conns = connections.write().await;
                            if let Some(conn) = conns.get_mut(&peer_id) {
                                conn.state = ConnectionState::Connected;
                                conn.stream = Some(stream);
                            }

                            // TODO: Send handshake
                            let _ = handshake;
                        }
                        Ok(Err(e)) => {
                            warn!("Failed to connect to peer {}: {}", peer_id, e);
                            let mut conns = connections.write().await;
                            if let Some(conn) = conns.get_mut(&peer_id) {
                                conn.state =
                                    ConnectionState::Failed(format!("Connection error: {}", e));
                            }
                        }
                        Err(_) => {
                            warn!("Connection to peer {} timed out", peer_id);
                            let mut conns = connections.write().await;
                            if let Some(conn) = conns.get_mut(&peer_id) {
                                conn.state = ConnectionState::Failed("Connection timed out".into());
                            }
                        }
                    }
                }

                // 等待重试间隔
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    /// 启动心跳
    async fn start_heartbeat(&self) {
        let connections = self.connections.clone();
        let interval_secs = self.config.network.heartbeat_interval;
        let node_id = self.config.node.node_id;

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(interval_secs));
            loop {
                ticker.tick().await;

                let heartbeat = Message::Heartbeat(crate::protocol::Heartbeat {
                    node_id,
                    timestamp: chrono::Utc::now(),
                });

                let heartbeat_bytes = match heartbeat.to_bytes() {
                    Ok(b) => b,
                    Err(e) => {
                        error!("Failed to serialize heartbeat: {}", e);
                        continue;
                    }
                };

                // 获取需要发送心跳的 peer 列表
                let connected_peers: Vec<String> = {
                    let conns = connections.read().await;
                    conns.iter()
                        .filter(|(_, c)| c.state == ConnectionState::Connected)
                        .map(|(id, _)| id.clone())
                        .collect()
                };

                // 对每个已连接 peer 发送心跳 (需要写锁来访问 stream)
                for peer_id in connected_peers {
                    let mut conns = connections.write().await;
                    if let Some(conn) = conns.get_mut(&peer_id) {
                        if let Some(ref mut stream) = conn.stream {
                            let len = heartbeat_bytes.len() as u32;
                            let mut framed = Vec::with_capacity(4 + heartbeat_bytes.len());
                            framed.extend_from_slice(&len.to_be_bytes());
                            framed.extend_from_slice(&heartbeat_bytes);
                            if let Err(e) = stream.write_all(&framed).await {
                                warn!("Failed to send heartbeat to {}: {}", peer_id, e);
                                conn.state = ConnectionState::Failed(format!("Heartbeat failed: {}", e));
                            }
                        }
                    }
                }
            }
        });
    }

    /// 发送消息到指定对等节点
    pub async fn send_to_peer(&self, peer_id: &str, message: &Message) -> Result<()> {
        let data = message
            .to_bytes()
            .context("Failed to serialize message")?;

        // 前缀长度 (4 bytes)
        let len = data.len() as u32;
        let mut framed = Vec::with_capacity(4 + data.len());
        framed.extend_from_slice(&len.to_be_bytes());
        framed.extend_from_slice(&data);

        let mut conns = self.connections.write().await;
        if let Some(conn) = conns.get_mut(peer_id) {
            if let Some(ref mut stream) = conn.stream {
                stream
                    .write_all(&framed)
                    .await
                    .context("Failed to send message")?;
                return Ok(());
            }
        }

        anyhow::bail!("Peer {} not connected", peer_id)
    }

    /// 获取所有连接状态
    pub async fn get_connection_states(&self) -> HashMap<String, ConnectionState> {
        let conns = self.connections.read().await;
        conns.iter()
            .map(|(id, conn)| (id.clone(), conn.state.clone()))
            .collect()
    }

    /// 停止网络服务
    pub async fn stop(&self) {
        *self.running.lock().await = false;
        // 关闭所有连接
        let mut conns = self.connections.write().await;
        for (_, conn) in conns.iter_mut() {
            conn.state = ConnectionState::Disconnected;
            conn.stream = None;
        }
        info!("Network manager stopped");
    }
}

/// 处理传入连接
async fn handle_incoming_connection(
    mut stream: TcpStream,
    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    _config: Arc<CrossBagConfig>,
    action_tx: Option<mpsc::UnboundedSender<SyncAction>>,
) {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    info!("Handling connection from {}", peer_addr);

    // 读取消息长度前缀
    let mut len_buf = [0u8; 4];
    if let Err(e) = stream.read_exact(&mut len_buf).await {
        error!("Failed to read message length from {}: {}", peer_addr, e);
        return;
    }

    let msg_len = u32::from_be_bytes(len_buf) as usize;
    if msg_len > 100 * 1024 * 1024 {
        error!("Message too large from {}: {} bytes", peer_addr, msg_len);
        return;
    }

    let mut msg_buf = vec![0u8; msg_len];
    if let Err(e) = stream.read_exact(&mut msg_buf).await {
        error!("Failed to read message from {}: {}", peer_addr, e);
        return;
    }

    match Message::from_bytes(&msg_buf) {
        Ok(Message::Handshake(handshake)) => {
            info!(
                "Received handshake from node {} ({})",
                handshake.node_name, handshake.node_id
            );

            let ack = Message::HandshakeAck(crate::protocol::HandshakeAck {
                accepted: true,
                node_id: Uuid::new_v4(),
                node_name: "crossbag-node".to_string(),
                message: None,
            });

            if let Ok(ack_bytes) = ack.to_bytes() {
                let len = ack_bytes.len() as u32;
                let mut framed = Vec::with_capacity(4 + ack_bytes.len());
                framed.extend_from_slice(&len.to_be_bytes());
                framed.extend_from_slice(&ack_bytes);
                let _ = stream.write_all(&framed).await;
            }

            handle_peer_messages(stream, peer_addr, connections, action_tx).await;
        }
        Ok(other) => {
            // 非握手消息也正常处理 (可能已握手)
            if let Some(ref tx) = action_tx {
                forward_to_daemon(tx, &other, &peer_addr);
            }
            handle_peer_messages(stream, peer_addr, connections, action_tx).await;
        }
        Err(e) => {
            error!("Failed to decode message from {}: {}", peer_addr, e);
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

/// 处理对等节点消息循环
async fn handle_peer_messages(
    mut stream: TcpStream,
    peer_addr: String,
    _connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    action_tx: Option<mpsc::UnboundedSender<SyncAction>>,
) {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) => {
                debug!("Connection closed from {}: {}", peer_addr, e);
                break;
            }
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 100 * 1024 * 1024 {
            error!("Message too large from {}", peer_addr);
            break;
        }

        let mut msg_buf = vec![0u8; msg_len];
        if stream.read_exact(&mut msg_buf).await.is_err() {
            break;
        }

        match Message::from_bytes(&msg_buf) {
            Ok(message) => {
                debug!("Received message from {}: {:?}", peer_addr, message);
                if let Some(ref tx) = action_tx {
                    forward_to_daemon(tx, &message, &peer_addr);
                }
            }
            Err(e) => {
                error!("Failed to decode message from {}: {}", peer_addr, e);
            }
        }
    }
}
