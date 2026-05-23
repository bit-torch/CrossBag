//! 配对码（Pairing Code）模块
//!
//! 实现两台机器之间的零配置配对连接。配对码编码了 Easytier 组网所需信息，
//! 使接收方能够自动加入同一虚拟网络并完成认证握手。
//!
//! # 配对码编码
//!
//! 18 字节 = 物理IP(4B) + 端口(2B) + 网络名Hash(4B) + 密钥Hash(4B) + 认证令牌(4B)
//!
//! 使用 Crockford Base32 编码，输出格式：`XXXXX-XXXXX-XXXXX-XXXXX-XXXXX`
//!
//! # 流程
//!
//! ```text
//! 机器 A: crossbag start-connect → 启动 Easytier → 生成配对码 → 等待连接
//! 机器 B: crossbag connect <code> → 解码 → 启动 Easytier 加入网络 → 认证握手
//! ```

use crate::config::CrossBagConfig;
use anyhow::{Context, Result};
use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

/// 配对码编码的字节数
const PAIRING_CODE_BYTES: usize = 18;

/// 配对码输出分组大小
const GROUP_SIZE: usize = 5;

// ============================================================
// Crockford Base32 编解码
// ============================================================

/// Crockford Base32 编码字符表（排除 I/L/O/U，无歧义）
const BASE32_CHARS: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Crockford Base32 解码表（256 字节查找表，无效字符映射为 0xFF）
const BASE32_DECODE: [u8; 256] = {
    let mut table = [0xFFu8; 256];
    let mut i = 0;
    while i < 32 {
        let ch = BASE32_CHARS[i];
        table[ch as usize] = i as u8;
        // 小写映射
        if ch >= b'A' && ch <= b'Z' {
            table[(ch + 32) as usize] = i as u8;
        }
        i += 1;
    }
    // 特殊映射: i/I -> 1, l/L -> 1, o/O -> 0
    table[b'i' as usize] = 1;
    table[b'I' as usize] = 1;
    table[b'l' as usize] = 1;
    table[b'L' as usize] = 1;
    table[b'o' as usize] = 0;
    table[b'O' as usize] = 0;
    table
};

/// Crockford Base32 编码
fn base32_encode(data: &[u8]) -> String {
    let mut result = String::new();
    let mut bits = 0u64;
    let mut n_bits = 0u32;

    for &byte in data {
        bits = (bits << 8) | (byte as u64);
        n_bits += 8;
        while n_bits >= 5 {
            n_bits -= 5;
            let idx = ((bits >> n_bits) & 0x1F) as usize;
            result.push(BASE32_CHARS[idx] as char);
        }
    }

    if n_bits > 0 {
        let idx = ((bits << (5 - n_bits)) & 0x1F) as usize;
        result.push(BASE32_CHARS[idx] as char);
    }

    result
}

/// Crockford Base32 解码
fn base32_decode(input: &str) -> Result<Vec<u8>> {
    let cleaned: String = input.chars().filter(|c| *c != '-').collect();

    if cleaned.is_empty() {
        anyhow::bail!("Empty pairing code");
    }

    let mut bits = 0u64;
    let mut n_bits = 0u32;
    let mut result = Vec::new();

    for ch in cleaned.chars() {
        let val = BASE32_DECODE[ch as usize];
        if val == 0xFF {
            anyhow::bail!("Invalid character in pairing code: '{}'", ch);
        }
        bits = (bits << 5) | (val as u64);
        n_bits += 5;
        if n_bits >= 8 {
            n_bits -= 8;
            result.push(((bits >> n_bits) & 0xFF) as u8);
        }
    }

    Ok(result)
}

/// 格式化配对码：每 GROUP_SIZE 个字符加一个 `-`
fn format_pairing_code(raw: &str) -> String {
    raw.chars()
        .enumerate()
        .fold(String::new(), |mut acc, (i, c)| {
            if i > 0 && i % GROUP_SIZE == 0 {
                acc.push('-');
            }
            acc.push(c);
            acc
        })
}

// ============================================================
// PairingCode
// ============================================================

/// 配对码结构
///
/// 编码 18 字节：物理IP(4) + Easytier端口(2) + 网络名Hash(4) + 密钥Hash(4) + 认证令牌(4)
#[derive(Debug, Clone)]
pub struct PairingCode {
    /// A 的物理可达 IP（0.0.0.0 表示不可直连，需通过共享节点发现）
    physical_ip: [u8; 4],
    /// A 的 Easytier 监听端口（默认 11010）
    easytier_port: u16,
    /// network-name 的 BLAKE3 前 4 字节
    network_name_hash: [u8; 4],
    /// network-secret 的 BLAKE3 前 4 字节
    network_secret_hash: [u8; 4],
    /// 随机认证令牌
    auth_token: [u8; 4],
}

impl PairingCode {
    /// 生成新的配对码
    pub fn generate(
        physical_ip: [u8; 4],
        easytier_port: u16,
        network_name: &str,
        network_secret: &str,
    ) -> Result<Self> {
        let mut auth_token = [0u8; 4];
        getrandom::getrandom(&mut auth_token)
            .map_err(|e| anyhow::anyhow!("Failed to generate random auth token: {}", e))?;

        let name_hash = blake3_hash4(network_name);
        let secret_hash = blake3_hash4(network_secret);

        Ok(PairingCode {
            physical_ip,
            easytier_port,
            network_name_hash: name_hash,
            network_secret_hash: secret_hash,
            auth_token,
        })
    }

    /// 从字符串解码配对码
    pub fn decode(code: &str) -> Result<Self> {
        let bytes = base32_decode(code)?;

        if bytes.len() != PAIRING_CODE_BYTES {
            anyhow::bail!(
                "Invalid pairing code length: expected {} bytes, got {}",
                PAIRING_CODE_BYTES,
                bytes.len()
            );
        }

        let physical_ip: [u8; 4] = bytes[0..4].try_into().unwrap();
        let easytier_port = u16::from_be_bytes([bytes[4], bytes[5]]);
        let network_name_hash: [u8; 4] = bytes[6..10].try_into().unwrap();
        let network_secret_hash: [u8; 4] = bytes[10..14].try_into().unwrap();
        let auth_token: [u8; 4] = bytes[14..18].try_into().unwrap();

        Ok(PairingCode {
            physical_ip,
            easytier_port,
            network_name_hash,
            network_secret_hash,
            auth_token,
        })
    }

    /// 编码为可读字符串
    pub fn encode(&self) -> String {
        let mut buf = Vec::with_capacity(PAIRING_CODE_BYTES);
        buf.extend_from_slice(&self.physical_ip);
        buf.extend_from_slice(&self.easytier_port.to_be_bytes());
        buf.extend_from_slice(&self.network_name_hash);
        buf.extend_from_slice(&self.network_secret_hash);
        buf.extend_from_slice(&self.auth_token);

        let raw = base32_encode(&buf);
        format_pairing_code(&raw)
    }

    /// 是否包含可达的物理 IP
    pub fn has_physical_ip(&self) -> bool {
        self.physical_ip != [0, 0, 0, 0]
    }

    /// 获取物理 IP 地址字符串
    pub fn physical_ip_str(&self) -> String {
        Ipv4Addr::from(self.physical_ip).to_string()
    }

    /// 获取 Easytier peer URL（用于 --peers 参数）
    pub fn peer_url(&self) -> String {
        format!("tcp://{}:{}", self.physical_ip_str(), self.easytier_port)
    }

    /// 获取 Easytier 监听端口
    pub fn easytier_port(&self) -> u16 {
        self.easytier_port
    }

    /// 获取认证令牌
    pub fn auth_token(&self) -> [u8; 4] {
        self.auth_token
    }

    /// 验证网络名和密钥是否匹配
    pub fn verify_network(&self, name: &str, secret: &str) -> bool {
        let name_hash = blake3_hash4(name);
        let secret_hash = blake3_hash4(secret);
        self.network_name_hash == name_hash && self.network_secret_hash == secret_hash
    }
}

/// BLAKE3 哈希取前 4 字节
fn blake3_hash4(input: &str) -> [u8; 4] {
    let hash = blake3::hash(input.as_bytes());
    let mut result = [0u8; 4];
    result.copy_from_slice(&hash.as_bytes()[..4]);
    result
}

// ============================================================
// PeerInfo - 配对成功后保存的节点信息
// ============================================================

/// 配对成功后获得的远端节点信息
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: Uuid,
    pub node_name: String,
    pub hostname: String,
    pub virtual_ip: String,
}

// ============================================================
// PairingListener - 机器 A 端
// ============================================================

/// 配对监听器（机器 A 运行 start-connect 时使用）
pub struct PairingListener {
    config: Arc<CrossBagConfig>,
    auth_token: [u8; 4],
}

impl PairingListener {
    pub fn new(config: Arc<CrossBagConfig>) -> Self {
        PairingListener {
            config,
            auth_token: [0u8; 4],
        }
    }

    /// 生成配对码
    pub fn generate_code(&mut self, physical_ip: [u8; 4]) -> Result<String> {
        let easytier_port = self
            .config
            .easytier
            .listeners
            .first()
            .and_then(|l| {
                if let Some(colon_pos) = l.rfind(':') {
                    l[colon_pos + 1..].parse().ok()
                } else {
                    l.parse().ok()
                }
            })
            .unwrap_or(11010);

        let code = PairingCode::generate(
            physical_ip,
            easytier_port,
            &self.config.easytier.network_name,
            &self.config.easytier.network_secret,
        )?;

        self.auth_token = code.auth_token();

        Ok(code.encode())
    }

    /// 等待配对连接
    pub async fn wait_for_pairing(&self, timeout_duration: Duration) -> Result<PeerInfo> {
        let listen_addr = format!("{}:{}", self.config.node.listen_addr, self.config.node.port);
        let listener = TcpListener::bind(&listen_addr)
            .await
            .with_context(|| format!("Failed to bind pairing listener to {}", listen_addr))?;

        info!("Pairing listener waiting on {}", listen_addr);

        let result = timeout(timeout_duration, async {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("Pairing connection from {}", addr);
                        match self.handle_pairing_connection(stream).await {
                            Ok(peer_info) => return Ok(peer_info),
                            Err(e) => {
                                warn!("Pairing failed from {}: {}", addr, e);
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                        continue;
                    }
                }
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => anyhow::bail!("Pairing timed out after {}s", timeout_duration.as_secs()),
        }
    }

    /// 处理单个配对连接
    async fn handle_pairing_connection(&self, mut stream: TcpStream) -> Result<PeerInfo> {
        use crate::protocol::{Message, PairResponse};

        // 读取第一条消息
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .context("Failed to read pairing message length")?;

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 10 * 1024 * 1024 {
            anyhow::bail!("Pairing message too large: {} bytes", msg_len);
        }

        let mut msg_buf = vec![0u8; msg_len];
        stream
            .read_exact(&mut msg_buf)
            .await
            .context("Failed to read pairing message body")?;

        let message = Message::from_bytes(&msg_buf).context("Failed to decode pairing message")?;

        match message {
            Message::PairRequest(req) => {
                // 验证 auth_token
                if req.auth_token != self.auth_token {
                    let ack = Message::PairResponse(PairResponse {
                        accepted: false,
                        node_id: self.config.node.node_id,
                        node_name: self.config.node.name.clone(),
                        virtual_ip: None,
                        message: Some("Auth token mismatch".into()),
                    });
                    send_message(&mut stream, &ack).await?;
                    anyhow::bail!("Auth token mismatch");
                }

                // 验证协议版本
                if req.protocol_version != crate::protocol::PROTOCOL_VERSION {
                    let ack = Message::PairResponse(PairResponse {
                        accepted: false,
                        node_id: self.config.node.node_id,
                        node_name: self.config.node.name.clone(),
                        virtual_ip: None,
                        message: Some("Protocol version mismatch".into()),
                    });
                    send_message(&mut stream, &ack).await?;
                    anyhow::bail!("Protocol version mismatch");
                }

                info!(
                    "Pairing accepted: '{}' (host: {})",
                    req.node_name, req.hostname
                );

                // 获取本机虚拟 IP（重试等待 Easytier 就绪）
                let virtual_ip = {
                    let mut ip = None;
                    for attempt in 0..10 {
                        if let Ok(vip) = get_easytier_virtual_ip().await {
                            ip = Some(vip);
                            break;
                        }
                        if attempt < 9 {
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                    ip
                };

                // 发送 PairResponse
                let response = Message::PairResponse(PairResponse {
                    accepted: true,
                    node_id: self.config.node.node_id,
                    node_name: self.config.node.name.clone(),
                    virtual_ip: virtual_ip.map(|ip| ip.to_string()),
                    message: None,
                });
                send_message(&mut stream, &response).await?;

                // 继续标准 Handshake（对方会发 Handshake）
                let mut len_buf = [0u8; 4];
                if stream.read_exact(&mut len_buf).await.is_err() {
                    return Ok(PeerInfo {
                        node_id: req.node_id,
                        node_name: req.node_name,
                        hostname: req.hostname,
                        virtual_ip: String::new(),
                    });
                }

                let hs_len = u32::from_be_bytes(len_buf) as usize;
                if hs_len > 10 * 1024 * 1024 {
                    return Ok(PeerInfo {
                        node_id: req.node_id,
                        node_name: req.node_name,
                        hostname: req.hostname,
                        virtual_ip: String::new(),
                    });
                }

                let mut hs_buf = vec![0u8; hs_len];
                if stream.read_exact(&mut hs_buf).await.is_err() {
                    return Ok(PeerInfo {
                        node_id: req.node_id,
                        node_name: req.node_name,
                        hostname: req.hostname,
                        virtual_ip: String::new(),
                    });
                }

                if let Ok(Message::Handshake(_hs)) = Message::from_bytes(&hs_buf) {
                    let hs_ack = Message::HandshakeAck(crate::protocol::HandshakeAck {
                        accepted: true,
                        node_id: self.config.node.node_id,
                        node_name: self.config.node.name.clone(),
                        message: None,
                    });
                    send_message(&mut stream, &hs_ack).await?;
                    info!("Handshake completed with '{}'", req.node_name);
                }

                Ok(PeerInfo {
                    node_id: req.node_id,
                    node_name: req.node_name,
                    hostname: req.hostname,
                    virtual_ip: virtual_ip.map(|ip| ip.to_string()).unwrap_or_default(),
                })
            }
            other => {
                anyhow::bail!(
                    "Expected PairRequest, got {:?}",
                    std::mem::discriminant(&other)
                );
            }
        }
    }
}

// ============================================================
// PairingConnector - 机器 B 端
// ============================================================

/// 配对连接器（机器 B 运行 connect 时使用）
pub struct PairingConnector {
    config: Arc<CrossBagConfig>,
}

impl PairingConnector {
    pub fn new(config: Arc<CrossBagConfig>) -> Self {
        PairingConnector { config }
    }

    /// 使用配对码连接到远端节点
    pub async fn connect(&self, code: &PairingCode, connect_timeout: Duration) -> Result<PeerInfo> {
        // 查找远端虚拟 IP
        let peer_virtual_ip = self.find_peer_virtual_ip().await?;

        let addr = format!("{}:{}", peer_virtual_ip, self.config.node.port);
        info!("Connecting to peer at {}", addr);

        let mut stream = timeout(connect_timeout, TcpStream::connect(&addr))
            .await
            .context("Connection timed out")?
            .with_context(|| format!("Failed to connect to {}", addr))?;

        // 发送 PairRequest
        let pair_req = crate::protocol::Message::PairRequest(crate::protocol::PairRequest {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            auth_token: code.auth_token(),
            node_id: self.config.node.node_id,
            node_name: self.config.node.name.clone(),
            hostname: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_default(),
        });

        send_message(&mut stream, &pair_req).await?;

        // 读取 PairResponse
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .context("Failed to read PairResponse length")?;

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 10 * 1024 * 1024 {
            anyhow::bail!("PairResponse too large: {} bytes", msg_len);
        }

        let mut msg_buf = vec![0u8; msg_len];
        stream
            .read_exact(&mut msg_buf)
            .await
            .context("Failed to read PairResponse body")?;

        let response = crate::protocol::Message::from_bytes(&msg_buf)
            .context("Failed to decode PairResponse")?;

        match response {
            crate::protocol::Message::PairResponse(resp) => {
                if !resp.accepted {
                    anyhow::bail!(
                        "Pairing rejected: {}",
                        resp.message.unwrap_or_else(|| "Unknown reason".into())
                    );
                }

                info!("Pairing accepted by '{}'", resp.node_name);

                // 继续标准 Handshake
                let handshake = crate::protocol::Message::Handshake(crate::protocol::Handshake {
                    protocol_version: crate::protocol::PROTOCOL_VERSION,
                    node_id: self.config.node.node_id,
                    node_name: self.config.node.name.clone(),
                    hostname: hostname::get()
                        .map(|h| h.to_string_lossy().to_string())
                        .unwrap_or_default(),
                });

                send_message(&mut stream, &handshake).await?;

                // 读取 HandshakeAck
                let mut ack_len_buf = [0u8; 4];
                stream
                    .read_exact(&mut ack_len_buf)
                    .await
                    .context("Failed to read HandshakeAck length")?;

                let ack_len = u32::from_be_bytes(ack_len_buf) as usize;
                if ack_len > 10 * 1024 * 1024 {
                    anyhow::bail!("HandshakeAck too large");
                }

                let mut ack_buf = vec![0u8; ack_len];
                stream
                    .read_exact(&mut ack_buf)
                    .await
                    .context("Failed to read HandshakeAck body")?;

                if let Ok(crate::protocol::Message::HandshakeAck(ack)) =
                    crate::protocol::Message::from_bytes(&ack_buf)
                {
                    if !ack.accepted {
                        anyhow::bail!("Handshake rejected: {:?}", ack.message);
                    }
                    info!("Handshake completed with '{}'", resp.node_name);
                }

                Ok(PeerInfo {
                    node_id: resp.node_id,
                    node_name: resp.node_name,
                    hostname: String::new(),
                    virtual_ip: resp.virtual_ip.unwrap_or_default(),
                })
            }
            other => {
                anyhow::bail!(
                    "Expected PairResponse, got {:?}",
                    std::mem::discriminant(&other)
                );
            }
        }
    }

    /// 查找远端节点的虚拟 IP
    async fn find_peer_virtual_ip(&self) -> Result<Ipv4Addr> {
        // 策略 1: 尝试 easytier-cli 查询网络中的其他节点
        if let Ok(ip) = find_peer_via_easytier_cli().await {
            return Ok(ip);
        }

        // 策略 2: 获取本机虚拟 IP，让 Easytier 路由
        if let Ok(my_ip) = get_easytier_virtual_ip().await {
            warn!("Could not find peer via easytier-cli, using own virtual IP subnet");
            return Ok(my_ip);
        }

        anyhow::bail!(
            "Could not determine peer virtual IP. Ensure Easytier network is established."
        )
    }
}

// ============================================================
// 辅助函数
// ============================================================

/// 发送消息（4字节长度前缀 + 消息体）
async fn send_message(stream: &mut TcpStream, message: &crate::protocol::Message) -> Result<()> {
    let payload = message.to_bytes().context("Failed to serialize message")?;
    let len = payload.len() as u32;
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&payload);
    stream
        .write_all(&framed)
        .await
        .context("Failed to send message")?;
    Ok(())
}

/// 获取本机的 Easytier 虚拟 IP
pub async fn get_easytier_virtual_ip() -> Result<Ipv4Addr> {
    // 尝试 easytier-cli
    let output = tokio::process::Command::new("easytier-cli")
        .arg("node")
        .arg("list")
        .output()
        .await;

    if let Ok(out) = output {
        if out.status.success() {
            let stdout = String::from_utf8_lossy(&out.stdout);
            if let Some(ip) = parse_virtual_ip_from_cli(&stdout) {
                return Ok(ip);
            }
        }
    }

    // 回退：枚举网卡找 Easytier 虚拟 IP
    find_easytier_ip_from_interfaces()
}

/// 从 easytier-cli 输出解析虚拟 IP
fn parse_virtual_ip_from_cli(output: &str) -> Option<Ipv4Addr> {
    for line in output.lines() {
        if line.contains("10.144.") || line.contains("10.0.0.") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            for part in parts {
                if let Ok(ip) = part.parse::<Ipv4Addr>() {
                    let octets = ip.octets();
                    if octets[0] == 10 {
                        return Some(ip);
                    }
                }
            }
        }
    }
    None
}

/// 从网络接口枚举找 Easytier 虚拟 IP
fn find_easytier_ip_from_interfaces() -> Result<Ipv4Addr> {
    #[cfg(windows)]
    {
        let output = std::process::Command::new("ipconfig")
            .output()
            .context("Failed to run ipconfig")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_easytier_ip_windows(&stdout)
    }

    #[cfg(not(windows))]
    {
        let output = std::process::Command::new("ip")
            .args(["addr", "show"])
            .output()
            .context("Failed to run ip addr")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_easytier_ip_unix(&stdout)
    }
}

#[cfg(windows)]
fn parse_easytier_ip_windows(output: &str) -> Result<Ipv4Addr> {
    let mut found_easytier = false;
    for line in output.lines() {
        let line_lower = line.to_lowercase();
        if line_lower.contains("easytier") || line_lower.contains("tun") {
            found_easytier = true;
        }
        if found_easytier && line.contains("IPv4") {
            if let Some(ip_str) = line.split(':').nth(1) {
                let ip_str = ip_str.trim().trim_end_matches('.');
                if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                    return Ok(ip);
                }
            }
        }
    }
    anyhow::bail!("Could not find Easytier virtual IP from ipconfig")
}

#[cfg(not(windows))]
fn parse_easytier_ip_unix(output: &str) -> Result<Ipv4Addr> {
    for line in output.lines() {
        if line.contains("inet ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let ip_part = parts[1];
                if let Some(ip_str) = ip_part.split('/').next() {
                    if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                        let octets = ip.octets();
                        if octets[0] == 10 {
                            return Ok(ip);
                        }
                    }
                }
            }
        }
    }
    anyhow::bail!("Could not find Easytier virtual IP from ip addr")
}

/// 通过 easytier-cli 查找网络中的其他节点
async fn find_peer_via_easytier_cli() -> Result<Ipv4Addr> {
    let output = tokio::process::Command::new("easytier-cli")
        .arg("node")
        .arg("list")
        .output()
        .await
        .context("Failed to run easytier-cli")?;

    if !output.status.success() {
        anyhow::bail!("easytier-cli failed");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let my_ip = get_easytier_virtual_ip().await.ok();

    for line in stdout.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        for part in parts {
            if let Ok(ip) = part.parse::<Ipv4Addr>() {
                let octets = ip.octets();
                if octets[0] == 10 {
                    if let Some(my) = my_ip {
                        if ip == my {
                            continue;
                        }
                    }
                    return Ok(ip);
                }
            }
        }
    }

    anyhow::bail!("No peer found in easytier-cli output")
}

/// 获取本机的物理 IP（非虚拟 IP、非回环）
pub fn get_physical_ip() -> Result<Ipv4Addr> {
    // 优先使用配置中手动指定的物理 IP
    // (调用方应该先检查 config.node.physical_ip)

    #[cfg(windows)]
    {
        let output = std::process::Command::new("ipconfig")
            .output()
            .context("Failed to run ipconfig")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        find_physical_ip_windows(&stdout)
    }

    #[cfg(not(windows))]
    {
        let output = std::process::Command::new("ip")
            .args(["addr", "show"])
            .output()
            .context("Failed to run ip addr")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        find_physical_ip_unix(&stdout)
    }
}

#[cfg(windows)]
fn find_physical_ip_windows(output: &str) -> Result<Ipv4Addr> {
    let mut found_ethernet_or_wifi = false;
    let mut best_ip: Option<Ipv4Addr> = None;

    for line in output.lines() {
        let line_lower = line.to_lowercase();
        if line_lower.contains("ethernet")
            || line_lower.contains("wi-fi")
            || line_lower.contains("wireless")
            || line_lower.contains("以太网")
            || line_lower.contains("无线")
        {
            found_ethernet_or_wifi = true;
        }
        if line_lower.contains("easytier")
            || line_lower.contains("tun")
            || line_lower.contains("vpn")
        {
            found_ethernet_or_wifi = false;
        }
        if found_ethernet_or_wifi && line.contains("IPv4") {
            if let Some(ip_str) = line.split(':').nth(1) {
                let ip_str = ip_str.trim().trim_end_matches('.');
                if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                    if !ip.is_loopback() && !is_easytier_ip(ip) {
                        best_ip = Some(ip);
                    }
                }
            }
        }
    }

    best_ip.context("Could not find physical IP")
}

#[cfg(not(windows))]
fn find_physical_ip_unix(output: &str) -> Result<Ipv4Addr> {
    let mut best_ip: Option<Ipv4Addr> = None;

    for line in output.lines() {
        if line.contains("inet ") && !line.contains("127.0.0.1") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Some(ip_str) = parts[1].split('/').next() {
                    if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                        if !ip.is_loopback() && !is_easytier_ip(ip)
                            && (best_ip.is_none() || is_lan_ip(ip))
                        {
                            best_ip = Some(ip);
                        }
                    }
                }
            }
        }
    }

    best_ip.context("Could not find physical IP")
}

/// 判断是否为 Easytier 虚拟 IP
fn is_easytier_ip(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 10 && octets[1] == 144
}

/// 判断是否为局域网 IP
fn is_lan_ip(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    (octets[0] == 192 && octets[1] == 168)
        || octets[0] == 10
        || (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31)
}

/// 将配对成功的 peer 保存到配置文件
pub fn save_peer_to_config(config_path: &Path, peer_info: &PeerInfo) -> Result<()> {
    let mut config = CrossBagConfig::load(config_path)?;

    let peer_id = peer_info.node_id.to_string();
    let peer_config = crate::config::PeerConfig {
        name: peer_info.node_name.clone(),
        address: format!("{}:{}", peer_info.virtual_ip, crate::protocol::DEFAULT_PORT),
    };

    config.network.peers.insert(peer_id, peer_config);

    config.save(config_path)?;
    info!("Peer '{}' saved to configuration", peer_info.node_name);
    Ok(())
}

// ============================================================
// tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base32_encode_decode_roundtrip() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE];
        let encoded = base32_encode(&data);
        let decoded = base32_decode(&encoded).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_base32_empty() {
        let encoded = base32_encode(&[]);
        assert_eq!(encoded, "");
    }

    #[test]
    fn test_base32_single_byte() {
        let data = vec![0xFF];
        let encoded = base32_encode(&data);
        let decoded = base32_decode(&encoded).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_base32_invalid_char() {
        let result = base32_decode("HELLO@WORLD");
        assert!(result.is_err());
    }

    #[test]
    fn test_base32_case_insensitive() {
        let upper = base32_encode(&[0x48, 0x65, 0x6C, 0x6C, 0x6F]);
        let decoded_upper = base32_decode(&upper).unwrap();
        let decoded_lower = base32_decode(&upper.to_lowercase()).unwrap();
        assert_eq!(decoded_upper, decoded_lower);
    }

    #[test]
    fn test_pairing_code_roundtrip() {
        let code = PairingCode::generate([192, 168, 1, 100], 11010, "test-network", "test-secret")
            .unwrap();

        let encoded = code.encode();
        let decoded = PairingCode::decode(&encoded).unwrap();

        assert_eq!(code.physical_ip, decoded.physical_ip);
        assert_eq!(code.easytier_port, decoded.easytier_port);
        assert_eq!(code.network_name_hash, decoded.network_name_hash);
        assert_eq!(code.network_secret_hash, decoded.network_secret_hash);
        assert_eq!(code.auth_token, decoded.auth_token);
    }

    #[test]
    fn test_pairing_code_no_physical_ip() {
        let code = PairingCode::generate([0, 0, 0, 0], 11010, "my-net", "my-secret").unwrap();

        assert!(!code.has_physical_ip());

        let encoded = code.encode();
        let decoded = PairingCode::decode(&encoded).unwrap();
        assert!(!decoded.has_physical_ip());
        assert_eq!(decoded.physical_ip, [0, 0, 0, 0]);
    }

    #[test]
    fn test_pairing_code_verify_network() {
        let code = PairingCode::generate([10, 0, 0, 1], 11010, "correct-network", "correct-secret")
            .unwrap();

        assert!(code.verify_network("correct-network", "correct-secret"));
        assert!(!code.verify_network("wrong-network", "correct-secret"));
        assert!(!code.verify_network("correct-network", "wrong-secret"));
        assert!(!code.verify_network("wrong-network", "wrong-secret"));
    }

    #[test]
    fn test_pairing_code_format() {
        let code = PairingCode::generate([192, 168, 1, 1], 11010, "net", "secret").unwrap();

        let encoded = code.encode();
        // 18 bytes → Base32 → 29 chars → 5+5+5+5+5+4 = 6 groups with 5 dashes
        assert_eq!(encoded.matches('-').count(), 5);
        let groups: Vec<&str> = encoded.split('-').collect();
        assert_eq!(groups.len(), 6);
        // 前 5 组每组 5 字符，最后一组 4 字符
        for (i, group) in groups.iter().enumerate() {
            if i < 5 {
                assert_eq!(group.len(), 5);
            } else {
                assert_eq!(group.len(), 4);
            }
        }
    }

    #[test]
    fn test_pairing_code_dashes_optional() {
        let code =
            PairingCode::generate([172, 16, 0, 1], 11010, "dash-test", "dash-secret").unwrap();

        let with_dashes = code.encode();
        let without_dashes = with_dashes.replace('-', "");

        let decoded_with = PairingCode::decode(&with_dashes).unwrap();
        let decoded_without = PairingCode::decode(&without_dashes).unwrap();

        assert_eq!(decoded_with.auth_token, decoded_without.auth_token);
        assert_eq!(decoded_with.physical_ip, decoded_without.physical_ip);
    }

    #[test]
    fn test_pairing_code_peer_url() {
        let code = PairingCode::generate([192, 168, 1, 100], 11010, "net", "secret").unwrap();

        assert_eq!(code.peer_url(), "tcp://192.168.1.100:11010");
    }

    #[test]
    fn test_blake3_hash4_deterministic() {
        let h1 = blake3_hash4("hello");
        let h2 = blake3_hash4("hello");
        assert_eq!(h1, h2);

        let h3 = blake3_hash4("world");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_format_pairing_code() {
        let raw = "ABCDE12345FGHIJ";
        let formatted = format_pairing_code(raw);
        assert_eq!(formatted, "ABCDE-12345-FGHIJ");
    }
}
