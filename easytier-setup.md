# Easytier 组网配置指南

本文档说明如何配置 Easytier 为 CrossBag 提供虚拟网络环境。

## 什么是 Easytier？

[Easytier](https://github.com/EasyTier/EasyTier) 是一个开源的 P2P 组网工具，可以:
- 在不同网络的机器之间建立加密的虚拟局域网
- 提供类似 VPN 的体验，但基于 WireGuard + QUIC
- 支持 NAT 穿透，无需公网 IP
- 自动发现同一网络内的对等节点

## 安装 Easytier

### Windows

```powershell
# 使用 winget
winget install Easytier.Easytier

# 或从 GitHub Release 下载
# https://github.com/EasyTier/EasyTier/releases
```

### Linux / macOS

```bash
# 使用一键安装脚本
curl -fsSL https://raw.githubusercontent.com/EasyTier/EasyTier/main/script/install.sh | bash

# 或使用 cargo 安装
cargo install easytier
```

### macOS (Homebrew)

```bash
brew install easytier
```

## 配置 Easytier

### 方案一: 手动配置 (推荐)

在每台机器上创建 Easytier 配置文件:

**机器 A (例如: 家里电脑)**

```bash
# ~/.easytier/config.toml
[instance]
instance_name = "home-pc"

[network_identity]
network_name = "crossbag-network"
network_secret = "your-secret-key-here"  # 所有节点使用相同的密钥

[listeners]
# 监听端口
listeners = [
    "tcp://0.0.0.0:11010",
    "udp://0.0.0.0:11010",
    "wg://0.0.0.0:11011",
]

[peer]
# 如果有公网节点，在这里配置
# peers = ["tcp://public-server:11010"]
```

**机器 B (例如: 办公室电脑)**

```bash
# ~/.easytier/config.toml
[instance]
instance_name = "office-pc"

[network_identity]
network_name = "crossbag-network"
network_secret = "your-secret-key-here"  # 相同的密钥

[listeners]
listeners = [
    "tcp://0.0.0.0:11010",
    "udp://0.0.0.0:11010",
    "wg://0.0.0.0:11011",
]
```

### 方案二: 命令行启动

```bash
# 机器 A
easytier-core \
  --instance-name home-pc \
  --network-name crossbag-network \
  --network-secret your-secret-key \
  --listeners tcp://0.0.0.0:11010 udp://0.0.0.0:11010 \
  --dhcp-port 20000

# 机器 B
easytier-core \
  --instance-name office-pc \
  --network-name crossbag-network \
  --network-secret your-secret-key \
  --listeners tcp://0.0.0.0:11010 udp://0.0.0.0:11010 \
  --dhcp-port 20001
```

### 使用 GUI (Windows/macOS)

Easytier 提供图形化界面:

1. 下载 Easytier GUI 版本
2. 在 "网络设置" 中输入网络名称和密钥
3. 点击 "连接"

## 验证连接

启动 Easytier 后，运行:

```bash
# 查看 Easytier 虚拟 IP
easytier-cli node list

# 或查看网络接口
# Linux/macOS
ip addr show easytier0

# Windows
ipconfig | findstr "easytier"
```

在两台机器上互相 ping 对方的 Easytier 虚拟 IP:

```bash
# 在机器 A 上 ping 机器 B 的虚拟 IP
ping 10.20.30.2

# 在机器 B 上 ping 机器 A 的虚拟 IP
ping 10.20.30.1
```

## 配置 CrossBag

Easytier 连接成功后，配置 CrossBag 使用虚拟 IP:

```toml
# crossbag.toml

# 本节点
[node]
name = "home-pc"
listen_addr = "0.0.0.0"  # 或 Easytier 虚拟 IP
port = 9527

# 对等节点 (使用 Easytier 虚拟 IP)
[network.peers.office-pc]
name = "Office PC"
address = "10.20.30.2:9527"  # 对方的 Easytier 虚拟 IP + CrossBag 端口
```

## 端口说明

| 服务 | 端口 | 说明 |
|------|------|------|
| Easytier TCP | 11010 | Easytier 监听端口 |
| Easytier UDP | 11010 | NAT 穿透 |
| Easytier WireGuard | 11011 | 内核级加密隧道 |
| CrossBag | 9527 | CrossBag 同步服务端口 |

## 防火墙设置

确保防火墙允许以下端口:

```bash
# Windows
netsh advfirewall firewall add rule name="Easytier" dir=in action=allow protocol=TCP localport=11010
netsh advfirewall firewall add rule name="CrossBag" dir=in action=allow protocol=TCP localport=9527

# Linux (iptables)
sudo iptables -A INPUT -p tcp --dport 11010 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 9527 -j ACCEPT

# Linux (firewalld)
sudo firewall-cmd --add-port=11010/tcp --permanent
sudo firewall-cmd --add-port=9527/tcp --permanent
sudo firewall-cmd --reload
```

## 自启动配置

### Systemd (Linux)

```ini
# /etc/systemd/system/easytier.service
[Unit]
Description=Easytier Network Service
After=network.target

[Service]
Type=simple
User=your-user
ExecStart=/usr/local/bin/easytier-core --config-path /home/your-user/.easytier/config.toml
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable easytier
sudo systemctl start easytier
```

### Windows 服务

```powershell
# 创建计划任务实现自启动
$action = New-ScheduledTaskAction -Execute "easytier-core.exe" -Argument "--config-path C:\Users\You\.easytier\config.toml"
$trigger = New-ScheduledTaskTrigger -AtLogon
Register-ScheduledTask -TaskName "Easytier" -Action $action -Trigger $trigger
```

## 常见问题

### Q: 两个节点无法互相 ping 通？

1. 检查两边的 Easytier 是否都在运行: `easytier-cli node list`
2. 确认 `network_name` 和 `network_secret` 完全相同
3. 检查防火墙是否放行端口
4. 如果有 NAT 问题，考虑配置中继服务器

### Q: Easytier 连接上了但 CrossBag 连不上？

1. 确认 CrossBag 的 `address` 配置使用的是 Easytier 虚拟 IP
2. 检查 CrossBag 端口 (默认 9527) 是否被占用
3. 确认两边的 CrossBag 都在运行

### Q: 如何更新密钥？

在所有节点上同步更新 `network_secret`，然后重启 Easytier。

## 参考链接

- [Easytier GitHub](https://github.com/EasyTier/EasyTier)
- [Easytier 文档](https://easytier.github.io/)
