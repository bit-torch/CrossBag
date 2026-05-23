# Changelog

All notable changes to CrossBag will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-23

### Added
- Protocol: 10 message types with BLAKE3 hashing (Handshake, FileIndex, FileChunk, etc.)
- Config: TOML-based configuration with sync pairs, peers, Easytier settings
- Watcher: Cross-platform file system monitoring with debouncing
- Network: TCP-based P2P communication over Easytier virtual network
- Sync: File index building, diff detection, chunked transfer (64KB)
- Easytier: Subprocess lifecycle management with auto-restart
- Service: Windows/Linux/macOS system service registration
- Daemon: Event-driven sync loop with WatcherBridge
- State: JSON-based sync metadata persistence for incremental startup
- CLI: 8 subcommands (serve, sync, status, init, add, list, service, version)
- CI: GitHub Actions workflows for test & release
- Tests: 43 total (26 unit + 8 core integration + 9 Easytier integration)
- Docs: README, Easytier setup guide, LICENSE (Apache 2.0)
