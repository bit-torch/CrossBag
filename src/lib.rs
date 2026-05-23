//! CrossBag 库 crate - 对外暴露公共 API
//!
//! 供 main.rs 和集成测试使用。

#![allow(dead_code)] // 公共 API 在后续集成中使用

pub mod cli;
pub mod config;
pub mod daemon;
pub mod easytier;
pub mod network;
pub mod protocol;
pub mod service;
pub mod sync;
pub mod watcher;
