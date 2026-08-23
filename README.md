# Rust 下载器库

一个功能强大的 Rust 异步下载器库，支持多线程下载、断点续传、速度限制和动态配置修改。

## 功能特性

- ✅ **多线程下载** - 支持并行连接下载大文件
- ✅ **断点续传** - 支持从上次中断的位置继续下载
- ✅ **速度限制** - 可设置下载速度上限
- ✅ **实时统计** - 提供下载速度、进度、剩余时间等统计信息
- ✅ **动态配置** - 支持在下载过程中修改连接数、速度限制等参数
- ✅ **块级追踪** - 详细跟踪每个下载块的状态和进度
- ✅ **进度订阅** - 通过 watch 通道订阅单任务实时进度快照，无需轮询
- ✅ **事件通知** - 任务完成/失败/取消时通过广播通道即时通知订阅者
- ✅ **异步支持** - 基于 Tokio 的完全异步实现

## 安装

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
downloader = { path = "." }
```

## 快速开始

### 简单示例

```rust
use downloader::{DownloadConfig, DownloadManager};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = DownloadManager::new();

    let config = DownloadConfig {
        max_connections: 4,
        chunk_size: 1 * 1024 * 1024,
        speed_limit: Some(1024 * 1024),
        auto_resume: true,
        // 启用读取超时：服务器中途断流但保持连接时，库会把挂起转成终态 Failed，
        // 监控循环得以退出；否则 response.chunk() 会永久阻塞、调用方无限等待
        read_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };

    let task_id = manager
        .add_task(
            "https://example.com/large-file.zip".to_string(),
            PathBuf::from("./downloads"), // 下载目录（文件名由服务端响应决定）
            config,
        )
        .await;

    manager.start_task(&task_id).await?;

    Ok(())
}
```

### 监控下载进度

推荐使用订阅式 `subscribe_progress` 实时获取进度快照，无需主动轮询：

```rust
use downloader::{DownloadConfig, DownloadManager, DownloadStatus};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = DownloadManager::new();
    let task_id = manager.add_task(/* ... */).await;

    // 在 start 之前订阅，避免错过早期进度
    let mut progress = manager.subscribe_progress(&task_id).await.expect("任务应存在");

    manager.start_task(&task_id).await?;

    loop {
        // 下载完成后通常不再有更新，用超时兜底并配合状态检查终止判定
        match tokio::time::timeout(Duration::from_secs(2), progress.changed()).await {
            Ok(Err(_)) => break, // 通道关闭：任务已被移除
            Err(_) => {
                // 超时：可能是下载极快已完成，检查终止状态
                if matches!(
                    manager.get_task_status(&task_id).await,
                    Some(DownloadStatus::Completed)
                        | Some(DownloadStatus::Failed(_))
                        | Some(DownloadStatus::Cancelled)
                ) {
                    break;
                }
                continue;
            }
            Ok(Ok(())) => {
                let stats = progress.borrow().clone();
                // total_size 为 0 时（大小未知/空文件）无法计算百分比，且不应据此误判完成
                let progress = if stats.total_size > 0 {
                    (stats.downloaded as f64 / stats.total_size as f64) * 100.0
                } else {
                    0.0
                };
                println!("进度: {:.1}% | 速度: {:.1} KB/s", progress, stats.speed / 1024.0);

                // 不能仅凭大小快照判定完成：进度快照在最后一个分块写入循环内发布
                // （downloaded == total_size），此时后台任务可能仍在收尾（临时文件
                // rename、状态写入）。若据此退出并调用 remove_task，会把正在收尾的
                // 任务取消、删掉已完整下载的临时文件，导致最终文件丢失。必须等待
                // 任务进入终态（Completed/Failed/Cancelled）后再移除。
                if stats.total_size > 0 && stats.downloaded >= stats.total_size {
                    if matches!(
                        manager.get_task_status(&task_id).await,
                        Some(DownloadStatus::Completed)
                            | Some(DownloadStatus::Failed(_))
                            | Some(DownloadStatus::Cancelled)
                    ) {
                        break;
                    }
                    // 非终态：后台任务仍在收尾，继续等待下一次进度更新或超时兜底
                }
            }
        }
    }

    Ok(())
}
```

> 也可用 `get_task_stats` + 定时轮询（如每秒一次）实现相同效果，但订阅式能避免无谓的轮询开销。

## API 参考

### DownloadConfig

下载配置结构体：

```rust
pub struct DownloadConfig {
    pub max_connections: usize,    // 最大并行连接数
    pub chunk_size: u64,           // 每个块的大小（字节，0 表示自动计算）
    pub speed_limit: Option<u64>,  // 速度限制（字节/秒）
    pub auto_resume: bool,         // 是否自动断点续传
    pub cookies: Option<String>,   // Cookies（仅建连时读取）
    pub connect_timeout: Duration, // 连接超时（TCP 建连，默认 30 秒）
    pub read_timeout: Option<Duration>, // 读取超时（None 表示不启用；覆盖等待响应头与读取数据，用于检测下载挂起）
}
```

### DownloadManager

下载管理器，用于管理多个下载任务：

```rust
let manager = DownloadManager::new();

// 添加任务（第二个参数为下载目录，最终文件名由服务端响应决定）
let task_id = manager.add_task(url, download_dir, config).await;

// 开始下载
manager.start_task(&task_id).await?;

// 暂停下载
manager.pause_task(&task_id).await;

// 恢复下载
manager.resume_task(&task_id).await;

// 取消下载
manager.cancel_task(&task_id).await;

// 获取状态（返回 Option）
let status = manager.get_task_status(&task_id).await;

// 获取统计信息（返回 Option）
let stats = manager.get_task_stats(&task_id).await;

// 获取最终文件名（返回 Option，下载开始前为 None）
let filename = manager.get_task_filename(&task_id).await;

// 获取当前配置快照（返回 Option，任务不存在时为 None；与 update_task_config 配对）
if let Some(current) = manager.get_task_config(&task_id).await {
    // 局部修改后写回……
    let mut new_config = current;
    new_config.speed_limit = Some(2 * 1024 * 1024);
    manager.update_task_config(&task_id, new_config).await?;
}

// 订阅进度（watch 接收端，实时收到最新统计快照）
if let Some(mut progress) = manager.subscribe_progress(&task_id).await {
    // 下载完成后不再有更新：仅以终态状态（或通道关闭）作为循环出口，
    // 否则 changed() 会永久阻塞（watch 通道在任务被移除前不会关闭）
    while progress.changed().await.is_ok() {
        let snapshot = progress.borrow().clone();
        // 处理最新统计快照……
        let _ = snapshot;

        if matches!(
            manager.get_task_status(&task_id).await,
            Some(DownloadStatus::Completed)
                | Some(DownloadStatus::Failed(_))
                | Some(DownloadStatus::Cancelled)
        ) {
            break;
        }
    }
}

// 移除任务（任务仍在下载时会先自动取消并清理残留，返回是否移除成功）
manager.remove_task(&task_id).await;
```

### 订阅下载事件

除逐任务轮询 `/ 订阅进度` 外，还可订阅**全局事件流**，在任务完成/失败/取消时无需轮询状态即可即时感知：

```rust
use downloader::DownloadEvent;

let manager = DownloadManager::new();

// 在 start 之前订阅，否则会漏掉已广播的终止事件
let mut events = manager.subscribe_events();

// …… add_task + start_task ……

// 阻塞等待下一个任务终止事件（广播接收端，`||` 可并发消费）
if let Ok(DownloadEvent::TaskCompleted { task_id }) = events.recv().await {
    println!("任务 {task_id} 已完成");
}
```

`DownloadEvent` 包含三种终止事件：`TaskCompleted`（完成）、`TaskFailed`（失败，携带原因字符串）、`TaskCancelled`（取消）。事件是「通知」而非权威状态——真实状态仍以 `get_task_status` 为准（订阅过慢或被广播丢弃时可能漏掉事件）。

### DownloadStats

下载统计信息：

```rust
pub struct DownloadStats {
    pub total_size: u64,        // 总大小（字节）；0 表示大小未知或空文件
    pub downloaded: u64,        // 已下载字节数
    pub speed: f64,             // 当前速度（字节/秒），基于滑动窗口
    pub elapsed: Duration,      // 已用时间（自开始起算的墙钟时间，含暂停时长）
    pub remaining: Duration,    // 剩余时间估计；速度未知时为 Duration::default()
    pub chunks: Vec<ChunkInfo>, // 块详细信息
}
```

## 示例程序

### 运行简单示例

```bash
cargo run --example simple_download
```

### 运行高级示例（动态配置）

```bash
cargo run --example advanced_download
```

## 错误处理

库使用 `thiserror` 提供详细的错误类型：

```rust
pub enum DownloadError {
    TokioError(tokio::task::JoinError),
    HttpError(reqwest::Error),
    IoError(std::io::Error),
    JsonError(serde_json::Error),
    Cancelled,
    InvalidStateFile,
    TaskNotFound,   // 任务不存在（task_id 未注册或已移除）
    RequestTimeout, // 等待响应头超时（超过 read_timeout）
    ReadTimeout,    // 读取数据超时（下载中途连接挂起/断流）
    Other(String),
}
```

## 性能优化建议

1. **连接数调整**: 根据网络状况动态调整 `max_connections`
2. **块大小优化**: 大文件使用较大的块大小（2-4MB），小文件使用较小的块大小；为 0 时，自动调节
3. **速度限制**: 在带宽受限的环境中使用速度限制避免网络拥塞
4. **断点续传**: 启用 `auto_resume` 避免重复下载

> 断点续传的状态保存间隔会随文件大小自动调整：小文件（≤256MB）每 1MB 保存一次，
> 大文件逐步放宽到最多 64MB 一次，使超大文件的序列化/I/O 开销保持在可控范围。
> 这意味着超大文件恢复下载时的“已保存进度粒度”会相应变粗（最多落后一个保存间隔）。

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
