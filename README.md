# Rust 下载器库

一个功能强大的 Rust 异步下载器库，支持多线程下载、断点续传、速度限制和动态配置修改。

## 功能特性

- ✅ **多线程下载** - 支持并行连接下载大文件
- ✅ **断点续传** - 支持从上次中断的位置继续下载
- ✅ **速度限制** - 可设置下载速度上限
- ✅ **实时统计** - 提供下载速度、进度、剩余时间等统计信息
- ✅ **动态配置** - 支持在下载过程中修改连接数、速度限制等参数
- ✅ **块级追踪** - 详细跟踪每个下载块的状态和进度
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = DownloadManager::new();
    
    let config = DownloadConfig {
        max_connections: 4,
        chunk_size: 1 * 1024 * 1024,
        speed_limit: Some(1024 * 1024),
        auto_resume: true,
        ..Default::default()
    };
    
    let task_id = manager.add_task(
        "https://example.com/large-file.zip".to_string(),
        PathBuf::from("downloaded_file.zip"),
        config,
    );
    
    manager.start_task(&task_id).await?;
    
    Ok(())
}
```

### 监控下载进度

```rust
use downloader::{DownloadConfig, DownloadManager};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = DownloadManager::new();
    let task_id = manager.add_task(/* ... */);
    
    manager.start_task(&task_id).await?;
    
    loop {
        sleep(Duration::from_secs(1)).await;
        
        if let Some(stats) = manager.get_task_stats(&task_id) {
            let progress = (stats.downloaded as f64 / stats.total_size as f64) * 100.0;
            println!("进度: {:.1}% | 速度: {:.1} KB/s", progress, stats.speed / 1024.0);
            
            if stats.downloaded >= stats.total_size {
                println!("下载完成!");
                break;
            }
        }
    }
    
    Ok(())
}
```

## API 参考

### DownloadConfig

下载配置结构体：

```rust
pub struct DownloadConfig {
    pub max_connections: usize,    // 最大并行连接数
    pub chunk_size: u64,           // 每个块的大小（字节）
    pub speed_limit: Option<u64>,  // 速度限制（字节/秒）
    pub auto_resume: bool,         // 是否自动断点续传
    pub temp_dir: Option<PathBuf>, // 临时文件目录
}
```

### DownloadManager

下载管理器，用于管理多个下载任务：

```rust
let manager = DownloadManager::new();

// 添加任务
let task_id = manager.add_task(url, output_path, config);

// 开始下载
manager.start_task(&task_id).await?;

// 暂停下载
manager.pause_task(&task_id);

// 恢复下载
manager.resume_task(&task_id);

// 取消下载
manager.cancel_task(&task_id);

// 获取状态
let status = manager.get_task_status(&task_id);

// 获取统计信息
let stats = manager.get_task_stats(&task_id);
```

### DownloadStats

下载统计信息：

```rust
pub struct DownloadStats {
    pub total_size: u64,        // 文件总大小
    pub downloaded: u64,        // 已下载字节数
    pub speed: f64,             // 当前速度（字节/秒）
    pub elapsed: Duration,      // 已用时间
    pub remaining: Duration,    // 剩余时间估计
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
    HttpError(reqwest::Error),
    IoError(std::io::Error),
    FileError(String),
    Cancelled,
    ResumeNotSupported,
    InvalidStateFile,
    Other(String),
}
```

## 性能优化建议

1. **连接数调整**: 根据网络状况动态调整 `max_connections`
2. **块大小优化**: 大文件使用较大的块大小（2-4MB），小文件使用较小的块大小
3. **速度限制**: 在带宽受限的环境中使用速度限制避免网络拥塞
4. **断点续传**: 启用 `auto_resume` 避免重复下载

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！