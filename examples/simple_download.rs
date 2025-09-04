use downloader::{DownloadConfig, DownloadManager};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::init();

    log::info!("开始下载示例...");

    // 创建下载管理器
    let manager = DownloadManager::new();

    // 配置下载参数
    let config = DownloadConfig {
        max_connections: 4,
        chunk_size: 1 * 1024 * 1024,    // 1MB
        speed_limit: Some(1024 * 1024), // 限制为 1MB/s
        auto_resume: true,
        ..Default::default()
    };

    // 添加下载任务
    let task_id = manager.add_task(
        "https://httpbin.org/bytes/10485760".to_string(), // 10MB 测试文件
        PathBuf::from("."),
        config,
    );

    log::info!("任务ID: {}", task_id);

    // 开始下载
    if let Err(e) = manager.start_task(&task_id).await {
        log::error!("下载失败: {}", e);
        return Err(e.into());
    }

    // 监控下载进度
    let mut last_downloaded = 0;
    loop {
        sleep(Duration::from_secs(1)).await;

        if let Some(stats) = manager.get_task_stats(&task_id) {
            let speed_mb = stats.speed / 1024.0 / 1024.0;
            let progress = if stats.total_size > 0 {
                (stats.downloaded as f64 / stats.total_size as f64) * 100.0
            } else {
                0.0
            };

            log::info!(
                "进度: {:.1}% | 速度: {:.2} MB/s | 已下载: {}/{} bytes | 剩余时间: {:?}",
                progress,
                speed_mb,
                stats.downloaded,
                stats.total_size,
                stats.remaining
            );

            // 检查是否完成
            if stats.downloaded >= stats.total_size && stats.total_size > 0 {
                log::info!("下载完成!");
                break;
            }

            // 检查是否卡住
            if stats.downloaded == last_downloaded && stats.downloaded > 0 {
                log::warn!("下载似乎卡住了，尝试重新开始...");
                manager.pause_task(&task_id);
                sleep(Duration::from_secs(1)).await;
                manager.resume_task(&task_id);
            }

            last_downloaded = stats.downloaded;
        }

        // 检查任务状态
        if let Some(status) = manager.get_task_status(&task_id) {
            match status {
                downloader::DownloadStatus::Completed => {
                    log::info!("下载完成!");
                    break;
                }
                downloader::DownloadStatus::Failed(reason) => {
                    log::error!("下载失败: {}", reason);
                    break;
                }
                _ => {}
            }
        }
    }

    Ok(())
}
