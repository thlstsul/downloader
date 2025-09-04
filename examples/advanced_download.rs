use downloader::{DownloadConfig, DownloadManager, DownloadStatus};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::init();

    log::info!("高级下载示例 - 动态配置修改演示");
    log::info!("==================================");

    // 创建下载管理器
    let manager = DownloadManager::new();

    // 初始配置
    let mut config = DownloadConfig {
        max_connections: 2,
        chunk_size: 512 * 1024,        // 512KB
        speed_limit: Some(512 * 1024), // 限制为 512KB/s
        auto_resume: true,
        ..Default::default()
    };

    // 添加下载任务
    let task_id = manager.add_task(
        "https://httpbin.org/bytes/5242880".to_string(), // 5MB 测试文件
        PathBuf::from("advanced_download.bin"),
        config.clone(),
    );

    log::info!("任务ID: {}", task_id);
    log::info!(
        "初始配置: {} 连接, {:.1}KB/s 限速",
        config.max_connections,
        config.speed_limit.unwrap_or(0) as f64 / 1024.0
    );

    // 开始下载
    if let Err(e) = manager.start_task(&task_id).await {
        log::error!("下载失败: {}", e);
        return Err(e.into());
    }

    // 监控下载进度并动态调整配置
    let mut phase = 0;
    let mut last_update = std::time::Instant::now();

    loop {
        sleep(Duration::from_secs(1)).await;

        if let Some(stats) = manager.get_task_stats(&task_id) {
            let speed_kb = stats.speed / 1024.0;
            let progress = if stats.total_size > 0 {
                (stats.downloaded as f64 / stats.total_size as f64) * 100.0
            } else {
                0.0
            };

            println!(
                "进度: {:5.1}% | 速度: {:6.1} KB/s | 已下载: {}/{} bytes",
                progress, speed_kb, stats.downloaded, stats.total_size
            );

            // 动态调整配置
            if last_update.elapsed() > Duration::from_secs(5) {
                phase += 1;
                last_update = std::time::Instant::now();

                match phase {
                    1 => {
                        // 阶段1: 增加连接数
                        config.max_connections = 4;
                        config.speed_limit = None; // 取消速度限制
                        log::info!("动态调整: 增加到 4 连接，取消速度限制");
                    }
                    2 => {
                        // 阶段2: 进一步增加连接数
                        config.max_connections = 8;
                        log::info!("动态调整: 增加到 8 连接");
                    }
                    3 => {
                        // 阶段3: 重新启用速度限制
                        config.max_connections = 4;
                        config.speed_limit = Some(1024 * 1024); // 1MB/s
                        log::info!("动态调整: 减少到 4 连接，启用 1MB/s 限速");
                    }
                    4 => {
                        // 阶段4: 减小块大小
                        config.chunk_size = 256 * 1024; // 256KB
                        log::info!("动态调整: 块大小减小到 256KB");
                    }
                    _ => {
                        // 保持当前配置
                    }
                }

                // 这里应该调用 task.update_config(config.clone())，但需要先实现这个方法
                log::warn!("配置更新功能需要实现 task.update_config() 方法");
            }

            // 检查是否完成
            if stats.downloaded >= stats.total_size && stats.total_size > 0 {
                log::info!("下载完成!");
                break;
            }
        }

        // 检查任务状态
        if let Some(status) = manager.get_task_status(&task_id) {
            match status {
                DownloadStatus::Completed => {
                    log::info!("下载完成!");
                    break;
                }
                DownloadStatus::Failed(reason) => {
                    log::error!("下载失败: {}", reason);
                    break;
                }
                DownloadStatus::Paused => {
                    log::info!("下载已暂停");
                    break;
                }
                _ => {}
            }
        }
    }

    // 显示最终统计信息
    if let Some(stats) = manager.get_task_stats(&task_id) {
        log::info!("下载统计:");
        log::info!("总大小: {} bytes", stats.total_size);
        log::info!("下载耗时: {:?}", stats.elapsed);
        log::info!("平均速度: {:.1} KB/s", stats.speed / 1024.0);

        log::info!("块信息:");
        for chunk in &stats.chunks {
            let chunk_size = chunk.end - chunk.start + 1;
            let chunk_progress = if chunk_size > 0 {
                (chunk.downloaded as f64 / chunk_size as f64) * 100.0
            } else {
                100.0
            };
            log::info!(
                "块 {}: {}-{} ({:.1}% 完成)",
                chunk.index,
                chunk.start,
                chunk.end,
                chunk_progress
            );
        }
    }

    Ok(())
}
