//! 运行中动态调整配置演示：下载过程中周期性修改 speed_limit 并立即生效。
//!
//! 修改走 get_task_config 读当前配置 → 局部修改 → update_task_config 写回的
//! 配对流程，而非在示例里手动维护一份可能漂移的配置副本。

use downloader::{DownloadConfig, DownloadManager, DownloadStatus};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    log::info!("高级下载示例 - 动态配置修改演示");
    log::info!("==================================");

    let manager = DownloadManager::new();

    let config = DownloadConfig {
        max_connections: 2,
        chunk_size: 512 * 1024,        // 512KB
        speed_limit: Some(512 * 1024), // 限制为 512KB/s
        auto_resume: true,
        // 启用读取超时：服务器中途断流但保持连接时，库会把挂起转成终态 Failed，
        // 监控循环得以退出；否则 response.chunk() 会永久阻塞、示例无限等待
        read_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };

    let task_id = manager
        .add_task(
            "https://httpbin.org/bytes/5242880".to_string(), // 5MB 测试文件
            PathBuf::from("."),
            config.clone(),
        )
        .await;

    log::info!("任务ID: {}", task_id);
    log::info!(
        "初始配置: {} 连接, {:.1}KB/s 限速",
        config.max_connections,
        config.speed_limit.unwrap_or(0) as f64 / 1024.0
    );

    if let Err(e) = manager.start_task(&task_id).await {
        log::error!("下载失败: {}", e);
        return Err(e.into());
    }

    // 监控下载进度并动态调整配置
    let mut phase = 0;
    let mut last_update = std::time::Instant::now();

    loop {
        sleep(Duration::from_secs(1)).await;

        if let Some(stats) = manager.get_task_stats(&task_id).await {
            let speed_kb = stats.speed / 1024.0;
            let progress = if stats.total_size > 0 {
                (stats.downloaded as f64 / stats.total_size as f64) * 100.0
            } else {
                0.0
            };

            log::info!(
                "进度: {:5.1}% | 速度: {:6.1} KB/s | 已下载: {}/{} bytes",
                progress,
                speed_kb,
                stats.downloaded,
                stats.total_size
            );

            // 动态调整配置：每 5 秒推进一个阶段。演示 get_task_config 与
            // update_task_config 配对——读取当前配置 → 局部修改 speed_limit →
            // 写回（speed_limit 立即生效），而非在示例里手动维护一份可能漂移的副本。
            if last_update.elapsed() > Duration::from_secs(5) && phase < 4 {
                phase += 1;
                last_update = std::time::Instant::now();

                let new_limit = match phase {
                    1 => {
                        log::info!("动态调整: 取消速度限制");
                        None
                    }
                    2 => {
                        log::info!("动态调整: 启用 512KB/s 限速");
                        Some(512 * 1024)
                    }
                    3 => {
                        log::info!("动态调整: 提高到 2MB/s 限速");
                        Some(2 * 1024 * 1024)
                    }
                    4 => {
                        log::info!("动态调整: 恢复 1MB/s 限速");
                        Some(1024 * 1024)
                    }
                    _ => unreachable!("phase 由 `phase < 4` 守卫约束在 1..=4"),
                };

                if let Some(mut current) = manager.get_task_config(&task_id).await {
                    current.speed_limit = new_limit;
                    if let Err(e) = manager.update_task_config(&task_id, current).await {
                        log::error!("更新配置失败: {}", e);
                    }
                }
            }

            // 完成判定以任务终态为准（见下方 get_task_status 检查），不能仅凭
            // 大小快照：进度显示 downloaded == total_size 时后台任务可能仍在收尾
            // （临时文件 rename、状态写入），若此时 break 并 remove_task 会把正在
            // 收尾的任务取消、删掉已完整下载的临时文件，导致最终文件丢失。
        }

        if let Some(status) = manager.get_task_status(&task_id).await {
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

    if let Some(stats) = manager.get_task_stats(&task_id).await {
        log::info!("下载统计:");
        log::info!("总大小: {} bytes", stats.total_size);
        log::info!("下载耗时: {:?}", stats.elapsed);
        log::info!("平均速度: {:.1} KB/s", stats.speed / 1024.0);

        log::info!("块信息:");
        for chunk in &stats.chunks {
            // 未知大小分块 end == u64::MAX，用饱和运算避免溢出
            let chunk_size = chunk.end.saturating_sub(chunk.start).saturating_add(1);
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

    // 完成后移除任务，避免任务表无限增长（与 resume_download 示例保持一致）
    manager.remove_task(&task_id).await;
    log::info!("任务已移除");

    Ok(())
}
