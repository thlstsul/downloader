use downloader::{DownloadConfig, DownloadManager, DownloadStatus};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志，设置日志级别为 info
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    log::info!("断点续传示例");
    log::info!("==============");

    // 创建下载管理器
    let manager = DownloadManager::new();

    // 配置下载参数 - 使用较低的速度限制以便有时间观察暂停/恢复
    let config = DownloadConfig {
        max_connections: 4,
        chunk_size: 512 * 1024,        // 512KB
        speed_limit: Some(512 * 1024), // 512KB/s
        auto_resume: true,             // 启用断点续传
        // 启用读取超时：服务器中途断流但保持连接时，库会把挂起转成终态 Failed，
        // 监控循环得以退出；否则 response.chunk() 会永久阻塞、示例无限等待
        read_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };

    // 添加下载任务 - 使用 httpbin 的 5MB 测试文件（支持 Range 请求）
    let download_dir = PathBuf::from(".");
    let task_id = manager
        .add_task(
            "https://httpbin.org/bytes/5242880".to_string(),
            download_dir.clone(),
            config,
        )
        .await;

    log::info!("任务ID: {}", task_id);
    log::info!("启用断点续传功能");

    // 订阅下载进度（在开始前订阅，避免错过早期进度）
    let mut progress = manager
        .subscribe_progress(&task_id)
        .await
        .expect("任务应存在");

    // 开始下载
    if let Err(e) = manager.start_task(&task_id).await {
        log::error!("下载失败: {}", e);
        return Err(e.into());
    }

    // 监控下载进度并在下载到一定规模后暂停，演示断点续传
    let mut paused_once = false;
    loop {
        // 等待进度更新；暂停期间无数据流动，用超时兜底并配合状态检查
        match tokio::time::timeout(Duration::from_secs(5), progress.changed()).await {
            // 通道关闭：任务已被移除
            Ok(Err(_)) => break,

            // 超时：可能是暂停/下载极快，检查终止状态
            Err(_) => {
                if let Some(status) = manager.get_task_status(&task_id).await
                    && matches!(
                        status,
                        DownloadStatus::Completed
                            | DownloadStatus::Failed(_)
                            | DownloadStatus::Cancelled
                    )
                {
                    log::info!("下载结束，状态: {:?}", status);
                    break;
                }
                continue;
            }

            // 收到进度更新
            Ok(Ok(())) => {
                let stats = progress.borrow().clone();
                let percent = if stats.total_size > 0 {
                    (stats.downloaded as f64 / stats.total_size as f64) * 100.0
                } else {
                    0.0
                };

                log::info!(
                    "进度: {:5.1}% | 速度: {:6.1} KB/s | 已下载: {}/{} bytes",
                    percent,
                    stats.speed / 1024.0,
                    stats.downloaded,
                    stats.total_size
                );

                // 首次下载至少 256KB 且未完成时暂停一次，然后恢复
                if !paused_once
                    && stats.downloaded >= 256 * 1024
                    && stats.downloaded < stats.total_size
                {
                    paused_once = true;
                    log::info!("暂停下载...");
                    manager.pause_task(&task_id).await;

                    // 显示暂停时的进度快照
                    if let Some(paused_stats) = manager.get_task_stats(&task_id).await {
                        log::info!(
                            "暂停时已下载: {}/{} bytes",
                            paused_stats.downloaded,
                            paused_stats.total_size
                        );
                    }

                    log::info!("等待 2 秒后恢复下载...");
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    log::info!("恢复下载...");
                    manager.resume_task(&task_id).await;
                }

                // 不能仅凭大小快照判定完成：进度快照在最后一个分块写入循环内
                // 发布（downloaded == total_size），此时后台任务仍在收尾（临时文件
                // rename、状态写入）。若据此退出并调用 remove_task，会把正在收尾的
                // 任务取消、删掉已完整下载的临时文件，最终文件丢失。因此必须等待
                // 任务进入终态（Completed/Failed/Cancelled）才退出。
                if stats.total_size > 0
                    && stats.downloaded >= stats.total_size
                    && let Some(status) = manager.get_task_status(&task_id).await
                    && matches!(
                        status,
                        DownloadStatus::Completed
                            | DownloadStatus::Failed(_)
                            | DownloadStatus::Cancelled
                    )
                {
                    log::info!("下载结束，状态: {:?}", status);
                    break;
                }
                // 非终态：后台任务仍在收尾，继续等待下一次进度更新或超时兜底
            }
        }
    }

    // 显示最终统计信息
    if let Some(stats) = manager.get_task_stats(&task_id).await {
        log::info!("最终统计:");
        log::info!("总大小: {} bytes", stats.total_size);
        log::info!("下载耗时: {:?}", stats.elapsed);
        log::info!("平均速度: {:.1} KB/s", stats.speed / 1024.0);
        log::info!("块数: {}", stats.chunks.len());
    }

    // 完成后移除任务，避免任务表无限增长
    manager.remove_task(&task_id).await;
    log::info!("任务已移除，状态文件已在下载完成时自动清理");

    Ok(())
}
