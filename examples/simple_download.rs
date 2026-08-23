use downloader::{DownloadConfig, DownloadManager};
use std::path::PathBuf;
use std::time::Duration;

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
        chunk_size: 1024 * 1024,        // 1MB
        speed_limit: Some(1024 * 1024), // 限制为 1MB/s
        auto_resume: true,
        // 启用读取超时：服务器中途断流但保持连接时，库会把挂起转成终态 Failed，
        // 监控循环得以退出；否则 response.chunk() 会永久阻塞、示例无限等待
        read_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };

    // 添加下载任务
    let task_id = manager
        .add_task(
            "https://httpbin.org/bytes/10485760".to_string(), // 10MB 测试文件
            PathBuf::from("."),
            config,
        )
        .await;

    log::info!("任务ID: {}", task_id);

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

    // 监控下载进度（订阅式，无需轮询 get_task_stats）
    loop {
        // 等待进度更新；下载完成后通常不再有更新，用超时兜底并配合状态检查
        match tokio::time::timeout(Duration::from_secs(2), progress.changed()).await {
            // 通道关闭：任务已被移除
            Ok(Err(_)) => break,

            // 超时：可能是下载极快已完成，检查终止状态
            Err(_) => {
                if let Some(status) = manager.get_task_status(&task_id).await
                    && matches!(
                        status,
                        downloader::DownloadStatus::Completed
                            | downloader::DownloadStatus::Failed(_)
                            | downloader::DownloadStatus::Cancelled
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
                let speed_mb = stats.speed / 1024.0 / 1024.0;
                let percent = if stats.total_size > 0 {
                    (stats.downloaded as f64 / stats.total_size as f64) * 100.0
                } else {
                    0.0
                };

                log::info!(
                    "进度: {:.1}% | 速度: {:.2} MB/s | 已下载: {}/{} bytes | 剩余时间: {:?}",
                    percent,
                    speed_mb,
                    stats.downloaded,
                    stats.total_size,
                    stats.remaining
                );

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
                        downloader::DownloadStatus::Completed
                            | downloader::DownloadStatus::Failed(_)
                            | downloader::DownloadStatus::Cancelled
                    )
                {
                    log::info!("下载结束，状态: {:?}", status);
                    break;
                }
                // 非终态：后台任务仍在收尾，继续等待下一次进度更新或超时兜底
            }
        }
    }

    // 完成后移除任务，避免任务表无限增长（与 resume_download 示例保持一致）
    manager.remove_task(&task_id).await;
    log::info!("任务已移除");

    Ok(())
}
