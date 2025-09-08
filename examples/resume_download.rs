use downloader::{DownloadConfig, DownloadManager, DownloadStatus};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::init();

    log::info!("断点续传示例");
    log::info!("==============");

    // 创建下载管理器
    let manager = DownloadManager::new();

    // 配置下载参数 - 使用较低的速度限制以便有时间暂停
    let config = DownloadConfig {
        max_connections: 4,
        chunk_size: 512 * 1024, // 512KB
        speed_limit: None,      // 500KB/s 限速
        auto_resume: true,      // 启用断点续传
        ..Default::default()
    };

    // 添加下载任务 - 使用httpbin的测试文件
    let download_dir = PathBuf::from(".");
    // https://httpbin.org/range/102400
    // https://download-installer.cdn.mozilla.net/pub/firefox/releases/139.0.4/win64/zh-CN/Firefox%20Setup%20139.0.4.msi
    let task_id = manager.add_task(
        "https://download-installer.cdn.mozilla.net/pub/firefox/releases/139.0.4/win64/zh-CN/Firefox%20Setup%20139.0.4.msi".to_string(), // 100KB 测试文件
        download_dir.clone(),
        config,
    );

    log::info!("任务ID: {}", task_id);
    log::info!("下载目录: {:?}", download_dir);
    log::info!("启用断点续传功能");

    // 开始下载
    log::info!("开始下载...");
    if let Err(e) = manager.start_task(&task_id).await {
        log::error!("下载失败: {}", e);
        return Err(e.into());
    }

    // 等待一段时间让下载开始
    sleep(Duration::from_millis(50)).await;

    // 暂停下载
    log::info!("暂停下载...");
    manager.pause_task(&task_id);

    // 显示暂停时的状态
    if let Some(stats) = manager.get_task_stats(&task_id) {
        let progress = if stats.total_size > 0 {
            (stats.downloaded as f64 / stats.total_size as f64) * 100.0
        } else {
            0.0
        };
        log::info!("暂停时进度: {:.1}%", progress);
        log::info!("已下载: {}/{} bytes", stats.downloaded, stats.total_size);
    }

    // 等待一段时间后恢复
    log::info!("等待1秒后恢复下载...");
    sleep(Duration::from_secs(2)).await;

    log::info!("恢复下载...");
    manager.resume_task(&task_id);

    // 恢复下载后不需要重新调用 start_task，resume_task 已经恢复下载

    // 等待下载完成
    log::info!("等待下载完成...");
    loop {
        sleep(Duration::from_secs(1)).await;

        if let Some(stats) = manager.get_task_stats(&task_id) {
            let progress = if stats.total_size > 0 {
                (stats.downloaded as f64 / stats.total_size as f64) * 100.0
            } else {
                0.0
            };

            log::info!(
                "进度: {:5.1}% | 速度: {:6.1} KB/s | 已下载: {}/{} bytes",
                progress,
                stats.speed / 1024.0,
                stats.downloaded,
                stats.total_size
            );

            if stats.downloaded >= stats.total_size && stats.total_size > 0 {
                log::info!("下载完成!");
                break;
            }
        }

        if let Some(status) = manager.get_task_status(&task_id) {
            if let DownloadStatus::Completed = status {
                log::info!("下载完成!");
                break;
            }
        }
    }

    // 显示最终统计信息
    if let Some(stats) = manager.get_task_stats(&task_id) {
        log::info!("最终统计:");
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

    // 检查状态文件是否已删除
    let output_path = download_dir.join("Firefox Setup 139.0.4.msi");
    let state_file_path = output_path.with_extension("download_state");
    if !state_file_path.exists() {
        log::info!("状态文件已自动删除");
    } else {
        log::warn!("状态文件仍然存在: {:?}", state_file_path);
        // 清理状态文件
        let _ = std::fs::remove_file(state_file_path);
    }

    // 清理下载的文件（如果存在）
    if output_path.exists() {
        // let _ = std::fs::remove_file(output_path);
    }

    Ok(())
}
