//! 端到端集成测试：通过本地 mock HTTP 服务器验证下载器核心流程。
//!
//! 单元测试（`src/lib.rs` 内的 `#[cfg(test)]`）已覆盖纯函数与状态机逻辑，
//! 但真实的 HTTP 探测、分块请求、写入临时文件与最终重命名等路径此前未被
//! 自动验证。这里用一个仅依赖标准库 `TcpListener` 的最小服务器补齐该盲区。

use downloader::{
    ChunkInfo, DownloadConfig, DownloadEvent, DownloadManager, DownloadState, DownloadStatus,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 测试文件大小：200KB，配合默认自动块大小（小文件固定 100KB）切分为 2 块
const TEST_DATA_LEN: usize = 200 * 1024;

/// 生成确定性的测试载荷，便于做字节级校验
fn test_data(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// 启动本地 mock HTTP 服务器，返回可下载的 URL。
///
/// 服务器支持断点续传语义：
/// - 带 `Range: bytes=a-b` 的请求返回 206 + 对应切片；
/// - 不带 Range 的请求返回 200 + 全量载荷。
fn spawn_mock_server(data: Vec<u8>) -> String {
    spawn_mock_server_capturing(data).0
}

/// 启动本地 mock HTTP 服务器，返回可下载的 URL，并收集每个请求的原始头块，
/// 供断言下载器实际发送了哪些头（如 User-Agent、Cookie）。
fn spawn_mock_server_capturing(data: Vec<u8>) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let captured = Arc::new(Mutex::new(Vec::new()));

    let captured_inner = Arc::clone(&captured);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            let captured = Arc::clone(&captured_inner);
            // 每个连接独立线程处理，支持并发分块与 keep-alive 多请求
            std::thread::spawn(move || handle_connection(stream, &data, captured));
        }
    });

    (format!("http://{addr}/file.bin"), captured)
}

/// 处理单个客户端连接：循环读取请求并响应，直到连接关闭
fn handle_connection(mut stream: TcpStream, data: &[u8], captured: Arc<Mutex<Vec<String>>>) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];

    loop {
        buf.clear();
        // 读取一个完整请求头块（以 \r\n\r\n 结尾）
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => return, // 客户端关闭
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        // 记录本次请求的原始头块，供断言下载器实际发送了哪些头
        captured
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(&buf).into_owned());

        // 头名大小写不敏感，统一小写后再解析 Range
        let request = String::from_utf8_lossy(&buf).to_lowercase();
        let range_spec = request
            .lines()
            .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

        match range_spec {
            Some(spec) => {
                let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
                let first = spec.split(',').next().unwrap_or("");
                let Some((a, b)) = first.split_once('-') else {
                    return;
                };

                let start: usize = a.trim().parse().unwrap_or(0);
                let end: usize = if b.trim().is_empty() {
                    total.saturating_sub(1)
                } else {
                    b.trim().parse().unwrap_or(total.saturating_sub(1))
                };

                // 范围不可满足（如请求空文件的 bytes=0-0）
                if start >= total {
                    respond(&mut stream, "416 Range Not Satisfiable", b"");
                    continue;
                }

                let end = end.min(total - 1);
                let body = &data[start..=end];
                let header = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nConnection: keep-alive\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).unwrap();
                stream.write_all(body).unwrap();
                stream.flush().unwrap();
            }
            None => {
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nConnection: keep-alive\r\n\r\n"
                );
                stream.write_all(header.as_bytes()).unwrap();
                stream.write_all(data).unwrap();
                stream.flush().unwrap();
            }
        }
    }
}

/// 发送一个简单响应（用于错误状态等场景）
fn respond(stream: &mut TcpStream, status: &str, body: &[u8]) {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    stream.flush().unwrap();
}

/// 启动一个对任意请求都返回指定状态码（如 `404 Not Found`）的 mock 服务器，
/// 用于验证「探测请求返回非 2xx 状态码」时任务进入 Failed 的错误路径。
fn spawn_status_server(status: &'static str) -> String {
    spawn_status_server_counting(status).0
}

/// 启动一个对任意请求都返回指定状态码的 mock 服务器，并统计接受的连接总数，
/// 用于验证永久性错误（非超时类）不会触发自动重试（连接数恒为 1）。
fn spawn_status_server_counting(status: &'static str) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));

    let counter = Arc::clone(&connections);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let counter = Arc::clone(&counter);
            std::thread::spawn(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                let mut stream = stream;
                // 读取完整请求头（GET 无请求体）后统一返回指定错误状态码
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let header =
                    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.flush();
            });
        }
    });
    (format!("http://{addr}/file.bin"), connections)
}

/// 并发连接跟踪器：记录 mock 服务器上「同一时刻打开」的 TCP 连接数峰值。
///
/// 用于验证 `max_connections` 确实约束了同时下载的分块数：若调度器误把全部分块
/// 一次性并发，峰值会远超配置值。
struct ConcurrencyTracker {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl ConcurrencyTracker {
    fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    /// 新连接建立时调用：当前值 +1，并更新峰值
    fn enter(&self) {
        let cur = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(cur, Ordering::SeqCst);
    }

    /// 连接关闭时调用：当前值 -1
    fn leave(&self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

/// 启动 mock 服务器并跟踪并发连接数峰值，返回 (URL, 跟踪器)。
fn spawn_concurrency_tracking_server(data: Vec<u8>) -> (String, Arc<ConcurrencyTracker>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let tracker = Arc::new(ConcurrencyTracker::new());

    let tracker_inner = Arc::clone(&tracker);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            let tracker = Arc::clone(&tracker_inner);
            std::thread::spawn(move || {
                tracker.enter();
                // 这里不需要捕获请求头，传一个空的收集器即可
                handle_connection(stream, &data, Arc::new(Mutex::new(Vec::new())));
                tracker.leave();
            });
        }
    });

    (format!("http://{addr}/file.bin"), tracker)
}

/// 等待任务进入终止状态（完成/失败/取消），带超时保护，避免测试因缺陷而永久挂起
async fn wait_for_terminal(
    manager: &DownloadManager,
    task_id: &str,
    timeout: Duration,
) -> DownloadStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = manager.get_task_status(task_id).await {
            match status {
                DownloadStatus::Completed
                | DownloadStatus::Failed(_)
                | DownloadStatus::Cancelled => return status,
                _ => {}
            }
        }
        assert!(Instant::now() < deadline, "等待任务进入终止状态超时");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 从事件流中等待终止事件（完成/失败/取消），跳过其前的生命周期事件
/// （`TaskStarted`/`TaskPaused`/`TaskResumed`——`start()` 会先发布 `TaskStarted`）。
/// 带超时保护，避免测试因缺陷而永久挂起。
async fn wait_for_terminal_event(
    events: &mut tokio::sync::broadcast::Receiver<DownloadEvent>,
    timeout: Duration,
) -> DownloadEvent {
    let deadline = Instant::now() + timeout;
    loop {
        let event = tokio::time::timeout(timeout, events.recv())
            .await
            .expect("应在超时内收到事件")
            .expect("事件通道应正常");
        match event {
            DownloadEvent::TaskCompleted { .. }
            | DownloadEvent::TaskFailed { .. }
            | DownloadEvent::TaskCancelled { .. } => return event,
            _ => assert!(Instant::now() < deadline, "等待终止事件超时"),
        }
    }
}

#[tokio::test]
async fn test_full_range_download_against_local_server() {
    let data = test_data(TEST_DATA_LEN);
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 文件名应由 URL 路径解析出来
    let filename = manager
        .get_task_filename(&task_id)
        .await
        .expect("应解析出最终文件名");
    assert_eq!(filename, "file.bin");

    // 字节级校验下载内容
    let output = std::fs::read(dir.path().join(&filename)).expect("读取下载文件失败");
    assert_eq!(output, data, "下载内容应与服务端载荷一致");

    // 完成后不应残留临时/状态文件
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

/// 多分块（远超并发上限）大文件的端到端正确性：验证「预填 + join-one/spawn-one」
/// 交错调度在百余个并发分块、多次 save_state 串行落盘下，最终产物字节级与源一致。
#[tokio::test]
async fn test_many_chunk_download_is_byte_identical() {
    // 10MB / 100KB ≈ 103 块，远超 max_connections=8，覆盖交错调度的持续补充路径
    const CHUNK: u64 = 100 * 1024;
    let data = test_data(10 * 1024 * 1024);
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        max_connections: 8,
        chunk_size: CHUNK,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(30)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("多分块下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 完成后应切分成远超并发上限的多个分块（证明交错调度路径被真实覆盖）
    let stats = manager.get_task_stats(&task_id).await.expect("任务应存在");
    assert!(
        stats.chunks.len() > 8,
        "应切分成超过并发上限的多个分块，实际 {} 块",
        stats.chunks.len()
    );

    // 字节级校验：并发分块 + 多次状态落盘不得损坏最终内容
    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "多分块并发下载后内容应与源一致");
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

#[tokio::test]
async fn test_pause_resume_download_against_local_server() {
    // 2MB 文件 + 限速，制造可确定、足够长的下载窗口，便于中途暂停
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024), // 512KB/s，2MB 约需 4s
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待产生初步进度后暂停（约 1s 后应已下载数百 KB，且远未完成）
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager.pause_task(&task_id).await;

    let stats = manager.get_task_stats(&task_id).await.expect("任务应存在");
    assert!(stats.downloaded > 0, "暂停前应有下载进度");
    assert!((stats.downloaded as usize) < data.len(), "暂停时不应已完成");

    // 恢复并等待完成
    manager.resume_task(&task_id).await;
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 重点：从暂停点续传后，最终内容与源一致（无重叠、无遗漏、无错位）
    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "暂停续传后内容应与服务端载荷一致");
}

/// 验证暂停时 `save_state` 确实把进度以状态文件形式落到磁盘，且完成后被清理。
///
/// `pause_and_resume`（进程内暂停/恢复）与「断点续传状态文件」是两套机制：后者
/// 依赖 `save_state` 在真实下载流程中写出 `version/total_size/chunks`，供崩溃或
/// 重新 `start()` 后由 `load_state` 恢复。这条路径此前仅有单元测试的合成数据覆盖，
/// 缺少端到端验证，这里用本地服务器补齐。
#[tokio::test]
async fn test_pause_persists_state_file_with_progress() {
    let data = test_data(2 * 1024 * 1024); // 2MB + 限速，制造可观测的下载窗口
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待产生进度后暂停，触发 pause() -> save_state() 写出状态文件
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager.pause_task(&task_id).await;

    let stats = manager.get_task_stats(&task_id).await.expect("任务应存在");
    assert!(stats.downloaded > 0, "暂停前应有下载进度");

    // 反序列化状态文件，校验它记录且未越过真实进度
    let state_path = dir.path().join("file.bin.download_state");
    let raw = std::fs::read_to_string(&state_path).expect("暂停后应写出状态文件");
    let state: DownloadState = serde_json::from_str(&raw).expect("状态文件应为合法 JSON");
    assert_eq!(state.version, 1, "状态文件版本应为当前 STATE_VERSION");
    assert_eq!(
        state.total_size,
        data.len() as u64,
        "状态文件应记录服务器总大小"
    );
    let persisted_downloaded: u64 = state.chunks.iter().map(|c| c.downloaded).sum();
    assert!(persisted_downloaded > 0, "状态文件应记录已下载字节");
    // 并发下状态文件快照可能早于内存中最后几块，但不应超过真实进度
    assert!(
        persisted_downloaded <= stats.downloaded,
        "状态文件已下载量不应超过内存进度: {persisted_downloaded} > {}",
        stats.downloaded
    );

    // 恢复下载并完成
    manager.resume_task(&task_id).await;
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 完成后状态文件应被清理
    assert!(!state_path.exists(), "完成后状态文件应被清理");
}

/// 启动一个忽略 `Range` 头、总是返回 200 + 全量载荷的服务器，模拟不支持断点续传的服务端。
fn spawn_ignore_range_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_ignore_range_connection(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 忽略 Range、总返回 200 的连接处理：每个请求都回全量载荷
fn handle_ignore_range_connection(mut stream: TcpStream, data: &[u8]) {
    loop {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => return,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            data.len()
        );
        if stream.write_all(header.as_bytes()).is_err() {
            return;
        }
        if stream.write_all(data).is_err() {
            return;
        }
        if stream.flush().is_err() {
            return;
        }
    }
}

/// 启动一个「前 `hang_connections` 个连接只回一半字节便挂起、之后完整返回」的非范围服务器。
///
/// 服务器忽略 Range、总返回 200 + 全量载荷（与 [`spawn_ignore_range_server`] 一致）。
/// 前 `hang_connections` 个连接在声明完整 Content-Length 后只发送约一半字节即挂起，
/// 触发客户端 `read_timeout`；之后的连接完整返回全量载荷，供重试成功。返回 URL 与
/// 「已接受的连接总数」计数器。
///
/// 用于端到端验证：非范围服务器在下载中途超时进入重试后，重试发起的是全新完整请求，
/// 必须从偏移 0 从头写起（不能按 `chunk.downloaded` 续传），否则数据错位、任务失败。
fn spawn_nonrange_half_then_full_server(
    data: Vec<u8>,
    hang_connections: usize,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let connections = Arc::new(AtomicUsize::new(0));

    let data_inner = Arc::clone(&data);
    let counter = Arc::clone(&connections);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data_inner);
            let counter = Arc::clone(&counter);
            std::thread::spawn(move || {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if n <= hang_connections {
                    handle_half_then_hang(stream, &data);
                } else {
                    handle_ignore_range_connection(stream, &data);
                }
            });
        }
    });

    (format!("http://{addr}/file.bin"), connections)
}

/// 读取请求头后返回 200 + 完整 Content-Length，但只发送一半字节，随后挂起
/// （不关闭连接），等待客户端因 `read_timeout` 关闭。
fn handle_half_then_hang(mut stream: TcpStream, data: &[u8]) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        data.len()
    );
    if stream.write_all(header.as_bytes()).is_err() {
        return;
    }
    if stream.write_all(&data[..data.len() / 2]).is_err() {
        return;
    }
    let _ = stream.flush();
    std::thread::park();
}

/// 启动一个「不返回 Content-Length」的服务器：忽略 Range，总返回 200 + 全量载荷，
/// 用 `Connection: close` 让客户端靠连接关闭判定 body 结束（模拟 chunked/流式响应）。
fn spawn_no_content_length_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_no_content_length_connection(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 读取请求头后返回无 Content-Length 的 200 全量响应并关闭连接。
fn handle_no_content_length_connection(mut stream: TcpStream, data: &[u8]) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    // 故意省略 Content-Length：客户端须以连接关闭（EOF）作为响应体结束标志
    let header = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
    if stream.write_all(header.as_bytes()).is_err() {
        return;
    }
    if stream.write_all(data).is_err() {
        return;
    }
    let _ = stream.flush();
    // 关闭连接以终止响应体
    drop(stream);
}

/// 启动一个在 `Content-Disposition` 中返回指定文件名的服务器（忽略 Range，
/// 总返回 200 + 全量载荷），用于验证超长文件名在真实下载流程中被截断。
fn spawn_content_disposition_server(data: Vec<u8>, filename: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let filename = Arc::new(filename);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            let filename = Arc::clone(&filename);
            std::thread::spawn(move || {
                handle_content_disposition_connection(stream, &data, &filename)
            });
        }
    });

    format!("http://{addr}/file.bin")
}

/// 读取请求头后返回 200 + `Content-Disposition`（指定文件名）+ 全量载荷。
fn handle_content_disposition_connection(mut stream: TcpStream, data: &[u8], filename: &str) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Disposition: attachment; filename=\"{}\"\r\nConnection: close\r\n\r\n",
        data.len(),
        filename
    );
    if stream.write_all(header.as_bytes()).is_err() {
        return;
    }
    if stream.write_all(data).is_err() {
        return;
    }
    let _ = stream.flush();
    drop(stream);
}

/// 启动一个「下载中途截断」的服务器：
/// - 对探测请求 `bytes=0-0` 正确返回 206（让下载器获知大小与范围支持）；
/// - 对真正的分块请求声明完整 Content-Length，但只写一半后关闭连接，模拟连接中断。
fn spawn_truncating_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_truncating_connection(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 单连接处理：探测请求返回正确 206，分块请求只写一半后中断连接
fn handle_truncating_connection(mut stream: TcpStream, data: &[u8]) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let request = String::from_utf8_lossy(&buf).to_lowercase();
    let range_spec = request
        .lines()
        .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

    let Some(spec) = range_spec else {
        // 无 Range 的请求直接关闭，触发连接错误
        return;
    };
    let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
    let first = spec.split(',').next().unwrap_or("");
    let Some((a, b)) = first.split_once('-') else {
        return;
    };

    let start: usize = a.trim().parse().unwrap_or(0);
    let end: usize = if b.trim().is_empty() {
        total.saturating_sub(1)
    } else {
        b.trim().parse().unwrap_or(total.saturating_sub(1))
    };

    // 探测请求：单字节范围，返回正确的 206 让下载器确认大小与范围支持
    if start == 0 && end == 0 {
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(header.as_bytes()).unwrap();
        stream.write_all(&data[0..1]).unwrap();
        stream.flush().unwrap();
        return;
    }

    // 分块请求：声明完整长度，但只写一半后关闭，制造“下载中途中断”
    let end = end.min(total.saturating_sub(1));
    let full_len = end.saturating_sub(start).saturating_add(1);
    let half = full_len / 2;
    let header = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {full_len}\r\nContent-Range: bytes {start}-{end}/{total}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(&data[start..start + half]).unwrap();
    stream.flush().unwrap();
    // 不再写入，函数返回时 stream 被 drop → 连接关闭
}

/// 启动一个「只失败一次」的抖动服务器：探测请求与大多数分块请求正常返回，
/// 但当某个分块的起始偏移 `>= fail_start_offset` 时，首次会声明完整长度、却只写
/// 一半数据后主动断开连接（模拟下载中途网络中断），其后的请求恢复为正常。
/// 用于端到端验证「真实失败 → 状态文件留存 → 重新 start() 断点续传成功」链路。
fn spawn_fail_once_server(data: Vec<u8>, fail_start_offset: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let already_failed = Arc::new(AtomicBool::new(false));

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            let already_failed = Arc::clone(&already_failed);
            std::thread::spawn(move || {
                handle_fail_once_connection(stream, &data, fail_start_offset, already_failed)
            });
        }
    });

    format!("http://{addr}/file.bin")
}

/// 单连接处理：探测请求正常返回 206；分块请求在首次命中 `fail_start_offset` 之后
/// 时截断一半后断开，其余请求正常返回。
fn handle_fail_once_connection(
    mut stream: TcpStream,
    data: &[u8],
    fail_start_offset: usize,
    already_failed: Arc<AtomicBool>,
) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];

    loop {
        buf.clear();
        // 读取一个完整请求头块（以 \r\n\r\n 结尾）
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => return, // 客户端关闭
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let request = String::from_utf8_lossy(&buf).to_lowercase();
        let range_spec = request
            .lines()
            .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));
        let Some(spec) = range_spec else {
            // 本测试不会走到无 Range 的完整请求（服务器支持范围），兜底返回 200
            let header =
                format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(data);
            let _ = stream.flush();
            return;
        };
        let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
        let first = spec.split(',').next().unwrap_or("");
        let Some((a, b)) = first.split_once('-') else {
            return;
        };
        let start: usize = a.trim().parse().unwrap_or(0);
        let end: usize = if b.trim().is_empty() {
            total.saturating_sub(1)
        } else {
            b.trim().parse().unwrap_or(total.saturating_sub(1))
        };

        // 探测请求（bytes=0-0）：正常返回 206，让下载器确认大小与范围支持
        if start == 0 && end == 0 {
            let header = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: keep-alive\r\n\r\n"
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&data[0..1]);
            let _ = stream.flush();
            continue;
        }

        let end = end.min(total - 1);
        let body = &data[start..=end];

        // 首次命中失败门槛之后的偏移：声明完整长度但只写一半后关闭连接，模拟中断
        if start >= fail_start_offset && !already_failed.swap(true, Ordering::SeqCst) {
            let half = body.len() / 2;
            let header = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&body[..half]);
            let _ = stream.flush();
            return; // drop 连接，客户端检测到响应体不足而失败
        }

        // 正常返回完整分块
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(body);
        let _ = stream.flush();
    }
}

#[tokio::test]
async fn test_empty_file_download_against_local_server() {
    // 空文件：探测请求 bytes=0-0 不可满足（416），应被当作 0 字节、非范围请求处理
    let url = spawn_mock_server(Vec::new());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("空文件下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, Vec::<u8>::new(), "空文件内容应为 0 字节");
}

#[tokio::test]
async fn test_non_range_server_fallback_against_local_server() {
    // 服务器忽略 Range、总返回 200：下载器应回退到单块全量下载并正确落盘
    let data = test_data(64 * 1024);
    let url = spawn_ignore_range_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("非范围服务器下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "非范围服务器下载内容应与服务端载荷一致");
}

/// 验证非范围服务器在下载中途超时进入重试后，重试的完整请求从头写起、最终成功。
///
/// 非范围服务器（忽略 Range、总返回 200 全量）不支持断点续传，重试发起的必然是
/// 全新完整请求。若重试沿用首次尝试已写入的 `chunk.downloaded` 偏移（如复用探测
/// 响应体读到一半触发 `read_timeout`），全量响应会被写到 `start+downloaded` 之后，
/// 字节计数超预期，任务以「分块大小不匹配」永久失败——而非重试成功。此用例用
/// 「首个连接只回一半字节便挂起」的服务器精确制造该时序，断言重试后任务 Completed
/// 且文件内容与原数据字节级一致。
#[tokio::test]
async fn test_nonrange_mid_download_retry_restarts_from_scratch() {
    let data = test_data(200 * 1024);
    // 首个连接（探测，返回 200 全量响应体供复用）只回一半字节后挂起，触发
    // read_timeout；之后的连接完整返回全量载荷，供重试成功。
    let (url, connections) = spawn_nonrange_half_then_full_server(data.clone(), 1);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(200)),
        max_retries: 3,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(30)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => {
            panic!("非范围服务器中途超时重试应从头重下并成功，实际失败: {reason}")
        }
        other => panic!("非范围服务器中途超时重试应成功，实际: {other:?}"),
    }

    assert!(
        connections.load(Ordering::SeqCst) >= 2,
        "中途超时应触发至少 1 次重试（连接数 ≥ 2），实际: {}",
        connections.load(Ordering::SeqCst)
    );

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(
        output, data,
        "非范围服务器重试后文件内容应与原数据字节级一致"
    );
}

/// 验证「200 响应无 Content-Length」（chunked/流式、总大小未知）时仍能正确下载。
///
/// 这是非范围服务器的另一变体：既忽略 Range，又不提供 Content-Length，下载器需
/// 复用探测返回的 200 全量响应体、以连接关闭作为结束标志，把完整载荷落盘。
/// 与 `test_unknown_total_size_download_against_local_server`（206 + `bytes 0-0/*`）
/// 不同，本用例走 `extract_response_info` 的 Content-Length 缺失分支，并覆盖
/// 「复用探测 200 响应体」而非“再发一次全量 GET”。
#[tokio::test]
async fn test_no_content_length_server_download_against_local_server() {
    let data = test_data(256 * 1024);
    let url = spawn_no_content_length_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("无 Content-Length 服务器下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(
        output, data,
        "无 Content-Length 服务器下载内容应与服务端载荷一致"
    );
}

/// 验证超长 `Content-Disposition` 文件名会在真实下载流程中被截断，且后续的
/// `.temp`/`.download_state`/`.download_state.tmp` 中间文件不因越过 255 字节
/// 上限而创建/重命名失败。
///
/// 这是对 `sanitize_filename` 长度截断的端到端回归：单测只校验了截断字节数，
/// 无法覆盖「截断后的文件名再拼接最长后缀是否仍 ≤ 255」。
/// 服务器返回 304 字节文件名，应被截断到 235 字节（为最长中间路径
/// `.download_state.tmp` 19 字节 + `_` 前缀 1 字节预留空间）并成功落盘。
#[tokio::test]
async fn test_overlong_content_disposition_filename_is_truncated() {
    let data = test_data(64 * 1024);
    // 304 字节超长名（300 个 'a' + ".bin"），超过程序内截断上限
    let overlong_name = format!("{}.bin", "a".repeat(300));
    let url = spawn_content_disposition_server(data.clone(), overlong_name);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("超长文件名下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 解析出的文件名应已被截断到 235 字节（300 个 'a' 的前 235 个），扩展名被裁掉
    let filename = manager
        .get_task_filename(&task_id)
        .await
        .expect("应解析出最终文件名");
    assert_eq!(
        filename.len(),
        235,
        "超长文件名应被截断到 235 字节: {filename}"
    );
    assert_eq!(filename, "a".repeat(235));

    // 截断后的文件名应成功落盘且内容一致
    let output = std::fs::read(dir.path().join(&filename)).expect("读取下载文件失败");
    assert_eq!(output, data, "截断文件名下载内容应与服务端载荷一致");
}

#[tokio::test]
async fn test_truncated_download_fails_against_local_server() {
    // 连接在下载中途被截断：任务应进入 Failed 而非挂起或误判为完成
    let data = test_data(TEST_DATA_LEN);
    let url = spawn_truncating_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Failed(_) => {}
        DownloadStatus::Completed => panic!("截断的下载不应被判定为完成"),
        other => panic!("意外状态: {other:?}"),
    }
}

#[tokio::test]
async fn test_cancel_download_cleans_up_files() {
    // 2MB 文件 + 限速，制造可确定、足够长的下载窗口，便于在中途取消
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024), // 512KB/s，2MB 约需 4s
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待产生初步进度后取消
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager.cancel_task(&task_id).await;

    let temp_path = dir.path().join("file.bin.temp");
    let state_path = dir.path().join("file.bin.download_state");

    // 取消后状态应为 Cancelled，且临时/状态文件最终被后台任务异步清理
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = manager.get_task_status(&task_id).await;
        let cancelled = matches!(status, Some(DownloadStatus::Cancelled));
        let temp_gone = tokio::fs::metadata(&temp_path).await.is_err();
        let state_gone = tokio::fs::metadata(&state_path).await.is_err();

        if cancelled && temp_gone && state_gone {
            break;
        }
        assert!(Instant::now() < deadline, "取消后应清理临时/状态文件");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 取消后不应产出最终下载文件
    assert!(
        !dir.path().join("file.bin").exists(),
        "取消后不应产出下载完成的目标文件"
    );
}

/// 验证「取消后立即重启」：上一代（已被取消、正在排空分块）的后台任务不得把
/// 新一代重启任务的状态覆盖为 Cancelled 或清理其临时/状态文件。最终应成功下载。
///
/// 这是对启动世代（epoch）守卫的端到端回归：旧任务在 `download_chunks` 返回后被
/// 识别为过期而静默退出，重启任务得以不受干扰地跑到 Completed。
#[tokio::test]
async fn test_cancel_then_restart_download_succeeds() {
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024), // 512KB/s，2MB 约需 4s，制造足够长的下载窗口
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");
    // 等待产生初步进度后取消，再立即重启
    tokio::time::sleep(Duration::from_millis(500)).await;
    manager.cancel_task(&task_id).await;
    manager.start_task(&task_id).await.expect("重启下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(30)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("重启后下载失败: {reason}"),
        other => panic!("重启后应成功完成，实际: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "重启下载后内容应与服务端载荷一致");
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "重启完成后不应残留临时文件"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "重启完成后不应残留状态文件"
    );
}

#[tokio::test]
async fn test_remove_task_cancels_running_download() {
    // 移除下载中的任务应自动取消，并让后台任务在 join 全部分块后清理残留文件
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024), // 512KB/s，2MB 约需 4s
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待产生初步进度后移除
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(manager.remove_task(&task_id).await, "应能移除下载中的任务");

    // 任务应立即从管理器中移除
    assert_eq!(
        manager.get_task_status(&task_id).await,
        None,
        "移除后任务不应仍存在于管理器中"
    );

    let temp_path = dir.path().join("file.bin.temp");
    let state_path = dir.path().join("file.bin.download_state");

    // 后台任务收到取消信号后 join 全部分块并清理临时/状态文件
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let temp_gone = tokio::fs::metadata(&temp_path).await.is_err();
        let state_gone = tokio::fs::metadata(&state_path).await.is_err();
        if temp_gone && state_gone {
            break;
        }
        assert!(Instant::now() < deadline, "移除后应清理临时/状态文件");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 移除后不应产出最终下载文件
    assert!(
        !dir.path().join("file.bin").exists(),
        "移除后不应产出下载完成的目标文件"
    );
}

/// 验证移除「已暂停」任务同样会取消它并清理残留文件。
///
/// `remove_task` 文档承诺会取消 `Pending`/`Downloading`/`Paused` 中的任务；下载中
/// 的路径已被 `test_remove_task_cancels_running_download` 覆盖，但暂停状态的取消链路
/// 依赖 `cancel()` 唤醒 `wait_while_paused` 里的分块、再由后台任务清理，需单独兜底。
#[tokio::test]
async fn test_remove_task_cancels_paused_download() {
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 产生初步进度后暂停
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager.pause_task(&task_id).await;
    assert_eq!(
        manager.get_task_status(&task_id).await,
        Some(DownloadStatus::Paused),
        "应已进入暂停状态"
    );

    assert!(manager.remove_task(&task_id).await, "应能移除暂停中的任务");
    assert_eq!(
        manager.get_task_status(&task_id).await,
        None,
        "移除后任务不应仍存在于管理器中"
    );

    // 后台任务收到取消信号后 join 分块并清理临时/状态文件
    let temp_path = dir.path().join("file.bin.temp");
    let state_path = dir.path().join("file.bin.download_state");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let temp_gone = tokio::fs::metadata(&temp_path).await.is_err();
        let state_gone = tokio::fs::metadata(&state_path).await.is_err();
        if temp_gone && state_gone {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "移除暂停任务后应清理临时/状态文件"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        !dir.path().join("file.bin").exists(),
        "移除暂停任务后不应产出下载完成的目标文件"
    );
}

/// 验证 remove_task 与「已写满全部字节、正在收尾」的后台任务竞态时的语义。
///
/// 进度快照在最后一个分块的写入循环内发布（`downloaded == total_size`），此刻
/// 后台任务仍处于 `Downloading`——临时文件 rename 与状态写入尚未完成。若此时调用
/// `remove_task`，正确语义是**等待收尾完成**（最终文件保留、任务以 `Completed`
/// 结束、事件为 `TaskCompleted`），而非把正在收尾的任务取消、删掉已完整下载的
/// 临时文件（最终文件丢失 / 误发 `TaskCancelled`）。
///
/// 通过限速下载制造「写满后收尾」的时间窗口，并在收到写满快照后立即移除，
/// 无论竞态是否命中该窗口，最终产物与终止事件都应一致。
#[tokio::test]
async fn test_remove_task_racing_finalization_preserves_file() {
    // 1MB + 512KB/s 限速：约 2s 下载时长，且最后一个分块的写入与上一次进度发布
    // 间隔 ≥ PROGRESS_PUBLISH_TICK，保证「写满快照」真实存在、可被观察到
    let data = test_data(1024 * 1024);
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    // 必须在 start 前订阅事件，否则会漏掉广播
    let mut events = manager.subscribe_events();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    // 在 start 前订阅进度，避免错过写满快照
    let mut progress = manager
        .subscribe_progress(&task_id)
        .await
        .expect("任务应存在");

    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待进度快照显示已写满全部字节（该快照在最后一个分块写入循环内发布，
    // 后台任务此刻仍在收尾：rename 与状态写入尚未完成）
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let stats = progress.borrow().clone();
        if stats.total_size > 0 && stats.downloaded >= stats.total_size {
            break;
        }
        assert!(Instant::now() < deadline, "等待下载完成快照超时");
        let _ = tokio::time::timeout(Duration::from_secs(5), progress.changed()).await;
    }

    // 收尾窗口内立即移除任务：修复后的语义是等待收尾完成而非取消
    assert!(manager.remove_task(&task_id).await, "应能移除任务");

    // 任务应从管理器中移除
    assert_eq!(
        manager.get_task_status(&task_id).await,
        None,
        "移除后任务不应仍存在于管理器中"
    );

    // 最终文件必须保留且字节级一致（竞态下不得丢失已完整下载的文件）
    let output = std::fs::read(dir.path().join("file.bin")).expect("最终文件应保留");
    assert_eq!(output, data, "最终文件内容应与服务端载荷一致");

    // 不应残留临时/状态文件（已完成任务由后台与 remove_task 双重清理）
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );

    // 终止事件必须是 TaskCompleted（而非误发的 TaskCancelled）
    let event = wait_for_terminal_event(&mut events, Duration::from_secs(5)).await;
    assert_eq!(
        event,
        DownloadEvent::TaskCompleted {
            task_id: task_id.clone()
        }
    );
}

/// 验证「取消后立即重启」的世代隔离语义。
///
/// `cancel()` 后立刻再次 `start()` 会并发运行两代后台任务：旧世代被取消后
/// 仍在排空已 abort 的分块，新世代则覆盖共享的 `active_chunks` 槽位。若槽位
/// 不带世代标识，旧世代的轮询/中止逻辑会把新世代的分块集合误当作自己的去
/// abort，导致新世代分块被中止、部分写入的临时文件被重命名为最终名并静默
/// 报 Completed（数据损坏）。本测试断言重启后任务以 Completed 结束、最终
/// 文件字节级完整。
#[tokio::test]
async fn test_cancel_then_immediately_restart_preserves_integrity() {
    // 2MB + 512KB/s 限速：约 4s 下载时长，取消时旧世代仍有分块在跑，保证
    // 「取消后立即重启」确实产生两代后台任务并发排空的窗口
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    manager
        .start_task(&task_id)
        .await
        .expect("首次开始下载失败");

    // 等待产生初步进度后取消，随后立即重启同一任务
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager.cancel_task(&task_id).await;
    manager
        .start_task(&task_id)
        .await
        .expect("取消后立即重启失败");

    // 重启后的任务应最终进入 Completed（而非被旧世代误 abort 导致 Failed/残缺）
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(30)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("重启后下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 最终文件必须字节级完整（不得是部分写入被重命名的残缺文件）
    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "重启后下载内容应与服务端载荷一致");

    // 不应残留临时/状态文件
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

/// 启动一个「接受连接后永不返回响应」的服务器，模拟网络黑洞。
///
/// 它读取完整请求头后保持连接打开但不返回任何字节，用于验证：仅靠
/// `connect_timeout` 不足以覆盖「建连后等待响应头」这一段，必须由
/// `read_timeout` 兜底，否则探测/分块请求会永久挂起。
fn spawn_hanging_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                let mut request = Vec::new();
                // 读完请求头后即停：不回任何响应字节，保持连接挂起
                loop {
                    match stream.read(&mut buf) {
                        Ok(0) => return, // 客户端关闭
                        Ok(n) => {
                            request.extend_from_slice(&buf[..n]);
                            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => return,
                    }
                }
                // 挂起不响应（无需主动关闭；测试进程结束时线程随之终止）
                std::thread::park();
            });
        }
    });

    format!("http://{addr}/file.bin")
}

#[tokio::test]
async fn test_read_timeout_on_hanging_server() {
    // 服务器接受连接但不响应：应在 read_timeout 内进入 Failed，而非永久挂起
    let url = spawn_hanging_server();

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(300)),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("超时"),
                "失败原因应包含超时信息: {reason}"
            );
        }
        other => panic!("挂起服务器应导致下载失败，实际: {other:?}"),
    }
}

/// 启动一个「探测（bytes=0-0）完整响应、但分块主体只回一半便挂起不关连接」的服务器。
///
/// 用于端到端验证 `read_timeout` 对「下载中途读数挂起」的兜底：`bytes=0-0` 探测正常
/// 返回，使下载进入分块阶段；随后每个分块只收到约一半字节，`Content-Length` 却声明
/// 完整长度，`next_chunk` 在后续 `response.chunk()` 上阻塞，最终被 `read_timeout` 中止。
fn spawn_mid_download_hanging_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_mid_download_hang(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 处理「中途挂起」服务器的单个连接：探测完整返回，分块只回一半后 park 挂起。
fn handle_mid_download_hang(mut stream: TcpStream, data: &[u8]) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut read_buf = [0u8; 8192];

    loop {
        buf.clear();
        // 读取一个完整请求头块（以 \r\n\r\n 结尾）
        loop {
            match stream.read(&mut read_buf) {
                Ok(0) => return, // 客户端关闭
                Ok(n) => {
                    buf.extend_from_slice(&read_buf[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let request = String::from_utf8_lossy(&buf).to_lowercase();
        let range_spec = request
            .lines()
            .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

        let Some(spec) = range_spec else {
            // 无 Range 请求（不应出现，探测也带 Range）：全量响应后关闭
            let header =
                format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(data);
            let _ = stream.flush();
            return;
        };

        let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
        let first = spec.split(',').next().unwrap_or("");
        let Some((a, b)) = first.split_once('-') else {
            return;
        };
        let start: usize = a.trim().parse().unwrap_or(0);

        // 探测：完整返回 1 字节，保证探测成功、下载进入分块阶段
        if a.trim() == "0" && b.trim() == "0" && start < total {
            let body = &data[0..1];
            let header = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: keep-alive\r\n\r\n"
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(body);
            let _ = stream.flush();
            continue;
        }

        let end: usize = if b.trim().is_empty() {
            total.saturating_sub(1)
        } else {
            b.trim().parse().unwrap_or(total.saturating_sub(1))
        };
        if start >= total {
            let _ = stream.write_all(
                b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            return;
        }
        let end = end.min(total - 1);
        let slice = &data[start..=end];
        // 声明完整长度，但只发送约一半字节，随后 park 挂起（不关闭连接）
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nConnection: keep-alive\r\n\r\n",
            slice.len()
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&slice[..slice.len() / 2]);
        let _ = stream.flush();
        std::thread::park();
    }
}

/// 启动一个「前 N 个请求完全不响应（挂起连接）、之后正常响应」的服务器。
///
/// 用于端到端验证探测请求（bytes=0-0）的瞬时错误自动重试：探测在前 N 次请求时
/// 因 `read_timeout` 失败，之后重试成功并继续完成整个下载。返回 URL 与
/// 「已接受的连接总数」计数器（= 原始尝试 + 重试次数）。
fn spawn_retry_then_succeed_server(
    data: Vec<u8>,
    hang_requests: usize,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let requests = Arc::new(AtomicUsize::new(0));

    let data_inner = Arc::clone(&data);
    let counter = Arc::clone(&requests);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data_inner);
            let counter = Arc::clone(&counter);
            std::thread::spawn(move || {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if n <= hang_requests {
                    // 接受连接但不发送任何字节，等待客户端因 read_timeout 关闭连接
                    let mut stream = stream;
                    let mut discard = [0u8; 1024];
                    loop {
                        match stream.read(&mut discard) {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                } else {
                    handle_connection(stream, &data, Arc::new(Mutex::new(Vec::new())));
                }
            });
        }
    });

    (format!("http://{addr}/file.bin"), requests)
}

/// 启动一个「探测正常、前 N 个分块请求只回一半字节便挂起、之后正常响应」的服务器。
///
/// 用于端到端验证分块级自动重试 + 续传：首个分块读取到一半触发 `read_timeout`，
/// 重试请求以 `start + downloaded` 续传，最终字节级一致。探测（bytes=0-0）始终
/// 正常响应，不计入挂起配额。返回 URL 与「收到的分块请求总数」计数器
/// （= 正常分块数 + 超时后的续传重试请求数）。
fn spawn_chunk_retry_then_succeed_server(
    data: Vec<u8>,
    hang_chunks: usize,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let chunk_requests = Arc::new(AtomicUsize::new(0));

    let data_inner = Arc::clone(&data);
    let counter = Arc::clone(&chunk_requests);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data_inner);
            let counter = Arc::clone(&counter);
            std::thread::spawn(move || handle_chunk_retry(stream, &data, hang_chunks, counter));
        }
    });

    (format!("http://{addr}/file.bin"), chunk_requests)
}

/// 处理「分块重试」服务器的单个连接：
/// - 探测（bytes=0-0）：完整返回 1 字节，保证探测成功、进入分块阶段；
/// - 分块请求：计数。若计数 ≤ hang_chunks，声明完整 Content-Length 但只回一半
///   字节后保持连接挂起（等待客户端因 read_timeout 关闭），触发读取超时；
///   否则完整返回请求区间，供重试续传成功。
fn handle_chunk_retry(
    mut stream: TcpStream,
    data: &[u8],
    hang_chunks: usize,
    counter: Arc<AtomicUsize>,
) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut read_buf = [0u8; 8192];

    loop {
        buf.clear();
        loop {
            match stream.read(&mut read_buf) {
                Ok(0) => return, // 客户端关闭
                Ok(n) => {
                    buf.extend_from_slice(&read_buf[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => return,
            }
        }

        let request = String::from_utf8_lossy(&buf).to_lowercase();
        let range_spec = request
            .lines()
            .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

        let Some(spec) = range_spec else {
            // 无 Range 请求（不应出现，探测也带 Range）：全量响应后关闭
            let header =
                format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(data);
            let _ = stream.flush();
            return;
        };

        let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
        let first = spec.split(',').next().unwrap_or("");
        let Some((a, b)) = first.split_once('-') else {
            return;
        };
        let start: usize = a.trim().parse().unwrap_or(0);

        // 探测请求：完整返回，不计入挂起配额
        if a.trim() == "0" && b.trim() == "0" && start < total {
            let header = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: keep-alive\r\n\r\n"
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&data[0..1]);
            let _ = stream.flush();
            continue;
        }

        let end: usize = if b.trim().is_empty() {
            total.saturating_sub(1)
        } else {
            b.trim().parse().unwrap_or(total.saturating_sub(1))
        };
        if start >= total {
            let _ = stream.write_all(
                b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            return;
        }
        let end = end.min(total - 1);
        let slice = &data[start..=end];

        let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nConnection: keep-alive\r\n\r\n",
            slice.len()
        );

        if n <= hang_chunks {
            // 只回一半字节，随后挂起等待客户端因 read_timeout 关闭连接
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&slice[..slice.len() / 2]);
            let _ = stream.flush();
            let mut discard = [0u8; 1024];
            loop {
                match stream.read(&mut discard) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            return; // 连接已被客户端关闭，结束本连接
        }

        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(slice);
        let _ = stream.flush();
    }
}

#[tokio::test]
async fn test_mid_download_read_timeout_fails_against_local_server() {
    // 200KB 默认切分为 2 块；服务器对每个分块只回一半字节便挂起，
    // 下载器应在 read_timeout 内以「读取超时」失败而非永久挂起。
    let data = test_data(200 * 1024);
    let url = spawn_mid_download_hanging_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(300)),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("超时"),
                "下载中途挂起应因读取超时失败，实际原因: {reason}"
            );
        }
        other => panic!("下载中途挂起应导致失败，实际: {other:?}"),
    }
}

#[tokio::test]
async fn test_probe_timeout_auto_retry_succeeds() {
    // 服务器对前 2 个请求（均为探测，bytes=0-0）完全不响应，触发 read_timeout；
    // 下载器应在 max_retries=2 内自动重试，探测第 3 次成功并完成整个下载。
    let data = test_data(TEST_DATA_LEN);
    let (url, connections) = spawn_retry_then_succeed_server(data.clone(), 2);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(300)),
        max_retries: 2,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("探测重试后仍失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 探测重试了 2 次：总连接数 ≥ 3（2 次挂起的探测 + 后续探测/分块连接），
    // 证明探测级自动重试确实发生
    assert!(
        connections.load(Ordering::SeqCst) >= 3,
        "应发生探测重试（连接数 >= 3），实际: {}",
        connections.load(Ordering::SeqCst)
    );

    let filename = manager
        .get_task_filename(&task_id)
        .await
        .expect("应解析出最终文件名");
    let output = std::fs::read(dir.path().join(&filename)).expect("读取下载文件失败");
    assert_eq!(output, data, "下载内容应与服务端载荷一致");
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

#[tokio::test]
async fn test_chunk_read_timeout_auto_retry_succeeds() {
    // 探测正常返回；首个分块请求只回一半字节便挂起触发 read_timeout，
    // 下载器应在 max_retries 内自动重试，从已写入偏移续传（Range 头以
    // `start + downloaded` 起始），最终字节级一致。
    let data = test_data(TEST_DATA_LEN);
    let (url, chunk_requests) = spawn_chunk_retry_then_succeed_server(data.clone(), 1);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(300)),
        max_retries: 2,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("分块重试后仍失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 200KB 默认 2 块；发生了一次分块超时重试，因此分块请求总数应为 3
    // （2 块成功 + 1 次超时后的续传请求），证明分块级自动重试确实发生
    assert!(
        chunk_requests.load(Ordering::SeqCst) >= 3,
        "应发生分块重试（分块请求数 >= 3），实际: {}",
        chunk_requests.load(Ordering::SeqCst)
    );

    let filename = manager
        .get_task_filename(&task_id)
        .await
        .expect("应解析出最终文件名");
    let output = std::fs::read(dir.path().join(&filename)).expect("读取下载文件失败");
    assert_eq!(output, data, "下载内容应与服务端载荷一致");
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

/// 验证 `max_retries: 0` 时禁用自动重试：探测请求在 `read_timeout` 内超时后
/// 立即失败，不发起任何重试（连接数恒为 1）。这是 `DownloadConfig::max_retries`
/// 文档承诺的行为（0 表示禁用），此前无端到端覆盖。
#[tokio::test]
async fn test_max_retries_zero_disables_retry() {
    // 服务器对任意请求（含探测）都挂起不响应；若重试被意外启用，连接数会 > 1
    let (url, connections) = spawn_retry_then_succeed_server(Vec::new(), usize::MAX);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(300)),
        max_retries: 0,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("超时"),
                "max_retries=0 时应因超时失败，实际原因: {reason}"
            );
        }
        other => panic!("max_retries=0 时应立即失败，实际: {other:?}"),
    }

    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "max_retries=0 时不应重试（连接数应为 1），实际: {}",
        connections.load(Ordering::SeqCst)
    );
}

/// 验证自动重试仅针对瞬时（超时类）错误：永久性错误（如 HTTP 404）即使配置了
/// `max_retries > 0` 也不会重试（连接数恒为 1），避免对无意义的错误做无谓重试。
#[tokio::test]
async fn test_permanent_error_not_retried() {
    let (url, connections) = spawn_status_server_counting("404 Not Found");

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        max_retries: 3, // 显式配置重试，验证 404 仍不重试
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("404"),
                "失败原因应包含状态码 404，实际: {reason}"
            );
        }
        other => panic!("预期 Failed，实际 {other:?}"),
    }

    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "永久性错误不应重试（连接数应为 1），实际: {}",
        connections.load(Ordering::SeqCst)
    );
}

/// 验证分块处于重试退避等待期时取消能及时生效。
///
/// 首次尝试超时后分块进入指数退避（最长达 10s），若取消只置标志位、等分块
/// 自然睡醒才检查，用户会感知到最长一个退避周期的延迟。此测试在退避窗口内
/// 取消，断言最终进入 `Cancelled` 且**不再发起第 2 次重试请求**（分块请求数
/// 恒为 1），覆盖「取消与重试交错」这一行为契约。
#[tokio::test]
async fn test_cancel_during_backoff_stops_retries() {
    // 探测正常返回；所有分块首次请求都只回一半字节后挂起（触发 read_timeout
    // 进入重试退避）。若取消不及时，第 2 次重试请求会把分块请求数推到 2。
    // 使用 50KB（< 默认最小块 100KB）保证只有 1 个分块，便于精确计数。
    let data = test_data(50 * 1024);
    let (url, chunk_requests) = spawn_chunk_retry_then_succeed_server(data.clone(), usize::MAX);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(200)),
        max_retries: 3,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待首个分块首次尝试超时（200ms）并进入退避窗口（500ms）后取消。
    // 即便系统调度使首次超时略晚于此刻，取消落在首次尝试期也能满足
    // 「不再发起第 2 次请求」的断言，时序稳健。
    tokio::time::sleep(Duration::from_millis(250)).await;
    manager.cancel_task(&task_id).await;

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Cancelled => {}
        DownloadStatus::Failed(reason) => {
            panic!("退避期间取消应得到 Cancelled，实际失败: {reason}")
        }
        other => panic!("退避期间取消应得到 Cancelled，实际: {other:?}"),
    }

    assert_eq!(
        chunk_requests.load(Ordering::SeqCst),
        1,
        "退避期间取消后不应发起第 2 次重试（分块请求数应为 1），实际: {}",
        chunk_requests.load(Ordering::SeqCst)
    );
}

/// 验证「暂停发生在重试退避窗口内」时暂停立即生效并**冻结**退避计时：
/// 暂停期间不发起下一次重试请求（分块请求数保持 1），状态置为 Paused；
/// 恢复后按剩余退避继续计时并最终重试成功、字节级一致。
///
/// 若不响应暂停（旧行为：退避休眠不可中断），pause() 在退避窗口内「看似失效」——
/// 需等满整个退避（这里 3s）重试请求才发出，期间下载仍在后台推进。断言窗口
/// 刻意超过整个退避时长，若旧行为会把分块请求数推到 2，即可被本用例捕获。
#[tokio::test]
async fn test_pause_during_backoff_freezes_retry_timer() {
    // 50KB < 默认最小块 100KB，单分块；首个分块请求挂起触发 read_timeout，
    // 后续请求完整返回供重试成功
    let data = test_data(50 * 1024);
    let (url, chunk_requests) = spawn_chunk_retry_then_succeed_server(data.clone(), 1);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(200)),
        max_retries: 3,
        retry_base_delay: Duration::from_secs(3), // 长退避，保证暂停落在退避窗口内
        retry_max_delay: Duration::from_secs(3),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 首个分块首次尝试约 200ms 超时进入 3s 退避；此刻（400ms）暂停。
    // 即便调度使首次超时略晚，暂停仍落在 3s 退避窗口内，时序稳健。
    tokio::time::sleep(Duration::from_millis(400)).await;
    manager.pause_task(&task_id).await;

    assert_eq!(
        manager.get_task_status(&task_id).await,
        Some(DownloadStatus::Paused),
        "退避期间暂停后状态应为 Paused"
    );

    // 断言窗口超过整个退避时长（3s + 余量）：若退避休眠不被暂停中断，
    // 重试会在此窗口内发出（分块请求数 -> 2）；被冻结则保持 1。
    tokio::time::sleep(Duration::from_millis(3500)).await;
    assert_eq!(
        chunk_requests.load(Ordering::SeqCst),
        1,
        "暂停期间不应发起重试（分块请求数应为 1），实际: {}",
        chunk_requests.load(Ordering::SeqCst)
    );

    // 恢复后按剩余退避继续，重试成功，任务最终完成且字节级一致
    manager.resume_task(&task_id).await;
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(30)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("恢复后重试失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let filename = manager
        .get_task_filename(&task_id)
        .await
        .expect("应解析出最终文件名");
    let output = std::fs::read(dir.path().join(&filename)).expect("读取下载文件失败");
    assert_eq!(output, data, "下载内容应与服务端载荷一致");
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

/// 验证「取消发生在探测请求的重试退避窗口内」时能及时生效并进入取消收尾。
///
/// 探测（bytes=0-0）首次请求超时后进入指数退避（最长 3s）；旧实现（`with_retry`）
/// 的退避休眠不可被取消中断，取消后仍需等满整个退避周期再发起第 2 次探测请求，
/// 且第 2 次仍超时后把瞬时错误一路向上抛、误报 `TaskFailed`。修复后
/// （`probe_with_retry`）取消立即中断退避并返回 `Cancelled`，由 `start()` 进入
/// `finish_cancelled` 取消收尾：状态置 `Cancelled`、发布 `TaskCancelled` 事件、
/// **不再发起第 2 次探测请求**（连接数恒为 1）。
#[tokio::test]
async fn test_cancel_during_probe_backoff_stops_retries() {
    // 首个连接（探测请求）挂起触发 read_timeout 进入退避；若取消不及时，第 2 次
    // 探测请求会把连接数推到 2。后续分块阶段无需真实完成，用 usize::MAX 挂起全部连接。
    let data = test_data(50 * 1024);
    let (url, connections) = spawn_retry_then_succeed_server(data, usize::MAX);

    let manager = DownloadManager::new();
    // 必须在 start 前订阅事件，否则会漏掉广播
    let mut events = manager.subscribe_events();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(200)),
        max_retries: 3,
        retry_base_delay: Duration::from_secs(3), // 长退避，保证取消落在退避窗口内
        retry_max_delay: Duration::from_secs(3),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 首个探测约 200ms 超时进入 3s 退避；此刻（250ms）取消。即便调度使首次超时
    // 略晚，取消仍落在退避窗口内，时序稳健。
    tokio::time::sleep(Duration::from_millis(250)).await;
    manager.cancel_task(&task_id).await;

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Cancelled => {}
        DownloadStatus::Failed(reason) => {
            panic!("探测退避期间取消应得到 Cancelled，实际失败: {reason}")
        }
        other => panic!("探测退避期间取消应得到 Cancelled，实际: {other:?}"),
    }

    // 终止事件应为 TaskCancelled（取消收尾），而非旧实现的 TaskFailed 误报
    let event = wait_for_terminal_event(&mut events, Duration::from_secs(5)).await;
    match event {
        DownloadEvent::TaskCancelled { task_id: id } => {
            assert_eq!(id, task_id, "取消事件应携带正确的任务 ID");
        }
        other => panic!("预期 TaskCancelled，实际 {other:?}"),
    }

    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "探测退避期间取消后不应发起第 2 次探测请求（连接数应为 1），实际: {}",
        connections.load(Ordering::SeqCst)
    );
}

/// 验证「暂停发生在探测请求的重试退避窗口内」时暂停立即生效并**冻结**退避计时：
/// 暂停期间不发起下一次探测请求（连接数保持 1），状态置为 Paused；恢复后按剩余
/// 退避继续并最终探测成功、字节级一致地完成下载。
///
/// 若不响应暂停（旧行为：`with_retry` 的退避休眠不可中断），pause() 在探测退避
/// 窗口内「看似失效」——需等满整个退避（这里 3s）第 2 次探测请求才发出，期间
/// 下载仍在后台推进。断言窗口刻意超过整个退避时长，若旧行为会把连接数推到 2，
/// 即可被本用例捕获。
#[tokio::test]
async fn test_pause_during_probe_backoff_freezes_retry_timer() {
    // 首个连接（探测请求）挂起触发 read_timeout 进入退避；之后连接正常响应，
    // 供重试探测成功并继续完成整个下载
    let data = test_data(50 * 1024);
    let (url, connections) = spawn_retry_then_succeed_server(data.clone(), 1);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        read_timeout: Some(Duration::from_millis(200)),
        max_retries: 3,
        retry_base_delay: Duration::from_secs(3), // 长退避，保证暂停落在退避窗口内
        retry_max_delay: Duration::from_secs(3),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 首个探测约 200ms 超时进入 3s 退避；此刻（400ms）暂停。即便调度使首次超时
    // 略晚，暂停仍落在 3s 退避窗口内，时序稳健。
    tokio::time::sleep(Duration::from_millis(400)).await;
    manager.pause_task(&task_id).await;

    assert_eq!(
        manager.get_task_status(&task_id).await,
        Some(DownloadStatus::Paused),
        "退避期间暂停后状态应为 Paused"
    );

    // 断言窗口超过整个退避时长（3s + 余量）：若退避休眠不被暂停中断，第 2 次
    // 探测请求会在此窗口内发出（连接数 -> 2）；被冻结则保持 1。
    tokio::time::sleep(Duration::from_millis(3500)).await;
    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "暂停期间不应发起探测重试（连接数应为 1），实际: {}",
        connections.load(Ordering::SeqCst)
    );

    // 恢复后按剩余退避继续，探测成功、分块下载完成且字节级一致
    manager.resume_task(&task_id).await;
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(30)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("恢复后重试失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let filename = manager
        .get_task_filename(&task_id)
        .await
        .expect("应解析出最终文件名");
    let output = std::fs::read(dir.path().join(&filename)).expect("读取下载文件失败");
    assert_eq!(output, data, "下载内容应与服务端载荷一致");
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

#[tokio::test]
async fn test_connection_refused_fails_against_local_server() {
    // 构造“无监听者”的地址（绑定临时端口后立即释放），模拟目标主机不可达。
    // 探测请求应在默认（无 read_timeout）路径下快速失败并进入 Failed，
    // 而非挂起到 30s connect_timeout 或误判为其它状态。这补上了与「挂起服务器
    // （接受连接但不响应，由 read_timeout 兜底）」互补的“连接层失败”端到端覆盖。
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").expect("绑定临时端口失败");
        listener.local_addr().unwrap().port()
    };
    let url = format!("http://127.0.0.1:{port}/file.bin");

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Failed(reason) => {
            assert!(!reason.to_string().is_empty(), "失败原因不应为空");
        }
        DownloadStatus::Completed => panic!("拒绝连接的下载不应被判定为完成"),
        other => panic!("意外状态: {other:?}"),
    }
}

/// 启动一个「Content-Range 响应头错位」的服务器。
///
/// 探测请求正常返回 206；对分块请求则返回正确长度的数据，但其 Content-Range
/// 声明从 0 开始（与实际请求偏移不符），模拟服务器错位返回区间。下载器应校验
/// 并拒绝，而不是静默把错误字节写到错误偏移、损坏文件。
fn spawn_wrong_content_range_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_wrong_content_range_connection(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 每个连接只处理一个请求：探测返回正确 206，分块请求返回错位 Content-Range
fn handle_wrong_content_range_connection(mut stream: TcpStream, data: &[u8]) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let request = String::from_utf8_lossy(&buf).to_lowercase();
    let range_spec = request
        .lines()
        .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

    let Some(spec) = range_spec else { return };
    let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
    let first = spec.split(',').next().unwrap_or("");
    let Some((a, b)) = first.split_once('-') else {
        return;
    };

    let start: usize = a.trim().parse().unwrap_or(0);
    let end: usize = if b.trim().is_empty() {
        total.saturating_sub(1)
    } else {
        b.trim().parse().unwrap_or(total.saturating_sub(1))
    };

    if start == 0 && end == 0 {
        // 探测请求：返回正确 206
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(header.as_bytes()).unwrap();
        stream.write_all(&data[0..1]).unwrap();
        stream.flush().unwrap();
        return;
    }

    // 分块请求：返回正确长度的数据，但 Content-Range 声明从 0 开始（错位）
    let end = end.min(total.saturating_sub(1));
    let len = end.saturating_sub(start).saturating_add(1);
    let header = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {len}\r\nContent-Range: bytes 0-{}/{total}\r\nConnection: close\r\n\r\n",
        len.saturating_sub(1)
    );
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(&data[start..=end]).unwrap();
    stream.flush().unwrap();
}

#[tokio::test]
async fn test_misaligned_content_range_fails_against_local_server() {
    // 服务器返回与请求不一致的 Content-Range：应校验并拒绝，避免错位写入损坏文件
    let data = test_data(TEST_DATA_LEN);
    let url = spawn_wrong_content_range_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("Content-Range"),
                "失败原因应涉及 Content-Range 校验: {reason}"
            );
        }
        DownloadStatus::Completed => panic!("错位的 Content-Range 不应被判定为完成"),
        other => panic!("意外状态: {other:?}"),
    }
}

/// 启动「分块响应缺失 Content-Range」的服务器。
///
/// 探测请求正常返回 206 + Content-Range（证明支持范围请求），但分块请求返回 206
/// 却**缺失** `Content-Range` 头——违反 RFC 7233。下载器应判定为协议违规并拒绝，
/// 而非静默地把未校验的数据写入文件。
fn spawn_missing_content_range_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_missing_content_range_connection(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 探测返回正确 206，分块请求返回 206 但缺失 Content-Range 头
fn handle_missing_content_range_connection(mut stream: TcpStream, data: &[u8]) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let request = String::from_utf8_lossy(&buf).to_lowercase();
    let range_spec = request
        .lines()
        .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

    let Some(spec) = range_spec else { return };
    let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
    let first = spec.split(',').next().unwrap_or("");
    let Some((a, b)) = first.split_once('-') else {
        return;
    };

    let start: usize = a.trim().parse().unwrap_or(0);
    let end: usize = if b.trim().is_empty() {
        total.saturating_sub(1)
    } else {
        b.trim().parse().unwrap_or(total.saturating_sub(1))
    };

    if start == 0 && end == 0 {
        // 探测请求：返回正确 206 + Content-Range（证明支持范围）
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(header.as_bytes()).unwrap();
        stream.write_all(&data[0..1]).unwrap();
        stream.flush().unwrap();
        return;
    }

    // 分块请求：返回正确长度与内容，但缺 Content-Range（违反 RFC 7233）
    let end = end.min(total.saturating_sub(1));
    let len = end.saturating_sub(start).saturating_add(1);
    let header = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(&data[start..=end]).unwrap();
    stream.flush().unwrap();
}

#[tokio::test]
async fn test_missing_content_range_fails_against_local_server() {
    // 服务器返回 206 但缺失 Content-Range：依 RFC 7233 属协议违规，应拒绝而非静默写入
    let data = test_data(TEST_DATA_LEN);
    let url = spawn_missing_content_range_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("Content-Range"),
                "失败原因应涉及 Content-Range 校验: {reason}"
            );
            // 失败原因应携带分块序号，便于在并发多分块下载中定位到具体问题分块
            assert!(
                reason.to_string().contains("分块"),
                "失败原因应包含分块序号以定位问题分块: {reason}"
            );
        }
        DownloadStatus::Completed => panic!("缺失 Content-Range 不应被判定为完成"),
        other => panic!("意外状态: {other:?}"),
    }
}

/// 启动「探测正常、分块请求一律失败」的服务器，并统计分块 Range 请求次数。
///
/// 用于验证「快速失败」：当一个分块失败时，下载器应在**生成剩余分块之前**中止，
/// 而不是把全部分块都逐个生成并请求（浪费带宽）。返回 `(URL, 分块请求计数器)`，
/// 探测请求（`bytes=0-0`）不计入。
fn spawn_fail_all_chunks_server(data: Vec<u8>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    let counter = Arc::new(AtomicUsize::new(0));

    let counter_inner = Arc::clone(&counter);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            let counter = Arc::clone(&counter_inner);
            std::thread::spawn(move || handle_fail_all_chunks_connection(stream, &data, counter));
        }
    });

    (format!("http://{addr}/file.bin"), counter)
}

/// 探测返回正常 206 + Content-Range；分块请求一律返回缺失 Content-Range 的 206（协议违规）。
fn handle_fail_all_chunks_connection(
    mut stream: TcpStream,
    data: &[u8],
    counter: Arc<AtomicUsize>,
) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let request = String::from_utf8_lossy(&buf).to_lowercase();
    let range_spec = request
        .lines()
        .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

    let Some(spec) = range_spec else { return };
    let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
    let first = spec.split(',').next().unwrap_or("");
    let Some((a, b)) = first.split_once('-') else {
        return;
    };

    let start: usize = a.trim().parse().unwrap_or(0);
    let end: usize = if b.trim().is_empty() {
        total.saturating_sub(1)
    } else {
        b.trim().parse().unwrap_or(total.saturating_sub(1))
    };

    // 探测请求：正常返回 206 + Content-Range（证明支持范围请求）
    if start == 0 && end == 0 {
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(header.as_bytes()).unwrap();
        stream.write_all(&data[0..1]).unwrap();
        stream.flush().unwrap();
        return;
    }

    // 分块请求：计数，并返回缺失 Content-Range 的 206，触发分块失败
    counter.fetch_add(1, Ordering::SeqCst);
    let header = "HTTP/1.1 206 Partial Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    stream.write_all(header.as_bytes()).unwrap();
    stream.flush().unwrap();
}

#[tokio::test]
async fn test_fast_fail_aborts_before_spawning_remaining_chunks() {
    // 1MB 文件 + 自动 100KB 块 = 11 块；max_connections=4 时，快速失败应保证
    // 分块请求数 < 总块数（旧实现会先把 11 块全部生成并请求，再在最后才发现失败）。
    let data = test_data(1024 * 1024);
    let (url, counter) = spawn_fail_all_chunks_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        max_connections: 4,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("Content-Range"),
                "失败原因应涉及 Content-Range 校验: {reason}"
            );
        }
        DownloadStatus::Completed => panic!("分块全部失败不应被判定为完成"),
        other => panic!("意外状态: {other:?}"),
    }

    // 快速失败：在首个失败分块被观察后即中止，未生成/请求的剩余分块不应抵达服务器
    let chunk_size = 100 * 1024u64;
    let total_chunks = (data.len() as u64).div_ceil(chunk_size);
    let chunk_requests = counter.load(Ordering::SeqCst);
    assert!(
        (chunk_requests as u64) < total_chunks,
        "快速失败应在生成全部分块前中止，实际分块请求 {chunk_requests} 应少于总块数 {total_chunks}"
    );
}

#[tokio::test]
async fn test_subscribe_progress_receives_live_updates() {
    // 通过管理器订阅进度，验证真实下载过程中订阅者能收到进度推进的快照
    // （total_size 已知且 downloaded > 0），而非仅覆盖单元层的 restore_state 路径
    let data = test_data(1024 * 1024);
    let url = spawn_mock_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(256 * 1024), // 256KB/s，1MB 约需 4s
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;

    let mut progress = manager
        .subscribe_progress(&task_id)
        .await
        .expect("订阅进度失败");
    // 标记初始快照为已读，确保首次 changed() 等待的是真正的新快照而非初始值
    progress.borrow_and_update();

    manager.start_task(&task_id).await.expect("开始下载失败");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut saw_progress = false;
    while Instant::now() < deadline {
        if progress.changed().await.is_err() {
            break; // 发送端关闭（任务终止）
        }
        let snapshot = progress.borrow_and_update().clone();
        if snapshot.total_size > 0 && snapshot.downloaded > 0 {
            saw_progress = true;
            break;
        }
    }

    assert!(
        saw_progress,
        "订阅者应观察到 total_size 已知且 downloaded > 0 的快照"
    );
}

/// 启动一个「超出请求区间返回多余字节」的服务器。
///
/// 探测请求正常返回 206；分块请求则返回「从请求起始位置到文件末尾」的完整剩余
/// 载荷（比请求的区间更长），但 Content-Range 的起始与总大小仍正确。下载器应通过
/// 分块大小校验（`!=`）发现实际写入字节超出期望，从而判定失败，而非静默越界写坏文件。
fn spawn_overdelivering_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_overdelivering_connection(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 每个连接处理一个请求：探测返回正确 206，分块请求返回超出请求范围的剩余载荷
fn handle_overdelivering_connection(mut stream: TcpStream, data: &[u8]) {
    let total = data.len();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let request = String::from_utf8_lossy(&buf).to_lowercase();
    let range_spec = request
        .lines()
        .find_map(|line| line.trim().strip_prefix("range:").map(str::trim));

    let Some(spec) = range_spec else { return };
    let spec = spec.strip_prefix("bytes=").unwrap_or(spec);
    let first = spec.split(',').next().unwrap_or("");
    let Some((a, b)) = first.split_once('-') else {
        return;
    };

    let start: usize = a.trim().parse().unwrap_or(0);
    let end: usize = if b.trim().is_empty() {
        total.saturating_sub(1)
    } else {
        b.trim().parse().unwrap_or(total.saturating_sub(1))
    };

    if start == 0 && end == 0 {
        // 探测请求：返回正确 206
        let header = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{total}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(header.as_bytes()).unwrap();
        stream.write_all(&data[0..1]).unwrap();
        stream.flush().unwrap();
        return;
    }

    // 分块请求：返回「从 start 到文件末尾」的完整剩余载荷（超出请求的 end）
    let body = &data[start..];
    let header = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{total}\r\nConnection: close\r\n\r\n",
        body.len(),
        total.saturating_sub(1)
    );
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    stream.flush().unwrap();
}

#[tokio::test]
async fn test_overdelivering_server_fails_against_local_server() {
    // 服务器对分块请求返回超出请求区间的多余字节：应通过分块大小校验拒绝，
    // 避免越界写坏文件（此前仅用 < 校验，无法捕获「返回过多」的情况）
    let data = test_data(TEST_DATA_LEN);
    let url = spawn_overdelivering_server(data);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("大小不匹配"),
                "失败原因应包含分块大小不匹配: {reason}"
            );
        }
        DownloadStatus::Completed => panic!("返回多余字节的服务器不应被判定为完成"),
        other => panic!("意外状态: {other:?}"),
    }
}

/// 启动一个「总大小未知」的服务器。
///
/// 探测请求 `Range: bytes=0-0` 返回 206，但其 `Content-Range` 为 `bytes 0-0/*`
/// （总大小未知，如直播流 / 动态生成内容）。下载器应把它视为未知大小、回退到
/// 单块全量下载（完整 GET），并将完整载荷正确落盘。
fn spawn_unknown_total_server(data: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定本地端口失败");
    let addr = listener.local_addr().unwrap();
    let data = Arc::new(data);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let data = Arc::clone(&data);
            std::thread::spawn(move || handle_unknown_total_connection(stream, &data));
        }
    });

    format!("http://{addr}/file.bin")
}

/// 每个连接只处理一个请求：
/// - 探测请求（带 `Range: bytes=0-0`）返回 206 + `Content-Range: bytes 0-0/*`；
/// - 完整 GET（无 Range）返回 200 全量载荷，不设 Content-Length、以关闭连接
///   标志正文结束，模拟“总大小未知、读到 EOF 为止”的流式响应。
fn handle_unknown_total_connection(mut stream: TcpStream, data: &[u8]) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let request = String::from_utf8_lossy(&buf).to_lowercase();
    let has_range = request
        .lines()
        .any(|line| line.trim().starts_with("range:"));

    if has_range {
        // 探测：仅回 1 字节，但总大小未知（bytes 0-0/*）
        let header = "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/*\r\nConnection: close\r\n\r\n";
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&data[..1]);
        let _ = stream.flush();
    } else {
        // 完整 GET：无 Content-Length，靠关闭连接标志正文结束
        let header = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(data);
        let _ = stream.flush();
    }
}

#[tokio::test]
async fn test_unknown_total_size_download_against_local_server() {
    // 服务器返回 `bytes 0-0/*`（总大小未知）：应回退到单块全量下载并正确落盘
    let data = test_data(64 * 1024);
    let url = spawn_unknown_total_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("未知总大小下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "未知总大小下载内容应与服务端载荷一致");
}

#[tokio::test]
async fn test_unknown_total_download_final_stats_report_actual_size() {
    // 未知总大小下载（流式、无 Content-Length）在完成前 total_size 恒为 0；
    // 完成后统计应把 total_size 更新为实际字节数，否则最终快照会显示
    //「总大小 0 + 已下载 N」的矛盾信息。
    let data = test_data(32 * 1024);
    let url = spawn_unknown_total_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;

    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        other => panic!("意外状态: {other:?}"),
    }

    let stats = manager.get_task_stats(&task_id).await.expect("任务应存在");
    assert_eq!(
        stats.total_size,
        data.len() as u64,
        "未知总大小下载完成后 total_size 应为实际字节数"
    );
    assert_eq!(
        stats.downloaded,
        data.len() as u64,
        "下载量应等于文件字节数"
    );
}

#[tokio::test]
async fn test_list_tasks_enumerates_managed_tasks() {
    // 管理器应能枚举全部任务 ID（字典序、稳定顺序），这是多任务管理的入口能力
    let manager = DownloadManager::new();
    assert!(manager.list_tasks().await.is_empty(), "初始不应有任务");

    let data = test_data(1024);
    let dir = tempfile::tempdir().unwrap();

    let id1 = manager
        .add_task(
            spawn_mock_server(data.clone()),
            dir.path().to_path_buf(),
            DownloadConfig::default(),
        )
        .await;
    let id2 = manager
        .add_task(
            spawn_mock_server(data.clone()),
            dir.path().to_path_buf(),
            DownloadConfig::default(),
        )
        .await;

    let ids = manager.list_tasks().await;
    assert_eq!(ids.len(), 2, "应列出两个任务");
    assert!(
        ids.contains(&id1) && ids.contains(&id2),
        "应包含两个任务的 ID"
    );
    assert!(
        ids.windows(2).all(|w| w[0] <= w[1]),
        "ID 列表应按字典序排序"
    );
}

#[tokio::test]
async fn test_dynamic_speed_limit_update_against_local_server() {
    // 下载进行中通过 update_task_config 动态调低速度限制，验证：
    // 动态配置变更不会打断/破坏正在进行的下载，最终仍以正确内容完成。这覆盖了
    // `speed_limit`「立即生效」这一文档化能力的端到端路径（此前仅有单元测试）。
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024), // 先以 512KB/s 启动，2MB 约需 4s，留出调整窗口
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待产生进度后在下载进行中动态调低速度限制
    tokio::time::sleep(Duration::from_millis(500)).await;

    let status = manager.get_task_status(&task_id).await.expect("任务应存在");
    assert!(
        matches!(status, DownloadStatus::Downloading),
        "更新配置时任务应仍在下载中，实际 {status:?}"
    );

    let updated = DownloadConfig {
        speed_limit: Some(256 * 1024), // 调低为 256KB/s
        ..Default::default()
    };
    manager
        .update_task_config(&task_id, updated)
        .await
        .expect("更新配置失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(20)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("动态限速调整后下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "动态限速调整后内容应与服务端载荷一致");
}

/// 验证「进程崩溃后从磁盘续传」的端到端路径：`load_state` → `restore_state_from_file`。
///
/// 进程内 pause/resume 走的是内存状态（`resume()`），而真正的断点续传依赖
/// `save_state` 落盘 + 重新 `start()` 时由 `load_state` 恢复。此前该路径仅有
/// 合成数据的单元测试；这里手工构造一份「已下载一半」的状态文件与临时文件，
/// 模拟上一次进程在写出状态后崩溃，随后由 `start()` 从磁盘接管剩余字节。
#[tokio::test]
async fn test_resume_from_persisted_state_against_local_server() {
    let data = test_data(100 * 1024); // 100KB：自动块大小恰为单块 [0, 102399]
    let url = spawn_mock_server(data.clone());
    let partial: usize = 50 * 1024;

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    // 手工铺设「崩溃现场」：状态文件记录单个分块已下载 partial 字节，临时文件
    // 保存对应前缀数据。文件名由 URL 路径（file.bin）决定。
    let state = DownloadState {
        version: 1,
        url: url.clone(),
        total_size: data.len() as u64,
        chunks: vec![ChunkInfo {
            index: 0,
            start: 0,
            end: (data.len() - 1) as u64,
            downloaded: partial as u64,
            completed: false,
        }],
    };
    std::fs::write(
        dir.path().join("file.bin.download_state"),
        serde_json::to_string(&state).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.path().join("file.bin.temp"), &data[..partial]).unwrap();

    // 全新任务 + start()：应发现磁盘状态文件并续传剩余字节，而非从头下载
    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("从磁盘续传失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    // 续传完成后内容应与源一致（前缀来自临时文件，剩余来自服务器）
    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "从磁盘续传后的内容应与服务端载荷一致");

    // 完成后临时/状态文件应被清理
    assert!(
        !dir.path().join("file.bin.temp").exists(),
        "临时文件应被清理"
    );
    assert!(
        !dir.path().join("file.bin.download_state").exists(),
        "状态文件应被清理"
    );
}

/// 验证「已满块长但未标记完成」的历史状态文件在续传时被归一化为完成、避免重复下载。
///
/// `save_state` 的快照可能落在分块「写完最后一块字节」与「标记 completed」之间，把
/// `downloaded == 块长 且 completed == false` 的分块写进状态文件。若恢复时不归一化，
/// 会以 `bytes=end+1..end` 的非法区间重复下载该块、导致续传失败。本测试用手工铺设的
/// 这类「陈旧快照」状态文件 + 计数服务器，验证恢复后仅请求未完成分块、内容与源一致。
#[tokio::test]
async fn test_resume_normalizes_full_chunk() {
    // 150KB → 自动块 100KB，切为 2 块 [0,102399] 与 [102400,153599]
    let data = test_data(150 * 1024);
    let (url, captured) = spawn_mock_server_capturing(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();

    // 手工铺设「陈旧快照」现场：块 0 已满块长（102400 字节）但 completed 仍为 false
    let state = DownloadState {
        version: 1,
        url: url.clone(),
        total_size: data.len() as u64,
        chunks: vec![
            ChunkInfo {
                index: 0,
                start: 0,
                end: 102399,
                downloaded: 102400,
                completed: false,
            },
            ChunkInfo {
                index: 1,
                start: 102400,
                end: 153599,
                downloaded: 0,
                completed: false,
            },
        ],
    };
    std::fs::write(
        dir.path().join("file.bin.download_state"),
        serde_json::to_string(&state).unwrap(),
    )
    .unwrap();
    // 临时文件已含块 0 的完整前缀数据
    std::fs::write(dir.path().join("file.bin.temp"), &data[..102400]).unwrap();

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("续传失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "续传后内容应与服务端载荷一致");

    // 仅探测（bytes=0-0）与未完成块 1（bytes=102400-153599）各一次 Range 请求；
    // 已满块长的块 0 应被归一化为完成并跳过，不应发出 `bytes=102400-102399` 的非法请求。
    let range_requests = captured
        .lock()
        .unwrap()
        .iter()
        .filter(|req| req.to_lowercase().contains("range:"))
        .count();
    assert_eq!(
        range_requests, 2,
        "已满块长的块 0 应被跳过，仅探测与块 1 各占一次 Range 请求"
    );
}

/// 验证「真实下载中途失败 → 状态文件留存 → 重新 start() 断点续传成功」链路。
///
/// `test_resume_from_persisted_state_against_local_server` 用手工铺设的状态/临时文件
/// 模拟崩溃现场；这里则让服务器在下载越过 1MB 保存边界后主动截断一个分块，
/// 触发真实的失败、真实的状态文件落盘，再重新 `start()` 由 `load_state` 从磁盘
/// 半途接管。用 `max_connections = 1`（串行）保证失败点与状态保存顺序是确定的。
#[tokio::test]
async fn test_resume_after_real_mid_download_failure() {
    let data = test_data(2 * 1024 * 1024); // 2MB
    // 分块起始偏移 >= 1.5MB 时首次截断（越过 1MB 保存边界，状态文件已落盘），此后正常
    let url = spawn_fail_once_server(data.clone(), 1500 * 1024);

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        max_connections: 1,     // 串行下载，失败点与状态保存顺序确定
        chunk_size: 100 * 1024, // 100KB × 20 块
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 第一次下载应中途失败，而非误判完成
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Failed(_) => {}
        other => panic!("第一次下载应中途失败，实际: {other:?}"),
    }

    // 失败后应留存临时文件（部分进度）与状态文件（越过保存边界后已落盘），供续传
    assert!(
        dir.path().join("file.bin.temp").exists(),
        "失败后应保留部分临时文件以支持续传"
    );
    assert!(
        dir.path().join("file.bin.download_state").exists(),
        "失败后应保留状态文件（越过保存间隔后已落盘）"
    );

    // 重新开始：服务器第二次起正常返回，应读状态文件续传剩余字节并完成
    manager
        .start_task(&task_id)
        .await
        .expect("重新开始下载失败");
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("续传失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
    assert_eq!(output, data, "续传后内容应与服务端载荷一致");
}

/// 验证「无文件名 URL」的下载同样可落盘状态文件并断点续传。
///
/// 服务器 URL 为 `http://host/`（无路径文件名、无 Content-Disposition）时，
/// 下载器应回退到任务 ID 作为文件名；若 `setup_filename` 未回退并留下 `None`，
/// `save_state` 会静默跳过，状态文件缺失、断点续传失效。本测试补齐该端到端盲区。
#[tokio::test]
async fn test_download_without_filename_is_resumable() {
    let data = test_data(2 * 1024 * 1024); // 2MB + 限速，制造可暂停的下载窗口
    let named_url = spawn_mock_server(data.clone());
    // 去掉 URL 中的 `/file.bin`，得到无文件名的根路径（服务器忽略路径，仍返回同一载荷）
    let base = named_url.rsplit_once('/').map(|(b, _)| b).unwrap();
    let url = format!("{base}/");

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待产生进度后暂停，触发 pause() -> save_state()
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager.pause_task(&task_id).await;

    // 解析出的最终文件名应是回退的任务 ID（UUID）
    let filename = manager
        .get_task_filename(&task_id)
        .await
        .expect("应解析出回退文件名");
    assert!(!filename.is_empty(), "回退文件名不应为空");

    // 状态文件应以回退文件名命名并落盘（证明 save_state 未因 None 静默跳过）
    let state_path = dir.path().join(format!("{filename}.download_state"));
    assert!(
        state_path.exists(),
        "无文件名下载暂停后也应写出状态文件: {state_path:?}"
    );

    // 恢复并完成，内容应与源一致
    manager.resume_task(&task_id).await;
    match wait_for_terminal(&manager, &task_id, Duration::from_secs(15)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let output = std::fs::read(dir.path().join(&filename)).expect("读取下载文件失败");
    assert_eq!(output, data, "无文件名下载内容应与服务端载荷一致");
}

/// 验证下载器实际发出的 HTTP 请求头：User-Agent 非空、Cookie 被透传。
///
/// `build_client` 通过 `.user_agent()` 与 `.default_headers()` 设置默认头，
/// 这两者是「未验证会静默失败」的典型盲区（reqwest 默认不附带 UA，缺 Cookie
/// 则登录态文件不可下载）。mock 服务器在此捕获每个请求的原始头块做端到端断言。
#[tokio::test]
async fn test_request_headers_include_user_agent_and_cookies() {
    let data = test_data(TEST_DATA_LEN);
    let (url, captured) = spawn_mock_server_capturing(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        cookies: Some("session=abc123".to_string()),
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        DownloadStatus::Failed(reason) => panic!("下载失败: {reason}"),
        other => panic!("意外状态: {other:?}"),
    }

    let requests = captured.lock().unwrap();
    assert!(!requests.is_empty(), "mock 服务器应至少收到一次请求");
    for req in requests.iter() {
        let lower = req.to_lowercase();

        // User-Agent 必须存在且值非空（reqwest 不会自动附带，缺失会被部分服务器拒绝）
        let ua_idx = lower
            .find("user-agent:")
            .unwrap_or_else(|| panic!("请求缺少 User-Agent 头: {req}"));
        let ua_value = lower[ua_idx + "user-agent:".len()..]
            .split("\r\n")
            .next()
            .unwrap_or("")
            .trim();
        assert!(!ua_value.is_empty(), "User-Agent 头值不应为空: {req}");

        // Cookie 应透传到每个请求（探测与分块共用同一客户端默认头）
        assert!(
            lower.contains("cookie: session=abc123"),
            "请求缺少配置的 Cookie 头: {req}"
        );
    }
}

/// 验证探测请求返回非 2xx 状态码（如 404）时，任务进入 Failed 且错误信息包含状态码。
///
/// 与 `test_connection_refused_fails_against_local_server`（连接层失败）互补：
/// 这里覆盖 HTTP 应用层错误——服务器可达但拒绝请求。该路径由 `start()` 的
/// 探测状态码检查 + `fail_and_notify` 处理，此前仅单元测试，缺端到端验证。
#[tokio::test]
async fn test_http_error_status_fails_against_local_server() {
    let url = spawn_status_server("404 Not Found");

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Failed(reason) => {
            assert!(
                reason.to_string().contains("404"),
                "失败原因应包含状态码 404，实际: {reason}"
            );
        }
        other => panic!("预期 Failed，实际 {other:?}"),
    }
}

/// 验证 `max_connections` 确实约束了同时下载的分块数（一项核心并发能力）。
///
/// 2MB 文件按默认 100KB 块切分为 20 块；若调度器未受信号量约束而把全部分块
/// 一次性并发，服务器上的峰值并发连接会接近 20、远超配置的 4。这里断言峰值
/// 落在 `[2, max_connections]`：下界证明确实在并行下载，上界证明并发被正确封顶。
#[tokio::test]
async fn test_max_connections_bounds_concurrent_requests() {
    let data = test_data(2 * 1024 * 1024); // 2MB → 20 块（每块默认 100KB）
    let (url, tracker) = spawn_concurrency_tracking_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        max_connections: 4,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {}
        other => panic!("意外状态: {other:?}"),
    }

    let peak = tracker.peak();
    assert!(
        peak >= 2,
        "应实际并行下载（峰值连接数应 >= 2），实际 {peak}"
    );
    assert!(
        peak <= 4,
        "峰值并发连接数不应超过 max_connections，实际 {peak}"
    );
}

/// 验证 `max_connections: 0`（用户的自然误配置）不会死锁。
///
/// `Semaphore::new(0)` 永无可用的并发许可，所有分块将永久阻塞在 acquire 上；
/// 下载器内部用 `max_connections.max(1)` 把并发数钳到至少 1，避免该死锁。
/// 此测试保护这个防御性钳制：若未来被移除，任务将永远停在 Downloading 导致超时失败。
#[tokio::test]
async fn test_max_connections_zero_does_not_deadlock() {
    let data = test_data(512 * 1024); // 512KB → 5 块（每块默认 100KB）
    let url = spawn_mock_server(data.clone());

    let manager = DownloadManager::new();
    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        max_connections: 0,
        ..Default::default()
    };

    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    match wait_for_terminal(&manager, &task_id, Duration::from_secs(10)).await {
        DownloadStatus::Completed => {
            let output = std::fs::read(dir.path().join("file.bin")).expect("读取下载文件失败");
            assert_eq!(output, data, "下载内容应与服务器一致");
        }
        other => panic!("预期 Completed，实际 {other:?}"),
    }
}

/// 验证 `subscribe_events` 能在任务完成时收到 `TaskCompleted` 终止事件。
#[tokio::test]
async fn test_subscribe_events_receives_completion() {
    let data = test_data(512 * 1024);
    let url = spawn_mock_server(data);

    let manager = DownloadManager::new();
    // 必须在 start 前订阅，否则会漏掉广播事件
    let mut events = manager.subscribe_events();

    let dir = tempfile::tempdir().unwrap();
    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    let event = wait_for_terminal_event(&mut events, Duration::from_secs(10)).await;
    assert_eq!(
        event,
        DownloadEvent::TaskCompleted {
            task_id: task_id.clone()
        }
    );
}

/// 验证 `subscribe_events` 能在任务失败时收到携带原因字符串的 `TaskFailed` 事件。
#[tokio::test]
async fn test_subscribe_events_receives_failure() {
    let url = spawn_status_server("404 Not Found");

    let manager = DownloadManager::new();
    let mut events = manager.subscribe_events();

    let dir = tempfile::tempdir().unwrap();
    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    let event = wait_for_terminal_event(&mut events, Duration::from_secs(10)).await;
    match event {
        DownloadEvent::TaskFailed { task_id: id, error } => {
            assert_eq!(id, task_id, "事件应携带正确的任务 ID");
            assert!(
                error.to_string().contains("404"),
                "失败事件应携带状态码: {error}"
            );
        }
        other => panic!("预期 TaskFailed，实际 {other:?}"),
    }
}

/// 验证 `subscribe_events` 能在任务取消时收到 `TaskCancelled` 终止事件。
#[tokio::test]
async fn test_subscribe_events_receives_cancellation() {
    // 2MB 文件 + 限速，制造足够长的下载窗口，便于在下载进行中取消
    let data = test_data(2 * 1024 * 1024);
    let url = spawn_mock_server(data);

    let manager = DownloadManager::new();
    // 必须在 start 前订阅，否则会漏掉广播事件
    let mut events = manager.subscribe_events();

    let dir = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        speed_limit: Some(512 * 1024),
        ..Default::default()
    };
    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), config)
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    // 等待产生初步进度后取消，确保任务处于下载中、随后被取消
    tokio::time::sleep(Duration::from_secs(1)).await;
    manager.cancel_task(&task_id).await;

    let event = wait_for_terminal_event(&mut events, Duration::from_secs(10)).await;
    assert_eq!(
        event,
        DownloadEvent::TaskCancelled {
            task_id: task_id.clone()
        }
    );
}

/// 验证下载**中途**失败（而非探测阶段失败）同样会发布 `TaskFailed` 终止事件。
///
/// `start()` 有两条失败事件路径：进入分块下载前的 `fail_and_notify`，以及
/// `download_chunks` 返回 `Err` 后由终止代码块统一发布。`test_subscribe_events_receives_failure`
/// 覆盖前者，这里用「下载中途截断连接」覆盖后者，确保两条路径都有事件产出。
#[tokio::test]
async fn test_subscribe_events_receives_mid_download_failure() {
    let data = test_data(TEST_DATA_LEN);
    let url = spawn_truncating_server(data);

    let manager = DownloadManager::new();
    let mut events = manager.subscribe_events();

    let dir = tempfile::tempdir().unwrap();
    let task_id = manager
        .add_task(url, dir.path().to_path_buf(), DownloadConfig::default())
        .await;
    manager.start_task(&task_id).await.expect("开始下载失败");

    let event = wait_for_terminal_event(&mut events, Duration::from_secs(15)).await;
    match event {
        DownloadEvent::TaskFailed { task_id: id, error } => {
            assert_eq!(id, task_id, "事件应携带正确的任务 ID");
            // 截断的连接表现为 IO/读取错误，而非探测 404；这里仅断言携带了失败原因
            assert!(
                !error.to_string().trim().is_empty(),
                "失败事件应携带非空原因: {error}"
            );
        }
        other => panic!("预期 TaskFailed，实际 {other:?}"),
    }
}
