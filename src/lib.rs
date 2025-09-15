use std::collections::HashMap;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use log::{error, info};
use reqwest::Client;
use reqwest::header::{COOKIE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::sync::{Notify, Semaphore};
use tokio::time::sleep;

/// 下载器错误类型
#[derive(Error, Debug)]
pub enum DownloadError {
    #[error("Tokio任务错误: {0}")]
    TokioError(#[from] tokio::task::JoinError),
    #[error("HTTP请求错误: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("IO错误: {0}")]
    IoError(#[from] std::io::Error),
    #[error("JSON序列化错误: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("文件操作错误: {0}")]
    FileError(String),
    #[error("下载被取消")]
    Cancelled,
    #[error("不支持断点续传")]
    ResumeNotSupported,
    #[error("无效的下载状态文件")]
    InvalidStateFile,
    #[error("其他错误: {0}")]
    Other(String),
}

/// 下载状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DownloadStatus {
    /// 等待中
    Pending,
    /// 下载中
    Downloading,
    /// 暂停中
    Paused,
    /// 已完成
    Completed,
    /// 失败
    Failed(String),
}

/// 下载块信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub index: usize,
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
    pub completed: bool,
}

/// 下载统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadStats {
    pub total_size: u64,
    pub downloaded: u64,
    pub speed: f64, // bytes per second
    pub elapsed: Duration,
    pub remaining: Duration,
    pub chunks: Vec<ChunkInfo>,
}

/// 下载配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadConfig {
    pub max_connections: usize,
    pub chunk_size: u64,
    pub speed_limit: Option<u64>, // bytes per second
    pub temp_dir: Option<PathBuf>,
    pub auto_resume: bool,
    pub cookies: Option<String>,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            max_connections: 4,
            chunk_size: 0, // 0表示自动计算块大小
            speed_limit: None,
            temp_dir: None,
            auto_resume: true,
            cookies: None,
        }
    }
}

/// 下载状态快照（用于序列化）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadState {
    pub url: String,
    pub output_path: PathBuf,
    pub total_size: u64,
    pub chunks: Vec<ChunkInfo>,
    pub config: DownloadConfig,
}

/// 下载任务
pub struct DownloadTask {
    pub url: String,
    pub download_dir: PathBuf,
    pub output_path: Arc<Mutex<Option<PathBuf>>>,
    pub config: DownloadConfig,
    pub status: Arc<Mutex<DownloadStatus>>,
    pub stats: Arc<Mutex<DownloadStats>>,
    pub is_cancelled: Arc<AtomicBool>,
    pub is_paused: Arc<AtomicBool>,
    pub pause_notify: Arc<Notify>,
    pub state_file_path: Arc<Mutex<PathBuf>>,
    pub supports_range: Arc<AtomicBool>,
}

impl DownloadTask {
    /// 创建新的下载任务
    pub fn new(url: String, download_dir: PathBuf, config: DownloadConfig) -> Self {
        let state_file_path = Arc::new(Mutex::new(download_dir.join("temp_download_state")));

        Self {
            url,
            download_dir,
            output_path: Arc::new(Mutex::new(None)),
            config,
            status: Arc::new(Mutex::new(DownloadStatus::Pending)),
            stats: Arc::new(Mutex::new(DownloadStats {
                total_size: 0,
                downloaded: 0,
                speed: 0.0,
                elapsed: Duration::default(),
                remaining: Duration::default(),
                chunks: Vec::new(),
            })),
            is_cancelled: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            state_file_path,
            supports_range: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 开始下载
    pub async fn start(&self) -> Result<(), DownloadError> {
        if let DownloadStatus::Downloading = *self.status.lock().await {
            return Ok(()); // 已经在下载中
        }

        *self.status.lock().await = DownloadStatus::Downloading;
        self.is_paused.store(false, Ordering::Relaxed);

        let task = self.clone_arc();
        tokio::spawn(async move {
            let client = match task.build_client() {
                Ok(client) => client,
                Err(e) => {
                    *task.status.lock().await =
                        DownloadStatus::Failed(format!("Failed to build HTTP client: {}", e));
                    return;
                }
            };
            let response = match client.head(&task.url).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    *task.status.lock().await =
                        DownloadStatus::Failed(format!("HTTP HEAD request failed: {}", e));
                    return;
                }
            };

            if !response.status().is_success() {
                *task.status.lock().await = DownloadStatus::Failed(format!(
                    "HTTP HEAD request failed with status: {}",
                    response.status()
                ));
                return;
            }

            // 从响应头中提取文件名并设置输出路径
            task.setup_output_path(response.headers(), &task.url).await;

            // 尝试从状态文件恢复
            let mut is_resume = false;
            if task.config.auto_resume
                && let Ok(state) = task
                    .load_state()
                    .await
                    .inspect_err(|e| error!("从状态文件恢复失败：{e}"))
            {
                task.restore_state_from_file(&state).await;

                info!("从状态文件恢复下载，已下载: {} bytes", {
                    let stats = task.stats.lock().await;
                    stats.downloaded
                });
                is_resume = true;
            } else {
                // 确保下载目录存在
                if let Err(e) = tokio::fs::create_dir_all(&task.download_dir).await {
                    *task.status.lock().await =
                        DownloadStatus::Failed(format!("创建目录失败: {}", e));
                    return;
                }
            }

            let (total_size, supports_range) = task.extract_response_info(response.headers());
            task.supports_range.store(supports_range, Ordering::Relaxed);

            if !is_resume || !supports_range {
                info!("初始化下载统计");
                let mut stats = task.stats.lock().await;
                stats.total_size = total_size;
                stats.chunks = task.create_chunks(total_size, supports_range);
            }

            // 执行下载
            let result = task.download_chunks(&client).await;

            // 下载完成后删除状态文件
            if result.is_ok() {
                let _ = tokio::fs::remove_file(task.get_current_state_file_path().await).await;
                *task.status.lock().await = DownloadStatus::Completed;
            } else {
                *task.status.lock().await = DownloadStatus::Failed("Download failed".to_string());
            }
        });

        Ok(())
    }

    /// 设置输出路径和状态文件路径
    async fn setup_output_path(&self, headers: &HeaderMap, url: &str) {
        let filename = extract_filename_from_headers(headers, url);
        let output_path = self.download_dir.join(filename);

        // 更新输出路径
        {
            let mut output_path_guard = self.output_path.lock().await;
            *output_path_guard = Some(output_path.clone());
        }
        {
            let mut state_file_path = self.state_file_path.lock().await;
            *state_file_path = Self::get_state_file_path(output_path.as_path());
        }
    }

    /// 构建带有配置的 HTTP 客户端
    fn build_client(&self) -> Result<Client, DownloadError> {
        let mut client_builder = Client::builder();
        if let Some(ref cookies) = self.config.cookies
            && let Ok(cookies) = HeaderValue::from_str(cookies)
        {
            let mut cookie_header = HeaderMap::new();
            cookie_header.insert(COOKIE, cookies);
            client_builder = client_builder.default_headers(cookie_header);
        }
        let user_agent = std::env::var("USER_AGENT").unwrap_or_default();
        client_builder
            .user_agent(user_agent)
            .build()
            .map_err(|e| DownloadError::Other(format!("Failed to build client: {}", e)))
    }

    /// 从状态文件恢复状态
    async fn restore_state_from_file(&self, state: &DownloadState) {
        {
            let mut stats = self.stats.lock().await;
            stats.total_size = state.total_size;
            stats.chunks = state.chunks.clone();
        }

        // 使用状态文件中保存的输出路径
        {
            let mut output_path = self.output_path.lock().await;
            *output_path = Some(state.output_path.clone());
        }
        {
            let mut state_file_path = self.state_file_path.lock().await;
            *state_file_path = Self::get_state_file_path(state.output_path.as_path());
        }
    }

    /// 从响应头中提取总大小和范围支持信息
    fn extract_response_info(&self, headers: &HeaderMap) -> (u64, bool) {
        let total_size = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|ct_len| ct_len.to_str().ok())
            .and_then(|ct_len| ct_len.parse().ok())
            .unwrap_or(0);

        let supports_range = headers
            .get(reqwest::header::ACCEPT_RANGES)
            .map(|h| h == "bytes")
            .unwrap_or(false);

        (total_size, supports_range)
    }

    /// 创建下载块
    fn create_chunks(&self, total_size: u64, supports_range: bool) -> Vec<ChunkInfo> {
        info!(
            "创建下载块，总大小：{}，支持范围请求：{}",
            total_size, supports_range
        );
        if !supports_range || total_size == 0 {
            return vec![ChunkInfo {
                index: 0,
                start: 0,
                end: total_size.saturating_sub(1),
                downloaded: 0,
                completed: false,
            }];
        }

        let chunk_size = self.calculate_chunk_size(total_size);
        info!("计算后的块大小: {} bytes", chunk_size);

        let mut chunks = Vec::new();
        let mut start = 0;
        let mut index = 0;

        while start < total_size {
            let end = (start + chunk_size - 1).min(total_size.saturating_sub(1));

            chunks.push(ChunkInfo {
                index,
                start,
                end,
                downloaded: 0,
                completed: false,
            });

            start = end + 1;
            index += 1;
        }

        chunks
    }

    /// 根据文件大小和线程数动态计算块大小
    fn calculate_chunk_size(&self, total_size: u64) -> u64 {
        // 最小块大小：100KB
        const MIN_CHUNK_SIZE: u64 = 100 * 1024;
        // 最大块大小：10MB
        const MAX_CHUNK_SIZE: u64 = 10 * 1024 * 1024;
        // 目标块大小：1MB
        const TARGET_CHUNK_SIZE: u64 = 1024 * 1024;

        // 如果配置中指定了块大小，优先使用配置的值
        if self.config.chunk_size > 0 {
            return self.config.chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
        }

        // 基于文件大小计算块大小
        let chunk_size = if total_size <= 5 * 1024 * 1024 {
            // 小文件（<=5MB）：使用固定100KB块大小
            MIN_CHUNK_SIZE
        } else if total_size <= 50 * 1024 * 1024 {
            // 中等文件（5MB-50MB）：使用500KB块
            500 * 1024
        } else {
            // 大文件（>50MB）：使用1MB块
            TARGET_CHUNK_SIZE
        };

        // 确保块大小在合理范围内
        chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE)
    }

    /// 下载所有块
    async fn download_chunks(&self, client: &Client) -> Result<(), DownloadError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_connections));
        let mut handles = Vec::new();

        let chunks = {
            let stats = self.stats.lock().await;
            stats.chunks.clone()
        };

        for chunk in chunks {
            if self.is_cancelled.load(Ordering::Relaxed) {
                return Err(DownloadError::Cancelled);
            }

            // 检查是否暂停 - 在获取锁之前检查，避免死锁
            if self.is_paused.load(Ordering::Relaxed) {
                // 等待恢复通知
                self.pause_notify.notified().await;

                if self.is_cancelled.load(Ordering::Relaxed) {
                    return Err(DownloadError::Cancelled);
                }
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let client = client.clone();
            let task = self.clone_arc();

            let handle = tokio::spawn(async move {
                let result = task.download_chunk(&client, chunk).await;
                drop(permit);
                result
            });

            handles.push(handle);
        }

        // 等待所有下载任务完成
        for handle in handles {
            if self.is_cancelled.load(Ordering::Relaxed) {
                return Err(DownloadError::Cancelled);
            }
            handle.await??;
        }

        Ok(())
    }

    /// 下载单个块
    async fn download_chunk(
        &self,
        client: &Client,
        mut chunk: ChunkInfo,
    ) -> Result<(), DownloadError> {
        if chunk.completed {
            return Ok(());
        }

        // 只有在服务器支持范围请求且块范围有效时才发送Range头
        let mut request = client.get(&self.url);

        if chunk.start <= chunk.end {
            // 检查服务器是否支持范围请求
            let supports_range = self.supports_range.load(Ordering::Relaxed);

            if supports_range {
                let range_header =
                    format!("bytes={}-{}", chunk.start + chunk.downloaded, chunk.end);
                request = request.header("Range", &range_header);
            }
        } else {
            return Ok(());
        }

        let mut response = request.send().await?;

        let output_path = self
            .output_path
            .lock()
            .await
            .as_ref()
            .ok_or_else(|| DownloadError::Other("Output path not set".to_string()))?
            .clone();
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(output_path)
            .await?;

        let start_time = Instant::now();
        let mut last_update = Instant::now();

        while let Some(chunk_data) = response.chunk().await? {
            if self.is_cancelled.load(Ordering::Relaxed) {
                return Err(DownloadError::Cancelled);
            }

            // 应用速度限制
            if let Some(limit) = self.config.speed_limit {
                let elapsed = last_update.elapsed();
                let expected_time = Duration::from_secs_f64(chunk_data.len() as f64 / limit as f64);
                if elapsed < expected_time {
                    sleep(expected_time - elapsed).await;
                }
            }

            // 写入文件
            let seek_pos = chunk.start + chunk.downloaded;
            file.seek(std::io::SeekFrom::Start(seek_pos)).await?;
            file.write_all(&chunk_data).await?;

            chunk.downloaded += chunk_data.len() as u64;

            // 更新统计信息
            let mut stats = self.stats.lock().await;
            stats.downloaded += chunk_data.len() as u64;

            // 计算速度
            let elapsed = start_time.elapsed();
            if elapsed.as_secs() > 0 {
                stats.speed = stats.downloaded as f64 / elapsed.as_secs_f64();
                stats.remaining = if stats.speed > 0.0 {
                    Duration::from_secs_f64(
                        (stats.total_size - stats.downloaded) as f64 / stats.speed,
                    )
                } else {
                    Duration::default()
                };
            }

            stats.elapsed = elapsed;

            // 更新块信息
            if let Some(stat_chunk) = stats.chunks.iter_mut().find(|c| c.index == chunk.index) {
                stat_chunk.downloaded = chunk.downloaded;
                stat_chunk.completed = chunk.downloaded >= (chunk.end - chunk.start + 1);
            }

            last_update = Instant::now();

            // 定期保存状态（每下载1MB保存一次）
            // 注意：在持有 stats 锁时不能调用 save_state，否则会导致死锁
            // 所以这里先释放锁再保存状态
            const SAVE_INTERVAL: u64 = 1024 * 1024; // 1MB
            let total_downloaded_before = stats.downloaded - chunk_data.len() as u64;
            let total_downloaded_after = stats.downloaded;

            // 检查是否跨越了1MB的边界
            let should_save = (total_downloaded_before / SAVE_INTERVAL)
                != (total_downloaded_after / SAVE_INTERVAL);

            if should_save {
                // 保存当前统计信息并释放锁
                let current_downloaded = stats.downloaded;
                let current_speed = stats.speed;
                let current_elapsed = stats.elapsed;
                let current_remaining = stats.remaining;
                let current_chunks = stats.chunks.clone();
                drop(stats); // 释放锁

                let _ = self
                    .save_state()
                    .await
                    .inspect_err(|e| error!("保存状态文件失败: {}", e));

                // 重新获取锁以继续更新统计信息
                let mut new_stats = self.stats.lock().await;
                // 恢复统计信息（防止其他线程修改）
                new_stats.downloaded = current_downloaded;
                new_stats.speed = current_speed;
                new_stats.elapsed = current_elapsed;
                new_stats.remaining = current_remaining;
                new_stats.chunks = current_chunks;
            }

            // 检查是否暂停 - 在获取锁之前检查，避免死锁
            if self.is_paused.load(Ordering::Relaxed) {
                // 等待恢复通知
                self.pause_notify.notified().await;

                if self.is_cancelled.load(Ordering::Relaxed) {
                    return Err(DownloadError::Cancelled);
                }
            }
        }

        Ok(())
    }

    /// 暂停下载
    pub async fn pause(&self) {
        *self.status.lock().await = DownloadStatus::Paused;
        self.is_paused.store(true, Ordering::Relaxed);
        self.pause_notify.notify_waiters(); // 通知所有等待的任务

        // 保存暂停状态（只有在文件名已设置时才保存）
        if self.output_path.lock().await.is_some()
            && let Err(e) = self.save_state().await
        {
            error!("保存暂停状态失败: {}", e);
        }
    }

    /// 恢复下载
    pub async fn resume(&self) {
        self.is_paused.store(false, Ordering::Relaxed);
        self.pause_notify.notify_waiters(); // 通知所有等待的任务
        *self.status.lock().await = DownloadStatus::Downloading;
    }

    /// 取消下载
    pub async fn cancel(&self) {
        *self.status.lock().await = DownloadStatus::Failed("Cancelled".to_string());
        self.is_cancelled.store(true, Ordering::Relaxed);

        // 删除状态文件
        let _ = tokio::fs::remove_file(self.get_current_state_file_path().await).await;
    }

    /// 获取下载状态
    pub async fn get_status(&self) -> DownloadStatus {
        self.status.lock().await.clone()
    }

    /// 获取下载统计
    pub async fn get_stats(&self) -> DownloadStats {
        self.stats.lock().await.clone()
    }

    /// 更新配置
    pub fn update_config(&self, _config: DownloadConfig) {
        // 这里需要实现配置更新的逻辑
        // 注意：某些配置（如最大连接数）可能需要在下载过程中动态调整
    }

    /// 获取状态文件路径
    fn get_state_file_path(output_path: &std::path::Path) -> PathBuf {
        let mut state_path = output_path.to_path_buf();
        state_path.set_extension("download_state");
        state_path
    }

    /// 获取当前状态文件路径
    async fn get_current_state_file_path(&self) -> PathBuf {
        self.state_file_path.lock().await.clone()
    }

    /// 保存下载状态到文件
    async fn save_state(&self) -> Result<(), DownloadError> {
        // 获取统计信息时只获取一次锁，避免死锁
        let stats = self.stats.lock().await;
        let total_size = stats.total_size;
        let chunks = stats.chunks.clone();

        let output_path = self
            .output_path
            .lock()
            .await
            .as_ref()
            .ok_or_else(|| DownloadError::Other("Output path not set".to_string()))?
            .clone();

        let state = DownloadState {
            url: self.url.clone(),
            output_path: output_path.clone(),
            total_size,
            chunks,
            config: self.config.clone(),
        };

        let state_json = serde_json::to_string(&state)?;
        tokio::fs::write(self.get_current_state_file_path().await, state_json).await?;
        Ok(())
    }

    /// 从文件加载下载状态
    async fn load_state(&self) -> Result<DownloadState, DownloadError> {
        let state_file_path = self.get_current_state_file_path().await;
        info!("加载状态文件：{:?}", state_file_path);
        if tokio::fs::metadata(&state_file_path).await.is_err() {
            return Err(DownloadError::InvalidStateFile);
        }

        let state_json = tokio::fs::read_to_string(&state_file_path).await?;
        let state: DownloadState = serde_json::from_str(&state_json)?;

        // 验证状态文件的有效性
        let current_output_path = self
            .output_path
            .lock()
            .await
            .as_ref()
            .ok_or_else(|| DownloadError::InvalidStateFile)?
            .clone();
        if state.url != self.url || state.output_path != current_output_path {
            return Err(DownloadError::InvalidStateFile);
        }

        Ok(state)
    }

    /// 克隆Arc包装的版本
    fn clone_arc(&self) -> Arc<Self> {
        Arc::new(Self {
            url: self.url.clone(),
            download_dir: self.download_dir.clone(),
            output_path: self.output_path.clone(),
            config: self.config.clone(),
            status: self.status.clone(),
            stats: self.stats.clone(),
            is_cancelled: self.is_cancelled.clone(),
            is_paused: self.is_paused.clone(),
            pause_notify: self.pause_notify.clone(),
            state_file_path: self.state_file_path.clone(),
            supports_range: self.supports_range.clone(),
        })
    }
}

/// 下载管理器
pub struct DownloadManager {
    tasks: Arc<Mutex<HashMap<String, Arc<DownloadTask>>>>,
}

impl DownloadManager {
    /// 创建新的下载管理器
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 添加下载任务
    pub async fn add_task(
        &self,
        url: String,
        download_dir: PathBuf,
        config: DownloadConfig,
    ) -> String {
        let task_id = uuid::Uuid::new_v4().to_string();
        let task = Arc::new(DownloadTask::new(url, download_dir, config));

        self.tasks.lock().await.insert(task_id.clone(), task);
        task_id
    }

    /// 开始下载任务
    pub async fn start_task(&self, task_id: &str) -> Result<(), DownloadError> {
        let task = self.tasks.lock().await.get(task_id).cloned();
        if let Some(task) = task {
            task.start().await?;
            Ok(())
        } else {
            Err(DownloadError::Other("Task not found".to_string()))
        }
    }

    /// 获取任务状态
    pub async fn get_task_status(&self, task_id: &str) -> Option<DownloadStatus> {
        if let Some(task) = self.tasks.lock().await.get(task_id) {
            Some(task.get_status().await)
        } else {
            None
        }
    }

    /// 获取任务统计
    pub async fn get_task_stats(&self, task_id: &str) -> Option<DownloadStats> {
        if let Some(task) = self.tasks.lock().await.get(task_id) {
            Some(task.get_stats().await)
        } else {
            None
        }
    }

    /// 暂停任务
    pub async fn pause_task(&self, task_id: &str) {
        if let Some(task) = self.tasks.lock().await.get(task_id) {
            task.pause().await;
        }
    }

    /// 恢复任务
    pub async fn resume_task(&self, task_id: &str) {
        if let Some(task) = self.tasks.lock().await.get(task_id) {
            task.resume().await;
        }
    }

    /// 取消任务
    pub async fn cancel_task(&self, task_id: &str) {
        if let Some(task) = self.tasks.lock().await.get(task_id) {
            task.cancel().await;
        }
    }
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 从HTTP响应头中提取文件名
fn extract_filename_from_headers(headers: &reqwest::header::HeaderMap, url: &str) -> String {
    // 首先尝试从 Content-Disposition 头中提取文件名
    if let Some(content_disposition) = headers.get(reqwest::header::CONTENT_DISPOSITION)
        && let Ok(disposition_str) = content_disposition.to_str()
    {
        let parsed = content_disposition::parse_content_disposition(disposition_str);
        if let Some(filename) = parsed.filename() {
            return format!("{}.{}", filename.0, filename.1.unwrap_or_default());
        }
    }

    // 如果Content-Disposition中没有文件名，从URL路径中提取
    extract_filename_from_url(url)
}

fn extract_filename_from_url(url: &str) -> String {
    // 尝试从URL路径中提取文件名
    if let Ok(parsed_url) = url::Url::parse(url) {
        let path = parsed_url.path();
        if let Some(filename) = path.split('/').next_back()
            && !filename.is_empty()
            && filename != "/"
        {
            // 解码URL编码的文件名
            if let Ok(decoded) = urlencoding::decode(filename) {
                return decoded.to_string();
            }
            return filename.to_string();
        }
    }

    // 如果无法从URL提取，使用默认文件名
    "downloaded_file".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_download_task_creation() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        assert_eq!(task.url, "https://example.com/file.txt");
        assert_eq!(task.download_dir, PathBuf::from("."));
        assert!(task.output_path.lock().await.is_none());
    }

    #[tokio::test]
    async fn test_download_manager() {
        let manager = DownloadManager::new();
        let temp_dir = tempdir().unwrap();

        let task_id = manager
            .add_task(
                "https://example.com/file.txt".to_string(),
                temp_dir.path().to_path_buf(),
                DownloadConfig {
                    max_connections: 1,
                    chunk_size: 512,
                    ..Default::default()
                },
            )
            .await;

        assert!(!task_id.is_empty());
    }

    #[tokio::test]
    async fn test_calculate_chunk_size() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // 测试小文件：1MB - 应该使用100KB块大小
        let chunk_size = task.calculate_chunk_size(1 * 1024 * 1024);
        println!(
            "1MB文件块大小: {} bytes (期望: {} bytes)",
            chunk_size,
            100 * 1024
        );
        assert_eq!(chunk_size, 100 * 1024);

        // 测试中等文件：10MB - 应该使用500KB块大小
        let chunk_size = task.calculate_chunk_size(10 * 1024 * 1024);
        println!(
            "10MB文件块大小: {} bytes (期望: {} bytes)",
            chunk_size,
            500 * 1024
        );
        assert_eq!(chunk_size, 500 * 1024);

        // 测试大文件：1GB - 应该使用1MB块大小
        let chunk_size = task.calculate_chunk_size(1024 * 1024 * 1024);
        println!(
            "1GB文件块大小: {} bytes (期望: {} bytes)",
            chunk_size,
            1 * 1024 * 1024
        );
        assert_eq!(chunk_size, 1 * 1024 * 1024);

        // 测试自定义块大小配置 - 应该使用配置的块大小
        let task_with_custom_chunk = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig {
                chunk_size: 2 * 1024 * 1024,
                ..Default::default()
            },
        );
        let chunk_size = task_with_custom_chunk.calculate_chunk_size(100 * 1024 * 1024);
        assert_eq!(chunk_size, 2 * 1024 * 1024);

        // 测试边界情况：0字节文件
        let chunk_size = task.calculate_chunk_size(0);
        assert_eq!(chunk_size, 100 * 1024); // 应该使用最小块大小
    }
}
