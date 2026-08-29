//! # downloader —— Rust 异步下载器库
//!
//! 基于 Tokio 与 reqwest 的异步下载器，支持多连接并行下载、断点续传、速度限制、
//! 实时进度统计与运行中动态调整配置。所有异步 API 均为 `async`，任务以 UUID 标识，
//! 由 [`DownloadManager`] 统一管理。
//!
//! ## 核心类型
//!
//! - [`DownloadManager`]：任务管理器，负责添加/启动/暂停/恢复/取消/移除任务，
//!   并提供全局事件总线（[`DownloadEvent`]）与任务枚举（[`DownloadManager::list_tasks`]）。
//! - [`DownloadTask`]：单个下载任务的共享句柄，提供进度快照（[`DownloadTask::get_stats`]）、
//!   实时进度订阅（[`DownloadTask::subscribe_progress`]）与 pause/resume/cancel 控制。
//! - [`DownloadConfig`]：下载配置（连接数、块大小、速度限制、超时、自动重试等）。
//! - [`DownloadStatus`] / [`DownloadError`]：结构化任务状态与错误，
//!   可程序化匹配 `RequestTimeout`/`ReadTimeout` 等瞬时错误并自动重试。
//!
//! ## 快速开始
//!
//! ```no_run
//! use std::path::PathBuf;
//! use std::time::Duration;
//!
//! use downloader::{DownloadConfig, DownloadManager, DownloadStatus};
//!
//! #[tokio::main]
//! async fn main() {
//!     let manager = DownloadManager::new();
//!     let task_id = manager
//!         .add_task(
//!             "https://example.com/file.zip".to_string(),
//!             PathBuf::from("."),
//!             DownloadConfig::default(),
//!         )
//!         .await;
//!     manager.start_task(&task_id).await.expect("开始下载失败");
//!
//!     // 轮询直至任务进入终态（也可用 subscribe_events / subscribe_progress 免轮询）
//!     loop {
//!         let status = manager.get_task_status(&task_id).await;
//!         if matches!(
//!             status,
//!             Some(DownloadStatus::Completed)
//!                 | Some(DownloadStatus::Failed(_))
//!                 | Some(DownloadStatus::Cancelled)
//!         ) {
//!             break;
//!         }
//!         tokio::time::sleep(Duration::from_millis(100)).await;
//!     }
//! }
//! ```
//!
//! 更完整的用法（进度订阅、事件监听、断点续传、速度限制等）见 `examples/` 目录
//! 与 README。

// 发布门禁：库的公开 API 必须全部有文档注释，新增公开项缺文档即编译失败，
// 避免对外暴露无说明的接口（与 rustdoc 质量、`missing_docs` 规范一致）。
#![deny(missing_docs)]

use std::collections::{BTreeMap, HashMap};

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use log::{error, info, warn};
use parking_lot::RwLock;
use reqwest::Client;
use reqwest::header::{COOKIE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, Notify, broadcast, watch};
use tokio::task::JoinSet;
use tokio::time::sleep;
use uuid::Uuid;

/// 下载器错误类型
///
/// 实现 `Clone`/`PartialEq`/`Eq`，可直接存入 [`DownloadStatus::Failed`] 与
/// [`DownloadEvent::TaskFailed`]，让调用方无需解析字符串即可程序化匹配
/// `RequestTimeout`/`ReadTimeout`/`TaskNotFound` 等具体错误变体。
/// 序列化采用 Display 字符串形式（反序列化还原为 [`DownloadError::Other`]），
/// 便于跨进程/持久化传递，同时保持 `DownloadStatus` 可序列化。
#[derive(Error, Debug, Clone)]
pub enum DownloadError {
    /// 内部 Tokio 任务错误（`JoinError`），如后台任务 panic 或被 join 时出错。
    #[error("Tokio任务错误: {0}")]
    TokioError(Arc<tokio::task::JoinError>),
    /// 内部 HTTP 请求错误（`reqwest::Error`），如建连失败、DNS 解析失败、服务器返回异常。
    #[error("HTTP请求错误: {0}")]
    HttpError(Arc<reqwest::Error>),
    /// 内部 IO 错误（`std::io::Error`），如本地文件读写失败、磁盘空间不足。
    #[error("IO错误: {0}")]
    IoError(Arc<std::io::Error>),
    /// 内部 JSON 序列化错误（`serde_json::Error`），如状态文件解析失败。
    #[error("JSON序列化错误: {0}")]
    JsonError(Arc<serde_json::Error>),
    /// 下载被用户取消（调用 `cancel`/`cancel_task` 后产生）。
    #[error("下载被取消")]
    Cancelled,
    /// 无效的下载状态文件：残留的 `.download_state` 内容损坏或格式不符合预期。
    #[error("无效的下载状态文件")]
    InvalidStateFile,
    /// 任务不存在：`task_id` 未在管理器中注册（多为调用方传入了错误/已移除的 ID）。
    /// 区别于下载本身的失败，便于调用方把「ID 无效」与「下载出错」分开处理。
    #[error("任务不存在")]
    TaskNotFound,
    /// 请求超时：超过 `read_timeout` 仍未收到响应头（连接已建立但服务器不返回）。
    /// 属瞬时/可重试的错误，区别于永久性的非法响应或内容变更。
    #[error("请求超时：服务器未在限定时长内返回响应")]
    RequestTimeout,
    /// 读取超时：下载中途超过 `read_timeout` 未收到任何数据（连接挂起/断流）。
    /// 同样属瞬时/可重试的错误。
    #[error("读取超时：连接可能在下载中途挂起")]
    ReadTimeout,
    /// 其他错误（自定义文本）。多用于非结构化错误信息，或从状态文件反序列化还原而来。
    #[error("其他错误: {0}")]
    Other(String),
}

impl DownloadError {
    /// 是否为瞬时/可重试错误（超时类）。
    ///
    /// 超时（请求/读取）通常由网络抖动或服务器过载引起，重试很可能成功；
    /// 而 HTTP/IO/内容一致性错误多为永久性，盲目重试无意义。调用方可据此
    /// 在下载失败后决定是否自动重试。
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            DownloadError::RequestTimeout | DownloadError::ReadTimeout
        )
    }
}

// 内部错误类型（`reqwest::Error`、`JoinError` 等）不实现 `Clone`/`PartialEq`/`Eq`，
// 故以 `Arc` 包裹以支持存储与复制，并手动实现 `From` 转换（保持 `?`/`map_err` 兼容）。
impl From<tokio::task::JoinError> for DownloadError {
    fn from(e: tokio::task::JoinError) -> Self {
        DownloadError::TokioError(Arc::new(e))
    }
}

impl From<reqwest::Error> for DownloadError {
    fn from(e: reqwest::Error) -> Self {
        DownloadError::HttpError(Arc::new(e))
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(e: std::io::Error) -> Self {
        DownloadError::IoError(Arc::new(e))
    }
}

impl From<serde_json::Error> for DownloadError {
    fn from(e: serde_json::Error) -> Self {
        DownloadError::JsonError(Arc::new(e))
    }
}

// 内部错误类型（Arc 包裹的 JoinError/reqwest::Error 等）无法直接比较。旧实现
// `self.to_string() == other.to_string()` 在每次比较时分配两个 Display 字符串，
// 且 `Other` 变体也要绕道 Display 往返。新实现先按变体判别：变体不同直接判不等
// （零分配，覆盖最常见的不等情形）；变体相同再比较内部数据——`Other` 直接比字符串，
// Arc 包裹的内部错误才退化为展示文本比较。不同变体 Display 前缀不同，不会误判，
// 与旧实现语义一致。
impl PartialEq for DownloadError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DownloadError::TokioError(a), DownloadError::TokioError(b)) => {
                a.to_string() == b.to_string()
            }
            (DownloadError::HttpError(a), DownloadError::HttpError(b)) => {
                a.to_string() == b.to_string()
            }
            (DownloadError::IoError(a), DownloadError::IoError(b)) => {
                a.to_string() == b.to_string()
            }
            (DownloadError::JsonError(a), DownloadError::JsonError(b)) => {
                a.to_string() == b.to_string()
            }
            (DownloadError::Cancelled, DownloadError::Cancelled)
            | (DownloadError::InvalidStateFile, DownloadError::InvalidStateFile)
            | (DownloadError::TaskNotFound, DownloadError::TaskNotFound)
            | (DownloadError::RequestTimeout, DownloadError::RequestTimeout)
            | (DownloadError::ReadTimeout, DownloadError::ReadTimeout) => true,
            (DownloadError::Other(a), DownloadError::Other(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for DownloadError {}

// 序列化为 Display 字符串（跨进程/持久化友好）；反序列化还原为 `Other(文本)`。
impl Serialize for DownloadError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DownloadError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(DownloadError::Other(s))
    }
}

/// 下载状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadStatus {
    /// 等待中
    Pending,
    /// 下载中
    Downloading,
    /// 暂停中
    Paused,
    /// 已完成
    Completed,
    /// 已取消
    Cancelled,
    /// 失败（携带结构化错误，可程序化匹配 `RequestTimeout`/`ReadTimeout` 等变体）
    Failed(DownloadError),
}

/// 下载块信息
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// 块序号
    pub index: usize,
    /// 块起始偏移（含）
    pub start: u64,
    /// 块结束偏移（含）；`u64::MAX` 表示开放区间（总大小未知，无确定终点）
    pub end: u64,
    /// 该块已下载字节数
    pub downloaded: u64,
    /// 是否已完成
    pub completed: bool,
}

/// 下载统计信息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DownloadStats {
    /// 总大小（字节）。`0` 表示大小未知或空文件——两者无法仅凭该字段区分，
    /// 可结合 `downloaded` 与任务状态判断。
    pub total_size: u64,
    /// 已下载字节数
    pub downloaded: u64,
    /// 当前速度（字节/秒），基于滑动窗口计算；刚启动或测速窗口尚未满时可能为 `0.0`
    pub speed: f64,
    /// 已用时间（自任务开始起算的墙钟时间，含暂停时长）
    pub elapsed: Duration,
    /// 预计剩余时间；速度未知（`speed == 0.0`）时为 `Duration::default()`（零值）
    pub remaining: Duration,
    /// 各分块的进度
    pub chunks: Vec<ChunkInfo>,
}

/// 下载配置
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadConfig {
    /// 最大并行连接数（并发分块数）。默认 4；最小按 1 处理，避免配置为 0 时
    /// 没有任何分块并发导致死锁。
    pub max_connections: usize,
    /// 每个分块的大小（字节）。`0` 表示自动计算（根据文件大小在
    /// 100KB ~ 10MB 之间动态确定）；非 0 值也会被夹在 100KB ~ 10MB 之间。
    pub chunk_size: u64,
    /// 总下载速度限制（字节/秒）。`None`（默认）表示不限速；通过
    /// [`DownloadManager::update_task_config`] 调整后立即生效。
    pub speed_limit: Option<u64>,
    /// 是否自动断点续传：任务启动时若检测到本地残留的 `.download_state` 状态文件，
    /// 自动恢复进度继续下载。默认 `true`。
    pub auto_resume: bool,
    /// Cookies（`key=value; key2=value2` 形式），仅建连时读取并随请求发送。
    pub cookies: Option<String>,
    /// 连接超时：TCP 建连阶段的最大等待时长。默认 30 秒，用于避免目标不可达时
    /// 无限挂起；区别于 [`DownloadConfig::read_timeout`]（仅覆盖响应头与数据读取，
    /// 不覆盖 TCP 建连）。不设置整包总超时，以免中断慢速大文件的流式下载。
    pub connect_timeout: Duration,
    /// 读取超时：单次网络读取（含等待响应头）的最长等待时间，用于检测“连接挂起”。
    ///
    /// `None`（默认）表示不启用超时。启用后：
    /// - 探测/分块请求若在超过该时长内未返回响应头，将报错并中止；
    /// - 分块下载读取中若在超过该时长内未收到任何数据，将报错并中止该分块。
    pub read_timeout: Option<Duration>,
    /// 瞬时错误（请求/读取超时）的最大自动重试次数。默认 3 次；`0` 表示禁用重试。
    ///
    /// 超时通常由网络抖动或服务器瞬时过载引起，重试很可能成功。重试按指数退避
    /// 等待（基数 [`DownloadConfig::retry_base_delay`]，逐次翻倍，封顶
    /// [`DownloadConfig::retry_max_delay`]），且分块重试会从上次已写入的偏移继续
    /// （Range 头以 `start + downloaded` 起始），不会重复下载已完成部分。
    /// HTTP/IO/内容一致性等永久性错误不参与重试，直接失败。
    pub max_retries: u32,
    /// 指数退避的起始等待时长。默认 500ms；第 1 次重试前等待该时长，此后每次翻倍。
    pub retry_base_delay: Duration,
    /// 指数退避的等待时长上限。默认 10s；翻倍超过该值时封顶，避免重试风暴。
    pub retry_max_delay: Duration,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            max_connections: 4,
            chunk_size: 0, // 0表示自动计算块大小
            speed_limit: None,
            auto_resume: true,
            cookies: None,
            connect_timeout: Duration::from_secs(30),
            read_timeout: None,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

/// 当前状态文件格式版本号
///
/// 状态文件用于断点续传。随格式演进而递增版本号后，旧版本状态文件会在加载时
/// 被识别并丢弃（重新下载），避免用不兼容的旧数据错位写入临时文件。
const STATE_VERSION: u32 = 1;

/// 进度通知的最小发布间隔。
///
/// 分块下载循环在写入每个网络块后都会更新统计；若逐块发布，会为大量小块反复
/// 深拷贝整个 `chunks` 快照（大文件时开销显著）。这里按时间节流，保证订阅者仍能
/// 以足够的频率（约 10 次/秒）感知进度，同时避免无谓的克隆开销。
const PROGRESS_PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

/// 根据文件大小计算状态保存间隔（自适应策略）。
///
/// 断点续传依赖 `save_state` 把 `version/total_size/chunks` 序列化到磁盘；每写入
/// 一个网络块就保存一次会在大文件（分块众多）下产生 O(分块数²) 的序列化与 I/O
/// 开销。这里让保存间隔随文件大小自适应：小文件（≤256MB）保持 1MB 精确保存；大文件
/// 逐步增大间隔，使总保存次数控制在 ~256 次以内，避免拖慢超大文件下载。
fn state_save_interval(total_size: u64) -> u64 {
    const MIN_INTERVAL: u64 = 1024 * 1024; // 1MB
    const MAX_INTERVAL: u64 = 64 * 1024 * 1024; // 64MB
    const TARGET_SAVES: u64 = 256; // 目标总保存次数

    if total_size <= TARGET_SAVES * MIN_INTERVAL {
        MIN_INTERVAL
    } else {
        (total_size / TARGET_SAVES).clamp(MIN_INTERVAL, MAX_INTERVAL)
    }
}

/// 验证状态文件中的分块列表是否结构一致且数据完整。
///
/// `load_state` 只检查 `url`/`version` 无法抵御「状态文件被篡改或损坏」的情形：
/// 分块重叠、留洞、越界或 `downloaded` 超过块长都会在续传时把错误字节写到错误
/// 偏移、损坏目标文件。这里在恢复前校验分块不变量，非法即拒绝加载、全量重下。
///
/// 合法状态需满足：
/// - `total_size > 0`：分块从 0 起连续编号，区间首尾严格衔接（`[0, total_size)` 全覆盖），
///   每块 `downloaded ≤ 块长`，且 `completed` 的分块必须恰好下载满块长。
/// - `total_size == 0`（未知大小/空文件）：仅允许单一「开放区间」分块
///   （`start == 0, end == u64::MAX`），其长度未知，不做 `downloaded` 上限校验。
fn is_valid_chunks(chunks: &[ChunkInfo], total_size: u64) -> bool {
    if chunks.is_empty() {
        return false;
    }

    // 未知大小/空文件：单一开放区间分块
    if total_size == 0 {
        if chunks.len() == 1 {
            let c = &chunks[0];
            return c.index == 0 && c.start == 0 && c.end == u64::MAX;
        }
        return false;
    }

    let mut prev_end: Option<u64> = None;
    for (i, chunk) in chunks.iter().enumerate() {
        // 分块序号需从 0 起、连续无重复地对应迭代位置（拒绝乱序/缺号/重复号）
        if chunk.index != i {
            return false;
        }
        // 已知大小时不允许出现开放区间（u64::MAX 结束仅用于未知大小）
        if chunk.end == u64::MAX {
            return false;
        }
        // 起始偏移必须与上一块末尾严格衔接（首块从 0 开始），且不越过结束偏移
        match prev_end {
            None => {
                if chunk.start != 0 {
                    return false;
                }
            }
            Some(prev) => {
                if chunk.start != prev + 1 {
                    return false;
                }
            }
        }
        if chunk.start > chunk.end || chunk.end >= total_size {
            return false;
        }

        let chunk_len = chunk.end - chunk.start + 1;
        // 已下载字节不得超过块长（拒绝越界/整型溢出类脏数据）
        if chunk.downloaded > chunk_len {
            return false;
        }
        // 标记完成的分块必须恰好下载满块长；否则数据与 `completed` 标志自相矛盾
        if chunk.completed && chunk.downloaded != chunk_len {
            return false;
        }

        prev_end = Some(chunk.end);
    }

    // 分块必须整体覆盖整个文件：最后一块的结束偏移 == 总大小 - 1
    chunks.last().is_some_and(|c| c.end == total_size - 1)
}

/// 下载状态快照（用于序列化）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadState {
    /// 状态文件格式版本号
    pub version: u32,
    /// 下载源 URL（用于校验续传目标是否一致）
    pub url: String,
    /// 文件总大小（字节）
    pub total_size: u64,
    /// 各分块进度（恢复下载时据此跳过已下载部分）
    pub chunks: Vec<ChunkInfo>,
}

/// 下载事件：任务终止时的离散通知（完成/失败/取消）。
///
/// 与 [`DownloadManager::subscribe_progress`] 提供的周期统计快照互补：后者用于
/// 观察下载进度，本事件流用于让订阅者**无需轮询状态**即可即时感知任务结束及其
/// 失败原因。终止事件是「通知」而非权威状态——真实状态仍以
/// [`DownloadManager::get_task_status`] 为准（例如订阅过慢导致事件被广播丢弃时）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadEvent {
    /// 任务下载完成
    TaskCompleted {
        /// 任务 ID（与 `add_task` 返回值一致）
        task_id: String,
    },
    /// 任务下载失败（携带结构化错误，避免订阅者再单独查询状态）
    TaskFailed {
        /// 任务 ID
        task_id: String,
        /// 失败原因（与 `DownloadStatus::Failed` 携带的 `DownloadError` 一致，
        /// 可直接程序化匹配具体错误变体）
        error: DownloadError,
    },
    /// 任务被取消
    TaskCancelled {
        /// 任务 ID
        task_id: String,
    },
}

/// 当前代次正在运行的分块集合类型（供 cancel() 直接 abort）。
///
/// 槽位中同时记录登记该集合的**启动世代**（`start()` 时捕获的 `epoch`）。
/// 「取消后立即重启」会让新旧两代后台任务并发运行：新世代 `download_chunks`
/// 会覆盖槽位换上自己的 JoinSet。若槽位不带世代标识，旧世代的中止/轮询逻辑
/// 会把新世代的 JoinSet 误当作自己的去 abort，导致新世代分块被中止（部分文件
/// 被重命名为最终名、静默报 Completed）。世代配对后，任何槽位操作都先校验
/// 世代一致，过期世代对槽位操作一律跳过。
type ActiveChunks = parking_lot::Mutex<Option<(u64, JoinSet<Result<(), DownloadError>>)>>;

/// 下载任务
///
/// `id`/`url`/`download_dir` 是任务的不可变身份与位置信息（公开）；其余字段均为
/// 内部状态，必须通过 `start`/`pause`/`resume`/`cancel`/`get_status`/`get_stats`/
/// `get_config`/`update_config`/`filename` 等方法访问，外部不得直接修改——直接改
/// `status`/`stats`/`config`/`filename` 或内部标志会绕过世代（epoch）等不变量校验，
/// 导致状态事件不发布、续传错位写坏文件等未定义行为。
#[derive(Clone)]
pub struct DownloadTask {
    /// 任务唯一标识（UUID，由 `new`/`add_task` 生成）
    pub id: Uuid,
    /// 下载源 URL
    pub url: String,
    /// 下载文件保存目录
    pub download_dir: PathBuf,
    filename: Arc<Mutex<Option<String>>>,
    config: Arc<RwLock<DownloadConfig>>,
    status: Arc<Mutex<DownloadStatus>>,
    stats: Arc<Mutex<DownloadStats>>,
    is_cancelled: Arc<AtomicBool>,
    is_paused: Arc<AtomicBool>,
    pause_notify: Arc<Notify>,
    /// 启动世代（epoch）：每次 `start()` 递增。后台任务在 `start()` 时捕获自己的
    /// 世代值，并于写终止状态/事件/清理文件前校验是否仍是「当前世代」。
    ///
    /// 「取消后立即重启」会产生两个并发后台任务：上一代（已被取消、正在排空分块）
    /// 与新一代（重启后下载）。若不区分世代，上一代任务会在退出时把新一代刚写入的
    /// `Downloading` 覆盖为 `Cancelled`，并发布一个错误的 `TaskCancelled` 事件、删掉
    /// 新一代正在使用的临时/状态文件。gen 使得过期任务能识别自身并静默退出。
    start_epoch: Arc<AtomicU64>,
    supports_range: Arc<AtomicBool>,
    /// 任务启动时间（用于计算整体耗时与速度）
    started_at: Arc<parking_lot::Mutex<Option<Instant>>>,
    /// 全局限速器（所有分块共享，保证总速度不超限）
    rate_limiter: Arc<RateLimiter>,
    /// 速度跟踪器
    speed_tracker: Arc<parking_lot::Mutex<SpeedTracker>>,
    /// 状态文件写入锁：串行化并发 save_state，避免多个分块同时写坏状态文件
    state_lock: Arc<tokio::sync::Mutex<()>>,
    /// 进度通知源（watch 通道，订阅者获取最新统计快照）
    progress_tx: watch::Sender<DownloadStats>,
    /// 上一次发布进度通知的时间（用于按时间节流发布）
    last_progress: Arc<parking_lot::Mutex<Instant>>,
    /// 全局事件发送端（终止时发布完成/失败/取消事件）。独立创建的任务默认指向
    /// 一个无订阅者的通道（事件静默丢弃）；由 `DownloadManager::add_task` 注入
    /// 管理器共享的事件总线，使 `subscribe_events` 能收到该任务的事件。
    event_tx: broadcast::Sender<DownloadEvent>,
    /// 当前代次正在运行的分块集合（JoinSet）及其启动世代。
    ///
    /// `download_chunks` 生成分块时放入其中；`cancel()` 借此**直接 abort** 阻塞中的
    /// 分块——网络读取未配置 `read_timeout` 时，分块可能永久卡在 `response.chunk()`
    /// 上，仅置取消标志无法让它退出，`join_next` 也会一直挂起。分块完成/中止后由
    /// `download_chunks` 清空。世代标识用于防止过期世代误操作新世代的分块集合。
    active_chunks: Arc<ActiveChunks>,
}

impl DownloadTask {
    /// 创建新的下载任务
    pub fn new(url: String, download_dir: PathBuf, config: DownloadConfig) -> Self {
        let initial_stats = DownloadStats {
            total_size: 0,
            downloaded: 0,
            speed: 0.0,
            elapsed: Duration::default(),
            remaining: Duration::default(),
            chunks: Vec::new(),
        };
        let (progress_tx, _) = watch::channel(initial_stats.clone());

        Self {
            id: Uuid::new_v4(),
            url,
            download_dir,
            filename: Arc::new(Mutex::new(None)),
            config: Arc::new(RwLock::new(config)),
            status: Arc::new(Mutex::new(DownloadStatus::Pending)),
            stats: Arc::new(Mutex::new(initial_stats)),
            is_cancelled: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            pause_notify: Arc::new(Notify::new()),
            start_epoch: Arc::new(AtomicU64::new(0)),
            supports_range: Arc::new(AtomicBool::new(false)),
            started_at: Arc::new(parking_lot::Mutex::new(None)),
            rate_limiter: Arc::new(RateLimiter::new()),
            speed_tracker: Arc::new(parking_lot::Mutex::new(SpeedTracker::new())),
            state_lock: Arc::new(tokio::sync::Mutex::new(())),
            progress_tx,
            last_progress: Arc::new(parking_lot::Mutex::new(Instant::now())),
            // 独立创建的任务默认无事件订阅者；`.0` 取 sender、接收端立即丢弃，
            // 此时 send 只会返回 Err 而无副作用。真正的共享总线由 add_task 注入。
            event_tx: broadcast::channel(1).0,
            active_chunks: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// 开始下载
    pub async fn start(&self) -> Result<(), DownloadError> {
        // 原子地完成「检查 -> 置为下载中」，避免并发 start() 同时通过守卫重复启动；
        // 同时在锁内递增启动世代，使「上一代」后台任务能在退出时识别自身已过期。
        let epoch;
        {
            let mut status = self.status.lock().await;
            if matches!(
                *status,
                DownloadStatus::Downloading | DownloadStatus::Paused
            ) {
                return Ok(());
            }
            *status = DownloadStatus::Downloading;
            // 在状态锁临界区内重置取消/暂停标志：start() 与 cancel() 在同一把锁上
            // 串行化。若把重置放在锁外，并发 cancel() 可能落在「锁释放」与「重置
            // 执行」之间——cancel 先置 Cancelled + is_cancelled=true，随后被这里的
            // 重置静默撤销，用户取消的任务仍会下载完成。锁内重置保证并发 cancel
            // 要么在重置前观察到旧标志、要么在重置后重新置位，不会丢失。
            self.is_cancelled.store(false, Ordering::Relaxed);
            self.is_paused.store(false, Ordering::Relaxed);
            epoch = self.start_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        }

        *self.started_at.lock() = Some(Instant::now());

        let task = self.clone();
        tokio::spawn(async move {
            let client = match task.build_client() {
                Ok(client) => client,
                Err(e) => {
                    task.fail_and_notify(epoch, e).await;
                    return;
                }
            };

            // 用 Range 探测请求（bytes=0-0）代替 HEAD：部分服务器不支持 HEAD，
            // 且 HEAD 的 Content-Length 可能与 GET 不一致；206 响应同时证明支持断点续传。
            // 探测请求同样适用瞬时错误自动重试：服务器瞬时过载时不至于让整个下载失败。
            // 探测退避与分块重试语义一致——暂停会冻结退避计时、取消/换代立即终止
            // （返回 Cancelled，进入取消收尾，而不是走失败路径误报 TaskFailed）。
            // 先拷贝 max_retries/退避参数再 await，避免把 RwLock 读锁守卫跨 await 持有
            let max_retries = task.config.read().max_retries;
            let retry_base = task.config.read().retry_base_delay;
            let retry_max = task.config.read().retry_max_delay;
            let response = match task
                .probe_with_retry(&client, epoch, max_retries, retry_base, retry_max)
                .await
            {
                Ok(resp) => resp,
                Err(DownloadError::Cancelled) => {
                    // 探测退避期间被取消/换代：取消收尾（状态/事件/快照/清理），
                    // 不发布虚假的 TaskFailed
                    task.finish_cancelled(epoch).await;
                    return;
                }
                Err(e) => {
                    task.fail_and_notify(epoch, e).await;
                    return;
                }
            };

            // 空文件场景：Range: bytes=0-0 不可满足，服务器返回 416，此时不应报错，
            // 而是按大小为 0、不支持范围请求处理（后续以完整 GET 下载得到 0 字节文件）
            let probe_status = response.status();
            let is_empty_file = probe_status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE;
            if !probe_status.is_success()
                && probe_status != reqwest::StatusCode::PARTIAL_CONTENT
                && !is_empty_file
            {
                task.fail_and_notify(
                    epoch,
                    DownloadError::Other(format!("HTTP 探测请求失败，状态码: {probe_status}")),
                )
                .await;
                return;
            }

            // 从响应头中提取文件名并设置输出路径
            task.setup_filename(response.headers(), &task.url).await;

            // 解析总大小与范围支持情况
            let (total_size, supports_range) =
                task.extract_response_info(probe_status, response.headers());
            task.supports_range.store(supports_range, Ordering::Relaxed);

            // 探测返回 200 说明服务器忽略 Range、把完整载荷当作响应体返回了。
            // 此时复用该响应体作为「非范围服务器」的单块下载，避免下载器发起第二次
            // 全量 GET（此前探测定会丢掉这个完整响应体、再重新下载一次，浪费一倍带宽）。
            // 仅精确匹配 200：416（空文件）与 206（`bytes 0-0/*` 未知大小）都无完整响应体，不可复用。
            let probe_response = if !supports_range && probe_status == reqwest::StatusCode::OK {
                Some(response)
            } else {
                None
            };

            // 确保下载目录存在
            if let Err(e) = tokio::fs::create_dir_all(&task.download_dir).await {
                task.fail_and_notify(epoch, e.into()).await;
                return;
            }

            // 尝试从状态文件恢复
            let mut is_resume = false;
            if task.config.read().auto_resume
                && let Ok(state) = task
                    .load_state()
                    .await
                    .inspect_err(|e| error!("从状态文件恢复失败：{e}"))
            {
                // 校验服务器文件大小与状态文件一致，避免服务器内容变化导致错位写入
                if state.total_size == total_size {
                    task.restore_state_from_file(&state).await;

                    info!("从状态文件恢复下载，已下载: {} bytes", {
                        let stats = task.stats.lock().await;
                        stats.downloaded
                    });
                    is_resume = true;
                } else {
                    info!(
                        "服务器文件大小已变化（{} -> {}），丢弃旧状态重新下载",
                        state.total_size, total_size
                    );
                }
            }

            // 新下载或服务器不支持范围请求时重新初始化统计与分块
            if !is_resume || !supports_range {
                info!("初始化下载统计");
                // 完整重新下载（非续传或服务器不支持范围）时清理旧临时文件，
                // 避免旧文件内容大于新文件时残留多余字节
                let _ = tokio::fs::remove_file(task.get_output_path().await).await;
                // 同时删除过期的状态文件：其记录的分块进度对应的字节已被上面的
                // 临时文件删除丢弃，若保留下来，将来服务器大小恢复一致时 load_state
                // 会成功并把「已完成」分块从待下载列表过滤掉，而那些字节区在重建的
                // 临时文件中已不存在，最终文件会在这些位置留下零填充空洞且无任何报错
                let _ = tokio::fs::remove_file(task.get_state_file_path().await).await;
                let mut stats = task.stats.lock().await;
                stats.total_size = total_size;
                stats.downloaded = 0;
                stats.speed = 0.0;
                stats.elapsed = Duration::default();
                stats.remaining = Duration::default();
                stats.chunks = task.create_chunks(total_size, supports_range);
                // 重置测速基线，避免复用旧任务累计字节导致进度/速度错乱或下溢
                task.speed_tracker.lock().reset(0);
                // 通知进度订阅者初始化后的快照（含 total_size 与分块信息）
                task.progress_tx.send_replace(stats.clone());
            }

            // 执行下载
            let result = task.download_chunks(&client, probe_response, epoch).await;

            // 根据结果更新任务状态，保留具体错误信息。
            // 先在同一把 status 锁内校验启动世代：若在下载期间有新的 start()（如
            // 「取消后立即重启」）递增了世代，说明新一代后台任务已接管，本代必须静默
            // 退出，不得把新一代的 Downloading 覆盖为 Cancelled/Failed，也不得清理
            // 新一代正在写入的临时/状态文件、发布误导性的终止事件。
            let mut status = task.status.lock().await;
            if task.start_epoch.load(Ordering::Acquire) != epoch {
                return;
            }
            *status = if task.is_cancelled.load(Ordering::Relaxed) {
                // 下载期间被取消：即便 download_chunks 已返回 Ok，也以取消状态为准
                DownloadStatus::Cancelled
            } else {
                match result {
                    Ok(()) => DownloadStatus::Completed,
                    // 与 cancel() 设置的状态保持一致，避免后台任务退出时把
                    // 刚设置的 Cancelled 覆盖成 Failed
                    Err(DownloadError::Cancelled) => DownloadStatus::Cancelled,
                    Err(e) => DownloadStatus::Failed(e),
                }
            };
            let is_cancelled = *status == DownloadStatus::Cancelled;
            let is_completed = *status == DownloadStatus::Completed;
            // 发布终止事件：让事件订阅者在完成/失败/取消时无需轮询状态即可感知
            let event = match &*status {
                DownloadStatus::Completed => Some(DownloadEvent::TaskCompleted {
                    task_id: task.id.to_string(),
                }),
                DownloadStatus::Failed(e) => Some(DownloadEvent::TaskFailed {
                    task_id: task.id.to_string(),
                    error: e.clone(),
                }),
                DownloadStatus::Cancelled => Some(DownloadEvent::TaskCancelled {
                    task_id: task.id.to_string(),
                }),
                // 分发下载前的失败已由 fail_and_notify 发布事件；此处不应有其它状态
                _ => None,
            };
            drop(status);
            if let Some(event) = event {
                let _ = task.event_tx.send(event);
            }

            // 清理前再次校验世代：此前的状态/事件写入已在锁内校验过，但从 drop(status)
            // 到这里的清理操作之间存在 await 点。若期间发生新的 start()（如「取消后
            // 立即重启」），本代已过期，必须静默退出——否则会删除新一代正在使用的
            // 临时/状态文件（Unix 上导致新一代 rename 失败；Windows 上删除失败残留）。
            if task.start_epoch.load(Ordering::Acquire) != epoch {
                return;
            }

            // 取消时清理残留的临时/状态文件。此时 download_chunks 已返回并 join 了
            // 所有分块（不再持有文件句柄），Windows 下删除也不会因共享冲突失败。
            if is_cancelled {
                let _ = tokio::fs::remove_file(task.get_state_file_path().await).await;
                let _ = tokio::fs::remove_file(task.get_output_path().await).await;
            } else if is_completed {
                // 防御性清理：download_chunks 的成功路径已删除状态文件，但 pause() 的
                // save_state 可能恰好在「download_chunks 删除状态文件之后、本任务标记
                // Completed 之前」完成落盘（暂停与完成并发）。此时会残留一个指向已完成
                // 分块的状态文件；这里再删一次，确保 Completed 任务不残留 `.download_state`。
                let _ = tokio::fs::remove_file(task.get_state_file_path().await).await;
            }

            // 发布最终统计快照，确保订阅者在完成/失败/取消时收到终止信号
            // （空文件等无数据写入的场景不会触发逐块发布）。
            //
            // 原子性：在持有 stats 锁时复查世代并同步发布（send_replace 无 await），
            // 避免「复查 epoch → 获取 stats 锁」之间被新的 start() 抢占——否则本代
            // 过期快照（downloaded == total_size）会发布到进度通道，覆盖新下载刚
            // 发布的初始快照，订阅者会误判「新下载已完成」而提前退出。
            {
                let stats = task.stats.lock().await;
                if task.start_epoch.load(Ordering::Acquire) != epoch {
                    return;
                }
                task.progress_tx.send_replace(stats.clone());
            }
        });

        Ok(())
    }

    /// 将任务标记为失败并发布最终进度快照与终止事件。
    ///
    /// 后台任务在「进入分块下载之前」的失败路径（建客户端、探测请求、建目录等）
    /// 需统一走这里，否则订阅者会一直等待一个永远不会到来的终止通知。
    ///
    /// `epoch` 为本后台任务在 `start()` 时捕获的启动世代；若在 await 期间已发生
    /// 新的 `start()`（世代被递增），则本任务已过期，跳过状态/事件写入，避免覆盖
    /// 新一代任务的状态。
    async fn fail_and_notify(&self, epoch: u64, error: DownloadError) {
        let mut status = self.status.lock().await;
        if self.start_epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        *status = DownloadStatus::Failed(error.clone());
        drop(status);

        // 在持有 stats 锁时复查世代并同步发布（send_replace 无 await），避免
        // 「复查 epoch → 获取 stats 锁」之间被新的 start() 抢占——过期世代不得
        // 为已在新世代下重启的任务发布进度快照或虚假的 TaskFailed 事件（订阅者
        // 会把失败归咎于健康的新下载）。复查之后到函数结束再无 await，事件发送
        // 与快照发布构成一个不可分割的原子段。
        {
            let stats = self.stats.lock().await;
            if self.start_epoch.load(Ordering::Acquire) != epoch {
                return;
            }
            self.progress_tx.send_replace(stats.clone());
        }
        // 同步发布失败事件（错误信息与 DownloadStatus::Failed 保持一致）
        let _ = self.event_tx.send(DownloadEvent::TaskFailed {
            task_id: self.id.to_string(),
            error,
        });
    }

    /// 取消收尾：确认世代后写入取消状态、发布取消事件与最终快照、清理残留文件。
    ///
    /// 用于「探测退避期间被取消」等**尚未进入分块下载**的取消路径：`cancel()` 只负责
    /// 置位状态与中止分块，事件/快照/清理统一在这里完成，否则订阅者会永远等不到
    /// 终止通知（进度通道不关闭、`TaskCancelled` 事件不发布）。
    async fn finish_cancelled(&self, epoch: u64) {
        let mut status = self.status.lock().await;
        if self.start_epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        *status = DownloadStatus::Cancelled;
        let event = DownloadEvent::TaskCancelled {
            task_id: self.id.to_string(),
        };
        drop(status);
        let _ = self.event_tx.send(event);

        // 清理前再次校验世代（与主终态路径一致）：期间若发生新的 start()，本代已
        // 过期，不得删除新一代正在使用的临时/状态文件。
        if self.start_epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        // 探测阶段尚未写入任何文件，remove_file 均为无害 no-op；防御性清理
        let _ = tokio::fs::remove_file(self.get_state_file_path().await).await;
        let _ = tokio::fs::remove_file(self.get_output_path().await).await;

        // 在持有 stats 锁时复查世代并同步发布（与 fail_and_notify 一致），避免
        // 「复查 epoch → 获取 stats 锁」之间被新的 start() 抢占。
        {
            let stats = self.stats.lock().await;
            if self.start_epoch.load(Ordering::Acquire) != epoch {
                return;
            }
            self.progress_tx.send_replace(stats.clone());
        }
    }

    /// 带重试地发送探测请求（`Range: bytes=0-0`），退避休眠可被暂停中断（冻结计时）、
    /// 取消/换代立即终止——与分块重试（[`DownloadTask::download_chunk`]）语义一致。
    ///
    /// 暂停发生在探测退避窗口内时，退避计时被冻结、不再发出下一次探测请求，恢复后
    /// 按剩余时间继续；取消/换代返回 `Cancelled`，由调用方（`start()`）进入取消收尾。
    async fn probe_with_retry(
        &self,
        client: &Client,
        epoch: u64,
        max_retries: u32,
        base: Duration,
        max: Duration,
    ) -> Result<reqwest::Response, DownloadError> {
        let mut attempt = 0u32;
        loop {
            // 退避等待期间可能已发生取消/重启：发起新请求前先校验世代，
            // 避免浪费带宽向一个已过期的任务发送请求
            if self.should_stop(epoch) {
                return Err(DownloadError::Cancelled);
            }
            let read_timeout = self.config.read().read_timeout;
            let probe_request = client
                .get(&self.url)
                .header(reqwest::header::RANGE, "bytes=0-0");
            match send_request(read_timeout, probe_request).await {
                Ok(resp) => return Ok(resp),
                Err(e) if e.is_retryable() && attempt < max_retries => {
                    attempt += 1;
                    let backoff = retry_backoff(attempt, base, max);
                    warn!(
                        "探测请求遇到瞬时错误({e})，第 {attempt}/{max_retries} 次重试，等待 {}ms",
                        backoff.as_millis()
                    );
                    // 退避休眠可被暂停中断并冻结计时（与分块重试一致）；
                    // 取消/换代返回 Cancelled
                    self.sleep_interruptible_by_pause(backoff, epoch).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 设置文件名
    async fn setup_filename(&self, headers: &HeaderMap, url: &str) {
        // 响应头与 URL 都解析不出文件名时回退到任务 ID（UUID），保证 `filename`
        // 在探测完成后始终为 `Some`。否则 `save_state` 的 `filename.is_none()`
        // 守卫会把此类下载永久误判为「仍在探测中」，导致断点续传状态从不落盘
        // （文件本身却通过 `get_filename` 的 UUID 回退正常写入，状态文件则缺失）。
        let filename =
            extract_filename_from_headers(headers, url).unwrap_or_else(|| self.id.to_string());

        let mut filename_guard = self.filename.lock().await;
        *filename_guard = Some(filename);
    }

    /// 构建带有配置的 HTTP 客户端
    fn build_client(&self) -> Result<Client, DownloadError> {
        let mut client_builder = Client::builder();
        if let Some(ref cookies) = self.config.read().cookies
            && let Ok(cookies) = HeaderValue::from_str(cookies)
        {
            let mut cookie_header = HeaderMap::new();
            cookie_header.insert(COOKIE, cookies);
            client_builder = client_builder.default_headers(cookie_header);
        }
        // 始终设置非空 User-Agent：reqwest 不会自动附加 UA，未设置时部分服务器会直接拒绝。
        // 默认用「库名/版本」，可通过 USER_AGENT 环境变量覆盖，但绝不允许为空。
        client_builder = client_builder.user_agent(default_user_agent());
        // 连接超时默认 30 秒（构建前拷贝出配置值，避免把 RwLock 读锁守卫跨后续调用持有），
        // 避免目标不可达时无限挂起；不设置总超时以免中断慢速大文件流式下载。
        let connect_timeout = self.config.read().connect_timeout;
        client_builder
            .connect_timeout(connect_timeout)
            .build()
            .map_err(|e| DownloadError::Other(format!("创建 HTTP 客户端失败: {}", e)))
    }

    /// 从状态文件恢复状态
    async fn restore_state_from_file(&self, state: &DownloadState) {
        // 恢复累计已下载字节数（各分块 downloaded 之和），否则断点续传后
        // 进度/速度/剩余时间会从 0 重新计算
        let mut chunks = state.chunks.clone();
        // 归一化「已下载满块长但尚未标记完成」的分块：save_state 的快照可能落在
        // 分块「写完最后一块字节」与「标记 completed」之间的窗口。若恢复后仍按
        // completed=false 处理，会以 `bytes=end+1..end` 的非法区间重复下载、导致
        // 续传失败。这里把已满块长的分块归为完成，跳过重复下载（开放区间块
        // end==u64::MAX 无确定长度，不做此归一化）。
        for chunk in &mut chunks {
            if chunk.end != u64::MAX {
                let chunk_len = chunk.end - chunk.start + 1;
                if chunk.downloaded >= chunk_len {
                    chunk.downloaded = chunk_len;
                    chunk.completed = true;
                }
            }
        }
        let downloaded: u64 = chunks.iter().map(|c| c.downloaded).sum();
        let mut stats = self.stats.lock().await;
        stats.total_size = state.total_size;
        stats.chunks = chunks;
        stats.downloaded = downloaded;
        // 重置速度/剩余时间，避免恢复后短暂显示暂停前的旧值（与全新初始化保持一致）
        stats.speed = 0.0;
        stats.remaining = Duration::default();
        // 重置测速基线，避免把恢复前的历史字节计入首次速度
        self.speed_tracker.lock().reset(downloaded);
        // 通知进度订阅者恢复后的快照
        self.progress_tx.send_replace(stats.clone());
    }

    /// 从探测响应头中提取总大小和范围支持信息
    ///
    /// 探测请求发送 `Range: bytes=0-0`，因此：
    /// - 206 响应：总大小在 `Content-Range: bytes 0-0/总大小` 中
    /// - 200 响应：服务器忽略 Range，总大小在 Content-Length 中，且不支持范围请求
    fn extract_response_info(
        &self,
        status: reqwest::StatusCode,
        headers: &HeaderMap,
    ) -> (u64, bool) {
        // 空文件：Range: bytes=0-0 不可满足，服务器返回 416，按大小 0、不支持范围处理
        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            return (0, false);
        }

        // 206 响应会携带 Content-Range 头（RFC 7233），此时 Content-Length 只是
        // 分片体长度（1 字节），不是文件总大小，不能回退读取
        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            return match headers
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|cr| cr.to_str().ok())
                .and_then(parse_total_from_content_range)
            {
                Some(total) => (total, true),
                // 总大小未知（bytes 0-0/*），或服务器违反 RFC 未返回可解析的
                // Content-Range：保持"未知大小"语义，走开放区间块。绝不能回退到
                // Content-Length——那只是 bytes=0-0 分片体的 1 字节，会把整个文件
                // 误判为 1 字节并导致后续块大小校验失败。
                None => (0, false),
            };
        }

        // 200 响应：服务器忽略 Range，总大小在 Content-Length 中，且不支持范围请求
        let total_size = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|ct_len| ct_len.to_str().ok())
            .and_then(|ct_len| ct_len.parse().ok())
            .unwrap_or(0);

        (total_size, false)
    }

    /// 创建下载块
    fn create_chunks(&self, total_size: u64, supports_range: bool) -> Vec<ChunkInfo> {
        info!(
            "创建下载块，总大小：{}，支持范围请求：{}",
            total_size, supports_range
        );
        if !supports_range || total_size == 0 {
            // 未知大小（total_size == 0）时用 u64::MAX 表示开放区间
            let end = if total_size == 0 {
                u64::MAX
            } else {
                total_size - 1
            };
            return vec![ChunkInfo {
                index: 0,
                start: 0,
                end,
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
            // 用饱和加法计算末块区间，防止 `start + chunk_size - 1` 溢出 u64：
            // `total_size` 源自服务器的 Content-Range，极端（恶意/损坏）值如
            // u64::MAX 时末块计算会越过 u64 上界——debug 下直接 panic，release
            // 下回绕成极小值生成错误分块。后台任务 panic 不会传播（见
            // fail_and_notify），任务会永久卡在 Downloading；饱和加法保证无论
            // 服务器上报多大，都只生成覆盖 [0, total_size) 的连续分块，
            // 正常大小下与普通加法行为完全一致。
            let end = start
                .saturating_add(chunk_size - 1)
                .min(total_size.saturating_sub(1));

            chunks.push(ChunkInfo {
                index,
                start,
                end,
                downloaded: 0,
                completed: false,
            });

            // end ≤ total_size - 1 ≤ u64::MAX - 1，故 end + 1 不会溢出
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
        let config = self.config.read();
        if config.chunk_size > 0 {
            return config.chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
        }

        // 基于文件大小计算块大小
        let chunk_size = if total_size <= 5 * 1024 * 1024 {
            // 小文件（<=5MB）：使用固定100KB块大小
            MIN_CHUNK_SIZE
        } else if total_size <= 50 * 1024 * 1024 {
            // 中等文件（5MB-50MB）：使用500KB块
            500 * 1024
        } else {
            // 大文件（>50MB）：块大小随文件大小自适应，目标约 1024 块。
            // ≤1GB 时保持 1MB；更大文件逐步增大块大小直到 10MB 上限，避免超大文件
            // 产生数十万个分块，导致 `JoinSet`/spawn 与调度开销随分块数线性膨胀。
            const TARGET_CHUNKS: u64 = 1024;
            total_size
                .div_ceil(TARGET_CHUNKS)
                .clamp(TARGET_CHUNK_SIZE, MAX_CHUNK_SIZE)
        };

        // 确保块大小在合理范围内
        chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE)
    }

    /// 下载所有块
    ///
    /// `probe_response`：当服务器不支持范围请求时，`start()` 会把「探测请求返回的
    /// 200 全量响应体」传入，供唯一的单块复用（避免二次全量下载）；其余情况为 `None`。
    async fn download_chunks(
        &self,
        client: &Client,
        probe_response: Option<reqwest::Response>,
        epoch: u64,
    ) -> Result<(), DownloadError> {
        // 并发上限至少为 1，避免配置为 0 时没有任何分块并发导致死锁
        let max_connections = self.config.read().max_connections.max(1);
        // 在共享槽位中登记本代次的 JoinSet（带上启动世代）：cancel() 借此直接
        // abort 阻塞中的分块（网络读取未配置 read_timeout 时，分块可能永久卡在
        // response.chunk() 上，仅置取消标志无法让它退出，join 等待也不会返回）。
        // 因 JoinSet 存放在共享槽位、parking_lot 守卫不能跨 await，下文用
        // try_join_next() 短轮询替代 join_next().await。
        *self.active_chunks.lock() = Some((epoch, JoinSet::new()));
        let mut probe_response = probe_response;

        // 过滤掉已完成分块（续传时已由 restore_state_from_file 归一化），仅保留待下载
        // 分块，避免为每个已完成分块额外 spawn 任务、短暂占用并发度。
        let pending_chunks: Vec<ChunkInfo> = {
            let stats = self.stats.lock().await;
            stats
                .chunks
                .iter()
                .filter(|c| !c.completed)
                .cloned()
                .collect()
        };
        let mut pending = pending_chunks.into_iter();

        // 预填「并发上限」个分块（分块不足则预填全部）。此处同样检查取消/暂停：
        // 暂停会阻塞等待恢复，取消则中断并进入下方统一的中止-join 路径。
        for _ in 0..max_connections {
            if self.should_stop(epoch) {
                break;
            }
            if self.wait_while_paused(epoch).await.is_err() {
                break;
            }
            let Some(chunk) = pending.next() else {
                break;
            };

            let client = client.clone();
            let task = self.clone();
            // 非范围服务器只有一个块，`take` 恰好把探测响应体交给这个块；范围下载时恒为 None
            let reused_response = probe_response.take();
            let mut guard = self.active_chunks.lock();
            match guard.as_mut() {
                // 仅当槽位仍属于本世代时才向其补充分块；槽位已被新世代接管时
                // 不得往新集合里 spawn 过期世代的分块（否则新世代会把这些
                // 过期分块的 JoinError 误当成自己的分块失败）
                Some((slot_epoch, set)) if *slot_epoch == epoch => {
                    set.spawn(async move {
                        task.download_chunk(&client, chunk, reused_response, epoch)
                            .await
                    });
                }
                _ => break, // 槽位被新世代接管：停止预填
            }
        }

        // 预填阶段即被取消/暂停打断时，统一中止并 join 已生成的分块，确保所有分块
        // 释放临时文件句柄后再返回，避免 start() 的取消清理在 Windows 下因共享冲突
        // 残留文件。空 JoinSet 时 abort_all/join 为无操作。
        if self.should_stop(epoch) {
            self.abort_all_chunks(epoch).await;
            return Err(DownloadError::Cancelled);
        }

        // 每完成一个分块，立即处理其结果并补充下一个待下载分块，实现真正的「快速失败」：
        // 取消或任一任务失败（含 panic）时会**在尚未生成剩余分块之前**中止全部，避免失败
        // 后仍有大量后继分块被逐个生成并下载、浪费带宽。JoinSet 按「实际完成/失败顺序」
        // 返回，使最早失败的分块能被尽快发现并中止其它仍在下载的分块。
        //
        // JoinSet 存放在共享槽位中（供 cancel() abort），parking_lot 守卫不能跨 await
        // 持有，因此不能直接 `join_next().await`；这里改用 `try_join_next()` 短锁轮询：
        // 每轮锁内只做一次非阻塞探测，取到结果就处理，取不到则短暂让出再重试。
        loop {
            // 取一个已完成分块的结果（非阻塞）。仅当槽位仍属于本世代时才读取：
            // 「取消后立即重启」时新世代 download_chunks 会覆盖槽位换上自己的
            // JoinSet，过期世代若读取/消费新集合的分块结果，会把新世代的分块
            // 误当成自己的去处理，甚至随后 abort 掉新世代的分块集合。
            let res = {
                let mut guard = self.active_chunks.lock();
                match guard.as_mut() {
                    Some((slot_epoch, set)) if *slot_epoch == epoch => set.try_join_next(),
                    // 槽位已被新世代接管：本世代立即退出，不得触碰新集合
                    _ => return Err(DownloadError::Cancelled),
                }
            };

            match res {
                // 分块内部 panic（JoinError）：连同其他分块一并中止
                Some(Err(e)) => {
                    self.abort_all_chunks(epoch).await;
                    return Err(e.into());
                }
                // 分块下载失败：中止剩余分块后向上传播具体错误
                Some(Ok(Err(e))) => {
                    self.abort_all_chunks(epoch).await;
                    return Err(e);
                }
                // 成功完成一个分块后补充下一个以维持并发度；补充前检查暂停（阻塞等待
                // 恢复）与取消（中断并进入中止-join 路径）。
                Some(Ok(Ok(()))) => {
                    if self.wait_while_paused(epoch).await.is_err() {
                        self.abort_all_chunks(epoch).await;
                        return Err(DownloadError::Cancelled);
                    }
                    if let Some(chunk) = pending.next() {
                        let client = client.clone();
                        let task = self.clone();
                        let reused_response = probe_response.take();
                        let mut guard = self.active_chunks.lock();
                        match guard.as_mut() {
                            Some((slot_epoch, set)) if *slot_epoch == epoch => {
                                set.spawn(async move {
                                    task.download_chunk(&client, chunk, reused_response, epoch)
                                        .await
                                });
                            }
                            // 槽位已被新世代接管：不得往新集合里补充过期分块
                            _ => return Err(DownloadError::Cancelled),
                        }
                    }
                }
                // 暂无完成者：检查是否应停止，或集合已空则结束
                None => {
                    if self.should_stop(epoch) {
                        self.abort_all_chunks(epoch).await;
                        return Err(DownloadError::Cancelled);
                    }
                    // 仅当槽位仍属于本世代时以其为空作为「全部完成」的判据；
                    // 槽位已被新世代接管时本世代直接退出（不得进入下方重命名路径）
                    let is_empty = {
                        let guard = self.active_chunks.lock();
                        match guard.as_ref() {
                            Some((slot_epoch, set)) if *slot_epoch == epoch => set.is_empty(),
                            _ => return Err(DownloadError::Cancelled),
                        }
                    };
                    if is_empty {
                        break;
                    }
                    // 集合非空但暂无完成者：短暂让出后重试
                    sleep(Duration::from_millis(5)).await;
                }
            }
        }

        // 全部完成后、重命名前再次校验世代：若「取消后立即重启」恰好落在完成循环
        // 退出与 rename 之间，新世代已重建并写入共享的 `{name}.temp`，过期世代
        // 不得把它重命名为最终名（否则会把新世代部分写入的临时文件当作最终文件、
        // 静默报 Completed）。与 start() 终态块/失败路径的世代复查保持一致。
        if self.should_stop(epoch) {
            self.abort_all_chunks(epoch).await;
            return Err(DownloadError::Cancelled);
        }

        // 未知大小下载（服务器未给出 Content-Length / 开放区间块 end == u64::MAX）
        // 期间 total_size 恒为 0；全部下载完成后补写实际字节数，让最终快照与
        // 后续 get_stats 的统计更准确（此前用户看到的是 total_size=0 与满下载量并存）。
        // 在持有 stats 锁时复查世代：若期间发生「取消后立即重启」，不得污染新一代统计。
        {
            let mut stats = self.stats.lock().await;
            if self.start_epoch.load(Ordering::Acquire) == epoch
                && stats.total_size == 0
                && stats.downloaded > 0
            {
                stats.total_size = stats.downloaded;
            }
        }

        // 改为最终文件名
        let from = self.get_output_path().await;
        let to = self.download_dir.join(self.get_filename().await);
        // Windows 上 rename 无法覆盖已存在的文件，需先删除旧文件
        #[cfg(windows)]
        if tokio::fs::metadata(&to).await.is_ok() {
            tokio::fs::remove_file(&to).await?;
        }
        // rename 是原子临界点：前面仍有多个 await（取文件名锁、删旧文件），
        // 最后一刻再校验一次世代，杜绝「取消后立即重启」抢占 rename 窗口。
        if self.should_stop(epoch) {
            return Err(DownloadError::Cancelled);
        }
        tokio::fs::rename(from, to).await?;
        let _ = tokio::fs::remove_file(self.get_state_file_path().await).await;

        Ok(())
    }

    /// 下载单个块
    ///
    /// 对瞬时错误（请求/读取超时）按配置的 [`DownloadConfig::max_retries`] 自动重试：
    /// 每次尝试从 `chunk.start + chunk.downloaded` 重新发起 Range 请求，接着上次已写入
    /// 的字节续传，不重复下载已完成部分。重试间按指数退避等待（见 [`retry_backoff`]），
    /// 给瞬时过载的服务器恢复时间；永久性错误不重试，直接向上传播。
    async fn download_chunk(
        &self,
        client: &Client,
        mut chunk: ChunkInfo,
        mut reused_response: Option<reqwest::Response>,
        epoch: u64,
    ) -> Result<(), DownloadError> {
        if chunk.completed {
            return Ok(());
        }

        let supports_range = self.supports_range.load(Ordering::Relaxed);
        let max_retries = self.config.read().max_retries;
        let retry_base = self.config.read().retry_base_delay;
        let retry_max = self.config.read().retry_max_delay;

        let mut attempt = 0u32;
        loop {
            // 重试退避等待期间可能已发生取消/重启：发起新请求前先校验世代，
            // 避免浪费带宽向一个已过期的任务发送请求
            if self.should_stop(epoch) {
                return Err(DownloadError::Cancelled);
            }

            match self
                .download_chunk_attempt(
                    client,
                    &mut chunk,
                    supports_range,
                    reused_response.take(),
                    epoch,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) if e.is_retryable() && attempt < max_retries => {
                    attempt += 1;
                    let backoff = retry_backoff(attempt, retry_base, retry_max);
                    warn!(
                        "分块 {} 遇到瞬时错误({e})，第 {attempt}/{max_retries} 次重试，等待 {}ms",
                        chunk.index,
                        backoff.as_millis()
                    );
                    // 退避休眠可被暂停中断：若暂停发生在退避窗口内，立即进入暂停等待
                    // 并冻结退避计时，恢复后再按剩余时间继续。否则 pause() 在退避窗口
                    // 内会「看似失效」——需等满整个退避（最长 retry_max_delay）重试请求
                    // 才发出，期间下载仍在后台推进，违背暂停即停的语义。
                    self.sleep_interruptible_by_pause(backoff, epoch).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// 分块下载的单次尝试：获取响应 -> 校验 -> 读完整块。
    ///
    /// 成功（块完整写入）返回 `Ok(())`；失败返回错误，其中超时类
    /// （`RequestTimeout`/`ReadTimeout`）由 [`DownloadTask::download_chunk`] 的重试
    /// 循环决定是否重试，其余错误视为永久性直接失败。
    ///
    /// 失败后 `chunk.downloaded` 保留已写入的字节数，重试时以 `start + downloaded`
    /// 作为 Range 起始续传。`reused_response` 仅在首次尝试时可用（非范围服务器复用
    /// 探测响应体），重试时置空、重新发起完整请求。
    async fn download_chunk_attempt(
        &self,
        client: &Client,
        chunk: &mut ChunkInfo,
        supports_range: bool,
        reused_response: Option<reqwest::Response>,
        epoch: u64,
    ) -> Result<(), DownloadError> {
        // 复用探测响应（服务器不支持范围时，探测请求已返回 200 + 全量载荷），
        // 否则按需发起 Range/完整请求
        let mut response = match reused_response {
            Some(response) => response,
            None => {
                // 只有在服务器支持范围请求时才发送 Range 头
                let mut request = client.get(&self.url);
                if supports_range {
                    let range_header =
                        format!("bytes={}-{}", chunk.start + chunk.downloaded, chunk.end);
                    request = request.header(reqwest::header::RANGE, &range_header);
                }
                // 先拷贝 read_timeout 再 await，避免把 RwLock 读锁守卫跨 await 持有
                let read_timeout = self.config.read().read_timeout;
                send_request(read_timeout, request).await?
            }
        };
        if !response.status().is_success() {
            return Err(DownloadError::Other(format!(
                "分块 {} 下载失败，HTTP 状态码: {}",
                chunk.index,
                response.status()
            )));
        }
        // 请求了 Range 但服务器返回 200：说明服务器实际不支持分块，继续写入会损坏文件
        if supports_range && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(DownloadError::Other(format!(
                "分块 {} 服务器未按 Range 请求返回（可能不支持分块下载）",
                chunk.index
            )));
        }

        // 校验服务器返回的 Content-Range 与请求区间一致：请求 `bytes=a-b`，返回的
        // Content-Range 也必须以 `a` 起始。否则意味着服务器错位/错序返回数据
        // （或文件在探测后发生变化），若继续写入会把错误字节落到错误偏移、损坏文件。
        //
        // 依 RFC 7233，206 响应必须携带 Content-Range。此处强制校验，缺失或格式
        // 非法即报错，而非像旧实现那样在解析失败时静默跳过校验、放行错位数据。
        if supports_range {
            let content_range = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    DownloadError::Other(format!(
                        "分块 {} 服务器返回 206 但缺少合法的 Content-Range 头",
                        chunk.index
                    ))
                })?;
            let (returned_start, returned_total) =
                parse_content_range(content_range).ok_or_else(|| {
                    DownloadError::Other(format!(
                        "分块 {} 无法解析 Content-Range 头: {content_range}",
                        chunk.index
                    ))
                })?;

            let requested_start = chunk.start + chunk.downloaded;
            if returned_start != requested_start {
                return Err(DownloadError::Other(format!(
                    "分块 {} 服务器返回的 Content-Range 起始字节 {returned_start} 与请求 {requested_start} 不一致",
                    chunk.index
                )));
            }
            // 已知总大小（>0）时，分块响应的总大小应与探测结果一致，不一致说明内容已变
            if let Some(total) = returned_total {
                let known_total = self.stats.lock().await.total_size;
                if known_total > 0 && total != known_total {
                    return Err(DownloadError::Other(format!(
                        "分块 {} 服务器文件大小发生变化（{known_total} -> {total}），内容可能已被修改",
                        chunk.index
                    )));
                }
            }
        }

        let output_path = self.get_output_path().await;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(output_path)
            .await?;

        // 非范围服务器无法按 Range 续传：重试/续传发起的都是「从头开始」的完整请求，
        // 不能沿用上一次失败尝试已写入的偏移。若首次尝试已部分写入（如 read_timeout），
        // 需重置 `chunk.downloaded` 并清空文件，否则全量响应会被写到 `start+downloaded`
        // 之后——字节计数超预期使任务以「分块大小不匹配」永久失败（而非重试成功），
        // 严重时更会错位写入、损坏文件。
        if !supports_range {
            chunk.downloaded = 0;
            file.set_len(0).await?;
        }

        // 块内顺序写入，定位一次即可
        file.seek(std::io::SeekFrom::Start(chunk.start + chunk.downloaded))
            .await?;

        loop {
            // 读取下一个网络块；read_timeout 用于检测下载中途的“连接挂起”
            // 先拷贝出 read_timeout 再 await，避免把 RwLock 读锁守卫跨 await 持有
            let read_timeout = self.config.read().read_timeout;
            let chunk_data = next_chunk(read_timeout, response.chunk()).await?;
            let Some(chunk_data) = chunk_data else {
                break;
            };

            if self.should_stop(epoch) {
                return Err(DownloadError::Cancelled);
            }

            // 全局限速（所有分块共享同一令牌桶，保证总速度不超过限制）
            let limit = self.config.read().speed_limit.unwrap_or(0);
            self.rate_limiter
                .throttle(chunk_data.len() as u64, limit)
                .await;

            // 限速等待期间可能已发生取消/重启（throttle 内部有 sleep）：
            // 写入前再次校验世代，避免旧世代分块把缓冲数据写入新一代正在使用的
            // 临时文件（Windows 下删除打开中的文件失败被忽略时会造成静默错位写入）
            if self.should_stop(epoch) {
                return Err(DownloadError::Cancelled);
            }

            file.write_all(&chunk_data).await?;
            chunk.downloaded += chunk_data.len() as u64;

            // 更新统计信息
            let mut stats = self.stats.lock().await;
            stats.downloaded += chunk_data.len() as u64;

            // 更新块信息。stats.chunks 恒按 index 连续排列（create_chunks 从 0 递增生成；
            // 续传恢复经 is_valid_chunks 校验 index == 迭代位置），因此可按 index 直接索引，
            // 避免对每个网络块做 O(n) 线性查找（大文件分块可达上千，逐块写放大明显）。
            // get_mut 返回 Option，防御性保留越界时的静默跳过，与原 find 语义一致。
            if let Some(stat_chunk) = stats.chunks.get_mut(chunk.index) {
                stat_chunk.downloaded = chunk.downloaded;
            }

            // 任务级耗时与滑动窗口速度
            stats.elapsed = self
                .started_at
                .lock()
                .as_ref()
                .map(|t| t.elapsed())
                .unwrap_or_default();
            let speed = self.speed_tracker.lock().update(stats.downloaded);
            if speed > 0.0 {
                stats.speed = speed;
                stats.remaining = Duration::from_secs_f64(
                    stats.total_size.saturating_sub(stats.downloaded) as f64 / speed,
                );
            }

            // 节流发布进度通知：仅在超过最小间隔时才深拷贝快照并发布，
            // 避免每个网络块都克隆整个 chunks 列表（大文件下有显著开销）
            let should_publish = {
                let mut last = self.last_progress.lock();
                let now = Instant::now();
                if now.duration_since(*last) >= PROGRESS_PUBLISH_INTERVAL {
                    *last = now;
                    true
                } else {
                    false
                }
            };
            if should_publish {
                self.progress_tx.send_replace(stats.clone());
            }

            // 定期保存状态（间隔随文件大小自适应）；保存时释放锁避免死锁
            let save_interval = state_save_interval(stats.total_size);
            let should_save = (stats.downloaded.saturating_sub(chunk_data.len() as u64)
                / save_interval)
                != (stats.downloaded / save_interval);
            drop(stats);

            if should_save {
                let _ = self
                    .save_state()
                    .await
                    .inspect_err(|e| error!("保存状态文件失败: {}", e));
            }

            // 暂停等待
            self.wait_while_paused(epoch).await?;
        }

        // 校验分块大小（已知大小才校验，未知大小块无法校验）。
        // 用 != 而非 <，同时捕获「下载不全」与「服务器返回超出请求区间的多余字节」
        // 两种破坏文件完整性的情况：多余字节会越界写到下一分块区域，静默损坏文件。
        let expected = if supports_range {
            // end == u64::MAX 表示开放区间（未知大小），此时无法校验块长度
            if chunk.end == u64::MAX {
                0
            } else {
                chunk.end - chunk.start + 1
            }
        } else {
            self.stats.lock().await.total_size
        };
        if expected > 0 && chunk.downloaded != expected {
            return Err(DownloadError::Other(format!(
                "分块 {} 大小不匹配: 实际 {} / 期望 {} 字节",
                chunk.index, chunk.downloaded, expected
            )));
        }

        // 标记块完成（分块按 index 连续排列，直接索引，见 download_chunk_attempt 内注释）
        if !self.should_stop(epoch) {
            let mut stats = self.stats.lock().await;
            if let Some(stat_chunk) = stats.chunks.get_mut(chunk.index) {
                stat_chunk.completed = true;
                stat_chunk.downloaded = chunk.downloaded;
            }
        }

        Ok(())
    }

    /// 判断当前后台任务所属的「世代」是否已过期，过期即应停止推进：
    /// - 任务被取消（`is_cancelled`）；
    /// - 或期间又发生了一次 `start()`（`start_epoch` 被递增，如「取消后立即重启」）。
    ///
    /// 两者任一成立都意味着本后台任务（及其所有分块）不应继续：`start()` 会复位
    /// `is_cancelled`/`is_paused` 这些**共享**标志，旧分块若仅据此判断会误以为已恢复而
    /// 继续写入共享临时文件，与新世代分块交错写坏文件。故必须以「世代未变」为前置条件。
    fn should_stop(&self, epoch: u64) -> bool {
        self.is_cancelled.load(Ordering::Relaxed)
            || self.start_epoch.load(Ordering::Acquire) != epoch
    }

    /// 暂停等待：任务处于暂停状态时阻塞，直到收到恢复/取消通知。
    ///
    /// 先创建 `notified()` future（它记录 `notify_waiters` 的调用计数）再检查暂停
    /// 标志，避免 resume/cancel 发出的通知落在「检查标志」与「创建 future」之间而
    /// 丢失（`notify_waiters` 不保存通知，见 tokio 的 `Notify` 语义）。被取消/被新一代
    /// 取代时返回 `Cancelled`，否则恢复后正常返回 `Ok`。
    async fn wait_while_paused(&self, epoch: u64) -> Result<(), DownloadError> {
        loop {
            let notified = self.pause_notify.notified();
            if !self.is_paused.load(Ordering::Relaxed) {
                // 未被暂停但可能已是过期世代（重启复位了 is_paused/is_cancelled）：
                // 同样必须立即退出，否则旧分块会继续写入共享临时文件。
                return if self.should_stop(epoch) {
                    Err(DownloadError::Cancelled)
                } else {
                    Ok(())
                };
            }
            // 已暂停且已取消/被新世代取代：cancel 或 start 的通知可能早于本 waiter 注册
            // （分块在写入/限速期间取消早已发生），若此时仍盲目 `notified.await` 会因通知
            // 已丢失而永久挂起。故在等待前显式检查，立即返回取消。
            if self.should_stop(epoch) {
                return Err(DownloadError::Cancelled);
            }
            notified.await;
            if self.should_stop(epoch) {
                return Err(DownloadError::Cancelled);
            }
        }
    }

    /// 退避休眠，但可被暂停/恢复中断（与 [`wait_while_paused`] 同源的 `Notify` 机制）。
    ///
    /// 在 [`DownloadTask::download_chunk`] 的重试退避等待中调用：
    /// - 暂停发生在退避窗口内时，立即进入暂停等待并**冻结**退避计时（暂停时长不计入
    ///   退避，恢复后按剩余时间继续），保证 `pause()` 在重试退避期间也即时生效；
    /// - 恢复后扣除已流逝的退避时间继续倒计时，期满返回 `Ok`；
    /// - 取消/换代时返回 `Cancelled`。
    ///
    /// 与 [`wait_while_paused`] 一样，先创建 `notified()` future 再检查暂停标志，
    /// 避免 `pause`/`resume`/`cancel` 的通知落在「检查标志」与「注册 waiter」之间丢失。
    async fn sleep_interruptible_by_pause(
        &self,
        duration: Duration,
        epoch: u64,
    ) -> Result<(), DownloadError> {
        let mut remaining = duration;
        loop {
            if self.should_stop(epoch) {
                return Err(DownloadError::Cancelled);
            }
            // 先注册通知再检查标志（同 wait_while_paused），避免丢失唤醒
            let notified = self.pause_notify.notified();
            if self.is_paused.load(Ordering::Relaxed) {
                // 已暂停且已取消/被新世代取代：与 wait_while_paused 相同的
                // 提前检查，防止通知先于本 waiter 注册而永久挂起
                if self.should_stop(epoch) {
                    return Err(DownloadError::Cancelled);
                }
                notified.await;
                continue; // 冻结期间不计入退避，恢复后按剩余时间继续
            }
            if remaining == Duration::ZERO {
                return Ok(());
            }
            let began = Instant::now();
            tokio::select! {
                // 被 pause/resume/cancel 唤醒：扣除已流逝的退避时间，回到顶部重新判断
                _ = notified => {
                    remaining = remaining.saturating_sub(began.elapsed());
                }
                _ = sleep(remaining) => return Ok(()),
            }
        }
    }

    /// 中止本世代所有运行中的分块并等待它们全部退出（释放临时文件句柄）。
    ///
    /// 由取消/失败路径统一调用：`cancel()` 可能已通过共享槽位调用了 `abort_all()`
    /// （见 [`DownloadTask::cancel`]），此处无论是否已中止，都轮询直到 JoinSet 清空，
    /// 确保后续的取消清理在 Windows 下不会因共享冲突失败。
    ///
    /// abort 是异步的——任务需在下一次被 poll 时才真正结束，故不能一次 join 完，
    /// 需轮询 `try_join_next()` 直至集合为空。
    ///
    /// 仅操作**属于本世代**的分块集合：槽位在「取消后立即重启」时会被新世代
    /// 覆盖，过期世代若直接 abort 槽位里的集合，会把新世代正在运行的分块全部
    /// 中止（导致新世代把部分写入的临时文件重命名为最终名、静默报 Completed）。
    async fn abort_all_chunks(&self, epoch: u64) {
        // 槽位不存在或已属于新世代：本世代无分块可中止，直接返回
        {
            let guard = self.active_chunks.lock();
            match guard.as_ref() {
                Some((slot_epoch, _)) if *slot_epoch == epoch => {}
                _ => return,
            }
        }
        if let Some((_, set)) = self.active_chunks.lock().as_mut() {
            set.abort_all();
        }
        loop {
            let done = {
                let mut guard = self.active_chunks.lock();
                match guard.as_mut() {
                    // 仅排空本世代的分块；期间若槽位被新世代接管则停止排空
                    Some((slot_epoch, set)) if *slot_epoch == epoch => {
                        // 消费已终止任务的 JoinError，直到没有更多结果
                        while set.try_join_next().is_some() {}
                        set.is_empty()
                    }
                    _ => true,
                }
            };
            if done {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    }

    /// 暂停下载
    pub async fn pause(&self) {
        // 持有状态锁完成「检查 -> 写入」，避免后台下载完成时把 Completed
        // 覆盖成 Paused 的竞态窗口（见 start() 中后台任务的最终状态写入）
        {
            let mut status = self.status.lock().await;
            if !matches!(*status, DownloadStatus::Downloading) {
                return;
            }
            self.is_paused.store(true, Ordering::Relaxed);
            self.pause_notify.notify_waiters(); // 唤醒所有等待中的分块
            *status = DownloadStatus::Paused;
        }

        // 保存暂停状态
        if let Err(e) = self.save_state().await {
            error!("保存暂停状态失败: {}", e);
        }
    }

    /// 恢复下载
    pub async fn resume(&self) {
        // 原子地完成「检查 -> 置为下载中」，避免与后台完成/取消任务产生竞态
        {
            let mut status = self.status.lock().await;
            if !matches!(*status, DownloadStatus::Paused) {
                return;
            }
            self.is_paused.store(false, Ordering::Relaxed);
            self.pause_notify.notify_waiters(); // 唤醒所有等待中的分块
            *status = DownloadStatus::Downloading;
        }

        // 复位测速/剩余时间基线：暂停期间没有数据流动，但其墙钟时长仍会计入
        // 测速滑动窗口，导致恢复后首个窗口把速度算成近 0、剩余时间被严重高估。
        // 这里以当前已下载量为新基线重启窗口，与 restore_state_from_file 一致。
        let downloaded = {
            let mut stats = self.stats.lock().await;
            stats.speed = 0.0;
            stats.remaining = Duration::default();
            stats.downloaded
        };
        self.speed_tracker.lock().reset(downloaded);
    }

    /// 取消下载
    pub async fn cancel(&self) {
        {
            let mut status = self.status.lock().await;
            // 终止状态（Completed/Failed）不再回退为 Cancelled：任务已客观结束，
            // 事后取消不应把结果改写成「已取消」（否则状态与已发布的终止事件、
            // 实际产物相矛盾）。仅对运行中/未开始的状态（Pending/Downloading/Paused）
            // 置为取消。
            if matches!(
                *status,
                DownloadStatus::Completed | DownloadStatus::Failed(_)
            ) {
                return;
            }
            *status = DownloadStatus::Cancelled;
        }
        self.is_cancelled.store(true, Ordering::Relaxed);
        // 唤醒暂停等待中的分块，使其尽快退出
        self.pause_notify.notify_waiters();
        // 直接中止阻塞中的分块：分块可能永久卡在网络读取上（未配置 read_timeout 时
        // `response.chunk()` 不会自行返回），仅置标志位无法让它们退出，download_chunks
        // 的 join 等待会一直挂起，临时/状态文件清理与进度通道关闭被无限期推迟。
        // abort 使分块在下一次 poll 时立即结束，download_chunks 随即进入中止-join
        // 统一清理路径。空槽位（任务尚未开始分块）时此操作为无操作。
        if let Some((_, set)) = self.active_chunks.lock().as_mut() {
            set.abort_all();
        }
        // 文件清理不在此处进行：取消时后台分块可能仍持有临时文件句柄，
        // Windows 下删除打开的文件会因共享冲突失败而残留。真正的清理交给
        // 后台任务（download_chunks 返回并 join 全部分块后）统一完成。
    }

    /// 获取下载状态
    pub async fn get_status(&self) -> DownloadStatus {
        self.status.lock().await.clone()
    }

    /// 获取下载统计
    pub async fn get_stats(&self) -> DownloadStats {
        self.stats.lock().await.clone()
    }

    /// 获取当前配置快照
    ///
    /// 与 [`DownloadTask::update_config`] 配对使用，便于读取后局部修改再写回
    /// （例如「临时调低 `speed_limit`，下载结束后恢复原值」）。返回的是配置副本，
    /// 修改该副本不会生效，需调用 `update_config`。
    pub fn get_config(&self) -> DownloadConfig {
        self.config.read().clone()
    }

    /// 获取任务最终文件名
    ///
    /// 文件名由服务端 `Content-Disposition` 头或 URL 路径决定；在下载开始、
    /// 探测请求完成前尚未解析，此时返回 `None`。该值即为下载完成后磁盘上
    /// 的实际文件名（不含 `.temp` 或 `.download_state` 后缀）。
    pub async fn filename(&self) -> Option<String> {
        self.filename.lock().await.clone()
    }

    /// 订阅下载进度：返回最新统计快照的 watch 接收端
    ///
    /// 订阅者会立即收到当前快照，并在下载推进时收到新快照（约每 100ms 节流发布）。
    /// 可用于替代主动轮询 [`DownloadTask::get_stats`]。
    pub fn subscribe_progress(&self) -> watch::Receiver<DownloadStats> {
        self.progress_tx.subscribe()
    }

    /// 更新配置
    ///
    /// 通过共享 RwLock 写入配置，正在运行的下载分块会在下一次读取时
    /// 自动应用新的配置。当前支持动态生效的字段：
    /// - `speed_limit`：立即生效（下载分块每次读取）
    /// - `read_timeout`：立即生效（下载分块每次读取网络块时应用）
    /// - `cookies`：仅在建连时读取，需重新开始下载才生效
    /// - `max_connections` / `chunk_size`：对已创建的分块不生效，需重新开始下载
    pub fn update_config(&self, config: DownloadConfig) {
        *self.config.write() = config;
    }

    /// 获取当前状态文件路径
    async fn get_state_file_path(&self) -> PathBuf {
        // 使用后缀拼接而非 with_extension，避免破坏多扩展名文件名（如 a.tar.gz）
        let name = self.get_filename().await;
        self.download_dir.join(format!("{name}.download_state"))
    }

    /// 获取当前输出路径（临时）
    async fn get_output_path(&self) -> PathBuf {
        let name = self.get_filename().await;
        self.download_dir.join(format!("{name}.temp"))
    }

    async fn get_filename(&self) -> String {
        self.filename
            .lock()
            .await
            .clone()
            .unwrap_or(self.id.to_string())
    }

    /// 保存下载状态到文件
    async fn save_state(&self) -> Result<(), DownloadError> {
        if self.filename.lock().await.is_none() {
            return Ok(());
        }

        // 先获取 state_lock 再取快照：确保并发调用 save_state 时，「最后一个完成
        // 写文件的」拿到的是「更晚（更新）的快照」。若快照在加锁前就拷贝，则可能
        // 出现较旧快照晚于较新快照写入，导致状态文件落后于真实进度（虽不致错乱，
        // 却会让断点续传多下载一段）。
        let _guard = self.state_lock.lock().await;

        // 拷贝快照后立即释放 stats 锁，避免在序列化与文件写入期间长时间占用 stats
        // 锁、阻塞其他分块任务的统计更新
        let (total_size, chunks) = {
            let stats = self.stats.lock().await;
            (stats.total_size, stats.chunks.clone())
        };

        let state = DownloadState {
            version: STATE_VERSION,
            url: self.url.clone(),
            total_size,
            chunks,
        };

        let state_json = serde_json::to_string(&state)?;

        // 先写临时文件再原子重命名，避免进程在写入中途崩溃留下截断文件，
        // 导致断点续传读取失败。
        let state_path = self.get_state_file_path().await;
        let mut tmp_os = state_path.as_os_str().to_os_string();
        tmp_os.push(".tmp");
        let tmp_path = PathBuf::from(tmp_os);

        tokio::fs::write(&tmp_path, &state_json).await?;
        // Windows 上 rename 无法覆盖已存在文件，需先删除旧文件
        #[cfg(windows)]
        if tokio::fs::metadata(&state_path).await.is_ok() {
            tokio::fs::remove_file(&state_path).await?;
        }
        tokio::fs::rename(&tmp_path, &state_path).await?;
        Ok(())
    }

    /// 从文件加载下载状态
    async fn load_state(&self) -> Result<DownloadState, DownloadError> {
        let state_file_path = self.get_state_file_path().await;
        info!("加载状态文件：{:?}", state_file_path);
        if tokio::fs::metadata(&state_file_path).await.is_err() {
            return Err(DownloadError::InvalidStateFile);
        }

        let state_json = tokio::fs::read_to_string(&state_file_path).await?;
        let state: DownloadState = serde_json::from_str(&state_json)?;

        // 验证状态文件的有效性
        if state.url != self.url {
            return Err(DownloadError::InvalidStateFile);
        }
        // 版本不匹配（通常是代码升级后的旧格式）时拒绝加载，避免错位写入
        if state.version != STATE_VERSION {
            return Err(DownloadError::InvalidStateFile);
        }
        // 校验分块结构与数据完整性：杜绝被篡改/损坏的状态文件在续传时错位写坏文件
        if !is_valid_chunks(&state.chunks, state.total_size) {
            return Err(DownloadError::InvalidStateFile);
        }

        Ok(state)
    }
}

/// 全局限速器（令牌桶算法）
///
/// 所有分块共享同一个限速器，保证下载总速度不超过配置的上限，
/// 而不是每个分块各自限速（那样总速度会变成 上限 × 连接数）。
struct RateLimiter {
    state: tokio::sync::Mutex<RateLimiterState>,
}

struct RateLimiterState {
    /// 令牌数（浮点表示，允许小字节消耗）
    tokens: f64,
    /// 上次补充令牌的时间
    last_refill: Instant,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            state: tokio::sync::Mutex::new(RateLimiterState {
                tokens: 0.0,
                last_refill: Instant::now(),
            }),
        }
    }

    /// 消耗 `bytes` 字节的流量额度，若当前速率不允许则等待
    async fn throttle(&self, bytes: u64, limit: u64) {
        if limit == 0 || bytes == 0 {
            return; // 不限速
        }

        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                state.last_refill = now;

                // 按流逝时间补充令牌。容量至少为单次请求大小，否则当某个网络
                // 块大于限速值时（bytes > limit），令牌永远补不满、throttle 死循环。
                let capacity = (limit as f64).max(bytes as f64);
                state.tokens = (state.tokens + elapsed * limit as f64).min(capacity);

                if state.tokens >= bytes as f64 {
                    state.tokens -= bytes as f64;
                    None
                } else {
                    // 关键：不要清空 tokens（旧实现 `state.tokens = 0.0` 会同时
                    // 丢弃其它并发分块应累计的令牌）。多个分块共享同一令牌桶时，
                    // 清空会导致每个分块都把 last_refill 刷新后重新等待完整时长，
                    // 实际速率降为限速值的 1/并发分块数。
                    let deficit = bytes as f64 - state.tokens;
                    Some(Duration::from_secs_f64(deficit / limit as f64))
                }
            };

            if let Some(duration) = wait {
                sleep(duration).await;
            } else {
                return;
            }
        }
    }
}

/// 速度跟踪器：基于滑动窗口计算当前下载速度
struct SpeedTracker {
    /// 窗口起始时间
    window_start: Instant,
    /// 窗口起始时的已下载字节数
    window_bytes: u64,
    /// 最近一次计算的速度（bytes/s）
    last_speed: f64,
}

impl SpeedTracker {
    fn new() -> Self {
        Self {
            window_start: Instant::now(),
            window_bytes: 0,
            last_speed: 0.0,
        }
    }

    /// 重置测量基线（断点续传恢复、重新下载时调用），从指定已下载字节数开始测速
    fn reset(&mut self, downloaded: u64) {
        self.window_start = Instant::now();
        self.window_bytes = downloaded;
        // 同时清空上次速度，避免重置后首个短窗口内的 update 沿用旧速度，
        // 把暂停前/上一轮下载的速度短暂展示到新下载上
        self.last_speed = 0.0;
    }

    /// 传入当前累计已下载字节数，返回最新速度（bytes/s）
    fn update(&mut self, downloaded: u64) -> f64 {
        let now = Instant::now();
        let elapsed = now.duration_since(self.window_start).as_secs_f64();
        if elapsed >= 1.0 {
            let delta = downloaded - self.window_bytes;
            self.last_speed = delta as f64 / elapsed;
            self.window_start = now;
            self.window_bytes = downloaded;
        }
        self.last_speed
    }
}

/// 下载管理器
///
/// 内部状态（任务表与事件总线）均为共享句柄，`Clone` 是廉价的手柄复制：
/// 克隆出的多个 `DownloadManager` 共享同一份任务与事件流，可用于把管理器
/// 交给多个 `tokio::spawn` 的任务分别驱动下载与监听事件（无需再包 `Arc`）。
#[derive(Clone)]
pub struct DownloadManager {
    tasks: Arc<Mutex<HashMap<String, Arc<DownloadTask>>>>,
    /// 全局事件总线（广播通道）：所有任务终止时发布完成/失败/取消事件，
    /// 由 `subscribe_events` 提供给订阅者。
    event_tx: broadcast::Sender<DownloadEvent>,
}

impl DownloadManager {
    /// 创建新的下载管理器
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
        }
    }

    /// 订阅全局下载事件：返回广播接收端，实时收到所有任务的终止事件
    /// （完成/失败/取消）。事件是「通知」而非权威状态，订阅过慢可能因广播
    /// 丢弃而漏掉事件，此时应以 `get_task_status` 为准。
    pub fn subscribe_events(&self) -> broadcast::Receiver<DownloadEvent> {
        self.event_tx.subscribe()
    }

    /// 列出当前管理器中所有任务的 ID（按字典序排序，保证遍历顺序稳定）。
    /// 任务 ID 是 UUID 字符串，可与 `get_task_status`/`get_task_stats` 等配合使用。
    pub async fn list_tasks(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.tasks.lock().await.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// 添加下载任务
    pub async fn add_task(
        &self,
        url: String,
        download_dir: PathBuf,
        config: DownloadConfig,
    ) -> String {
        // 先创建任务，再注入管理器共享的事件总线，最后放入任务表
        let mut task = DownloadTask::new(url, download_dir, config);
        task.event_tx = self.event_tx.clone();
        let task = Arc::new(task);

        let task_id = task.id.to_string();
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
            Err(DownloadError::TaskNotFound)
        }
    }

    /// 获取任务状态
    pub async fn get_task_status(&self, task_id: &str) -> Option<DownloadStatus> {
        let task = self.tasks.lock().await.get(task_id).cloned();
        match task {
            Some(task) => Some(task.get_status().await),
            None => None,
        }
    }

    /// 获取任务统计
    pub async fn get_task_stats(&self, task_id: &str) -> Option<DownloadStats> {
        let task = self.tasks.lock().await.get(task_id).cloned();
        match task {
            Some(task) => Some(task.get_stats().await),
            None => None,
        }
    }

    /// 获取任务最终文件名（尚未从响应头/URL 解析出来时为 `None`）
    pub async fn get_task_filename(&self, task_id: &str) -> Option<String> {
        let task = self.tasks.lock().await.get(task_id).cloned();
        match task {
            Some(task) => task.filename().await,
            None => None,
        }
    }

    /// 订阅任务进度：返回最新统计快照的 watch 接收端（任务不存在时返回 `None`）
    pub async fn subscribe_progress(
        &self,
        task_id: &str,
    ) -> Option<watch::Receiver<DownloadStats>> {
        self.tasks
            .lock()
            .await
            .get(task_id)
            .map(|task| task.subscribe_progress())
    }

    /// 暂停任务
    pub async fn pause_task(&self, task_id: &str) {
        // 先取出句柄并释放任务表锁，避免在 pause -> save_state 的磁盘写入期间
        // 一直占用整个任务表，阻塞其它并发任务的管理操作（与 remove_task 一致）
        let task = self.tasks.lock().await.get(task_id).cloned();
        if let Some(task) = task {
            task.pause().await;
        }
    }

    /// 恢复任务
    pub async fn resume_task(&self, task_id: &str) {
        let task = self.tasks.lock().await.get(task_id).cloned();
        if let Some(task) = task {
            task.resume().await;
        }
    }

    /// 取消任务
    pub async fn cancel_task(&self, task_id: &str) {
        let task = self.tasks.lock().await.get(task_id).cloned();
        if let Some(task) = task {
            task.cancel().await;
        }
    }

    /// 移除任务并释放相关资源
    ///
    /// 若任务仍在下载中（`Pending`/`Downloading`/`Paused`），会先取消它，避免其
    /// 后台分块继续写入、留下孤儿任务；临时/状态文件由后台任务在退出时异步清理。
    /// 任务已处于终止状态（`Completed`/`Failed`/`Cancelled`）时，同步清理其可能
    /// 残留的临时/状态文件（无后台分块持有句柄，可安全删除；`Failed` 通常为
    /// 断点续传保留了部分文件）。
    ///
    /// 返回是否成功移除（任务不存在时返回 `false`）。
    pub async fn remove_task(&self, task_id: &str) -> bool {
        // 先取出任务句柄并判断状态；不在跨 await 期间持有任务表锁
        let task = self.tasks.lock().await.get(task_id).cloned();

        if let Some(task) = task {
            let status = task.get_status().await;
            if matches!(
                status,
                DownloadStatus::Pending | DownloadStatus::Downloading | DownloadStatus::Paused
            ) {
                // 若字节已全部写满（downloaded >= total_size），说明后台任务正处于
                // 「最后一个分块写完 → rename → 状态写入 Completed」的收尾窗口：
                // 此时取消会 abort 掉正在收尾的后台任务、跳过 rename，并把已完整
                // 下载的临时文件删掉，导致最终文件丢失。正确语义是等待收尾完成
                // （有界等待），而非取消；只有真正未完成的下载才直接取消。
                let stats = task.get_stats().await;
                let fully_downloaded = stats.total_size > 0 && stats.downloaded >= stats.total_size;
                if fully_downloaded {
                    // 有界等待后台任务进入终态（rename 为本地磁盘操作，通常毫秒级）
                    let deadline = Instant::now() + Duration::from_secs(5);
                    loop {
                        let s = task.get_status().await;
                        if matches!(
                            s,
                            DownloadStatus::Completed
                                | DownloadStatus::Failed(_)
                                | DownloadStatus::Cancelled
                        ) {
                            break;
                        }
                        if Instant::now() >= deadline {
                            break; // 超时兜底：视为卡住，走下方取消
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    if matches!(
                        task.get_status().await,
                        DownloadStatus::Pending
                            | DownloadStatus::Downloading
                            | DownloadStatus::Paused
                    ) {
                        // 等待后仍未进入终态（如卡在暂停中）：回退为取消，
                        // 由后台任务统一清理残留文件
                        task.cancel().await;
                    } else {
                        // 已进入终态：清理可能残留的临时/状态文件。Completed 时通常
                        // 已由后台清理（此处防御）；若等待期间任务以 Failed 结束，
                        // 断点续传保留的部分文件也在移除任务时一并清除。
                        let _ = tokio::fs::remove_file(task.get_state_file_path().await).await;
                        let _ = tokio::fs::remove_file(task.get_output_path().await).await;
                    }
                } else {
                    task.cancel().await;
                }
            } else {
                // 终止状态：清理可能残留的临时/状态文件
                let _ = tokio::fs::remove_file(task.get_state_file_path().await).await;
                let _ = tokio::fs::remove_file(task.get_output_path().await).await;
            }
        }

        self.tasks.lock().await.remove(task_id).is_some()
    }

    /// 更新任务配置（动态生效，详见 [`DownloadTask::update_config`]）
    pub async fn update_task_config(
        &self,
        task_id: &str,
        config: DownloadConfig,
    ) -> Result<(), DownloadError> {
        let task = self.tasks.lock().await.get(task_id).cloned();
        match task {
            Some(task) => {
                task.update_config(config);
                Ok(())
            }
            None => Err(DownloadError::TaskNotFound),
        }
    }

    /// 获取任务当前配置快照（任务不存在时返回 `None`）
    ///
    /// 与 [`DownloadManager::update_task_config`] 配对，便于「读取 → 局部修改 → 写回」。
    pub async fn get_task_config(&self, task_id: &str) -> Option<DownloadConfig> {
        let task = self.tasks.lock().await.get(task_id).cloned();
        task.map(|t| t.get_config())
    }
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 计算请求使用的 User-Agent 字符串。
///
/// 优先使用 `USER_AGENT` 环境变量（非空时）；否则回退到「库名/版本」。
/// 保证返回非空值：reqwest 不会自动附带 UA，缺失该头会被部分服务器（CDN/反爬）拒绝。
fn default_user_agent() -> String {
    std::env::var("USER_AGENT")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")))
}

/// 发送请求并返回响应；在配置了读取超时时，也对「等待响应头」施加同样的超时。
///
/// reqwest 的 `connect_timeout` 只覆盖 TCP 建连阶段；若服务器接受连接后一直不
/// 返回响应头，仅靠建连超时仍会永久挂起。这里复用 `read_timeout` 的语义
/// （“超过该时长没有任何数据到来即视为挂起”），覆盖「建连后等待首个响应包」
/// 这一段，避免探测/分块请求无限等待。
async fn send_request(
    read_timeout: Option<Duration>,
    request: reqwest::RequestBuilder,
) -> Result<reqwest::Response, DownloadError> {
    match read_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, request.send()).await {
            Ok(result) => result.map_err(DownloadError::from),
            Err(_) => Err(DownloadError::RequestTimeout),
        },
        None => request.send().await.map_err(DownloadError::from),
    }
}

/// 读取下一个网络块；在配置了读取超时时用 `timeout` 检测“连接挂起”。
///
/// `read_timeout` 为 `None` 时退化为直接 `await`，与未配置超时行为一致。
/// 注意：这里针对的是“单次读取”而非整个请求，慢速但持续有数据流入的下载
/// 不会因此被中断。
async fn next_chunk<T>(
    read_timeout: Option<Duration>,
    chunk: impl std::future::Future<Output = Result<Option<T>, reqwest::Error>>,
) -> Result<Option<T>, DownloadError> {
    match read_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, chunk).await {
            Ok(result) => result.map_err(DownloadError::from),
            Err(_) => Err(DownloadError::ReadTimeout),
        },
        None => chunk.await.map_err(DownloadError::from),
    }
}

/// 计算第 `attempt` 次重试前的等待时长（指数退避）。
///
/// 从 `base` 起步，每次重试翻倍，封顶 `max`。`attempt` 从 1 开始计（第 1 次
/// 重试等待 `base`）。退避让瞬时过载的服务器有时间恢复，同时避免重试风暴；
/// 基数与上限可经 [`DownloadConfig::retry_base_delay`] /
/// [`DownloadConfig::retry_max_delay`] 配置。
fn retry_backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    // 2^20 倍（约 100 万倍）已远超任何合理上限，再大没有意义且会触发 u32 移位溢出。
    // 极端基数（如 1ms）也能经封顶正确触顶，无需特殊分支。
    let shift = attempt.saturating_sub(1).min(20);
    base.saturating_mul(1u32 << shift).min(max)
}

/// 从 Content-Range 响应头中解析总大小
///
/// 格式示例：`bytes 0-0/10485760`，返回 `Some(10485760)`；
/// 若为 `bytes 0-0/*`（总大小未知）则返回 `None`。
fn parse_total_from_content_range(value: &str) -> Option<u64> {
    parse_content_range(value).and_then(|(_, total)| total)
}

/// 解析 Content-Range 响应头，返回 `(起始字节, 总大小)`。
///
/// 格式：`bytes start-end/total`，其中 `total` 为 `*` 时表示总大小未知（返回
/// `None`）。用于校验服务器返回的实际区间与请求一致，防止错位/错序写入。
fn parse_content_range(value: &str) -> Option<(u64, Option<u64>)> {
    let spec = value.trim().strip_prefix("bytes ")?.trim();
    let (range, total) = spec.split_once('/')?;
    let start = range.split_once('-')?.0.trim().parse().ok()?;
    let total = match total.trim() {
        "*" => None,
        total => Some(total.parse().ok()?),
    };
    Some((start, total))
}

/// 从HTTP响应头中提取文件名
fn extract_filename_from_headers(
    headers: &reqwest::header::HeaderMap,
    url: &str,
) -> Option<String> {
    // 首先尝试从 Content-Disposition 头中提取文件名。
    if let Some(content_disposition) = headers.get(reqwest::header::CONTENT_DISPOSITION)
        && let Ok(disposition_str) = content_disposition.to_str()
    {
        let parsed = content_disposition::parse_content_disposition(disposition_str);

        // RFC 6266 §4.3：扩展参数 `filename*`（可携带任意字符集，通常为 UTF-8）应
        // 优先于普通 `filename`（仅 ASCII）。`content_disposition` crate 仅在「无普通
        // filename」时才会把 `filename*` 解码并入 `filename`；两者并存时 `filename*`
        // 会原样保留在 params 中，须手动优先解码，否则会退回 ASCII 回退名（如把
        // `filename*=UTF-8''%E4%B8%AD%E6%96%87.txt` 误判为 `filename="file.bin"`）。
        if let Some(raw) = parsed.params.get("filename*")
            && let Some(decoded) = decode_extended_filename(raw)
            && let Some(filename) = sanitize_filename(&decoded)
        {
            return Some(filename);
        }

        // RFC 2231 §3 续段形式（`filename*0`/`filename*1`/…）：content_disposition crate
        // 仅在「无普通 filename」时才会把续段解码并合并进 `filename`；两者并存时它会
        // 跳过合并，把续段留成独立的 `filename*0`、`filename*1`（值已按字符集/百分号
        // 解码）。续段与 `filename*` 同属扩展参数，RFC 6266 §4.3 要求它们优先于普通
        // `filename`，故在此按序拼接优先返回，避免退回 ASCII 回退名。
        if let Some(joined) = join_continuation_segments(&parsed.params, "filename")
            && let Some(filename) = sanitize_filename(&joined)
        {
            return Some(filename);
        }

        // 注意：使用 filename_full() 而非 filename()，后者会把无扩展名文件名
        // （如 "README"）解析成 ("", Some("README"))，导致生成 ".README"
        if let Some(filename) = parsed.filename_full() {
            return sanitize_filename(&filename);
        }
    }

    // 如果Content-Disposition中没有文件名，从URL路径中提取
    extract_filename_from_url(url)
}

/// 解码 RFC 6266 `filename*` 扩展参数值。
///
/// 形式为 `charset'lang'percent-encoded`（lang 可为空）。仅支持必需实现的 UTF-8
/// 字符集（大小写不敏感）；其余字符集或无合法 UTF-8 内容时返回 `None`，由调用方
/// 回退到普通 `filename`。
fn decode_extended_filename(value: &str) -> Option<String> {
    let mut parts = value.splitn(3, '\'');
    let charset = parts.next()?;
    let _lang = parts.next()?;
    let encoded = parts.next()?;
    if !charset.eq_ignore_ascii_case("utf-8") {
        return None;
    }
    let bytes = urlencoding::decode_binary(encoded.as_bytes());
    String::from_utf8(bytes.into_owned()).ok()
}

/// 拼接 RFC 2231 §3 的续段参数（`{base}*0`、`{base}*1`、…），还原完整参数值。
///
/// `content_disposition` crate 在「无同名普通参数」时会把续段解码并合并进普通键
/// （如 `filename*0`+`filename*1` → `filename`）；但当普通 `filename` 与续段并存时，
/// 其合并步骤会被跳过（要求无普通 `filename`），续段便以独立键 `filename*0`、
/// `filename*1` 残留——且这些残留值已被 crate 按字符集/百分号解码为 UTF-8。
///
/// 此处从 `base*0` 起按序拼接，直到遇到缺号。若连首段 `{base}*0` 都不存在则返回
/// `None`，由调用方回退到普通参数。
fn join_continuation_segments(params: &BTreeMap<String, String>, base: &str) -> Option<String> {
    let first = params.get(&format!("{base}*0"))?;
    let mut joined = String::with_capacity(first.len());
    let mut index = 0usize;
    while let Some(segment) = params.get(&format!("{base}*{index}")) {
        joined.push_str(segment);
        index += 1;
    }
    Some(joined)
}

fn extract_filename_from_url(url: &str) -> Option<String> {
    // 尝试从URL路径中提取文件名
    let parsed_url = url::Url::parse(url).ok()?;
    let path = parsed_url.path();
    let filename = path.split('/').next_back()?;
    if filename.is_empty() || filename == "/" {
        return None;
    }

    // 解码URL编码的文件名
    let decoded = if let Ok(decoded) = urlencoding::decode(filename) {
        decoded.to_string()
    } else {
        filename.to_string()
    };

    sanitize_filename(&decoded)
}

/// 状态文件写入时的最长中间路径后缀 `.download_state.tmp` 的字节数。
///
/// `save_state` 先写 `{filename}.download_state.tmp` 再原子重命名
/// （见 [`DownloadTask::save_state`]），故实际创建的最长路径组件带 `.tmp`，
/// 而非最终状态文件 `.download_state` 本身；它也是 `.temp`（5 字节）与
/// `.download_state.tmp`（19 字节）中较长者，文件名需按其预留空间，
/// 否则路径组件会超过文件系统的 255 字节上限（`ENAMETOOLONG`）。
const STATE_FILE_SUFFIX_LEN: usize = ".download_state.tmp".len();

/// 文件名本身的最大字节数。
///
/// 文件系统单路径组件上限约 255 字节（Linux ext4 与 Windows NTFS）。下载过程中
/// 会用到 `{filename}.temp`、`{filename}.download_state` 与写入状态文件时的
/// `{filename}.download_state.tmp`，最长者为最后一个；另外 Windows 保留设备名
/// （CON/PRN/...）需加 `_` 前缀（1 字节）。故文件名截断到
/// `255 - .download_state.tmp(19) - 下划线前缀(1) = 235` 字节，确保这三种
/// 中间路径组件（含带前缀的形态）都不越界。
const MAX_FILENAME_BYTES: usize = 255 - STATE_FILE_SUFFIX_LEN - 1;

/// 清理文件名：仅保留最后一个路径段，剔除路径分隔符、`.`/`..` 与控制字符，
/// 防止恶意服务器通过 Content-Disposition 或 URL 注入路径穿越（如 `../../evil.exe`）。
fn sanitize_filename(name: &str) -> Option<String> {
    // 先统一分隔符为 `/`，再取最后一个非空路径段
    let normalized = name.replace('\\', "/");
    let base = normalized
        .split('/')
        .map(str::trim)
        .rfind(|s| !s.is_empty())?;

    if base == "." || base == ".." {
        return None;
    }

    // 同时剔除控制字符与 Windows 文件系统禁止出现在文件名中的字符
    // （< > : " | ? *），否则在 Windows 上创建/重命名会失败
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control() && !is_windows_invalid_char(*c))
        .collect();

    // Windows 会静默截断文件名末尾的点与空格（如 `file.txt.` 实际落盘为 `file.txt`），
    // 导致 `.temp`/`.download_state` 后缀与最终文件名无法对应；这里统一剔除。
    let mut trimmed = cleaned.trim_end_matches([' ', '.']).to_string();
    if trimmed.is_empty() {
        return None;
    }

    // 文件系统对单路径组件长度有限制（Linux ext4/Windows NTFS 均为约 255 字节）。
    // 服务器返回的超长文件名（如极长 Content-Disposition）会在此再次创建/重命名
    // 时触发 ENAMETOOLONG；统一截断到 255 字节，且在 UTF-8 字符边界处截断以免
    // 把多字节字符裁成两半产生非法序列。
    truncate_to_char_boundary(&mut trimmed, MAX_FILENAME_BYTES);

    // 截断可能恰好落在点/空格之后（如 `foo.bar` 截成 `foo.`），需再次剔除末尾
    // 点与空格；剔除后可能为空，同样拒绝。
    let trimmed = trimmed.trim_end_matches([' ', '.']);
    if trimmed.is_empty() {
        return None;
    }

    // Windows 保留设备名（CON/PRN/AUX/NUL/COM1-9/LPT1-9）不能作为文件名，
    // 加下划线前缀规避，避免创建/重命名失败。
    let stem = trimmed.split('.').next().unwrap_or(trimmed);
    if is_reserved_device_name(stem) {
        // `_` 前缀会额外占 1 字节，可能把已截断到上限的名字推超限（MAX_FILENAME_BYTES
        // 已为此预留 1 字节，但带前缀的名字 + `.download_state.tmp` 仍需 ≤ 255，
        // 这里按 MAX_FILENAME_BYTES 再截断一次双保险），且需再次剔除末尾点/空格
        let mut prefixed = format!("_{trimmed}");
        truncate_to_char_boundary(&mut prefixed, MAX_FILENAME_BYTES);
        let prefixed = prefixed.trim_end_matches([' ', '.']);
        if prefixed.is_empty() {
            None
        } else {
            Some(prefixed.to_string())
        }
    } else {
        Some(trimmed.to_string())
    }
}

/// 将字符串按字节数截断到不超过 `max_bytes`，并保证落在 UTF-8 字符边界上。
///
/// 超长文件名经 `sanitize_filename` 清洗后在此截断；直接在字节处切割可能把
/// 一个多字节 UTF-8 字符切成两半，产生非法字符串并导致后续文件操作失败。
fn truncate_to_char_boundary(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    // 从 max_bytes 向前回退，找到最近的一个字符边界
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

/// 判断是否为 Windows 保留设备名（不区分大小写）
fn is_reserved_device_name(stem: &str) -> bool {
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

/// 判断是否为 Windows 文件系统不允许出现在文件名中的字符
///
/// 这些字符在 Windows 上有特殊含义（通配、流分隔、驱动器分隔、引号等），
/// 出现在文件名中会导致 `fs::rename`/`create` 失败，因此需在清洗阶段剔除。
fn is_windows_invalid_char(c: char) -> bool {
    matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*')
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
    }

    #[test]
    fn test_download_error_partial_eq_semantics() {
        use std::io::ErrorKind;

        // 相同变体 + 相同载荷 => 相等
        assert_eq!(
            DownloadError::Other("x".to_string()),
            DownloadError::Other("x".to_string())
        );
        assert_eq!(DownloadError::Cancelled, DownloadError::Cancelled);
        assert_eq!(DownloadError::RequestTimeout, DownloadError::RequestTimeout);
        // `Other` 直接按字符串比较（不经 Display 往返，前缀不参与）
        assert_ne!(
            DownloadError::Other("x".to_string()),
            DownloadError::Other("y".to_string())
        );
        // 相同变体但内部错误文本不同 => 不等（Arc 包裹，退化为文本比较）
        let io_a: DownloadError = std::io::Error::new(ErrorKind::NotFound, "a").into();
        let io_b: DownloadError = std::io::Error::new(ErrorKind::NotFound, "b").into();
        assert_ne!(io_a, io_b);
        assert_eq!(io_a.clone(), io_a);
        // 不同变体 => 不等（即使 Display 文本可能相似，前缀不同不会误判）
        assert_ne!(DownloadError::Cancelled, DownloadError::TaskNotFound);
        assert_ne!(
            DownloadError::RequestTimeout,
            DownloadError::Other("请求超时：服务器未在限定时长内返回响应".to_string())
        );
        // 与旧实现（Display 文本相等即相等）语义保持一致：空 Other 与任意变体不等
        assert_ne!(
            DownloadError::Other(String::new()),
            DownloadError::ReadTimeout
        );
    }

    #[test]
    fn test_download_error_is_retryable_only_for_timeouts() {
        // 超时类（瞬时/可重试）
        assert!(DownloadError::RequestTimeout.is_retryable());
        assert!(DownloadError::ReadTimeout.is_retryable());
        // 用户主动/永久性错误不可重试
        assert!(!DownloadError::Cancelled.is_retryable());
        assert!(!DownloadError::TaskNotFound.is_retryable());
        assert!(!DownloadError::InvalidStateFile.is_retryable());
        assert!(!DownloadError::Other("自定义错误".to_string()).is_retryable());
    }

    #[test]
    fn test_download_error_serde_roundtrip() {
        // 无载荷变体：序列化为 Display 文本（JSON 字符串）
        assert_eq!(
            serde_json::to_string(&DownloadError::Cancelled).unwrap(),
            serde_json::to_string("下载被取消").unwrap(),
        );
        // 带载荷变体：文本拼入 Display
        let json = serde_json::to_string(&DownloadError::Other("x".to_string())).unwrap();
        assert_eq!(json, serde_json::to_string("其他错误: x").unwrap());
        // 反序列化恒还原为 Other(文本)——丢失原变体是文档化语义
        let restored: DownloadError = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, DownloadError::Other("其他错误: x".to_string()));
        // DownloadStatus::Failed 整体可往返（状态文件/持久化场景）
        let status = DownloadStatus::Failed(DownloadError::RequestTimeout);
        let status_json = serde_json::to_string(&status).unwrap();
        match serde_json::from_str::<DownloadStatus>(&status_json).unwrap() {
            DownloadStatus::Failed(DownloadError::Other(s)) => {
                assert!(s.contains("请求超时"), "还原文本应含超时语义: {s}")
            }
            other => panic!("应还原为 Failed(Other), 得到 {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_pause_is_noop_when_not_downloading() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // 初始状态为 Pending，pause 应为无操作
        task.pause().await;
        assert_eq!(task.get_status().await, DownloadStatus::Pending);
        assert!(!task.is_paused.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_resume_is_noop_when_not_paused() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        task.resume().await;
        assert_eq!(task.get_status().await, DownloadStatus::Pending);
        assert!(!task.is_paused.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_resume_resets_speed_baseline() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );

        // 模拟暂停前已有进度、速度与剩余时间，并让测速窗口持有旧的 last_speed
        *task.status.lock().await = DownloadStatus::Paused;
        task.is_paused.store(true, Ordering::Relaxed);
        {
            let mut stats = task.stats.lock().await;
            stats.downloaded = 1024;
            stats.speed = 2048.0;
            stats.remaining = Duration::from_secs(10);
        }
        task.speed_tracker.lock().last_speed = 999.0;

        task.resume().await;

        // 状态恢复为 Downloading、暂停标志清除
        assert_eq!(task.get_status().await, DownloadStatus::Downloading);
        assert!(!task.is_paused.load(Ordering::Relaxed));

        // 速度/剩余时间应被复位，避免跨暂停的墙钟时长污染恢复后的估算
        let stats = task.get_stats().await;
        assert_eq!(stats.speed, 0.0);
        assert_eq!(stats.remaining, Duration::default());
        // 测速窗口应以当前已下载量为新基线重启，旧的 last_speed 不再沿用
        let tracker = task.speed_tracker.lock();
        assert_eq!(tracker.last_speed, 0.0);
        assert_eq!(tracker.window_bytes, 1024);
    }

    #[tokio::test]
    async fn test_pause_sets_paused_when_downloading() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );

        *task.status.lock().await = DownloadStatus::Downloading;
        task.pause().await;

        assert_eq!(task.get_status().await, DownloadStatus::Paused);
        assert!(task.is_paused.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_cancel_sets_cancelled() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );

        *task.status.lock().await = DownloadStatus::Downloading;
        task.cancel().await;

        assert_eq!(task.get_status().await, DownloadStatus::Cancelled);
        assert!(task.is_cancelled.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_cancel_does_not_overwrite_terminal_states() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );

        // 已完成的任务取消后应保持 Completed，且不置取消标志——任务已客观结束，
        // 事后取消不应把结果改写成 Cancelled，否则状态与已发布的完成事件、磁盘产物矛盾。
        *task.status.lock().await = DownloadStatus::Completed;
        task.cancel().await;
        assert_eq!(task.get_status().await, DownloadStatus::Completed);
        assert!(!task.is_cancelled.load(Ordering::Relaxed));

        // 已失败的任务同理：保留失败原因，而非回退为 Cancelled
        *task.status.lock().await =
            DownloadStatus::Failed(DownloadError::Other("下载失败".to_string()));
        task.cancel().await;
        assert_eq!(
            task.get_status().await,
            DownloadStatus::Failed(DownloadError::Other("下载失败".to_string()))
        );
        assert!(!task.is_cancelled.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_fail_and_notify_publishes_final_snapshot() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );

        let mut progress = task.subscribe_progress();

        // 模拟后台任务在进入分块下载之前失败（探测/建客户端/建目录失败）；
        // 该任务从未 start()，其启动世代为 0
        task.fail_and_notify(0, DownloadError::Other("模拟前置失败".to_string()))
            .await;

        // 订阅者应收到一次新的统计快照，而不是一直等待一个永不发出的通知
        tokio::time::timeout(Duration::from_secs(2), progress.changed())
            .await
            .expect("前置失败后应发布最终快照")
            .expect("watch 通道不应已关闭");

        assert_eq!(
            task.get_status().await,
            DownloadStatus::Failed(DownloadError::Other("模拟前置失败".to_string()))
        );
    }

    #[tokio::test]
    async fn test_fail_and_notify_stale_epoch_is_noop() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );

        // 模拟「取消后立即重启」：新一代 start() 递增世代并置为下载中
        *task.status.lock().await = DownloadStatus::Downloading;
        task.start_epoch.fetch_add(1, Ordering::AcqRel);

        // 上一代后台任务（世代 0）迟到的失败通知应被忽略，不得覆盖新一代状态
        task.fail_and_notify(0, DownloadError::Other("陈旧任务的失败".to_string()))
            .await;

        assert_eq!(task.get_status().await, DownloadStatus::Downloading);
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
    async fn test_manager_returns_task_not_found_for_unknown_id() {
        let manager = DownloadManager::new();

        // 不存在的任务 ID：start_task 应返回可区分的 TaskNotFound，而非笼统的 Other
        assert!(matches!(
            manager.start_task("nonexistent").await,
            Err(DownloadError::TaskNotFound)
        ));

        // 同样地，update_task_config 对不存在的任务也应返回 TaskNotFound
        assert!(matches!(
            manager
                .update_task_config("nonexistent", DownloadConfig::default())
                .await,
            Err(DownloadError::TaskNotFound)
        ));
    }

    #[tokio::test]
    async fn test_manager_clone_shares_tasks_and_events() {
        let manager = DownloadManager::new();
        let clone = manager.clone();
        let temp_dir = tempdir().unwrap();

        // 克隆出的管理器与原始句柄共享同一份任务表与事件总线
        let task_id = manager
            .add_task(
                "https://example.com/file.txt".to_string(),
                temp_dir.path().to_path_buf(),
                DownloadConfig::default(),
            )
            .await;

        // 通过克隆句柄应能看到原始句柄添加的任务
        assert!(clone.get_task_status(&task_id).await.is_some());
        assert_eq!(
            clone.get_task_stats(&task_id).await.unwrap().total_size,
            0,
            "克隆句柄应与原始句柄共享同一任务统计"
        );

        // 任务表共享：原始句柄移除后，克隆句柄也看不到该任务
        assert!(manager.remove_task(&task_id).await);
        assert!(clone.get_task_status(&task_id).await.is_none());
    }

    #[tokio::test]
    async fn test_remove_task() {
        let manager = DownloadManager::new();
        let temp_dir = tempdir().unwrap();

        let task_id = manager
            .add_task(
                "https://example.com/file.txt".to_string(),
                temp_dir.path().to_path_buf(),
                DownloadConfig::default(),
            )
            .await;

        // 任务存在时可被移除
        assert!(manager.remove_task(&task_id).await);
        // 再次移除同一任务返回 false
        assert!(!manager.remove_task(&task_id).await);
        // 不存在的任务返回 false
        assert!(!manager.remove_task("nonexistent").await);
    }

    #[tokio::test]
    async fn test_remove_task_cleans_residual_files() {
        let manager = DownloadManager::new();
        let temp_dir = tempdir().unwrap();

        let task_id = manager
            .add_task(
                "https://example.com/file.txt".to_string(),
                temp_dir.path().to_path_buf(),
                DownloadConfig::default(),
            )
            .await;

        // 模拟下载失败后的残留：断点续传会为后续重试保留临时/状态文件
        let task = manager.tasks.lock().await.get(&task_id).unwrap().clone();
        *task.status.lock().await =
            DownloadStatus::Failed(DownloadError::Other("mock".to_string()));
        *task.filename.lock().await = Some("file.txt".to_string());

        let temp_path = temp_dir.path().join("file.txt.temp");
        let state_path = temp_dir.path().join("file.txt.download_state");
        tokio::fs::write(&temp_path, b"partial").await.unwrap();
        tokio::fs::write(&state_path, "{}").await.unwrap();

        // 移除终止状态任务应同步清理残留的临时/状态文件（无后台分块持有句柄）
        assert!(manager.remove_task(&task_id).await);
        assert!(
            tokio::fs::metadata(&temp_path).await.is_err(),
            "临时文件应被清理"
        );
        assert!(
            tokio::fs::metadata(&state_path).await.is_err(),
            "状态文件应被清理"
        );
    }

    #[tokio::test]
    async fn test_calculate_chunk_size() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // 测试小文件：1MB - 应该使用100KB块大小
        let chunk_size = task.calculate_chunk_size(1024 * 1024);
        assert_eq!(chunk_size, 100 * 1024);

        // 测试中等文件：10MB - 应该使用500KB块大小
        let chunk_size = task.calculate_chunk_size(10 * 1024 * 1024);
        assert_eq!(chunk_size, 500 * 1024);

        // 测试大文件：1GB - 应该使用1MB块大小
        let chunk_size = task.calculate_chunk_size(1024 * 1024 * 1024);
        assert_eq!(chunk_size, 1024 * 1024);

        // 超大文件：块大小随文件大小递增，控制分块总数在 ~1024 内
        assert_eq!(
            task.calculate_chunk_size(2 * 1024 * 1024 * 1024),
            2 * 1024 * 1024,
            "2GB 应使用 2MB 块"
        );
        assert_eq!(
            task.calculate_chunk_size(10 * 1024 * 1024 * 1024),
            10 * 1024 * 1024,
            "10GB 应使用 10MB 块"
        );
        assert_eq!(
            task.calculate_chunk_size(100 * 1024 * 1024 * 1024),
            10 * 1024 * 1024,
            "100GB 应钳制到 10MB 上限"
        );

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

        // 自定义块大小应被 clamp 到 [100KB, 10MB]（硬约束）：过小值钳到下限、过大值钳到上限
        let task_with_tiny_chunk = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig {
                chunk_size: 1, // 远小于最小值 100KB
                ..Default::default()
            },
        );
        assert_eq!(
            task_with_tiny_chunk.calculate_chunk_size(1024 * 1024),
            100 * 1024,
            "过小的自定义块大小应被钳制到最小值 100KB"
        );

        let task_with_huge_chunk = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig {
                chunk_size: 1024 * 1024 * 1024, // 1GB，远超最大值 10MB
                ..Default::default()
            },
        );
        assert_eq!(
            task_with_huge_chunk.calculate_chunk_size(100 * 1024 * 1024),
            10 * 1024 * 1024,
            "过大的自定义块大小应被钳制到最大值 10MB"
        );

        // 测试边界情况：0字节文件
        let chunk_size = task.calculate_chunk_size(0);
        assert_eq!(chunk_size, 100 * 1024); // 应该使用最小块大小
    }

    #[test]
    fn test_parse_total_from_content_range() {
        // 标准格式
        assert_eq!(
            parse_total_from_content_range("bytes 0-0/10485760"),
            Some(10485760)
        );
        // 未知总大小
        assert_eq!(parse_total_from_content_range("bytes 0-0/*"), None);
        // 非法格式
        assert_eq!(parse_total_from_content_range("garbage"), None);
        assert_eq!(parse_total_from_content_range(""), None);
    }

    #[test]
    fn test_parse_content_range() {
        // 标准格式：解析起始字节与总大小
        assert_eq!(
            parse_content_range("bytes 100-199/10485760"),
            Some((100, Some(10485760)))
        );
        // 总大小未知（*）
        assert_eq!(parse_content_range("bytes 0-0/*"), Some((0, None)));
        // 非法格式
        assert_eq!(parse_content_range("garbage"), None);
        assert_eq!(parse_content_range(""), None);
        // 缺少 bytes 前缀
        assert_eq!(parse_content_range("0-0/10"), None);
    }

    #[tokio::test]
    async fn test_extract_response_info() {
        use reqwest::StatusCode;
        use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, HeaderMap, HeaderValue};
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // 206 + 已知总大小：从 Content-Range 解析
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-0/10485760"),
        );
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("1"));
        assert_eq!(
            task.extract_response_info(StatusCode::PARTIAL_CONTENT, &headers),
            (10485760, true)
        );

        // 206 + 未知总大小：不得回退到 1 字节的 Content-Length
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-0/*"));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("1"));
        assert_eq!(
            task.extract_response_info(StatusCode::PARTIAL_CONTENT, &headers),
            (0, false)
        );

        // 200 响应（无 Content-Range）：从 Content-Length 解析
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("4096"));
        assert_eq!(
            task.extract_response_info(StatusCode::OK, &headers),
            (4096, false)
        );

        // 无任何大小头
        let headers = HeaderMap::new();
        assert_eq!(
            task.extract_response_info(StatusCode::OK, &headers),
            (0, false)
        );

        // 416 空文件：即便带有 Content-Range: bytes */0，也按 0 大小、不支持范围处理
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes */0"));
        assert_eq!(
            task.extract_response_info(StatusCode::RANGE_NOT_SATISFIABLE, &headers),
            (0, false)
        );
    }

    #[test]
    fn test_extract_filename_from_url() {
        // 普通路径
        assert_eq!(
            extract_filename_from_url("https://example.com/file.txt"),
            Some("file.txt".to_string())
        );
        // URL 编码的文件名会被解码
        assert_eq!(
            extract_filename_from_url("https://example.com/my%20file.txt"),
            Some("my file.txt".to_string())
        );
        // 根路径没有文件名
        assert_eq!(extract_filename_from_url("https://example.com/"), None);
        // 空路径
        assert_eq!(extract_filename_from_url("https://example.com"), None);
        // 非法 URL
        assert_eq!(extract_filename_from_url("not a url"), None);
    }

    #[test]
    fn test_extract_filename_from_headers() {
        use reqwest::header::{CONTENT_DISPOSITION, HeaderMap, HeaderValue};

        // 带扩展名的文件名
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=\"report.pdf\""),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("report.pdf".to_string())
        );

        // 无扩展名的文件名不应产生尾点（如 "README."）
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=\"README\""),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("README".to_string())
        );

        // 没有 Content-Disposition 时回退到 URL 提取
        let headers = HeaderMap::new();
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/a/b.zip"),
            Some("b.zip".to_string())
        );
    }

    #[test]
    fn test_extract_filename_prefers_rfc5987_filename_star() {
        use reqwest::header::{CONTENT_DISPOSITION, HeaderMap, HeaderValue};

        // 两者并存：RFC 6266 要求扩展参数 filename*（UTF-8）优先于 ASCII 回退 filename
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static(
                "attachment; filename=\"file.bin\"; filename*=UTF-8''%E4%B8%AD%E6%96%87.txt",
            ),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("中文.txt".to_string())
        );

        // 仅有 filename*（无普通 filename）：crate 已解码并入 filename，结果应一致
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename*=UTF-8''caf%C3%A9.txt"),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("café.txt".to_string())
        );

        // 不支持的字符集应回退到普通 filename
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static(
                "attachment; filename=\"fallback.txt\"; filename*=ISO-8859-1''caf%E9.txt",
            ),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("fallback.txt".to_string())
        );
    }

    #[test]
    fn test_extract_filename_joins_rfc2231_continuation_segments() {
        use reqwest::header::{CONTENT_DISPOSITION, HeaderMap, HeaderValue};

        // 续段 + 普通 filename 并存：续段（扩展参数）应优先于 ASCII 回退名。
        // crate 会解码续段为 `filename*0`/`filename*1`，但跳过合并；此处按序拼接。
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static(
                "attachment; filename=\"fallback.bin\"; \
                 filename*0*=UTF-8''%E4%B8%AD; filename*1*=%E6%96%87.txt",
            ),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("中文.txt".to_string())
        );

        // 无 charset 的普通续段（RFC 2231 §3 字面值续段）：直接按序拼接
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static(
                "attachment; filename=\"f.bin\"; filename*0=fo; filename*1=o.bin",
            ),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("foo.bin".to_string())
        );

        // 仅有续段、无普通 filename：crate 已把续段并入 filename，结果不受影响
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static(
                "attachment; filename*0*=UTF-8''%E4%B8%AD; filename*1*=%E6%96%87.txt",
            ),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("中文.txt".to_string())
        );
    }

    #[test]
    fn test_sanitize_filename_prevents_path_traversal() {
        // 相对路径穿越应被剥离，只保留末段
        assert_eq!(
            sanitize_filename("../../evil.exe"),
            Some("evil.exe".to_string())
        );
        assert_eq!(
            sanitize_filename("..\\..\\evil.exe"),
            Some("evil.exe".to_string())
        );
        // 绝对路径同样只保留末段
        assert_eq!(sanitize_filename("/etc/passwd"), Some("passwd".to_string()));
        // 纯目录引用应拒绝
        assert_eq!(sanitize_filename(".."), None);
        assert_eq!(sanitize_filename("."), None);
        // 正常文件名保持不变
        assert_eq!(
            sanitize_filename("normal.txt"),
            Some("normal.txt".to_string())
        );
        // 控制字符被剔除
        assert_eq!(sanitize_filename("a\u{0}b.txt"), Some("ab.txt".to_string()));
    }

    #[test]
    fn test_extract_filename_from_headers_path_traversal() {
        use reqwest::header::{CONTENT_DISPOSITION, HeaderMap, HeaderValue};

        // 恶意服务器通过 Content-Disposition 注入路径穿越，应被清洗
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=\"../../evil.exe\""),
        );
        assert_eq!(
            extract_filename_from_headers(&headers, "https://example.com/download"),
            Some("evil.exe".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url_path_traversal() {
        // %2f 是编码的斜杠，解码后应剥离目录部分
        assert_eq!(
            extract_filename_from_url("https://example.com/a%2fb.txt"),
            Some("b.txt".to_string())
        );
    }

    #[test]
    fn test_create_chunks_unknown_size() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // 未知大小（0）：单个开放区间块，end 应为 u64::MAX
        let chunks = task.create_chunks(0, false);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, u64::MAX);

        // 不支持范围请求：同样只有一个块
        let chunks = task.create_chunks(1024, false);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].end, 1023);

        // 支持范围请求：分块覆盖整个文件
        let chunks = task.create_chunks(10, true);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, 9);
    }

    #[tokio::test]
    async fn test_update_config() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig {
                max_connections: 2,
                speed_limit: Some(1024 * 1024),
                ..Default::default()
            },
        );

        // 更新配置后读取应生效（全字段比较，借助刚派生的 PartialEq/Eq）
        let expected = DownloadConfig {
            max_connections: 8,
            chunk_size: 1024 * 1024,
            speed_limit: Some(512 * 1024),
            auto_resume: false,
            cookies: Some("session=abc".to_string()),
            connect_timeout: Duration::from_secs(45),
            read_timeout: Some(Duration::from_secs(15)),
            max_retries: 5,
            retry_base_delay: Duration::from_millis(250),
            retry_max_delay: Duration::from_secs(5),
        };
        task.update_config(expected.clone());

        assert_eq!(task.config.read().clone(), expected);
    }

    #[test]
    fn test_max_retries_default_and_backoff() {
        // 硬约束：默认开启 3 次瞬时错误自动重试（0 表示禁用）
        assert_eq!(DownloadConfig::default().max_retries, 3);
        // 退避默认值：基数 500ms、上限 10s
        assert_eq!(
            DownloadConfig::default().retry_base_delay,
            Duration::from_millis(500)
        );
        assert_eq!(
            DownloadConfig::default().retry_max_delay,
            Duration::from_secs(10)
        );

        // 默认参数下：指数退避，首次 500ms，随后逐次翻倍，封顶 10 秒
        let base = DownloadConfig::default().retry_base_delay;
        let max = DownloadConfig::default().retry_max_delay;
        assert_eq!(retry_backoff(1, base, max), Duration::from_millis(500));
        assert_eq!(retry_backoff(2, base, max), Duration::from_millis(1000));
        assert_eq!(retry_backoff(3, base, max), Duration::from_millis(2000));
        assert_eq!(retry_backoff(5, base, max), Duration::from_millis(8000));
        // 第 6 次起达到 10s 上限，不再增长
        assert_eq!(retry_backoff(6, base, max), Duration::from_millis(10_000));
        assert_eq!(retry_backoff(100, base, max), Duration::from_millis(10_000));

        // 自定义基数/上限：100ms 起、封顶 500ms
        let fast = Duration::from_millis(100);
        let cap = Duration::from_millis(500);
        assert_eq!(retry_backoff(1, fast, cap), Duration::from_millis(100));
        assert_eq!(retry_backoff(2, fast, cap), Duration::from_millis(200));
        assert_eq!(retry_backoff(3, fast, cap), Duration::from_millis(400));
        // 100 * 2^3 = 800 > 500，封顶到上限
        assert_eq!(retry_backoff(4, fast, cap), Duration::from_millis(500));
        assert_eq!(retry_backoff(10, fast, cap), Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_get_task_config_reads_current_config() {
        let manager = DownloadManager::new();
        let temp_dir = tempdir().unwrap();
        let initial = DownloadConfig {
            max_connections: 4,
            speed_limit: Some(1024 * 1024),
            ..Default::default()
        };

        let task_id = manager
            .add_task(
                "https://example.com/file.txt".to_string(),
                temp_dir.path().to_path_buf(),
                initial.clone(),
            )
            .await;

        // 读取应返回完整、一致的配置副本（借助 PartialEq/Eq 全字段比较）
        assert_eq!(manager.get_task_config(&task_id).await, Some(initial));

        // 局部修改写回后再读取，应反映新值，验证 getter/setter 配对可用
        let mut updated = manager.get_task_config(&task_id).await.unwrap();
        updated.speed_limit = None;
        manager
            .update_task_config(&task_id, updated.clone())
            .await
            .expect("更新配置失败");
        assert_eq!(manager.get_task_config(&task_id).await, Some(updated));

        // 不存在的任务返回 None
        assert_eq!(manager.get_task_config("nonexistent").await, None);
    }

    #[tokio::test]
    async fn test_rate_limiter_no_limit() {
        // 不限速时 throttle 应立即返回
        let limiter = RateLimiter::new();
        let start = Instant::now();
        limiter.throttle(1024 * 1024, 0).await;
        assert!(start.elapsed().as_millis() < 500);
    }

    #[tokio::test]
    async fn test_rate_limiter_chunk_larger_than_limit() {
        // 单次请求字节数大于限速值时，throttle 必须能正常返回而非死循环
        let limiter = RateLimiter::new();
        let bytes = 64 * 1024; // 64KB 单块
        let limit = 10 * 1024; // 限速 10KB/s
        let start = Instant::now();
        limiter.throttle(bytes, limit).await;
        // 至少需要等待补充 64KB-容量后剩余的令牌，验证未死循环且等待时长合理
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 10,
            "throttle 不应死循环, 实际耗时 {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_rate_limiter_shares_tokens_across_concurrent_chunks() {
        // 多分块并发共享同一个令牌桶。4 个分块各消耗 250KB、限速 1MB/s，
        // 总 1MB 在正确共享令牌桶下应约 1s 完成；若像旧实现那样在等待时把
        // `tokens` 清空，每个分块都会重新等待完整时长，总耗时放大约并发数倍。
        let limiter = Arc::new(RateLimiter::new());
        let start = Instant::now();

        let mut handles = Vec::new();
        for _ in 0..4 {
            let limiter = limiter.clone();
            handles.push(tokio::spawn(async move {
                limiter.throttle(250 * 1024, 1024 * 1024).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let elapsed = start.elapsed();
        // 下界：限速确实在起作用，总吞吐不应明显快于 1MB/s
        assert!(
            elapsed >= Duration::from_millis(800),
            "限速器应把总吞吐限制在约 1MB/s，实际仅耗时 {elapsed:?}"
        );
        // 上界：令牌应在并发分块间共享（而非每块各自等待完整时长导致 ~4s）
        assert!(
            elapsed < Duration::from_secs(3),
            "并发分块应共享令牌桶、总耗时约 1s，实际 {elapsed:?}（疑为 tokens 被逐块清空）"
        );
    }

    #[tokio::test]
    async fn test_restore_state_from_file_restores_downloaded() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // 模拟一次断点续传：两个分块中第一个已下载 300 字节
        let state = DownloadState {
            version: STATE_VERSION,
            url: task.url.clone(),
            total_size: 1000,
            chunks: vec![
                ChunkInfo {
                    index: 0,
                    start: 0,
                    end: 499,
                    downloaded: 300,
                    completed: false,
                },
                ChunkInfo {
                    index: 1,
                    start: 500,
                    end: 999,
                    downloaded: 0,
                    completed: false,
                },
            ],
        };

        task.restore_state_from_file(&state).await;

        let stats = task.get_stats().await;
        assert_eq!(
            stats.downloaded, 300,
            "恢复后累计下载量应等于各分块 downloaded 之和"
        );
        assert_eq!(stats.total_size, 1000);
        assert_eq!(stats.chunks.len(), 2);
        // 恢复时应重置速度/剩余时间，避免短暂显示暂停前的旧值
        assert_eq!(stats.speed, 0.0);
        assert_eq!(stats.remaining, Duration::default());
    }

    #[tokio::test]
    async fn test_restore_state_from_file_normalizes_full_chunk() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // save_state 的快照可能落在「分块写完最后一块字节」与「标记 completed」之间，
        // 此时磁盘上会出现 downloaded == 块长 但 completed == false 的分块。恢复后应
        // 将其归一化为完成，否则会以 `bytes=end+1..end` 的非法区间重复下载导致失败。
        let state = DownloadState {
            version: STATE_VERSION,
            url: task.url.clone(),
            total_size: 1000,
            chunks: vec![
                ChunkInfo {
                    index: 0,
                    start: 0,
                    end: 499,
                    downloaded: 500, // 已满块长（500 字节）
                    completed: false,
                },
                ChunkInfo {
                    index: 1,
                    start: 500,
                    end: 999,
                    downloaded: 100, // 部分下载，应保持未完成
                    completed: false,
                },
            ],
        };

        task.restore_state_from_file(&state).await;

        let stats = task.get_stats().await;
        assert!(stats.chunks[0].completed, "已满块长的分块应归一化为完成");
        assert_eq!(stats.chunks[0].downloaded, 500);
        assert!(!stats.chunks[1].completed, "未满块长的分块应保持未完成");
        assert_eq!(stats.chunks[1].downloaded, 100);
        // 累计下载量应与归一化后的分块一致（500 + 100）
        assert_eq!(stats.downloaded, 600);
    }

    #[tokio::test]
    async fn test_subscribe_progress_receives_updates() {
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        // 订阅后立即可见初始快照
        let mut rx = task.subscribe_progress();
        assert_eq!(
            rx.borrow_and_update().downloaded,
            0,
            "初始快照 downloaded 应为 0"
        );

        // 模拟断点续传恢复，触发进度发布
        let state = DownloadState {
            version: STATE_VERSION,
            url: task.url.clone(),
            total_size: 1000,
            chunks: vec![ChunkInfo {
                index: 0,
                start: 0,
                end: 999,
                downloaded: 300,
                completed: false,
            }],
        };
        task.restore_state_from_file(&state).await;

        // 订阅者应收到更新后的快照
        rx.changed().await.expect("应收到进度更新");
        assert_eq!(rx.borrow().downloaded, 300, "订阅者应收到恢复后的下载量");
    }

    #[tokio::test]
    async fn test_subscribe_progress_closes_on_remove() {
        let manager = DownloadManager::new();
        let temp_dir = tempdir().unwrap();
        let task_id = manager
            .add_task(
                "https://example.com/file.txt".to_string(),
                temp_dir.path().to_path_buf(),
                DownloadConfig::default(),
            )
            .await;

        let mut rx = manager
            .subscribe_progress(&task_id)
            .await
            .expect("任务应存在");
        // 消费初始快照，使 changed() 等待后续更新或通道关闭
        let _ = rx.borrow_and_update();

        assert!(manager.remove_task(&task_id).await, "应成功移除任务");

        // watch 的 Sender 随任务被丢弃后，Receiver.changed() 返回 Err（通道关闭），
        // 订阅者据此结束监听循环（resume_download 示例依赖该行为来退出）
        assert!(rx.changed().await.is_err(), "移除任务后进度通道应关闭");
    }

    #[tokio::test]
    async fn test_filename_getter() {
        let manager = DownloadManager::new();
        let temp_dir = tempdir().unwrap();

        let task_id = manager
            .add_task(
                "https://example.com/file.txt".to_string(),
                temp_dir.path().to_path_buf(),
                DownloadConfig::default(),
            )
            .await;

        // 下载开始前文件名为 None
        assert_eq!(manager.get_task_filename(&task_id).await, None);

        // 模拟下载开始后从响应解析出文件名，getter 应返回该名称
        let task = manager.tasks.lock().await.get(&task_id).unwrap().clone();
        *task.filename.lock().await = Some("report.pdf".to_string());
        assert_eq!(
            manager.get_task_filename(&task_id).await,
            Some("report.pdf".to_string())
        );
    }

    #[tokio::test]
    async fn test_setup_filename_falls_back_to_task_id() {
        // URL 无路径文件名、响应头也无 Content-Disposition 时，setup_filename 应
        // 回退到任务 ID，而非留下 None——否则 save_state 的 is_none() 守卫会让
        // 此类下载永久不写状态文件，断点续传静默失效。
        let task = DownloadTask::new(
            "https://example.com/".to_string(),
            PathBuf::from("."),
            DownloadConfig::default(),
        );

        let headers = HeaderMap::new();
        task.setup_filename(&headers, "https://example.com/").await;

        let resolved = task.filename.lock().await.clone();
        assert_eq!(resolved.as_deref(), Some(task.id.to_string().as_str()));
    }

    #[test]
    fn test_create_chunks_exact_multiple() {
        // 注意：configured chunk_size 会被 clamp 到 [100KB, 10MB]，这里取最小值
        const CHUNK: u64 = 100 * 1024;
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            PathBuf::from("."),
            DownloadConfig {
                chunk_size: CHUNK,
                ..Default::default()
            },
        );

        // 恰好 2 块，边界精确无重叠、无遗漏
        let chunks = task.create_chunks(CHUNK * 2, true);
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks[0],
            ChunkInfo {
                index: 0,
                start: 0,
                end: CHUNK - 1,
                downloaded: 0,
                completed: false
            }
        );
        assert_eq!(
            chunks[1],
            ChunkInfo {
                index: 1,
                start: CHUNK,
                end: CHUNK * 2 - 1,
                downloaded: 0,
                completed: false
            }
        );

        // 大小为 1 时应恰为单块 [0, 0]
        let chunks = task.create_chunks(1, true);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, 0);
    }

    #[test]
    fn test_sanitize_filename_trims_and_rejects_empty() {
        // 首尾空白应被剔除
        assert_eq!(
            sanitize_filename("  file.txt  "),
            Some("file.txt".to_string())
        );
        // 剔除控制字符后为空应拒绝
        assert_eq!(sanitize_filename("\u{0}\u{1}"), None);
    }

    #[test]
    fn test_sanitize_filename_strips_trailing_dots_and_reserved_names() {
        // 末尾的点应被剔除（Windows 会静默截断，否则后续缀名无法对应）
        assert_eq!(sanitize_filename("file.txt."), Some("file.txt".to_string()));
        // 纯点应被拒绝
        assert_eq!(sanitize_filename("..."), None);
        // Windows 保留设备名应加下划线前缀规避
        assert_eq!(sanitize_filename("CON.txt"), Some("_CON.txt".to_string()));
        assert_eq!(sanitize_filename("nul"), Some("_nul".to_string()));
        assert_eq!(sanitize_filename("COM1"), Some("_COM1".to_string()));
        // 正常文件名不受影响
        assert_eq!(
            sanitize_filename("report.pdf"),
            Some("report.pdf".to_string())
        );
    }

    #[test]
    fn test_sanitize_filename_strips_windows_invalid_chars() {
        // Windows 禁止的字符（< > : " | ? *）应被剔除，避免创建/重命名失败
        assert_eq!(sanitize_filename("a<b>c.txt"), Some("abc.txt".to_string()));
        assert_eq!(
            sanitize_filename("fi:le*.txt"),
            Some("file.txt".to_string())
        );
        assert_eq!(
            sanitize_filename("a\"b|c?d.txt"),
            Some("abcd.txt".to_string())
        );
        // 无效字符被剔除后仍需走保留设备名检测（C:on -> Con -> _Con）
        assert_eq!(sanitize_filename("C:on.txt"), Some("_Con.txt".to_string()));
        // 全为无效字符时应被拒绝（不为空文件生成非法名）
        assert_eq!(sanitize_filename("<:*?>"), None);
    }

    #[test]
    fn test_sanitize_filename_truncates_overlong_name() {
        // 超长 ASCII 名（300 字节）应被截断到 MAX_FILENAME_BYTES，为状态/临时文件后缀预留空间
        let long = "a".repeat(300);
        let truncated = sanitize_filename(&long).expect("超长名应被截断而非拒绝");
        assert_eq!(truncated.len(), MAX_FILENAME_BYTES);
        assert_eq!(truncated, "a".repeat(MAX_FILENAME_BYTES));

        // 多字节字符（é 为 2 字节）跨越 MAX_FILENAME_BYTES 边界时应退回到完整字符
        // 边界，不产生非法 UTF-8：200 个 é = 400 字节，截断后应为边界前整数个 é。
        let long_multibyte = "é".repeat(200);
        let truncated = sanitize_filename(&long_multibyte).expect("超长多字节名应被截断");
        assert!(truncated.len() <= MAX_FILENAME_BYTES);
        assert_eq!(
            truncated,
            "é".repeat(MAX_FILENAME_BYTES / 2),
            "应在字符边界处截断且内容有效"
        );

        // 截断后恰好落在点之后（`foo.bar` 截成 `foo.`）时应剔除末尾点
        let dotted = format!("{}.{}", "x".repeat(MAX_FILENAME_BYTES - 1), "y");
        let sanitized = sanitize_filename(&dotted).expect("截断到点后应回退并保留有效名");
        assert!(!sanitized.ends_with('.'), "截断尾部不应残留点: {sanitized}");

        // 正常长度名不受影响
        assert_eq!(
            sanitize_filename("report.pdf"),
            Some("report.pdf".to_string())
        );
    }

    #[test]
    fn test_state_save_interval_is_adaptive() {
        const MB: u64 = 1024 * 1024;

        // 小文件（≤256MB）保持 1MB 精确保存
        assert_eq!(state_save_interval(0), MB);
        assert_eq!(state_save_interval(MB), MB);
        assert_eq!(state_save_interval(256 * MB), MB);

        // 大文件逐步增大间隔，使总保存次数控制在 ~256 次
        assert_eq!(state_save_interval(512 * MB), 2 * MB); // 512MB / 256 = 2MB
        assert_eq!(state_save_interval(1024 * MB), 4 * MB); // 1GB / 256 = 4MB
        assert_eq!(state_save_interval(8 * 1024 * MB), 32 * MB); // 8GB / 256 = 32MB

        // 超上限时收敛到 64MB，避免保存粒度无限增大
        assert_eq!(state_save_interval(64 * 1024 * MB), 64 * MB);
    }

    #[tokio::test]
    async fn test_next_chunk_times_out_on_stalled_connection() {
        // 永不完成的 future + 极短超时，应返回读取超时错误
        let result = next_chunk(
            Some(Duration::from_millis(5)),
            std::future::pending::<Result<Option<Vec<u8>>, reqwest::Error>>(),
        )
        .await;
        assert!(matches!(result, Err(DownloadError::ReadTimeout)));
    }

    #[tokio::test]
    async fn test_next_chunk_returns_ready_value() {
        let result = next_chunk(Some(Duration::from_secs(1)), async {
            Ok::<Option<Vec<u8>>, reqwest::Error>(Some(vec![1, 2, 3]))
        })
        .await;
        assert_eq!(result.unwrap(), Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn test_next_chunk_no_timeout_returns_value() {
        let result = next_chunk(None, async {
            Ok::<Option<Vec<u8>>, reqwest::Error>(Some(vec![9]))
        })
        .await;
        assert_eq!(result.unwrap(), Some(vec![9]));
    }

    #[tokio::test]
    async fn test_save_and_load_state_with_version() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );

        // 设置文件名与统计信息，模拟下载进行中
        *task.filename.lock().await = Some("file.txt".to_string());
        {
            let mut stats = task.stats.lock().await;
            stats.total_size = 500;
            stats.chunks = vec![ChunkInfo {
                index: 0,
                start: 0,
                end: 499,
                downloaded: 100,
                completed: false,
            }];
        }

        task.save_state().await.expect("保存状态应成功");

        let state_path = temp_dir.path().join("file.txt.download_state");
        let raw = tokio::fs::read_to_string(&state_path)
            .await
            .expect("读取状态文件失败");
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        // 写入的状态应包含当前版本号
        assert_eq!(value["version"].as_u64(), Some(STATE_VERSION as u64));

        // 版本匹配时应能正常加载
        assert!(task.load_state().await.is_ok());

        // 版本不匹配（模拟旧/未来格式）时应拒绝加载
        let mut mismatched = value;
        mismatched["version"] = serde_json::json!(STATE_VERSION + 1);
        tokio::fs::write(&state_path, mismatched.to_string())
            .await
            .unwrap();
        assert!(matches!(
            task.load_state().await,
            Err(DownloadError::InvalidStateFile)
        ));
    }

    #[test]
    fn test_is_valid_chunks_rejects_corruption() {
        // 合法：两块完整覆盖 [0, 999]，其中一块有部分进度
        let valid = vec![
            ChunkInfo {
                index: 0,
                start: 0,
                end: 499,
                downloaded: 300,
                completed: false,
            },
            ChunkInfo {
                index: 1,
                start: 500,
                end: 999,
                downloaded: 0,
                completed: false,
            },
        ];
        assert!(is_valid_chunks(&valid, 1000));

        // 未知大小/空文件：允许单一开放区间分块
        let open = vec![ChunkInfo {
            index: 0,
            start: 0,
            end: u64::MAX,
            downloaded: 42,
            completed: false,
        }];
        assert!(is_valid_chunks(&open, 0));

        // 空分块列表：无论大小均非法
        assert!(!is_valid_chunks(&[], 1000));
        assert!(!is_valid_chunks(&[], 0));

        // 索引缺号（0, 2 而非 0, 1）
        let gap_index = vec![
            ChunkInfo {
                index: 0,
                start: 0,
                end: 499,
                downloaded: 0,
                completed: false,
            },
            ChunkInfo {
                index: 2,
                start: 500,
                end: 999,
                downloaded: 0,
                completed: false,
            },
        ];
        assert!(!is_valid_chunks(&gap_index, 1000));

        // 首块起始偏移不为 0
        let bad_start = vec![ChunkInfo {
            index: 0,
            start: 100,
            end: 499,
            downloaded: 0,
            completed: false,
        }];
        assert!(!is_valid_chunks(&bad_start, 1000));

        // 分块重叠
        let overlap = vec![
            ChunkInfo {
                index: 0,
                start: 0,
                end: 499,
                downloaded: 0,
                completed: false,
            },
            ChunkInfo {
                index: 1,
                start: 400,
                end: 999,
                downloaded: 0,
                completed: false,
            },
        ];
        assert!(!is_valid_chunks(&overlap, 1000));

        // 分块间留洞
        let hole = vec![
            ChunkInfo {
                index: 0,
                start: 0,
                end: 499,
                downloaded: 0,
                completed: false,
            },
            ChunkInfo {
                index: 1,
                start: 600,
                end: 999,
                downloaded: 0,
                completed: false,
            },
        ];
        assert!(!is_valid_chunks(&hole, 1000));

        // 覆盖不完整：最后一块结束偏移 != 总大小 - 1
        let incomplete = vec![ChunkInfo {
            index: 0,
            start: 0,
            end: 499,
            downloaded: 0,
            completed: false,
        }];
        assert!(!is_valid_chunks(&incomplete, 1000));

        // downloaded 超过块长（块长 500，downloaded 501 越界）
        let over_download = vec![
            ChunkInfo {
                index: 0,
                start: 0,
                end: 499,
                downloaded: 501,
                completed: false,
            },
            ChunkInfo {
                index: 1,
                start: 500,
                end: 999,
                downloaded: 0,
                completed: false,
            },
        ];
        assert!(!is_valid_chunks(&over_download, 1000));

        // completed 但未下载满块长（标志与数据自相矛盾）
        let bad_completed = vec![
            ChunkInfo {
                index: 0,
                start: 0,
                end: 499,
                downloaded: 300,
                completed: true,
            },
            ChunkInfo {
                index: 1,
                start: 500,
                end: 999,
                downloaded: 0,
                completed: false,
            },
        ];
        assert!(!is_valid_chunks(&bad_completed, 1000));

        // 已知大小下出现开放区间分块
        let open_in_known = vec![ChunkInfo {
            index: 0,
            start: 0,
            end: u64::MAX,
            downloaded: 0,
            completed: false,
        }];
        assert!(!is_valid_chunks(&open_in_known, 1000));

        // 未知大小但形状错误（end 不是 u64::MAX）
        let wrong_open = vec![ChunkInfo {
            index: 0,
            start: 0,
            end: 100,
            downloaded: 0,
            completed: false,
        }];
        assert!(!is_valid_chunks(&wrong_open, 0));
    }

    #[tokio::test]
    async fn test_load_state_rejects_corrupted_chunks() {
        let temp_dir = tempdir().unwrap();
        let task = DownloadTask::new(
            "https://example.com/file.txt".to_string(),
            temp_dir.path().to_path_buf(),
            DownloadConfig::default(),
        );
        *task.filename.lock().await = Some("file.txt".to_string());

        let valid = DownloadState {
            version: STATE_VERSION,
            url: task.url.clone(),
            total_size: 1000,
            chunks: vec![
                ChunkInfo {
                    index: 0,
                    start: 0,
                    end: 499,
                    downloaded: 100,
                    completed: false,
                },
                ChunkInfo {
                    index: 1,
                    start: 500,
                    end: 999,
                    downloaded: 0,
                    completed: false,
                },
            ],
        };
        let state_path = task.get_state_file_path().await;
        tokio::fs::write(&state_path, serde_json::to_string(&valid).unwrap())
            .await
            .unwrap();
        assert!(
            task.load_state().await.is_ok(),
            "结构合法的状态文件应能加载"
        );

        // 篡改为重叠分块后应被拒绝，避免续传时错位写坏文件
        let tampered = DownloadState {
            chunks: vec![
                ChunkInfo {
                    index: 0,
                    start: 0,
                    end: 499,
                    downloaded: 100,
                    completed: false,
                },
                ChunkInfo {
                    index: 1,
                    start: 400,
                    end: 999,
                    downloaded: 0,
                    completed: false,
                },
            ],
            ..valid
        };
        tokio::fs::write(&state_path, serde_json::to_string(&tampered).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            task.load_state().await,
            Err(DownloadError::InvalidStateFile)
        ));
    }

    #[test]
    fn test_speed_tracker_reset_clears_last_speed() {
        let mut tracker = SpeedTracker::new();

        // 推进时间并产生下载进度，让 update 进入 >=1s 分支、得到非零速度
        std::thread::sleep(Duration::from_millis(1100));
        let speed = tracker.update(1024 * 1024);
        // 速度 = delta_bytes / elapsed，1MB 在约 1.1s 内下载，量级应为 ~1e6 bytes/s。
        // 仅断言 `> 0` 会漏掉「分子分母颠倒 / delta 用错」这类把速度算成极小值或
        // 极大值的 bug，故再校验量级下限。
        assert!(
            speed > 1000.0,
            "滑动窗口速度量级应接近 1MB/s（~1e6），实际 {speed}"
        );

        // 窗口未再次跨越 1s 时应沿用上一次算出的速度，避免短窗口内速度抖动
        assert_eq!(tracker.update(2048 * 1024), speed);

        // reset 后首个短窗口内的 update 应返回 0，而非沿用旧速度
        tracker.reset(0);
        assert_eq!(tracker.update(0), 0.0, "reset 后不应沿用旧速度");
    }

    #[test]
    fn test_default_user_agent_is_non_empty() {
        // 无论是否设置 USER_AGENT 环境变量，默认 UA 都不应为空，否则请求会缺失
        // User-Agent 头，部分服务器（CDN/反爬）会直接拒绝（reqwest 不会自动附带 UA）。
        let ua = default_user_agent();
        assert!(!ua.is_empty(), "默认 User-Agent 不应为空");
    }

    #[test]
    fn test_connect_timeout_default_and_plumb() {
        // 硬约束：默认连接超时 30 秒，避免目标不可达时无限挂起
        assert_eq!(
            DownloadConfig::default().connect_timeout,
            Duration::from_secs(30),
            "默认连接超时应为 30 秒"
        );

        // 自定义连接超时经 build_client 透传后不应报错（reqwest 无公开 getter，
        // 这里仅校验建连配置被接受且能成功构建客户端）
        let custom = DownloadConfig {
            connect_timeout: Duration::from_secs(7),
            ..Default::default()
        };
        let task = DownloadTask::new(
            "https://example.com/file.bin".to_string(),
            PathBuf::from("."),
            custom,
        );
        task.build_client()
            .expect("自定义连接超时配置下构建客户端应成功");
    }
}
