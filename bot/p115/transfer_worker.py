"""
传输 Worker — 负责 115 离线任务轮询、文件下载、rclone 上传

适配 Pyrogram: 使用 bot.send_message / bot.edit_message_text 替代 aiogram
"""
import asyncio
import logging
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Awaitable

from .database import Database
from .p115_manager import P115Manager
from .rclone_runner import RcloneRunner
from . import emby_notifier

logger = logging.getLogger(__name__)

# 通知回调类型：async def callback(chat_id, text)
NotifyCallback = Callable[[int, str], Awaitable[None]]


class ProgressThrottler:
    """进度更新节流器，适配 Pyrogram"""

    def __init__(self, bot, min_interval: int = 3):
        self.bot = bot
        self.min_interval = min_interval
        self.last_updates: dict[int, float] = {}  # {task_id: last_time}

    async def update(self, chat_id: int, message_id: int, task_id: int, text: str):
        now = time.time()
        if task_id in self.last_updates and now - self.last_updates[task_id] < self.min_interval:
            return

        try:
            await self.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
            self.last_updates[task_id] = now
        except Exception as e:
            err_str = str(e).lower()
            if "not modified" in err_str or "message_not_modified" in err_str:
                return
            logger.warning(f"更新进度失败: {e}")


class TransferWorker:
    def __init__(self, db: Database, p115: P115Manager, rclone: RcloneRunner,
                 stall_timeout_minutes: int = 30):
        self.db = db
        self.p115 = p115
        self.rclone = rclone
        self.is_running = False
        self.throttler = None
        self.temp_dir = Path("data/downloads")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.active_progress: dict[int, str] = {}
        # 用于通知管理员，由外部设置，避免循环导入
        self._notify_admin: NotifyCallback | None = None
        self._admin_id: int = 0
        # 0% 停滞超时（分钟）
        self.STALL_TIMEOUT_MINUTES = stall_timeout_minutes

    def set_throttler(self, throttler):
        self.throttler = throttler

    def set_admin_notifier(self, admin_id: int, callback: NotifyCallback):
        """设置管理员通知回调，避免循环导入"""
        self._admin_id = admin_id
        self._notify_admin = callback

    async def _notify(self, text: str):
        """向管理员发送通知"""
        if self._notify_admin and self._admin_id:
            try:
                await self._notify_admin(self._admin_id, text)
            except Exception as e:
                logger.warning(f"发送管理员通知失败: {e}")

    async def _update_progress(self, task, text: str):
        self.active_progress[task['id']] = text
        if self.throttler and task['msg_id']:
            await self.throttler.update(task['user_id'], task['msg_id'], task['id'], text)

    def _cleanup_progress(self, task_id: int):
        """清理已完成任务的进度记录，防止内存泄漏"""
        self.active_progress.pop(task_id, None)
        if self.throttler:
            self.throttler.last_updates.pop(task_id, None)

    async def start(self):
        self.is_running = True
        self._last_cleanup_time = time.time()
        logger.info("115 传输 Worker 已启动...")

        # 启动时恢复逻辑
        try:
            await self._recover_interrupted_tasks()
        except Exception as e:
            logger.error(f"恢复任务时出错: {e}")

        while self.is_running:
            try:
                await self._process_cycle()
                # 每小时清理一次孤立临时目录
                if time.time() - self._last_cleanup_time > 3600:
                    await self._periodic_cleanup()
                    self._last_cleanup_time = time.time()
            except Exception as e:
                if self._handle_fatal_error(e):
                    break
                logger.error(f"Worker 核心循环出错: {e}", exc_info=True)
            await asyncio.sleep(10)

    def _handle_fatal_error(self, e: Exception) -> bool:
        """处理致命错误，返回 True 表示应停止 Worker"""
        error_str = str(e)

        if "405" in error_str and "Method Not Allowed" in error_str:
            logger.error("🚨 检测到 115 WAF 拦截 (405)，已暂停队列。")
            self.is_running = False
            asyncio.create_task(self._notify(
                "🚨 **警报：115 触发了 WAF 拦截 (405错误)**\n\n"
                "Token 可能已失效，传输队列已自动暂停。\n\n"
                "👉 请使用 /p115_token 重新设置 Token，然后发送 /p115_resume 恢复队列。"
            ))
            return True

        if "403" in error_str or "quota" in error_str.lower():
            logger.error(f"检测到 GDrive 配额限制，暂停队列: {e}")
            self.is_running = False
            asyncio.create_task(self._notify(
                "⚠️ **GDrive 配额耗尽**，队列已暂停。请检查配额。"
            ))
            return True

        return False

    async def _recover_interrupted_tasks(self):
        """系统启动时恢复中断的传输任务"""
        tasks = await self.db.get_pending_tasks()
        for task in tasks:
            if task['status'] == 'TRANSFERRING':
                logger.info(f"发现中断任务 {task['id']}，准备恢复...")
                # 清理已标记为上传成功的本地残余文件
                temp_task_dir = self.temp_dir / f"task_{task['id']}"
                if temp_task_dir.exists():
                    task_files = await self.db.get_task_files(task['id'])
                    for f in task_files:
                        if f['status'] == 'UPLOADED':
                            local_path = temp_task_dir / f['file_path']
                            if local_path.exists():
                                local_path.unlink()
            elif task['status'] == 'PAUSED':
                logger.info(f"任务 {task['id']} 处于暂停状态，等待手动恢复或磁盘空间。")

    async def _process_cycle(self):
        """一个处理周期：先批量检查离线状态，再处理一个传输任务"""
        tasks = await self.db.get_pending_tasks()
        if not tasks:
            await asyncio.sleep(2)
            return

        # --- 阶段 1: 一次性获取离线列表，批量检查所有离线中的任务 ---
        offline_tasks = [t for t in tasks if t['status'] in ('DOWNLOADING_115', 'OFFLINING_115')]
        if offline_tasks:
            try:
                offline_list = await self.p115.get_offline_list()
            except Exception as e:
                logger.error(f"获取离线列表失败: {e}")
                offline_list = None
            for task in offline_tasks:
                await self._check_offline_task(task, offline_list=offline_list)

        # --- 阶段 2: 找到第一个待传输的任务处理 ---
        # 重新获取任务列表（状态可能已被阶段 1 更新）
        tasks = await self.db.get_pending_tasks()
        for task in tasks:
            if task['status'] in ('WAITING_LOCAL_TRANSFER', 'TRANSFERRING'):
                await self._transfer_task(task)
                break  # 串行：一次只传一个

    async def _check_offline_task(self, task, offline_list: list[dict] | None = None):
        """检查单个 115 离线任务的进度"""
        task_id = task['id']
        task_name = task['task_name']

        try:
            p115_status = await self.p115.check_offline_status(task['task_hash'], offline_list=offline_list)
        except Exception as e:
            logger.error(f"检查离线状态失败: {e}")
            return

        if p115_status is None:
            # 任务不在 115 离线列表中，检查是否超时
            await self._check_timeout(task, percent=0)
            return

        # 更新任务名称（如果之前是占位符）
        if task_name == "解析中..." and p115_status.get('name'):
            task_name = p115_status['name']
            await self.db.update_task(task_id, task_name=task_name)

        percent_done = p115_status.get('percentDone', 0)
        status_text = p115_status.get('status_text', '')
        # 115 API 的 status 字段：-1 表示失败
        cloud_status = p115_status.get('status', 0)

        # 检测 115 云端报告的下载失败
        _FAIL_KEYWORDS = ('失败', '错误', 'fail', 'error')
        is_cloud_failed = (
            cloud_status == -1
            or any(kw in status_text for kw in _FAIL_KEYWORDS)
        )
        if is_cloud_failed:
            error_msg = f"115 离线下载失败：{status_text or '未知原因'}"
            logger.warning(f"任务 {task_id} 云端失败: {error_msg}")
            await self.db.update_task(task_id, status='FAILED', error_msg=error_msg)
            await self._update_progress(task, f"❌ {task_name}\n{error_msg}")
            self._cleanup_progress(task_id)
            return

        if percent_done == 100:
            logger.info(f"任务 {task_id} ({task_name}) 115 离线完成")
            await self._update_progress(task, f"✅ 115 离线已完成：{task_name}\n正在准备下载至 VPS...")

            # 获取文件列表并入库
            files = await self.p115.get_task_files(task['task_hash'], task_name)
            if not files:
                await self.db.update_task(task_id, status='FAILED', error_msg="115 离线完成但未找到文件")
                self._cleanup_progress(task_id)
                return

            # 检查文件总大小是否超过限额
            total_size = sum(f.get('size', 0) for f in files)
            # 从 config 获取默认限额
            from bot import p115_config
            max_file_size = p115_config.max_file_size_gb * (1024 ** 3)
            if total_size > max_file_size:
                max_gb = p115_config.max_file_size_gb
                actual_gb = total_size / (1024 ** 3)
                error_msg = f"文件总大小 {actual_gb:.1f}GB 超过限额 {max_gb}GB"
                logger.warning(f"任务 {task_id} {error_msg}")
                await self.db.update_task(task_id, status='FAILED', error_msg=error_msg)
                await self._update_progress(task, f"❌ {error_msg}")
                self._cleanup_progress(task_id)
                return

            await self.db.add_task_files_batch(task_id, files)
            await self.db.update_task(task_id, status='WAITING_LOCAL_TRANSFER')
            return  # 完成，不再检查超时

        # 尚未完成，检查是否超时
        await self._check_timeout(task, percent=percent_done)

    async def _check_timeout(self, task, percent: int = 0):
        """检查任务超时：
        - 0% 停滞超过 STALL_TIMEOUT_MINUTES 分钟 → 自动失败
        - 通用超时：超过 24 小时仍未完成 → 自动失败
        """
        task_id = task['id']
        task_name = task['task_name']
        try:
            create_time = datetime.strptime(task['created_at'], '%Y-%m-%d %H:%M:%S')
            elapsed = datetime.utcnow() - create_time

            # 1. 0% 停滞超时
            if percent == 0 and elapsed > timedelta(minutes=self.STALL_TIMEOUT_MINUTES):
                mins = self.STALL_TIMEOUT_MINUTES
                logger.warning(f"任务 {task_id} 停滞超时：0% 超过 {mins} 分钟")
                error_msg = f"115 离线停滞超时：进度 0% 超过 {mins} 分钟，可能是无效资源"
                await self.db.update_task(task_id, status='FAILED', error_msg=error_msg)
                await self._update_progress(task, f"❌ {task_name}\n{error_msg}")
                self._cleanup_progress(task_id)
                return

            # 2. 通用 24h 超时
            if elapsed > timedelta(hours=24):
                logger.error(f"任务 {task_id} 115 离线超时")
                await self.db.update_task(task_id, status='FAILED', error_msg="115 离线超时 (>24h)")
                await self._update_progress(task, f"❌ 任务已超时：{task_name} (115 离线超过 24 小时)")
                self._cleanup_progress(task_id)
        except Exception as e:
            logger.error(f"检查超时失败: {e}")

    async def _transfer_task(self, task):
        """执行单个任务的下载+上传传输"""
        task_id = task['id']
        await self.db.update_task(task_id, status='TRANSFERRING')
        temp_task_dir = self.temp_dir / f"task_{task_id}"

        try:
            task_files = await self.db.get_task_files(task_id)
            temp_task_dir.mkdir(parents=True, exist_ok=True)

            for file_item in task_files:
                if file_item['status'] == 'UPLOADED':
                    continue

                # 1. 空间预检
                if not self._check_disk_space(file_item['file_size']):
                    logger.warning(f"磁盘空间不足，任务 {task_id} 暂停")
                    await self.db.update_task(task_id, status='PAUSED')
                    return

                local_path = temp_task_dir / file_item['file_path']
                local_path.parent.mkdir(parents=True, exist_ok=True)

                # 2. 下载 (含重试)
                await self._download_file(task, file_item, local_path)

                # 3. 上传 (含重试)
                await self._upload_file(task, file_item, local_path)

            # 所有文件处理完毕
            await self.db.update_task(task_id, status='DONE')
            logger.info(f"任务 {task_id} 全部处理完成")
            self._cleanup_progress(task_id)

            # 通知 Emby 扫描新上传的任务文件夹
            if task['task_name']:
                await emby_notifier.notify_media_updated(task['task_name'])

            # 清理任务临时目录
            if temp_task_dir.exists():
                shutil.rmtree(temp_task_dir)

        except Exception as e:
            logger.error(f"任务 {task_id} 传输过程中出错: {e}")
            await self.db.update_task(task_id, status='FAILED', error_msg=str(e))
            self._cleanup_progress(task_id)
            # 清理失败任务的临时文件，防止磁盘爆满
            if temp_task_dir.exists():
                shutil.rmtree(temp_task_dir, ignore_errors=True)
                logger.info(f"已清理失败任务的临时目录: {temp_task_dir}")

    async def _download_file(self, task, file_item, local_path: Path):
        """下载单个文件，最多重试 3 次"""
        for attempt in range(3):
            try:
                logger.info(f"正在下载文件: {file_item['file_path']} (尝试 {attempt + 1}/3)")
                await self._update_progress(
                    task, 
                    f"📥 准备下载至 VPS：{file_item['file_path']} (尝试 {attempt + 1}/3)..."
                )

                downloaded_bytes = 0
                if local_path.exists():
                    downloaded_bytes = local_path.stat().st_size

                start_time = time.time()
                last_update_time = start_time
                last_update_bytes = downloaded_bytes
                total_size = file_item['file_size']

                async def dl_progress_cb(chunk_size):
                    nonlocal downloaded_bytes, last_update_time, last_update_bytes
                    downloaded_bytes += chunk_size
                    now = time.time()
                    if now - last_update_time >= 2:
                        speed = (downloaded_bytes - last_update_bytes) / (now - last_update_time)
                        last_update_time = now
                        last_update_bytes = downloaded_bytes

                        percent = (downloaded_bytes / total_size * 100) if total_size > 0 else 0

                        if speed > 1024 * 1024:
                            speed_str = f"{speed / 1024 / 1024:.1f} MB/s"
                        else:
                            speed_str = f"{speed / 1024:.1f} KB/s"

                        if speed > 0 and total_size > 0:
                            eta_sec = (total_size - downloaded_bytes) / speed
                            eta_str = f"{int(eta_sec // 60)}m{int(eta_sec % 60)}s"
                        else:
                            eta_str = "-"

                        text = (
                            f"📥 正在下载至 VPS：{file_item['file_path']}\n"
                            f"进度：{percent:.1f}% | 速度：{speed_str} | ETA：{eta_str}"
                        )
                        await self._update_progress(task, text)

                await self.p115.download_file_to_vps(
                    file_item['pickcode'],
                    str(local_path),
                    progress_callback=dl_progress_cb,
                )
                await self.db.update_task_file(file_item['id'], status='DOWNLOADED')
                return  # 成功

            except Exception as e:
                logger.warning(f"下载失败 ({attempt + 1}/3): {e}", exc_info=True)
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

        raise Exception(f"文件 {file_item['file_path']} 下载多次失败")

    async def _upload_file(self, task, file_item, local_path: Path):
        """上传单个文件，最多重试 2 次

        远程路径格式: <remote>:<target_path>/<task_name>/<filename>
        """
        task_name = task['task_name'] or f"task_{task['id']}"
        remote_path = f"{task_name}/{file_item['file_path']}"

        async def progress_cb(info):
            text = (
                f"🚀 正在上传：{file_item['file_path']}\n"
                f"进度：{info['percent']}% | 速度：{info['speed']} | ETA：{info['eta']}"
            )
            await self._update_progress(task, text)

        for attempt in range(2):
            try:
                logger.info(f"正在上传文件: {remote_path} (尝试 {attempt + 1}/2)")
                await self._update_progress(
                    task,
                    f"🚀 准备上传至 Google Drive：{file_item['file_path']} (尝试 {attempt + 1}/2)..."
                )
                
                await self.rclone.copy_to_remote(
                    str(local_path),
                    remote_path=remote_path,
                    progress_callback=progress_cb,
                )
                await self.db.update_task_file(file_item['id'], status='UPLOADED')

                # copy 成功后手动删除本地文件
                if local_path.exists():
                    local_path.unlink()
                    logger.info(f"已删除本地文件: {local_path}")

                return  # 成功

            except Exception as e:
                logger.warning(f"上传失败 ({attempt + 1}/2): {e}")
                if attempt < 1:
                    await asyncio.sleep(5)

        raise Exception(f"文件 {file_item['file_path']} 上传多次失败")

    async def _periodic_cleanup(self):
        """定期清理孤立的临时目录（已完成/失败/取消的任务残留）"""
        try:
            if not self.temp_dir.exists():
                return
            for dir_path in self.temp_dir.iterdir():
                if dir_path.is_dir() and dir_path.name.startswith("task_"):
                    try:
                        task_id = int(dir_path.name.split("_")[1])
                    except (ValueError, IndexError):
                        continue
                    task = await self.db.get_task(task_id)
                    if task and task['status'] in ('DONE', 'FAILED', 'CANCELED'):
                        shutil.rmtree(dir_path, ignore_errors=True)
                        logger.info(f"清理孤立临时目录: {dir_path}")
        except Exception as e:
            logger.warning(f"定期清理出错: {e}")

    def _check_disk_space(self, required_size_bytes: int) -> bool:
        """检查 VPS 剩余空间是否足够 (保留 2GB 缓冲区)"""
        if required_size_bytes <= 0:
            return True
        total, used, free = shutil.disk_usage(self.temp_dir)
        buffer = max(2 * 1024 ** 3, required_size_bytes * 1.2)
        return free > buffer
