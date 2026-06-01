"""
115 OpenAPI 管理器 — 负责 Token 管理、离线任务操作、文件下载
"""
import asyncio
import logging
import os
import re
import time
import aiofiles
import aiohttp
from p115client import P115OpenClient, tool
from .database import Database
from . import http_session

logger = logging.getLogger(__name__)


class P115Manager:
    def __init__(self, db: Database):
        self.db = db
        self.client: P115OpenClient | None = None
        self._auth_credential: str | None = None
        self._last_refresh_time: float = 0.0

    async def _get_client(self) -> P115OpenClient:
        """延迟加载并获取 115 OpenClient 实例"""
        access_token = await self.db.get_setting("p115_access_token")
        refresh_token = await self.db.get_setting("p115_refresh_token")
        
        if access_token: access_token = access_token.strip()
        if refresh_token: refresh_token = refresh_token.strip()

        if not access_token or not refresh_token:
            logger.error("115_manager: 数据库中未找到完整的 Token (access_token 或 refresh_token 为空)")
            raise ValueError("115 未配置 Token，请使用 /p115_token 进行认证。")

        # 详细日志：打印截断的 Token 和长度，以证明 Token 读取正常
        logger.info(f"115_manager: 从数据库读取到 Token。Access: [{access_token[:6]}...{access_token[-6:]}] (长度: {len(access_token)}), Refresh: [{refresh_token[:6]}...{refresh_token[-6:]}] (长度: {len(refresh_token)})")

        now = time.time()

        # 如果客户端已存在
        if self.client is not None:
            # 检查是否由于 /p115_token 覆盖了数据库导致 token 变化
            cache_key = f"{access_token}|{refresh_token}"
            if self._auth_credential != cache_key:
                logger.info("115_manager: 检测到数据库 Token 已改变 (用户重新设置了Token)，正在重置内部客户端状态...")
                self.client = None
            else:
                # 检查是否需要自动刷新 (超过 3600 秒)
                if (now - self._last_refresh_time) > 3600:
                    try:
                        logger.info("Token 已使用超过 1 小时，正在自动刷新...")
                        await self.client.refresh_access_token(async_=True)
                        # refresh_access_token 内部已更新 self.client.access_token / refresh_token
                        await self._save_client_tokens()
                        self._last_refresh_time = now
                        logger.info("115 Token 自动刷新成功")
                    except Exception as e:
                        logger.error(f"115 Token 自动刷新失败，将重新初始化客户端: {e}")
                        self.client = None
                        # 不直接 return，落入下方重建逻辑
                
                if self.client is not None:
                    return self.client

        # 使用 from_token 方法创建客户端
        try:
            self.client = P115OpenClient.from_token(access_token, refresh_token)
            logger.info("使用 Tokens 创建 115 OpenClient")
            
            # 刚创建时，立即尝试刷新一次防止历史 Token 已过期
            try:
                await self.client.refresh_access_token(async_=True)
                await self._save_client_tokens()
                logger.info("初始化时自动刷新 115 Token 成功")
            except Exception as e:
                err_str = str(e)
                if "Errno 61" in err_str or "ENODATA" in err_str:
                    logger.warning("初始化时自动刷新 Token 失败: 您的 Token 已完全失效！原因可能是:\n1. 您的 115_bot 还在后台运行，抢占并作废了新 Token。\n2. 您的 VPS IP 被 115 的 WAF/防火墙拦截 (表现为返回了一段无法解析的加密乱码)。")
                else:
                    logger.warning(f"初始化时自动刷新 Token 失败 (可能仍有效，继续尝试): {e}")

            self._last_refresh_time = time.time()
            self._auth_credential = f"{self.client.access_token}|{self.client.refresh_token}"
        except Exception as e:
            logger.error(f"创建 115 OpenClient 失败: {e}")
            raise

        return self.client

    async def _save_client_tokens(self):
        """将客户端内存中的最新 token 持久化到数据库"""
        await self.db.set_setting("p115_access_token", self.client.access_token)
        await self.db.set_setting("p115_refresh_token", self.client.refresh_token)
        self._auth_credential = f"{self.client.access_token}|{self.client.refresh_token}"

    async def get_offline_quota(self) -> dict:
        """获取 115 离线下载配额信息

        返回: {"total": int, "remaining": int, "used": int} 或在异常时返回 None
        """
        client = await self._get_client()
        try:
            resp = await client.offline_quota_info_open(async_=True)
            logger.info(f"115 离线配额原始响应: {resp}")

            if not isinstance(resp, dict) or not resp.get("state"):
                logger.warning(f"获取离线配额失败: {resp}")
                return None

            data = resp.get("data", {})
            # 真实 API 返回的字段为 count, surplus, used
            total = data.get("count", data.get("quota", data.get("total", 0)))
            remaining = data.get("surplus", data.get("remain", data.get("remaining", 0)))
            used = data.get("used", total - remaining if total and remaining is not None else 0)

            return {"total": total, "remaining": remaining, "used": used}
        except Exception as e:
            logger.error(f"获取离线配额异常: {e}")
            return None

    async def add_offline_task(self, url: str) -> dict:
        """添加离线下载任务到 115"""
        client = await self._get_client()
        resp = await client.offline_add_urls_open(url, async_=True)

        # 处理成功情况
        if isinstance(resp, dict) and resp.get("state"):
            try:
                # 官方 Open API 返回 data 为列表
                data = resp.get("data", [])
                if isinstance(data, list) and len(data) > 0:
                    result = data[0]
                    info_hash = result.get("info_hash")
                    if info_hash:
                        return {"info_hash": info_hash, "name": result.get("name", "")}
            except Exception as e:
                logger.error(f"解析 115 离线响应失败: {e}, 响应: {resp}")

            raise Exception("任务提交成功，但未能获取到 info_hash")

        # 处理 "任务已存在" (错误码 10008)
        if isinstance(resp, dict) and (resp.get("code") == 10008 or resp.get("errcode") == 10008):
            logger.info("任务已存在，尝试从链接中提取 hash")
            match = re.search(r'urn:btih:([a-zA-Z0-9]+)', url, re.IGNORECASE)
            if match:
                info_hash = match.group(1).lower()
                return {"info_hash": info_hash, "name": "已存在的任务(解析中)"}
            else:
                raise Exception("任务已存在，但无法从原链接解析 info_hash")

        # 其他错误
        error_msg = resp.get("message") if isinstance(resp, dict) else "未知错误"
        logger.error(f"离线任务添加失败，原始响应: {resp}")
        raise Exception(f"115 离线任务添加失败: {error_msg}")

    async def add_share_task(self, url: str) -> dict:
        """添加 115 分享链接转存任务"""
        from p115client.tool import share_extract_payload
        client = await self._get_client()

        try:
            payload = share_extract_payload(url)
            share_code = payload.get("share_code")
            receive_code = payload.get("receive_code", "")
        except Exception as e:
            logger.error(f"提取分享链接失败: {e}")
            raise Exception("无法解析 115 分享链接，格式可能不正确")

        # 1. 获取分享快照
        snap_resp = await client.share_snap({"share_code": share_code, "receive_code": receive_code}, async_=True)
        if not isinstance(snap_resp, dict) or not snap_resp.get("state"):
            error_msg = snap_resp.get("error", snap_resp.get("message", "未知错误")) if isinstance(snap_resp, dict) else "未知错误"
            raise Exception(f"获取分享详情失败: {error_msg}")

        data = snap_resp.get("data", {})
        file_list = data.get("list", [])
        if not file_list:
            raise Exception("该分享链接为空")

        # 收集文件ID
        file_ids = [str(f.get("file_id") or f.get("id")) for f in file_list if f.get("file_id") or f.get("id")]
        if not file_ids:
            raise Exception("无法从分享链接中提取有效文件 ID")

        # 确定任务名称
        task_name = file_list[0].get("name", "未命名分享")
        if len(file_list) > 1:
            task_name += f" 等{len(file_list)}个文件"

        # 2. 在根目录创建一个专属的接收文件夹，避免散落
        import time
        folder_name = f"{task_name}_{int(time.time())}"
        mkdir_resp = await client.fs_mkdir({"cname": folder_name}, pid=0, async_=True)
        if not isinstance(mkdir_resp, dict) or not mkdir_resp.get("state"):
            error_msg = mkdir_resp.get("error", mkdir_resp.get("message", "未知错误")) if isinstance(mkdir_resp, dict) else "未知错误"
            raise Exception(f"创建转存目录失败: {error_msg}")

        target_cid = mkdir_resp.get("cid") or mkdir_resp.get("file_id") or mkdir_resp.get("id")
        if not target_cid:
            target_cid = mkdir_resp.get("data", {}).get("cid") or mkdir_resp.get("data", {}).get("file_id")

        if not target_cid:
            raise Exception(f"创建转存目录成功，但无法获取其目录 ID: {mkdir_resp}")

        # 3. 执行转存
        receive_payload = {
            "share_code": share_code,
            "receive_code": receive_code,
            "file_id": ",".join(file_ids),
            "cid": target_cid
        }
        receive_resp = await client.share_receive(receive_payload, async_=True)
        if not isinstance(receive_resp, dict) or not receive_resp.get("state"):
            error_msg = receive_resp.get("error", receive_resp.get("message", "未知错误")) if isinstance(receive_resp, dict) else "未知错误"
            raise Exception(f"转存分享失败: {error_msg}")

        return {"name": task_name, "folder_name": folder_name, "is_share": True}

    async def get_offline_list(self) -> list[dict]:
        """获取 115 离线任务列表（一次调用，多次复用）"""
        client = await self._get_client()
        resp = await client.offline_list_open(async_=True)

        if not isinstance(resp, dict):
            logger.warning(f"offline_list_open 返回非字典: {type(resp)}")
            return []

        data = resp.get("data", {})
        if isinstance(data, dict):
            return data.get("tasks", []) or []
        return []

    async def check_offline_status(self, info_hash: str, offline_list: list[dict] | None = None) -> dict | None:
        """检查离线任务进度

        参数:
            info_hash: 任务哈希
            offline_list: 可选的预获取离线列表，避免重复调用 API

        返回匹配的任务字典，或 None（如果任务不在列表中）。
        """
        if offline_list is None:
            offline_list = await self.get_offline_list()

        if not offline_list:
            logger.debug("未找到任务列表")
            return None

        # 在 tasks 中查找匹配的 info_hash
        for task in offline_list:
            if not isinstance(task, dict):
                continue

            task_hash = task.get("info_hash")
            if task_hash == info_hash:
                percent = task.get("percentDone", 0)
                status_text = task.get('status_text', 'N/A')
                # 有实际进度或状态异常时用 INFO，0% 正常等待降为 DEBUG 避免日志刷屏
                is_noteworthy = percent > 0 or '失败' in status_text or '错误' in status_text
                if is_noteworthy:
                    logger.info(f"找到任务 {info_hash}: 进度={percent}%, 状态={status_text}")
                else:
                    logger.debug(f"找到任务 {info_hash}: 进度={percent}%, 状态={status_text}")
                return task

        logger.debug(f"未找到哈希 {info_hash} 的任务")
        return None

    async def get_task_files(self, info_hash: str, task_name: str) -> list[dict]:
        """获取 115 离线任务完成后的文件清单

        使用 asyncio.to_thread 包装同步的 tool.iterdir 调用，
        避免阻塞事件循环。
        """
        client = await self._get_client()
        files_found = []

        # 使用 asyncio.to_thread 包装同步调用
        def _find_item():
            logger.info(f"开始查找任务文件夹: {task_name}")
            for item in tool.iterdir(client, cid=0):
                if item.get("name") == task_name:
                    logger.info(f"找到任务文件夹: {item.get('name')}")
                    return item
            return None

        found_item = await asyncio.to_thread(_find_item)

        if not found_item:
            logger.warning(f"未找到名为 {task_name} 的文件夹或文件")
            return []

        is_directory = found_item.get("is_dir") or found_item.get("is_directory")

        if is_directory:
            def _list_files():
                all_files = []
                logger.info(f"找到目录，准备遍历内部文件，cid={found_item['id']}")
                for sub_item in tool.iterdir(client, cid=found_item["id"]):
                    if not (sub_item.get("is_dir") or sub_item.get("is_directory")):
                        pickcode = sub_item.get("pickcode") or sub_item.get("pick_code", "")
                        all_files.append({
                            "file_id": sub_item.get("id", ""),
                            "pickcode": pickcode,
                            "name": sub_item.get("name", ""),
                            "size": sub_item.get("size", 0),
                            "path": sub_item.get("name", ""),
                        })
                
                if not all_files:
                    return []

                # 寻找最大文件的大小和名称，作为判断小体积广告的基准
                max_file = max(all_files, key=lambda x: x["size"])
                max_size = max_file["size"]
                max_name = max_file["name"]
                
                # 提取最大文件名的核心前缀
                core_prefix = re.split(r'[-_\s]', max_name.split('.')[0])[:2]
                core_prefix = [p.lower() for p in core_prefix if len(p) > 2]
                
                # 广告过滤逻辑
                results = []
                junk_exts = ('.url', '.htm', '.html', '.txt', '.chm', '.exe', '.apk', '.nfo', '.vbs', '.bat')
                ad_keywords = ['下载app', '最新网址', '免费玩', '兑换码', '澳门', '赌场', '注册免费', '平台', '发牌', '18禁游戏', '获取最新', '手游', '游戏盒子', '直播视频']
                video_exts = ('.mp4', '.mkv', '.avi', '.wmv', '.rmvb', '.flv', '.ts')
                
                for f in all_files:
                    name_lower = f["name"].lower()
                    size = f["size"]
                    
                    # 1. 过滤垃圾后缀
                    if name_lower.endswith(junk_exts):
                        logger.info(f"过滤广告文件 (后缀): {f['name']}")
                        continue
                        
                    # 2. 过滤广告关键词
                    if any(kw in name_lower for kw in ad_keywords):
                        logger.info(f"过滤广告文件 (关键词): {f['name']}")
                        continue
                        
                    # 3. 过滤体积显著较小的广告视频
                    is_small_video = name_lower.endswith(video_exts) and size < max_size * 0.05 and size < 100 * 1024 * 1024
                    has_core_prefix = any(p in name_lower for p in core_prefix) if core_prefix else False
                    
                    if is_small_video and not has_core_prefix:
                        logger.info(f"过滤小体积无关媒体广告: {f['name']} (大小: {size / 1024 / 1024:.2f} MB)")
                        continue

                    logger.info(f"保留文件: {f['name']}, pickcode: {f['pickcode']}")
                    results.append(f)
                    
                return results

            files_found = await asyncio.to_thread(_list_files)
        else:
            # 单文件任务
            pickcode = found_item.get("pickcode") or found_item.get("pick_code", "")
            logger.info(f"单文件任务: {found_item.get('name')}, pickcode: {pickcode}")
            files_found.append({
                "file_id": found_item.get("id", ""),
                "pickcode": pickcode,
                "name": found_item.get("name", ""),
                "size": found_item.get("size", 0),
                "path": found_item.get("name", ""),
            })

        logger.info(f"获取到 {len(files_found)} 个文件，pickcodes: {[f['pickcode'] for f in files_found]}")
        return files_found

    async def download_file_to_vps(self, pickcode: str, local_path: str, progress_callback=None):
        """流式下载 115 文件到本地 VPS，支持断点续传"""
        client = await self._get_client()
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            logger.info(f"开始下载文件，pickcode={pickcode}")

            # 获取下载链接，增加超时机制防止无限挂起
            try:
                url_info = await asyncio.wait_for(
                    client.download_url_info_open(pickcode, async_=True),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                raise Exception("获取下载链接超时")

            url = None
            data = url_info.get("data", {})
            for file_id, file_info in data.items():
                url = file_info.get("url", {}).get("url")
                if url:
                    break

            if not url:
                raise Exception("无法从 API 响应中获取下载 URL")

            logger.info(f"获得下载链接: {str(url)[:80]}...")

            # 检查是否已部分下载（断点续传）
            existing_size = 0
            if os.path.exists(local_path):
                existing_size = os.path.getsize(local_path)

            # User-Agent 必须为空：115 CDN 的签名校验包含 UA，
            # API 生成下载链接时默认使用空 UA，不匹配会返回 403
            headers = {"User-Agent": ""}
            if existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"
                logger.info(f"检测到已下载部分文件 ({existing_size} bytes)，准备续传")

            # 大文件下载超时设置
            timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=120)
            session = await http_session.get_session()
            async with session.get(str(url), headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    mode = 'wb'
                elif resp.status == 206:
                    mode = 'ab'
                else:
                    raise Exception(f"HTTP {resp.status}: {await resp.text()}")

                async with aiofiles.open(local_path, mode=mode) as f:
                    async for chunk in resp.content.iter_chunked(1024 * 1024):
                        await f.write(chunk)
                        if progress_callback:
                            await progress_callback(len(chunk))

            logger.info(f"文件下载成功: {local_path}")
            return True

        except Exception as e:
            logger.error(f"下载文件失败: {e}", exc_info=True)
            raise
