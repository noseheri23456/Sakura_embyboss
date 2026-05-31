"""
Emby 通知器 — 通知 Emby 扫描新上传的媒体文件夹

从 Sakura 的 config 读取 emby_url 和 emby_api，
从 p115_config 读取 emby_media_path。
"""
import asyncio
import logging

import aiohttp
from . import http_session

logger = logging.getLogger(__name__)


def _get_emby_config():
    """延迟导入 Sakura 配置，避免循环导入"""
    from bot import emby_url, emby_api, p115_config
    return emby_url, emby_api, p115_config.emby_media_path


def is_configured() -> bool:
    """检查 Emby 是否已配置"""
    emby_url, emby_api, _ = _get_emby_config()
    return bool(emby_url and emby_api)


async def notify_media_updated(task_name: str) -> bool:
    """通知 Emby 扫描指定任务文件夹

    Args:
        task_name: 任务名称，对应 Google Drive 上的子文件夹名

    Returns:
        True 如果通知成功，False 如果失败或未配置
    """
    if not is_configured():
        logger.debug("Emby 未配置，跳过通知")
        return False

    emby_url, emby_api_key, emby_media_path = _get_emby_config()

    # 构造 Emby 容器内的完整路径
    emby_path = f"{emby_media_path.rstrip('/')}/{task_name}"

    # 等待 rclone FUSE 挂载缓存刷新，确保 Emby 能看到新文件
    await asyncio.sleep(5)

    url = f"{emby_url.rstrip('/')}/emby/Library/Media/Updated"
    params = {"api_key": emby_api_key}
    payload = {
        "Updates": [
            {
                "Path": emby_path,
                "UpdateType": "Created"
            }
        ]
    }

    try:
        session = await http_session.get_session()
        async with session.post(url, params=params, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 204 or resp.status == 200:
                logger.info(f"Emby 通知成功: {emby_path}")
                return True
            else:
                body = await resp.text()
                logger.warning(f"Emby 通知返回非预期状态码 {resp.status}: {body}")
                return False
    except Exception as e:
        logger.error(f"Emby 通知失败: {e}")
        return False
