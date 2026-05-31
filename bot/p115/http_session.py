"""全局共享 aiohttp.ClientSession，避免每次请求都新建/销毁连接池"""

import aiohttp

_session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    """获取全局共享的 HTTP 会话（惰性初始化）"""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def close_session():
    """关闭全局 HTTP 会话（应用退出时调用）"""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None
