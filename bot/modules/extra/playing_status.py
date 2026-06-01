import os
import asyncio
from pathlib import Path
from pyrogram.errors import MessageIdInvalid, MessageNotModified, RPCError

from bot import bot, main_group, group, LOGGER
from bot.func_helper.scheduler import scheduler
from bot.func_helper.emby import emby as emby_service

MSG_ID_FILE = Path("data/playing_status_msg.txt")

async def update_playing_status():
    """
    定时任务：获取 Emby 当前播放信息，更新群组内的置顶消息。
    """
    if not main_group:
        return
        
    try:
        details = await emby_service.get_current_playing_details()
        playing_count = len(details)
            
        # 构建消息内容
        text = f"📺 **Emby 实时播放状态** (每分钟更新)\n\n"
        text += f"👥 当前在线播放人数: **{playing_count}**\n"
        text += "━" * 20 + "\n"
        
        if details:
            for i, d in enumerate(details):
                added_text = f"👤 `{d['username']}` 正在观看:\n🎬 {d['title']}\n" + "━" * 20 + "\n"
                # Telegram 消息长度限制为 4096，这里预留一些余量
                if len(text) + len(added_text) > 3800:
                    text += f"...\n(由于长度限制，仅显示前 {i} 位用户的播放状态)\n"
                    break
                text += added_text
        else:
            text += "💤 当前没有人在观看任何内容\n"
            
        text += "\n*(此消息自动刷新)*"

        # 尝试读取保存的 message_id
        msg_id = None
        if MSG_ID_FILE.exists():
            try:
                with open(MSG_ID_FILE, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content.isdigit():
                        msg_id = int(content)
            except Exception as e:
                LOGGER.error(f"读取 playing_status_msg.txt 失败: {e}")

        group_id = group[0]
        new_msg_sent = False

        if msg_id:
            try:
                # 尝试编辑原消息
                await bot.edit_message_text(chat_id=group_id, message_id=msg_id, text=text)
            except MessageNotModified:
                # 内容未变，不需要报错
                pass
            except (MessageIdInvalid, RPCError) as e:
                LOGGER.warning(f"编辑置顶播放状态消息失败 ({e})，将发送新消息。")
                msg_id = None  # 重置为 None，触发新建逻辑
                
        # 如果 msg_id 为空或原消息被删除/失效，则发送新消息并置顶
        if not msg_id:
            try:
                sent_msg = await bot.send_message(chat_id=group_id, text=text)
                msg_id = sent_msg.id
                # 置顶消息 (disable_notification=True 防止打扰群员)
                await bot.pin_chat_message(chat_id=group_id, message_id=msg_id, disable_notification=True)
                new_msg_sent = True
                # 保存 msg_id
                MSG_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(MSG_ID_FILE, "w", encoding="utf-8") as f:
                    f.write(str(msg_id))
            except Exception as e:
                LOGGER.error(f"发送或置顶播放状态消息失败: {e}")

    except Exception as e:
        LOGGER.error(f"更新播放状态定时任务异常: {e}")

# 在模块被 import 时注册定时任务 (每分钟执行一次)
# 由于 scheduler.add_job 需要协程对象或者普通函数，如果是 async 函数，调度器支持 async
scheduler.add_job(update_playing_status, 'interval', minutes=1, id='update_playing_status', replace_existing=True)
