"""
115 网盘磁力转存 — Pyrogram 命令处理器

用户命令 (已注册 Emby 用户):
  - 发送磁力链接 → 自动添加 115 离线 + rclone 上传任务
  - /p115_status   → 查看我的任务进度
  - /p115_history  → 查看历史任务
  - /p115_cancel   → 取消任务

管理员命令 (owner + admins):
  - /p115_check    → 检查 115 登录状态
  - /p115_quota    → 查看离线配额
  - /p115_token    → 设置 115 Token
  - /p115_queue    → 全局队列
  - /p115_tasks    → 全局任务列表
  - /p115_pause    → 暂停传输
  - /p115_resume   → 恢复传输
"""
import asyncio
import logging

from pyrogram import filters

from bot import bot, prefixes, owner, p115_config, save_config
from bot.func_helper.filters import admins_on_filter
from bot.func_helper.msg_utils import sendMessage, deleteMessage, editMessage, callAnswer, callListen
from bot.func_helper.utils import judge_admins
from bot.func_helper.fix_bottons import p115_panel_ikb, p115_admin_panel_ikb
from bot.sql_helper.sql_emby import sql_get_emby, sql_update_emby, Emby

from bot.p115.database import Database, init_db
from bot.p115.p115_manager import P115Manager
from bot.p115.rclone_runner import RcloneRunner
from bot.p115.transfer_worker import TransferWorker, ProgressThrottler
from bot.p115 import http_session

logger = logging.getLogger(__name__)

# --- 全局单例 (延迟初始化) ---
_db: Database | None = None
_p115: P115Manager | None = None
_rclone: RcloneRunner | None = None
_worker: TransferWorker | None = None
_initialized = False


async def _ensure_init():
    """确保 115 模块已初始化（首次调用时执行）"""
    global _db, _p115, _rclone, _worker, _initialized
    if _initialized:
        return
    _initialized = True

    await init_db()
    _db = Database()
    _p115 = P115Manager(_db)
    _rclone = RcloneRunner(
        remote_name=p115_config.rclone_remote,
        target_path=p115_config.rclone_target_path,
    )
    _worker = TransferWorker(
        _db, _p115, _rclone,
        stall_timeout_minutes=p115_config.stall_timeout_minutes,
    )

    # 设置 throttler 和管理员通知
    throttler = ProgressThrottler(bot)
    _worker.set_throttler(throttler)
    _worker.set_admin_notifier(owner, lambda chat_id, text: bot.send_message(chat_id, text))

    # 启动后台 Worker
    asyncio.create_task(_worker.start())
    logger.info("115 转存模块已初始化，Worker 已启动")


# --- 权限检查 ---

def _check_p115_enabled():
    """检查 115 功能是否启用"""
    return p115_config.status


def _check_user_permission(user_id: int) -> bool:
    """检查用户是否有权使用 115 功能（已注册 Emby 且 lv 在允许列表中）"""
    emby_data = sql_get_emby(user_id)
    if not emby_data:
        return False
    return emby_data.lv in p115_config.allowed_lv


# --- 用户命令 ---

@bot.on_message(filters.regex(r"(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*|https?://(?:[a-zA-Z0-9-]+\.)?115(?:cdn)?\.com/[^\s]+)") & filters.private)
async def handle_download_link(_, msg):
    """自动检测磁力链接和 115 分享链接"""
    if not _check_p115_enabled():
        return

    user_id = msg.from_user.id

    if not _check_user_permission(user_id):
        await sendMessage(msg, "❌ 你没有使用 115 转存功能的权限。\n需要已注册 Emby 账户。")
        return

    await _ensure_init()

    # 检查排队任务限额
    pending_count = await _db.count_user_pending_tasks(user_id)
    if pending_count >= p115_config.max_pending_tasks:
        await sendMessage(msg, f"❌ 排队任务已达上限 ({p115_config.max_pending_tasks})")
        return

    # 检查总任务限额
    extra_quota = await _db.get_user_extra_quota(user_id)
    total_allowed = p115_config.max_total_tasks + extra_quota
    total_count = await _db.count_user_total_tasks(user_id)
    if total_count >= total_allowed:
        await sendMessage(msg, f"❌ 总任务数已达上限 ({total_allowed})。\n"
                               f"你可以使用 /p115_buy 购买更多配额，或联系管理员。")
        return

    source_url = msg.text.strip()
    is_share = not source_url.lower().startswith("magnet:")

    # 检查 115 离线配额 (分享链接不需要离线配额)
    if not is_share:
        quota = await _p115.get_offline_quota()
        if quota is not None and quota['remaining'] <= 0:
            await sendMessage(msg,
                f"❌ 115 离线下载配额已用完（{quota['used']}/{quota['total']}）\n"
                f"请等待下月重置或联系管理员。"
            )
            return

    # 115 提交重试逻辑 (3次)
    info_hash = None
    share_folder_name = None
    task_name = "解析中..."
    last_error = ""
    for attempt in range(3):
        try:
            if is_share:
                resp = await _p115.add_share_task(source_url)
                task_name = resp.get("name")
                share_folder_name = resp.get("folder_name")
                info_hash = f"share_{share_folder_name}"
            else:
                resp = await _p115.add_offline_task(source_url)
                info_hash = resp.get("info_hash")
            break
        except Exception as e:
            last_error = str(e)
            logger.warning(f"115 提交尝试 {attempt + 1}/3 失败: {e}")
            await asyncio.sleep(2 ** attempt)

    if not info_hash:
        await sendMessage(msg, f"❌ 115 提交任务失败（已重试3次）: {last_error}")
        return

    try:
        if is_share:
            reply = await msg.reply("✅ 分享链接已成功转存至 115！\n正在准备下载至 VPS...")
            task_id = await _db.add_task(
                user_id=user_id,
                source_url=source_url,
                task_name=task_name,
            )
            await _db.update_task(task_id, task_hash=info_hash, status='WAITING_LOCAL_TRANSFER', msg_id=reply.id)
            
            # 立即获取文件列表并入库
            files = await _p115.get_task_files(info_hash, share_folder_name)
            if files:
                await _db.add_task_files_batch(task_id, files)
            else:
                await _db.update_task(task_id, status='FAILED', error_msg="转存成功但未找到文件")
                await bot.edit_message_text(msg.chat.id, reply.id, "❌ 转存成功但未找到文件")
        else:
            reply = await msg.reply("✅ 任务已提交至 115！\n正在解析并排队中...")
            task_id = await _db.add_task(
                user_id=user_id,
                source_url=source_url,
                task_name="解析中...",
            )
            await _db.update_task(task_id, task_hash=info_hash, status='DOWNLOADING_115', msg_id=reply.id)
    except Exception as e:
        await sendMessage(msg, f"❌ 数据库记录失败: {e}")


@bot.on_message(filters.command('p115_status', prefixes) & filters.private)
async def cmd_p115_status(_, msg):
    """查看任务进度"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    if not _check_user_permission(msg.from_user.id):
        return await sendMessage(msg, "❌ 你没有使用 115 转存功能的权限。")

    await _ensure_init()
    user_id = msg.from_user.id
    tasks = await _db.get_pending_tasks()

    if not judge_admins(user_id):
        tasks = [t for t in tasks if t['user_id'] == user_id]

    if not tasks:
        return await sendMessage(msg, "当前没有正在进行或排队的任务。")

    text = "📊 **115 任务进度预览：**\n\n"
    for t in tasks:
        icon = "⏳" if t['status'] in ('PENDING', 'DOWNLOADING_115', 'OFFLINING_115') else "🚀"
        text += f"{icon} ID: {t['id']} | {t['status']} | {t['task_name'] or '解析中...'}\n"
        if t['status'] == 'TRANSFERRING' and t['id'] in _worker.active_progress:
            text += f"  └ {_worker.active_progress[t['id']]}\n"
    await sendMessage(msg, text)


@bot.on_message(filters.command('p115_myquota', prefixes) & filters.private)
async def cmd_p115_myquota(_, msg):
    """查看我的115配额"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")
    if not _check_user_permission(msg.from_user.id):
        return await sendMessage(msg, "❌ 你没有使用 115 转存功能的权限。")

    await _ensure_init()
    user_id = msg.from_user.id

    extra_quota = await _db.get_user_extra_quota(user_id)
    total_allowed = p115_config.max_total_tasks + extra_quota
    total_used = await _db.count_user_total_tasks(user_id)
    remaining = max(0, total_allowed - total_used)
    pending_count = await _db.count_user_pending_tasks(user_id)

    text = (f"📊 **你的 115 任务配额：**\n\n"
            f"📦 **总任务配额:** {total_used} / {total_allowed}\n"
            f"   └ 剩余可提交: **{remaining}** 次\n"
            f"   └ 基础额度 {p115_config.max_total_tasks} + 额外购买 {extra_quota}\n\n"
            f"⏳ **当前并发排队:** {pending_count} / {p115_config.max_pending_tasks}\n"
            f"💾 **单文件大小限制:** {p115_config.max_file_size_gb} GB\n\n"
            f"💡 提示: 发送 /p115_buy 或在面板中点击“购买配额”可增加总次数上限。")
    await sendMessage(msg, text)


@bot.on_message(filters.command('p115_history', prefixes) & filters.private)
async def cmd_p115_history(_, msg):
    """查看历史任务"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    if not _check_user_permission(msg.from_user.id):
        return await sendMessage(msg, "❌ 你没有使用 115 转存功能的权限。")

    await _ensure_init()

    # 解析页码
    args = msg.text.split()
    page = 1
    if len(args) > 1 and args[1].isdigit():
        page = int(args[1])
    if page < 1:
        page = 1

    limit = 10
    offset = (page - 1) * limit
    user_id = msg.from_user.id

    tasks = await _db.get_user_tasks(user_id, limit=limit, offset=offset)
    total_count = await _db.count_user_total_tasks(user_id)

    if not tasks:
        if page == 1:
            return await sendMessage(msg, "你还没有添加过任何 115 任务。")
        return await sendMessage(msg, f"第 {page} 页没有历史任务。")

    text = f"🗂 **你的 115 历史任务** (共 {total_count} 个, 当前第 {page} 页)：\n\n"
    for t in tasks:
        if t['status'] == 'DONE':
            icon = "✅"
        elif t['status'] == 'FAILED':
            icon = "❌"
        elif t['status'] == 'CANCELED':
            icon = "🛑"
        elif t['status'] in ('PENDING', 'DOWNLOADING_115', 'OFFLINING_115'):
            icon = "⏳"
        else:
            icon = "🚀"

        create_time = t['created_at'].split()[0] if t['created_at'] else "未知时间"
        task_name = t['task_name'] or "未命名任务"
        if len(task_name) > 30:
            task_name = task_name[:27] + "..."

        text += f"{icon} ID: {t['id']} | {create_time} | {t['status']}\n"
        text += f"  └ {task_name}\n\n"

    total_pages = (total_count + limit - 1) // limit
    if total_pages > 1:
        text += f"提示: 使用 /p115_history <页码> 查看更多 (共 {total_pages} 页)"

    await sendMessage(msg, text)


@bot.on_message(filters.command('p115_cancel', prefixes) & filters.private)
async def cmd_p115_cancel(_, msg):
    """取消任务"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    if not _check_user_permission(msg.from_user.id):
        return await sendMessage(msg, "❌ 你没有使用 115 转存功能的权限。")

    await _ensure_init()
    user_id = msg.from_user.id
    args = msg.text.replace("/p115_cancel", "").strip()
    if not args.isdigit():
        return await sendMessage(msg, "用法: /p115_cancel <任务ID>")

    task_id = int(args)
    task = await _db.get_task(task_id)
    if not task or (task['user_id'] != user_id and not judge_admins(user_id)):
        return await sendMessage(msg, "❌ 任务不存在或无权操作。")

    if task['status'] in ('DONE', 'FAILED', 'CANCELED'):
        return await sendMessage(msg, f"❌ 任务已处于 {task['status']} 状态。")

    await _db.update_task(task_id, status='CANCELED')
    import shutil
    from pathlib import Path
    temp_path = Path(f"data/downloads/task_{task_id}")
    if temp_path.exists():
        shutil.rmtree(temp_path)
    await sendMessage(msg, f"✅ 任务 {task_id} 已取消。")


@bot.on_message(filters.command('p115_buy', prefixes) & filters.private)
async def cmd_p115_buy(_, msg):
    """使用积分购买 115 任务配额"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    if not p115_config.allow_buy:
        return await sendMessage(msg, "❌ 管理员未开启配额购买功能。")

    if not _check_user_permission(msg.from_user.id):
        return await sendMessage(msg, "❌ 你没有使用 115 转存功能的权限。")

    await _ensure_init()
    user_id = msg.from_user.id
    emby_data = sql_get_emby(user_id)
    if not emby_data:
        return await sendMessage(msg, "❌ 找不到你的账号信息。")

    price = p115_config.task_price
    amount = p115_config.tasks_per_purchase

    args = msg.text.split()
    if len(args) == 1:
        # 只输入 /p115_buy 时显示购买信息和确认提示
        extra_quota = await _db.get_user_extra_quota(user_id)
        current_total = p115_config.max_total_tasks + extra_quota
        text = (f"🛒 **购买 115 任务配额**\n\n"
                f"当前你的积分: **{emby_data.iv}**\n"
                f"当前总配额: **{current_total}** (默认 {p115_config.max_total_tasks} + 额外 {extra_quota})\n\n"
                f"价格: **{price}** 积分 / **{amount}** 个任务配额\n\n"
                f"如需购买，请发送 `/p115_buy confirm`")
        return await sendMessage(msg, text)
    
    if args[1].lower() == 'confirm':
        if emby_data.iv < price:
            return await sendMessage(msg, f"❌ 积分不足！购买需要 {price} 积分，你当前有 {emby_data.iv} 积分。")
        
        new_iv = emby_data.iv - price
        if sql_update_emby(Emby.tg == user_id, iv=new_iv):
            await _db.add_user_extra_quota(user_id, amount)
            new_extra = await _db.get_user_extra_quota(user_id)
            new_total = p115_config.max_total_tasks + new_extra
            await sendMessage(msg, f"✅ 购买成功！\n\n"
                                   f"扣除 {price} 积分，当前剩余: {new_iv} 积分\n"
                                   f"新增 {amount} 个配额，当前总配额: {new_total} 个")
            logger.info(f"用户 {user_id} 花费 {price} 积分购买了 {amount} 个 115 任务配额")
        else:
            await sendMessage(msg, "❌ 数据库更新积分失败，请联系管理员。")


# --- 管理员命令 ---

@bot.on_message(filters.command('p115_check', prefixes) & filters.private & admins_on_filter)
async def cmd_p115_check(_, msg):
    """检查 115 登录状态"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    await _ensure_init()
    await sendMessage(msg, "正在检查 115 登录状态...")
    try:
        client = await _p115._get_client()
        user_info = await client.user_info_open(async_=True)
        if user_info and user_info.get("state"):
            await sendMessage(msg, "✅ 115 状态正常：已登录")
        else:
            await sendMessage(msg, "❌ 115 状态异常：Token 可能已失效，请使用 /p115_token 重新设置")
    except Exception as e:
        err_str = str(e)
        if "Errno 61" in err_str or "ENODATA" in err_str:
            logger.error(f"检查 115 状态失败: {e}")
            await sendMessage(msg, "❌ 115 状态异常：您的 Token 已失效 (Errno 61)。\n原因可能是您的 115_bot 仍在运行并抢占了 Token，导致这边的 Token 报废。请关闭旧的 115_bot 后，重新获取新 Token 并设置！")
        else:
            logger.error(f"检查 115 状态失败: {e}")
            await sendMessage(msg, f"❌ 115 状态异常：{e}")


@bot.on_message(filters.command('p115_quota', prefixes) & filters.private & admins_on_filter)
async def cmd_p115_quota(_, msg):
    """查看 115 离线下载配额"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    await _ensure_init()
    quota = await _p115.get_offline_quota()
    if quota is None:
        return await sendMessage(msg, "❌ 无法获取配额信息，请检查 Token 状态。")

    await sendMessage(msg,
        f"📊 **115 离线下载配额：**\n"
        f"  总配额: {quota['total']}\n"
        f"  已使用: {quota['used']}\n"
        f"  剩余: {quota['remaining']}"
    )


@bot.on_message(filters.command('p115_token', prefixes) & filters.private & admins_on_filter)
async def cmd_p115_token(_, msg):
    """设置 115 Token"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    await _ensure_init()
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 3:
        return await sendMessage(msg,
            "用法: /p115_token <access_token> <refresh_token>\n\n"
            "例子：\n"
            "/p115_token 4v4n6.xxx... 4v4n6.yyy..."
        )

    access_token = parts[1].strip()
    refresh_token = parts[2].strip()

    try:
        await _db.set_setting("p115_access_token", access_token)
        await _db.set_setting("p115_refresh_token", refresh_token)
        await sendMessage(msg, "✅ 115 Token 已保存")
        logger.info(f"p115_commands: 用户 {msg.from_user.id} 设置了 115 Token。Access长度: {len(access_token)}, Refresh长度: {len(refresh_token)}")
    except Exception as e:
        await sendMessage(msg, f"❌ 保存失败: {e}")
        logger.error(f"p115_commands: 保存 115 Token 失败: {e}")


@bot.on_message(filters.command('p115_queue', prefixes) & filters.private & admins_on_filter)
async def cmd_p115_queue(_, msg):
    """查看全局排队状态"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    await _ensure_init()
    tasks = await _db.get_pending_tasks()
    if not tasks:
        return await sendMessage(msg, "当前 115 队列为空。")

    text = "📋 **115 全局队列状态：**\n"
    for t in tasks:
        text += f"ID: {t['id']} | User: {t['user_id']} | 状态: {t['status']} | {t['task_name'] or '未知'}\n"
    await sendMessage(msg, text)


@bot.on_message(filters.command('p115_tasks', prefixes) & filters.private & admins_on_filter)
async def cmd_p115_tasks(_, msg):
    """管理员：全局任务列表 (支持状态筛选和分页)"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    await _ensure_init()

    _VALID_STATUSES = frozenset({
        'PENDING', 'OFFLINING_115', 'DOWNLOADING_115', 'WAITING_LOCAL_TRANSFER',
        'TRANSFERRING', 'DONE', 'FAILED', 'CANCELED', 'PAUSED',
    })

    args = msg.text.split()[1:]
    status_filter = None
    page = 1

    for arg in args:
        if arg.upper() in _VALID_STATUSES:
            status_filter = arg.upper()
        elif arg.isdigit():
            page = max(1, int(arg))

    limit = 15
    offset = (page - 1) * limit

    tasks = await _db.get_all_tasks(limit=limit, offset=offset, status_filter=status_filter)
    total = await _db.count_all_tasks(status_filter=status_filter)

    if not tasks:
        hint = f"（筛选: {status_filter}）" if status_filter else ""
        if page == 1:
            return await sendMessage(msg, f"没有找到任何 115 任务。{hint}")
        return await sendMessage(msg, f"第 {page} 页没有任务。{hint}")

    filter_label = f" | 筛选: {status_filter}" if status_filter else ""
    total_pages = (total + limit - 1) // limit
    text = f"📋 **115 全局任务列表** (共 {total} 个{filter_label})\n"
    text += "━" * 26 + "\n\n"

    for t in tasks:
        st = t['status']
        if st == 'DONE':
            icon = "✅"
        elif st == 'FAILED':
            icon = "❌"
        elif st == 'CANCELED':
            icon = "🛑"
        elif st in ('PENDING', 'DOWNLOADING_115', 'OFFLINING_115'):
            icon = "⏳"
        elif st == 'PAUSED':
            icon = "⏸"
        else:
            icon = "🚀"

        task_name = t['task_name'] or "未命名任务"
        if len(task_name) > 28:
            task_name = task_name[:25] + "..."
        created = t['created_at'].split()[0] if t['created_at'] else ""

        text += f"{icon} #{t['id']} | User:{t['user_id']} | {st}\n"
        text += f"  └ {task_name} ({created})\n"

    text += "\n" + "━" * 26 + "\n"
    text += f"第 {page}/{total_pages} 页"
    if total_pages > 1:
        hint_cmd = f"/p115_tasks {status_filter} " if status_filter else "/p115_tasks "
        text += f" | 翻页: {hint_cmd}<页码>"

    await sendMessage(msg, text)


@bot.on_message(filters.command('p115_pause', prefixes) & filters.private & admins_on_filter)
async def cmd_p115_pause(_, msg):
    """暂停传输"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    await _ensure_init()
    _worker.is_running = False
    await sendMessage(msg, "⏸ 115 传输队列已暂停。")


@bot.on_message(filters.command('p115_resume', prefixes) & filters.private & admins_on_filter)
async def cmd_p115_resume(_, msg):
    """恢复传输"""
    if not _check_p115_enabled():
        return await sendMessage(msg, "❌ 115 转存功能未启用。")

    await _ensure_init()
    if not _worker.is_running:
        asyncio.create_task(_worker.start())
        await sendMessage(msg, "▶️ 115 传输队列已恢复运行。")
    else:
        await sendMessage(msg, "115 传输队列正在运行中。")

# --- 内联键盘面板与回调 ---

@bot.on_callback_query(filters.regex('^p115_panel$'))
async def cb_p115_panel(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
    if not _check_user_permission(call.from_user.id):
        return await callAnswer(call, "❌ 你没有使用 115 转存功能的权限。", show_alert=True)
        
    await callAnswer(call, "🗂️ 进入 115 网盘面板")
    text = "🗂️ **115 网盘自动转存面板**\n\n请选择你需要进行的操作："
    await editMessage(call, text, buttons=p115_panel_ikb())


@bot.on_callback_query(filters.regex('^p115_cb_status$'))
async def cb_p115_status(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
    if not _check_user_permission(call.from_user.id):
        return await callAnswer(call, "❌ 无权限。", show_alert=True)

    await callAnswer(call, "📊 获取任务状态...")
    await _ensure_init()
    user_id = call.from_user.id
    tasks = await _db.get_pending_tasks()

    if not judge_admins(user_id):
        tasks = [t for t in tasks if t['user_id'] == user_id]

    if not tasks:
        text = "当前没有正在进行或排队的任务。"
    else:
        text = "📊 **115 任务进度预览：**\n\n"
        for t in tasks:
            icon = "⏳" if t['status'] in ('PENDING', 'DOWNLOADING_115', 'OFFLINING_115') else "🚀"
            text += f"{icon} ID: {t['id']} | {t['status']} | {t['task_name'] or '解析中...'}\n"
            if t['status'] == 'TRANSFERRING' and t['id'] in _worker.active_progress:
                text += f"  └ {_worker.active_progress[t['id']]}\n"
    
    await editMessage(call, text, buttons=p115_panel_ikb())


@bot.on_callback_query(filters.regex('^p115_cb_history$'))
async def cb_p115_history(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
    if not _check_user_permission(call.from_user.id):
        return await callAnswer(call, "❌ 无权限。", show_alert=True)

    await callAnswer(call, "🗂️ 获取历史任务...")
    await _ensure_init()
    user_id = call.from_user.id
    
    # 简单展示第一页的10条历史任务
    limit = 10
    tasks = await _db.get_user_tasks(user_id, limit=limit, offset=0)
    total_count = await _db.count_user_total_tasks(user_id)

    if not tasks:
        text = "你还没有添加过任何 115 任务。"
    else:
        text = f"🗂 **你的 115 最近历史任务** (共 {total_count} 个)：\n\n"
        for t in tasks:
            if t['status'] == 'DONE':
                icon = "✅"
            elif t['status'] == 'FAILED':
                icon = "❌"
            elif t['status'] == 'CANCELED':
                icon = "🛑"
            elif t['status'] in ('PENDING', 'DOWNLOADING_115', 'OFFLINING_115'):
                icon = "⏳"
            else:
                icon = "🚀"

            create_time = t['created_at'].split()[0] if t['created_at'] else "未知时间"
            task_name = t['task_name'] or "未命名任务"
            if len(task_name) > 30:
                task_name = task_name[:27] + "..."

            text += f"{icon} ID: {t['id']} | {create_time} | {t['status']}\n"
            text += f"  └ {task_name}\n\n"
        
        if total_count > limit:
            text += "提示: 更多历史记录请使用命令 `/p115_history <页码>` 查看。"

    await editMessage(call, text, buttons=p115_panel_ikb())


@bot.on_callback_query(filters.regex('^p115_cb_myquota$'))
async def cb_p115_myquota(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
    if not _check_user_permission(call.from_user.id):
        return await callAnswer(call, "❌ 无权限。", show_alert=True)

    await callAnswer(call, "获取配额信息...")
    user_id = call.from_user.id

    extra_quota = await _db.get_user_extra_quota(user_id)
    total_allowed = p115_config.max_total_tasks + extra_quota
    total_used = await _db.count_user_total_tasks(user_id)
    remaining = max(0, total_allowed - total_used)
    pending_count = await _db.count_user_pending_tasks(user_id)

    text = (f"📊 **你的 115 任务配额：**\n\n"
            f"📦 **总任务配额:** {total_used} / {total_allowed}\n"
            f"   └ 剩余可提交: **{remaining}** 次\n"
            f"   └ 基础额度 {p115_config.max_total_tasks} + 额外购买 {extra_quota}\n\n"
            f"⏳ **当前并发排队:** {pending_count} / {p115_config.max_pending_tasks}\n"
            f"💾 **单文件大小限制:** {p115_config.max_file_size_gb} GB\n\n"
            f"💡 提示: 点击“🛒 购买配额”可使用积分增加总次数上限。")
    await editMessage(call, text, buttons=p115_panel_ikb())


@bot.on_callback_query(filters.regex('^p115_cb_cancel$'))
async def cb_p115_cancel(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
    if not _check_user_permission(call.from_user.id):
        return await callAnswer(call, "❌ 无权限。", show_alert=True)
        
    await callAnswer(call, "🛑 取消任务")
    await editMessage(call, "🛑 请在 60 秒内输入要取消的任务 ID (仅限你自己的任务)：\n\n输入 `/cancel` 取消操作。", buttons=p115_panel_ikb())
    
    txt = await callListen(call, 60, buttons=p115_panel_ikb())
    if txt is False:
        return
    await txt.delete()
    if txt.text == '/cancel':
        return await editMessage(call, "✅ 已取消操作", buttons=p115_panel_ikb())
        
    if not txt.text.isdigit():
        return await editMessage(call, "❌ 输入错误，任务 ID 必须是数字。", buttons=p115_panel_ikb())
        
    await _ensure_init()
    user_id = call.from_user.id
    task_id = int(txt.text)
    task = await _db.get_task(task_id)
    
    if not task or (task['user_id'] != user_id and not judge_admins(user_id)):
        return await editMessage(call, "❌ 任务不存在或无权操作。", buttons=p115_panel_ikb())

    if task['status'] in ('DONE', 'FAILED', 'CANCELED'):
        return await editMessage(call, f"❌ 任务已处于 {task['status']} 状态。", buttons=p115_panel_ikb())

    await _db.update_task(task_id, status='CANCELED')
    import shutil
    from pathlib import Path
    temp_path = Path(f"data/downloads/task_{task_id}")
    if temp_path.exists():
        shutil.rmtree(temp_path)
    
    await editMessage(call, f"✅ 任务 {task_id} 已成功取消。", buttons=p115_panel_ikb())


@bot.on_callback_query(filters.regex('^p115_cb_buy$'))
async def cb_p115_buy(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
    if not p115_config.allow_buy:
        return await callAnswer(call, "❌ 管理员未开启配额购买功能。", show_alert=True)
    if not _check_user_permission(call.from_user.id):
        return await callAnswer(call, "❌ 无权限。", show_alert=True)

    await callAnswer(call, "🛒 购买配额")
    await _ensure_init()
    user_id = call.from_user.id
    emby_data = sql_get_emby(user_id)
    if not emby_data:
        return await editMessage(call, "❌ 找不到你的账号信息。", buttons=p115_panel_ikb())

    price = p115_config.task_price
    amount = p115_config.tasks_per_purchase
    extra_quota = await _db.get_user_extra_quota(user_id)
    current_total = p115_config.max_total_tasks + extra_quota

    text = (f"🛒 **购买 115 任务配额**\n\n"
            f"当前你的积分: **{emby_data.iv}**\n"
            f"当前总配额: **{current_total}** (默认 {p115_config.max_total_tasks} + 额外 {extra_quota})\n\n"
            f"价格: **{price}** 积分 / **{amount}** 个任务配额\n\n"
            f"⚠️ 请回复 `确认购买` 以扣除积分，回复 `/cancel` 取消操作。")
            
    await editMessage(call, text, buttons=p115_panel_ikb())
    
    txt = await callListen(call, 60, buttons=p115_panel_ikb())
    if txt is False:
        return
    await txt.delete()
    
    if txt.text == '/cancel':
        return await editMessage(call, "✅ 已取消购买", buttons=p115_panel_ikb())
        
    if txt.text == '确认购买':
        emby_data = sql_get_emby(user_id) # 重新获取最新积分
        if emby_data.iv < price:
            return await editMessage(call, f"❌ 积分不足！购买需要 {price} 积分，你当前有 {emby_data.iv} 积分。", buttons=p115_panel_ikb())
        
        new_iv = emby_data.iv - price
        if sql_update_emby(Emby.tg == user_id, iv=new_iv):
            await _db.add_user_extra_quota(user_id, amount)
            new_extra = await _db.get_user_extra_quota(user_id)
            new_total = p115_config.max_total_tasks + new_extra
            success_text = (f"✅ **购买成功！**\n\n"
                            f"扣除 {price} 积分，当前剩余: {new_iv} 积分\n"
                            f"新增 {amount} 个配额，当前总配额: {new_total} 个")
            await editMessage(call, success_text, buttons=p115_panel_ikb())
            logger.info(f"用户 {user_id} 面板内花费 {price} 积分购买了 {amount} 个 115 任务配额")
        else:
            await editMessage(call, "❌ 数据库更新积分失败，请联系管理员。", buttons=p115_panel_ikb())
    else:
        await editMessage(call, "❌ 未输入确认口令，已取消。", buttons=p115_panel_ikb())


# --- 管理员面板与回调 ---

@bot.on_callback_query(filters.regex('^p115_admin_panel$') & admins_on_filter)
async def cb_p115_admin_panel(_, call):
    await callAnswer(call, "🗂️ 115网盘管理面板")
    await editMessage(call, "🗂️ **115网盘管理面板**\n\n请选择你要进行的操作：", buttons=p115_admin_panel_ikb())

@bot.on_callback_query(filters.regex('^p115_cb_toggle$') & admins_on_filter)
async def cb_p115_toggle(_, call):
    p115_config.status = not p115_config.status
    save_config()
    
    status_text = "已开启" if p115_config.status else "已关闭"
    await callAnswer(call, f"✅ 115 转存功能{status_text}", show_alert=True)
    await editMessage(call, "🗂️ **115网盘管理面板**\n\n请选择你要进行的操作：", buttons=p115_admin_panel_ikb())

@bot.on_callback_query(filters.regex('^p115_cb_check$') & admins_on_filter)
async def cb_p115_check(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
        
    await callAnswer(call, "正在检查 115 登录状态...")
    await _ensure_init()
    try:
        client = await _p115._get_client()
        user_info = await client.user_info_open(async_=True)
        if user_info and user_info.get("state"):
            text = "✅ 115 状态正常：已登录"
        else:
            text = "❌ 115 状态异常：Token 可能已失效，请重新设置"
    except Exception as e:
        logger.error(f"检查 115 状态失败: {e}")
        text = f"❌ 115 状态异常：{e}"
        
    await editMessage(call, text, buttons=p115_admin_panel_ikb())

@bot.on_callback_query(filters.regex('^p115_cb_quota$') & admins_on_filter)
async def cb_p115_quota(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
        
    await callAnswer(call, "获取离线配额...")
    await _ensure_init()
    quota = await _p115.get_offline_quota()
    if quota is None:
        text = "❌ 无法获取配额信息，请检查 Token 状态。"
    else:
        text = (f"📊 **115 离线下载配额：**\n"
                f"  总配额: {quota['total']}\n"
                f"  已使用: {quota['used']}\n"
                f"  剩余: {quota['remaining']}")
                
    await editMessage(call, text, buttons=p115_admin_panel_ikb())

@bot.on_callback_query(filters.regex('^p115_cb_token$') & admins_on_filter)
async def cb_p115_token(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
        
    await callAnswer(call, "🔑 设置Token")
    await editMessage(call, "🔑 请在 60 秒内发送新的 Token:\n\n格式：`<access_token> <refresh_token>`\n\n回复 `/cancel` 取消操作。", buttons=p115_admin_panel_ikb())
    
    txt = await callListen(call, 60, buttons=p115_admin_panel_ikb())
    if txt is False:
        return
    await txt.delete()
    if txt.text == '/cancel':
        return await editMessage(call, "✅ 已取消设置", buttons=p115_admin_panel_ikb())
        
    parts = txt.text.split(maxsplit=1)
    if len(parts) < 2:
        return await editMessage(call, "❌ 格式错误。格式需为：`<access_token> <refresh_token>`", buttons=p115_admin_panel_ikb())
        
    access_token = parts[0].strip()
    refresh_token = parts[1].strip()
    
    await _ensure_init()
    try:
        await _db.set_setting("p115_access_token", access_token)
        await _db.set_setting("p115_refresh_token", refresh_token)
        await editMessage(call, "✅ 115 Token 已保存", buttons=p115_admin_panel_ikb())
        logger.info(f"管理员 {call.from_user.id} 面板设置了 115 Token")
    except Exception as e:
        await editMessage(call, f"❌ 保存失败: {e}", buttons=p115_admin_panel_ikb())
        logger.error(f"保存 115 Token 失败: {e}")

@bot.on_callback_query(filters.regex('^p115_cb_queue$') & admins_on_filter)
async def cb_p115_queue(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
        
    await callAnswer(call, "获取全局队列...")
    await _ensure_init()
    tasks = await _db.get_pending_tasks()
    if not tasks:
        text = "当前 115 队列为空。"
    else:
        text = "📋 **115 全局队列状态：**\n"
        for t in tasks:
            text += f"ID: {t['id']} | User: {t['user_id']} | 状态: {t['status']} | {t['task_name'] or '未知'}\n"
            
    await editMessage(call, text, buttons=p115_admin_panel_ikb())

@bot.on_callback_query(filters.regex('^p115_cb_pause$') & admins_on_filter)
async def cb_p115_pause(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
        
    await callAnswer(call, "⏸ 暂停队列", show_alert=True)
    await _ensure_init()
    _worker.is_running = False
    await editMessage(call, "⏸ 115 传输队列已暂停。", buttons=p115_admin_panel_ikb())

@bot.on_callback_query(filters.regex('^p115_cb_resume$') & admins_on_filter)
async def cb_p115_resume(_, call):
    if not _check_p115_enabled():
        return await callAnswer(call, "❌ 115 转存功能未启用。", show_alert=True)
        
    await callAnswer(call, "▶️ 恢复队列", show_alert=True)
    await _ensure_init()
    if not _worker.is_running:
        asyncio.create_task(_worker.start())
        await editMessage(call, "▶️ 115 传输队列已恢复运行。", buttons=p115_admin_panel_ikb())
    else:
        await editMessage(call, "▶️ 115 传输队列正在运行中，无需恢复。", buttons=p115_admin_panel_ikb())

