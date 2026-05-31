"""
115 任务数据库 — 使用独立 SQLite (与 Sakura 主 MySQL 隔离)

保留 tasks / task_files / settings 表结构，移除 users 表（权限由 Sakura emby 表管理）。
"""
import aiosqlite
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path("data/p115_bot.db")

# --- 列名白名单，防止 SQL 注入 ---
_TASK_COLUMNS = frozenset({
    'status', 'task_name', 'task_hash', 'msg_id',
    'remote_path', 'error_msg',
})
_TASK_FILE_COLUMNS = frozenset({
    'status', 'error_msg',
})


async def init_db():
    """初始化数据库表结构"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        # 1. 任务表
        # status: PENDING, OFFLINING_115, DOWNLOADING_115, WAITING_LOCAL_TRANSFER,
        #         TRANSFERRING, DONE, FAILED, CANCELED, PAUSED
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                source_url TEXT,
                task_name TEXT,
                task_hash TEXT,
                status TEXT DEFAULT 'PENDING',
                msg_id INTEGER,
                remote_path TEXT,
                error_msg TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. 任务文件表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS task_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                file_id TEXT,
                pickcode TEXT,
                file_path TEXT,
                file_size INTEGER DEFAULT 0,
                status TEXT DEFAULT 'PENDING',
                error_msg TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(task_id) REFERENCES tasks(id)
            )
        """)

        # 3. 配置表 (存储 115 Token 等)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # 4. 用户购买配额表
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_quotas (
                user_id INTEGER PRIMARY KEY,
                extra_tasks INTEGER DEFAULT 0
            )
        """)

        # 5. 高频查询索引
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_task_files_task_id ON task_files(task_id)")

        await db.commit()


def _validate_columns(kwargs: dict, allowed: frozenset, table: str):
    """校验更新列名是否在白名单中，防止 SQL 注入"""
    bad = set(kwargs.keys()) - allowed
    if bad:
        raise ValueError(f"不允许更新 {table} 表的列: {bad}")


class Database:
    """异步 SQLite 数据库封装，使用持久连接"""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._conn: aiosqlite.Connection | None = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            # WAL 模式：大幅提升并发读写性能
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")
        return self._conn

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ===================== tasks =====================

    async def add_task(self, user_id: int, source_url: str, task_name: str = "") -> int:
        conn = await self._get_conn()
        cursor = await conn.execute(
            "INSERT INTO tasks (user_id, source_url, task_name) VALUES (?, ?, ?)",
            (user_id, source_url, task_name),
        )
        task_id = cursor.lastrowid
        await conn.commit()
        return task_id

    async def update_task(self, task_id: int, **kwargs):
        if not kwargs:
            return
        _validate_columns(kwargs, _TASK_COLUMNS, "tasks")
        keys = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [task_id]
        conn = await self._get_conn()
        await conn.execute(
            f"UPDATE tasks SET {keys}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values,
        )
        await conn.commit()

    async def get_task(self, task_id: int):
        conn = await self._get_conn()
        async with conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cursor:
            return await cursor.fetchone()

    async def get_pending_tasks(self):
        """获取所有未完成的任务"""
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT * FROM tasks WHERE status NOT IN ('DONE', 'FAILED', 'CANCELED') ORDER BY created_at ASC"
        ) as cursor:
            return await cursor.fetchall()

    async def count_user_pending_tasks(self, user_id: int) -> int:
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE user_id = ? AND status NOT IN ('DONE', 'FAILED', 'CANCELED')",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def count_user_total_tasks(self, user_id: int) -> int:
        """统计用户创建过的所有任务数（含已完成/失败/取消）"""
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_user_tasks(self, user_id: int, limit: int = 10, offset: int = 0):
        """获取用户的历史任务列表（按创建时间倒序）"""
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT * FROM tasks WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        ) as cursor:
            return await cursor.fetchall()

    async def get_all_tasks(self, limit: int = 20, offset: int = 0, status_filter: str = None):
        """获取所有用户的任务列表（管理员用，按创建时间倒序）"""
        conn = await self._get_conn()
        if status_filter:
            async with conn.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (status_filter, limit, offset),
            ) as cursor:
                return await cursor.fetchall()
        else:
            async with conn.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ) as cursor:
                return await cursor.fetchall()

    async def count_all_tasks(self, status_filter: str = None) -> int:
        """统计全部任务数"""
        conn = await self._get_conn()
        if status_filter:
            async with conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status = ?", (status_filter,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        else:
            async with conn.execute("SELECT COUNT(*) FROM tasks") as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    # ===================== task_files =====================

    async def add_task_file(self, task_id: int, file_id: str, pickcode: str, file_path: str, file_size: int):
        conn = await self._get_conn()
        await conn.execute(
            "INSERT INTO task_files (task_id, file_id, pickcode, file_path, file_size) VALUES (?, ?, ?, ?, ?)",
            (task_id, file_id, pickcode, file_path, file_size),
        )
        await conn.commit()

    async def add_task_files_batch(self, task_id: int, files: list[dict]):
        """批量插入任务文件（单次 commit，避免逐条 fsync）"""
        if not files:
            return
        conn = await self._get_conn()
        await conn.executemany(
            "INSERT INTO task_files (task_id, file_id, pickcode, file_path, file_size) "
            "VALUES (?, ?, ?, ?, ?)",
            [(task_id, f['file_id'], f['pickcode'], f['path'], f['size']) for f in files],
        )
        await conn.commit()

    async def get_task_files(self, task_id: int):
        conn = await self._get_conn()
        async with conn.execute("SELECT * FROM task_files WHERE task_id = ?", (task_id,)) as cursor:
            return await cursor.fetchall()

    async def update_task_file(self, file_id: int, **kwargs):
        if not kwargs:
            return
        _validate_columns(kwargs, _TASK_FILE_COLUMNS, "task_files")
        keys = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [file_id]
        conn = await self._get_conn()
        await conn.execute(
            f"UPDATE task_files SET {keys}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values,
        )
        await conn.commit()

    # ===================== settings =====================

    async def set_setting(self, key: str, value: str):
        conn = await self._get_conn()
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=?",
            (key, value, value),
        )
        await conn.commit()

    async def get_setting(self, key: str):
        conn = await self._get_conn()
        async with conn.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    # ===================== user_quotas =====================

    async def get_user_extra_quota(self, user_id: int) -> int:
        conn = await self._get_conn()
        async with conn.execute("SELECT extra_tasks FROM user_quotas WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def add_user_extra_quota(self, user_id: int, extra_tasks: int):
        conn = await self._get_conn()
        await conn.execute(
            "INSERT INTO user_quotas (user_id, extra_tasks) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET extra_tasks = extra_tasks + ?",
            (user_id, extra_tasks, extra_tasks),
        )
        await conn.commit()
