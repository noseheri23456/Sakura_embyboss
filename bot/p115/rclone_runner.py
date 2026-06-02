"""
Rclone 运行器 — 从 config 读取 remote 和 target path
"""
import asyncio
import re
import logging

logger = logging.getLogger(__name__)


class RcloneRunner:
    def __init__(self, remote_name: str = "GDriveRemote", target_path: str = "/115_bot"):
        self.remote_name = remote_name
        self.target_path = target_path

    async def copy_to_remote(self, local_path: str, remote_path: str = None,
                              progress_callback=None, timeout_sec: int = 21600):
        """调用 rclone copy 将本地文件复制到云端

        使用 copy 而非 move，上传成功后由调用方决定是否删除本地文件，
        避免部分上传失败时丢失本地文件。
        """
        target = f"{self.remote_name}:{self.target_path}"
        if remote_path:
            target = f"{self.remote_name}:{self.target_path}/{remote_path}"

        cmd = [
            "rclone", "copyto",
            local_path,
            target,
            "-v",
            "--stats", "3s",
            "--stats-one-line",
        ]

        for attempt in range(2):
            try:
                logger.info(f"执行 Rclone 命令 (尝试 {attempt + 1}/2): {' '.join(cmd)}")

                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                async def read_stderr(reader):
                    while True:
                        line = await reader.readline()
                        if not line:
                            break
                        line_str = line.decode('utf-8', errors='ignore').strip()
                        if line_str:
                            progress_info = self._parse_progress(line_str)
                            if progress_info and progress_callback:
                                await progress_callback(progress_info)

                try:
                    await asyncio.wait_for(
                        asyncio.gather(read_stderr(process.stderr), process.wait()),
                        timeout=timeout_sec,
                    )
                except asyncio.TimeoutError:
                    try:
                        process.kill()
                    except Exception:
                        pass
                    raise Exception(f"Rclone 上传超时（{timeout_sec}s）")

                if process.returncode != 0:
                    raise Exception(f"Rclone 执行失败，退出码: {process.returncode}")

                return True

            except Exception as e:
                if attempt == 1:
                    raise
                logger.warning(f"Rclone 上传失败，5秒后重试: {e}")
                await asyncio.sleep(5)

    def _parse_progress(self, line: str) -> dict | None:
        """解析 Rclone 输出的一行进度

        示例格式: 23.501 MiB / 23.501 MiB, 100%, 1.952 MiB/s, ETA 0s
        """
        pattern = r"([\d.]+\s\w+)\s/\s([\d.]+\s\w+),\s(\d+)%,\s([\d.]+\s\w+/s),\sETA\s([\w.]+)"
        match = re.search(pattern, line)
        if match:
            return {
                "transferred": match.group(1),
                "total": match.group(2),
                "percent": int(match.group(3)),
                "speed": match.group(4),
                "eta": match.group(5),
            }
        return None

    async def get_folder_id(self, remote_path: str) -> str | None:
        """获取云端目标文件夹的 Google Drive ID (带自动降级重试机制)"""
        import json
        import os
        
        target = f"{self.remote_name}:{self.target_path}/{remote_path}"
        
        # 1. 首选方案: 使用 --stat 获取单个目录信息 (高效)
        cmd_stat = ["rclone", "lsjson", "--stat", target]
        try:
            logger.info(f"尝试用 --stat 获取ID: {' '.join(cmd_stat)}")
            proc_stat = await asyncio.create_subprocess_exec(
                *cmd_stat, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc_stat.communicate()
            if proc_stat.returncode == 0:
                data = json.loads(stdout.decode('utf-8'))
                if isinstance(data, dict) and data.get('ID'):
                    return data.get('ID')
        except Exception as e:
            logger.warning(f"--stat 方案失败，准备降级尝试: {e}")
            
        # 2. 降级方案: 列出父目录，遍历查找 (慢，但极其稳定)
        remote_path = remote_path.replace('\\', '/')
        parent_dir = os.path.dirname(remote_path)
        folder_name = os.path.basename(remote_path)
        
        target_parent = f"{self.remote_name}:{self.target_path}/{parent_dir}" if parent_dir else f"{self.remote_name}:{self.target_path}"
        cmd_ls = ["rclone", "lsjson", target_parent]
        
        try:
            logger.info(f"降级使用父目录遍历获取ID: {' '.join(cmd_ls)}")
            proc_ls = await asyncio.create_subprocess_exec(
                *cmd_ls, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc_ls.communicate()
            if proc_ls.returncode == 0:
                items = json.loads(stdout.decode('utf-8'))
                for item in items:
                    if item.get('Name') == folder_name and item.get('IsDir'):
                        return item.get('ID')
            logger.error(f"彻底获取文件夹ID失败: {stderr.decode('utf-8')}")
        except Exception as e:
            logger.error(f"降级获取ID出错: {e}")
            
        return None
