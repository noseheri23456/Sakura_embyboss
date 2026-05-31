# 第一阶段：构建阶段
FROM python:3.12-alpine AS builder

# 安装必要的构建依赖
RUN apk add --no-cache gcc musl-dev openssl-dev coreutils git libffi-dev

# 环境变量：不生成.pyc文件，禁用pip缓存
ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# 创建并激活虚拟环境
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
COPY requirements.txt .

# 安装 Python 包
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 清理无用的编译文件以精简依赖体积
RUN find /opt/venv -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true && \
    find /opt/venv -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true && \
    find /opt/venv -type f -name "*.pyc" -delete || true


# 第二阶段：运行阶段
FROM python:3.12-alpine

# 设置环境变量
ENV TZ=Asia/Shanghai \
    DOCKER_MODE=1 \
    PYTHONUNBUFFERED=1 \
    WORKDIR=/app \
    PATH="/opt/venv/bin:$PATH"

# 安装必要的系统运行时依赖包（已移除体积较大的构建依赖和 git）
# uvloop 等编译库可能依赖 libstdc++
RUN apk add --no-cache \
    mariadb-connector-c \
    tzdata \
    mysql-client \
    libstdc++ && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 设置默认工作目录
WORKDIR ${WORKDIR}

# 仅复制干净的虚拟环境
COPY --from=builder /opt/venv /opt/venv

# 复制本地项目代码 (已通过 .dockerignore 排除冗余文件)
COPY . .

# 保持镜像体积精简，仅保留 bot 默认图片
RUN find ./image -type f ! -name "bot2.png" -delete

# 设置启动命令
ENTRYPOINT [ "python3" ]
CMD [ "main.py" ]
