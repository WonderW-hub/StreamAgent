# 使用官方轻量级 Python 3.12 镜像
FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量：防止 Python 缓冲 stdout/stderr（对日志流极其重要）
# 设置 PYTHONPATH 确保框架内的绝对路径导入生效
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# 安装系统级基础依赖（比如 C++ 编译环境，如果你的某些包需要）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 优先拷贝依赖清单并安装，利用 Docker 缓存层加速构建
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 将整个项目代码拷贝进容器
COPY . .

# 默认入口设为 python，具体执行的脚本由 docker-compose 动态传入
ENTRYPOINT ["python"]