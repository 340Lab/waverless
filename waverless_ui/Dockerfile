# 基于 Node.js 的 Docker 镜像
FROM node:18

# 安装 pnpm
RUN npm install -g pnpm

# 设置工作目录
WORKDIR /usr/src/app

# 将项目文件复制到容器中
COPY . .

# 安装项目依赖
RUN pnpm install

# 暴露端口（如果需要）
# EXPOSE 3000

# 运行开发服务器
CMD ["pnpm", "run", "dev"]
