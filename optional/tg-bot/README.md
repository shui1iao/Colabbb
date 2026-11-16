# Telegram Bot（可选，纯 Docker Compose）

无需宿主 Python 或独立服务。先完成主 README 的 Compose 三步启动与管理员初始化，然后在根目录 `.env` 配置：

```dotenv
COLABBB_ENABLE_TGBOT=1
TG_BOT_TOKEN=
TG_ALLOWED_USERS=
```

从 Bot 管理工具取得 Token；在 `TG_ALLOWED_USERS` 填以逗号分隔的授权数字用户 ID。保持 `.env` 为 `0600`，切勿提交 Token。空授权名单拒绝所有请求，只接受授权用户的私人聊天。

```bash
docker compose up -d --force-recreate
docker compose logs -f colabbb
```

入口在容器内后台启动 `tg_bot.py`，通过本机 IPC 读取 `/data/state/panel_auth.json`，复用面板的相同队列与随机灵感库。Bot 用 `getMe` 获取真实用户名；不需要额外映射端口或挂载。容器中任一启用进程退出会一并重启。

命令：`/help`、`/image 提示词`、`/video 分镜`、`/llm 创作要求`、`/random 1~10`、`/status`。结果在 Web 工作台查看；不向频道同步、不自动发送生成文件。

通常无需配置 `COLABBB_PANEL_URL`，它自动使用容器回环面板端口。若覆盖，只允许本机 HTTP 地址，不要把 IPC Token 发送到远程地址；宿主映射端口不改变容器面板端口 `17860`。模块的 `tgbot.env.example` 仅为字段参考，Compose 统一从根目录 `.env` 读取。

禁用：在 `.env` 设 `COLABBB_ENABLE_TGBOT=0`，再执行 `docker compose up -d --force-recreate`。核心面板不依赖 Bot，授权数据和 Colab 登录态仍在同一 `./data` 卷中。
