# Telegram Bot（可选）

核心面板完全不依赖此模块。运行 `sudo ./install.sh --with-tgbot` 安装服务模板。
将 `tgbot.env.example` 复制为本目录的 `tgbot.env`，填写 Token 和授权用户数字 ID；配置权限应为 `0600`。空授权名单拒绝所有请求，仅接受授权用户的私人聊天。

先运行面板的 `--init-admin`，再执行 `sudo systemctl enable --now colabbb-tgbot`。Bot 通过本机面板 IPC 使用同一任务队列与摄影灵感库，启动时通过 `getMe` 获取实际用户名。

命令：`/help`、`/image 提示词`、`/video 分镜`、`/llm 创作要求`、`/random 1~10`、`/status`。任务结果与文件在 Web 工作台中查看；不向频道同步、不自动发送生成文件。

`COLABBB_PANEL_URL` 必须为本机 HTTP 地址；不要将 IPC Token 发往远程地址。更改面板端口后同步更新此变量。
