# Colabbb v0.2.6

面向个人自托管的 AI 视觉创作工作台：中文 Web 面板管理远端 GPU 会话、多账号并发余额与防双开切换、二阶段 3K 生图、24fps 声画视频、提示词对话与作品库。包含深浅色主题、工作流参数配置、透明品牌图标、独立 FIFO 队列、随机灵感、取消任务与闲置关机租约。

**100% Docker Compose 部署**：宿主机只需 Docker Engine 与 Compose 插件，不需要 Python、Colab CLI、systemd 服务或宿主机 SSH 配置。MIT 许可；源码不含账号、密钥、模型或生成作品。上游工具与权重各遵循其许可和服务条款。

## 三步启动

### 1. 准备 `.env`

解压源码并进入目录，然后：

```bash
cp config.env.example .env
chmod 600 .env
# 编辑 .env，只需将 COLABBB_TUNNEL_HOST 改为 GPU 可访问的公网 IP 或域名
```

保留默认 `COLABBB_TUNNEL_PORT=22222`、`COLABBB_TUNNEL_USER=tunnel`。防火墙/云安全组放行 **TCP 22222**；如经路由器需映射此端口。只有这一端口需要供 GPU 回连，**无需修改宿主机 sshd 或 authorized_keys**。

### 2. 启动容器

```bash
docker compose up -d
# 若预构建镜像尚未公开，或要使用当前源码：
# docker compose up -d --build --pull never
docker compose logs -f colabbb
```

Compose 使用 `ghcr.io/shui1iao/colabbb:latest`，同时支持 `build: .`。所有持久化数据都在 `./data:/data`，更新/重建容器不会丢失登录态与作品。不要删除 `data`；源码归档中不包含运行数据。

### 3. 登录 Colab 并初始化管理员

```bash
docker compose exec -it colabbb colab auth login
docker compose exec -it colabbb python3 /app/web_panel.py --init-admin --username admin
```

根据上游 CLI 的浏览器授权提示完成登录；管理员密码至少 12 字符，没有默认密码。未初始化时仅公开登录页/图标，API 全部拒绝访问。需要账户配额、网络与兼容 GPU 才能运行远端任务。

默认面板仅发布在宿主机 `127.0.0.1:17860`。请经受控的 HTTPS 反向代理或 VPN 访问，不要在公网明文 HTTP 输入密码。可用 `COLABBB_BIND_IP` / `COLABBB_PANEL_PORT` 调整宿主发布地址/端口；容器内始终监听 `17860`。反代应覆盖而非透传伪造的 `X-Forwarded-For` / `X-Forwarded-Proto`，并移除外部 `X-G4-Internal` 头。

无人值守首次启动可在 `.env` 临时配置 `COLABBB_ADMIN_USER`、`COLABBB_ADMIN_PASS`（至少 12 字符）。入口仅在认证文件不存在时初始化，密码只保存 PBKDF2 哈希；**初始化后删除明文变量并执行 `docker compose up -d --force-recreate`**，避免它继续留在容器配置中。也可随时执行上面的交互命令重置密码；认证文件原子替换并轮换会话签名和 IPC Token，旧 Cookie 立即失效。重置时应暂停 Bot 提交新任务。

## 容器内置隔离隧道

```text
浏览器 ── HTTPS 反代 / VPN ── 面板 :17860 ── JOB 双队列 ── colab-ai
Telegram Bot（可选、同容器）────── 本机 IPC ──────┘          │
                                                        Colab CLI
GPU 上的 ComfyUI / LLM / metrics ── ssh -N -R ── 宿主 :22222
                                                   │ Docker 映射
                                                   └─ 容器 sshd :2222
                                                      127.0.0.1:28188/28189/28190
ComfyUI TCP 代理（可选、同容器） :18188 ────────────────┘
```

入口自动生成并持久化 `/data/state/tunnel_ed25519` 与 `/data/state/ssh_host_ed25519_key`，将受限公钥授权给**容器内**无 Shell 专用账户 `tunnel`。账户无可用密码，SSH 禁用密码/PAM/TTY/X11/本地转发，只允许该账户密钥登录和三个回环地址远端转发；强制 `/bin/false` 禁止执行命令。`restrict,port-forwarding,permitlisten=...` 进一步限制授权。Host Key 重建时复用，宿主机 SSH 配置完全不受影响。`COLABBB_INTERNAL_SSHD=0` 可关闭内置 sshd；如自行使用其他 SSH 端点，需自行授权密钥并覆盖用户与端口。

`colab-ai` 将隧道地址/端口/用户、动态密钥及容器内置 sshd 的主机公钥（`COLABBB_TUNNEL_HOST_PUBKEY`）注入受信的 GPU 会话，远端默认通过 `StrictHostKeyChecking=yes` 固定校验宿主机公钥；密钥可被本机高权限进程和远端环境读取，不要共享进程读取权限或将日志/环境公开。隧道端口不是通用管理 SSH 入口，不提供 Shell。

## 登录态、权限与备份

镜像固定默认环境：

| 环境变量 | 容器值 |
|---|---|
| `COLABBB_HOME` | `/app` |
| `COLABBB_STATE_DIR` | `/data/state` |
| `COLABBB_OUTPUT_DIR` | `/data/outputs` |
| `HOME` | `/data/home` |
| `PYTHON_KEYRING_BACKEND` | `keyrings.alt.file.PlaintextKeyring` |

Colab 配置位于 `/data/home/.config/colab-cli`，keyring 位于 `/data/home/.local/share/python_keyring/keyring_pass.cfg`。入口修正现有目录为 `0700`、敏感文件为 `0600`；Colab 包装入口设 `umask 077`，即使通过 `docker compose exec` 登录，新凭据也不公开。上游 0.8.0 的 `auth` 用于 VM 内认证而非本机登录，因此包装入口提供 `colab auth login`，直接调用上游 OAuth provider 的浏览器复制授权流程并确认 keyring 写入；另补充 `colab --version`。其他参数原样交给上游 CLI。

**PlaintextKeyring 不是加密存储**：保护整个 `./data`，备份时停止容器并使用加密、受控备份位置。镜像进程以容器 root 启动独立 sshd，但无需特权模式、host 网络或 Docker socket。不要把 `data` 放进源码、镜像、Git 或公开归档。

```bash
docker compose down
# 离线备份整个 data 目录至你自己的受控位置后再启动
docker compose up -d
# 更新已发布镜像：
docker compose pull && docker compose up -d
# 从源码更新：
docker compose up -d --build --pull never
```

## 可选模块

### Telegram Bot

在同一个 `.env` 设置 `COLABBB_ENABLE_TGBOT=1`、`TG_BOT_TOKEN` 与 `TG_ALLOWED_USERS`，再执行 `docker compose up -d --force-recreate`。入口在同一容器后台管理 Bot，通过回环 IPC 复用 Web 队列；无需额外进程管理或服务。空授权名单拒绝所有请求，仅支持授权用户私人聊天。详见 [Bot 文档](optional/tg-bot/README.md)。

### ComfyUI TCP 代理

在 `.env` 设置 `COLABBB_ENABLE_COMFY_PROXY=1` 后重建容器。Compose 明确发布 `127.0.0.1:18188:18188`；代理容器内监听 `0.0.0.0:18188`，转发到内置隧道 `127.0.0.1:28188`，流量自动续期闲置租约。代理**没有认证**，不能直接暴露公网，请先配置带认证 HTTPS 反代或 VPN。可删除/注释 Compose 中此端口映射以完全不发布它。`COLABBB_COMFY_URL` 留空使用当前页面主机的 `18188` 入口，填完整 HTTPS URL 使用自定义入口，设 `hide`/`off` 隐藏按钮。

### Google Drive 加密镜像与 Telegram 频道同步

在 `.env` 设置 `COLABBB_ENABLE_DRIVE_SYNC=1`（可选配置 `COLABBB_DRIVE_AES_KEY_HEX`、`COLABBB_DRIVE_GCP_PROJECT`、`TG_SYNC_CHANNEL_ID`）后重建容器。作品图片在本地经 `AES-256-GCM`（独立随机 Nonce + 文件名混淆）全密文上传至私有云盘，面板作品库工具栏自动解锁「谷歌同步 ▾」浮层，支持「仅同步谷歌云盘」与「同步到 TG 频道（6 张相册 + 联动撤回）」分开可控。详见 [加密同步文档](optional/drive-sync/README.md)。

三个可选模块均默认关闭；设相应开关为 `0` 并重建容器即可禁用，不影响核心队列。入口监控所有启用的进程，任一异常退出会终止其他进程，由 Compose `restart: unless-stopped` 一并重启，信号通过 tini 正确回收。

## 创作与配置

- **多账号共存与安全切换**：点击面板顶栏 CU 余额胶囊展开账号管理浮层（或执行 `colab-ai accounts` / `colab-ai switch <email>`），支持面板内在线添加 Google OAuth 新账号、并发查询全部账号实时 CU 余额与总和，以及切换前自动检测并强制关机释放原账号实例防止后台双边扣费。
- **图片**：兼容键 `v5_int8`，竖/横/方画幅，三档步数与默认二阶段 3K，支持参考图（上传素材与历史生图按时间倒序混排并支持横滑无限懒加载）。
- **视频**：24fps 声画输出，832p / 1088p / 2K，三种画幅，可选首帧参考图。
- **提示词大师**：写实摄影/电影视觉描述，保留 `<提示词>...</提示词>` 提取约定；系统提示词、采样与工作流参数保存于 `/data/state/studio_settings.json`。
- **随机生图**：每次 1～10 张，从 16 组主题不重复取样；LLM 就绪则扩写，否则用内置提示词直接进入相同 JOB 队列。
- **模型覆盖**：`COLABBB_IMAGE_*` / `COLABBB_VIDEO_*` / `COLABBB_LLM_*`，详见 `config.env.example`。更换仓库不保证模型架构兼容，须匹配 ComfyUI 节点与工作流。INT8 / 27B 是兼容标签，示例实际为 FP8 生图与 Qwen3-30B-A3B GGUF；以权重说明为准。
- **下载凭据**：可选 `HF_TOKEN` / `CIVITAI_TOKEN`，不要提交真实 `.env`。远端按需下载依赖与权重，首次加载较慢。
- **闲置保护**：`COLAB_IDLE_MIN` 配置会话闲置分钟，租约保护长任务；Bot 活跃续期可用 `COLABBB_TG_IDLE_MIN` 调整。

`ui/panel_ui.html` 为唯一权威前端；`/data/state/panel_ui.html` 可热覆盖完整模板。修改内联脚本须同步更新 CSP 的 SHA-256。

## 验证与发布

部署只需 Docker；开发离线回归额外需要 Python、Node.js 与 SSH 工具（可在开发容器内使用）。

```bash
python3 tests/smoke_test.py
# 以下测试需 Docker、宿主 SSH 客户端；使用临时卷和随机回环端口，不连接 GPU/Telegram：
python3 tests/docker_e2e.py --build
```

离线测试覆盖 Python/Bash/JS 语法、CSP、Docker 配置契约、随机队列、管理员、Cookie、API、图标和模板热覆盖。Docker 实测构建镜像，检查 Colab/keyring，建立真实受限反向 SSH HTTP 隧道、拒绝 Shell/越权端口，验证面板登录和认证持久化、关闭 sshd 与清理临时容器/数据。不会替代真实 GPU 推理、模型许可/可下载性或交互 OAuth 授权验证。

GitHub Actions 在 `main` 或 `v*` tag 推送时构建 `linux/amd64` 并用内置 `GITHUB_TOKEN` 推送 GHCR `latest` 及版本 tag；未执行推送前镜像可能尚未公开。源码归档采用提交内容，不包含 `.git`、缓存或数据。

## 源码布局

```text
Dockerfile / .dockerignore / docker-compose.yml
 docker/entrypoint.sh     初始化、受限 sshd、可选模块与进程管理
 docker/colab.sh          Colab 安全写入包装入口
 docker/colab_login.py    上游 OAuth provider 登录适配
colab-ai                 总控与远端环境注入
web_panel.py             登录、API、双通道队列
idle_guard.py            闲置保护
remote_*.py              图像/视频/LLM/指标/卸载远端执行器
ui/                      权威模板与品牌图标
optional/                Bot 与 ComfyUI TCP 代理
config.env.example       无凭据的 .env 模板
 tests/smoke_test.py      离线和 HTTP 回归
 tests/docker_e2e.py      真实 Docker 与反向隧道验证
.github/workflows/       GHCR 镜像发布
```
