# Colabbb v0.1.0

Colabbb 是面向个人自托管的 AI 视觉创作工作台：用统一的中文 Web 界面管理远端 GPU 会话、3K 二阶段生图、24fps 声画视频、提示词对话与作品库。支持自适应深浅色主题、流畅切页动效、可视化工作流参数配置与透明矢量品牌图标。媒体与提示词各有独立 FIFO 队列，支持取消、任务记录与闲置关机租约。

源码使用 [MIT License](LICENSE)。不包含账号、密钥、模型权重、运行记录或生成作品；模型、上游 CLI 与其他依赖遵循各自许可证和服务条款。

## 架构

```text
浏览器 ── HTTPS 反代（建议）── Web 面板 / 登录鉴权
                                  │
Telegram Bot（可选）── 本机 IPC ────┤ JOB 双通道队列
                                  └─ colab-ai 总控 CLI
                                      ├─ Colab CLI：会话与远端脚本运行
                                      ├─ idle_guard：闲置保护与租约
                                      └─ SSH 回连的本机端口
                                          ├─ ComfyUI → 图像 / 视频
                                          ├─ llama-server → 提示词
                                          └─ metrics → 资源采样
ComfyUI TCP 代理（可选）───────── 本机 ComfyUI 隧道端口
```

`ui/panel_ui.html` 是唯一权威前端模板，`web_panel.py` 从磁盘读取，不内嵌重复 HTML。`state/panel_ui.html` 可热覆盖完整模板；自行改动脚本时必须同步重算 CSP 的 SHA-256。`ui/icons/` 保存原始透明图标。

## 快速部署

要求：Linux、Python 3.10+、OpenSSH 客户端；systemd 用于安装服务。离线验证另外需要 Node.js。宿主机需安装并完成 `google-colab-cli` 的登录授权，且其 Python 环境可以导入 `colab_cli`；请按该上游工具的当前说明配置 GPU 配额与账户。

```bash
# 解压源码后进入目录
chmod +x install.sh colab-ai
sudo ./install.sh --username admin
# 交互输入至少12字符的管理员密码；脚本不安装或启动 GPU 作业
sudo editor /opt/colabbb/config.env
# 填写 COLABBB_TUNNEL_HOST 等，然后安装受限隧道公钥（见下一节）
sudo systemctl enable --now colabbb-panel
```

默认安装到 `/opt/colabbb`。使用 `--prefix /path/to/colabbb` 可自定义路径；模板会相应改写。已有 `config.env`、`state/` 和 `outputs/` 不会被覆盖；已有其他位置的 `colab-ai` 系统命令不会被强行替换。

非 systemd 环境可用 `--no-services --no-bin-link`，之后在安装目录运行 `python3 web_panel.py`。所有模块均可通过环境变量覆盖 `COLABBB_HOME`、`COLABBB_STATE_DIR`、`COLABBB_OUTPUT_DIR`；配置文件默认读取安装目录 `config.env`，也可通过 `COLABBB_CONFIG_FILE` 指定。环境变量优先。

默认面板监听 `0.0.0.0:17860`。建议改为 `127.0.0.1` 并通过带 TLS 的反向代理访问；公网不要使用明文 HTTP 传输登录信息。反代应覆盖而非透传客户端伪造的 `X-Forwarded-For` / `X-Forwarded-Proto`，不得转发外部 `X-G4-Internal` 头。

### 管理员初始化 / 重置

没有默认密码，也没有免密模式。没有 `state/panel_auth.json` 时可以看到登录页和图标，但所有 API 均拒绝访问，页面明确提示初始化。

```bash
cd /opt/colabbb
python3 web_panel.py --init-admin --username admin
# 或 ./install.sh init-admin --username admin
```

无人值守初始化支持 `COLABBB_ADMIN_USER` 与 `COLABBB_ADMIN_PASS` 环境变量；不要将真实密码写入示例、命令行参数或 Git。设置 PBKDF2-HMAC-SHA256 哈希后以 `0600` 保存认证文件，重新初始化会轮换会话签名密钥与内部 IPC Token，使旧 Cookie 失效。建议在停止面板后重置并重新启动。`--skip-admin` 只跳过安装阶段交互，不会放行登录。

## 反向 SSH 隧道

在 `config.env` 填写远端 GPU 能访问的宿主 SSH 地址：

```dotenv
COLABBB_TUNNEL_HOST=gpu-gateway.example.com
COLABBB_TUNNEL_PORT=22
COLABBB_TUNNEL_USER=root
```

`install.sh` 或首次远端运行的 `colab-ai` 自动生成 `state/tunnel_ed25519`；已有私钥不会重建。安装输出以下形式的公钥授权行（必须使用实际打印的公钥）：

```text
restrict,port-forwarding,command="/bin/false" ssh-ed25519 YOUR_GENERATED_PUBLIC_KEY colabbb-tunnel
```

将它加入 **上述 SSH 宿主对应用户** 的 `~/.ssh/authorized_keys`。只添加公钥，禁止上传私钥到代码仓库。确保 SSH 目录/文件权限分别为 `0700` / `0600`，sshd 允许 TCP 转发；建议专用隧道用户，`GatewayPorts no`。可额外用 `permitlisten` 限制仅监听 `127.0.0.1:28188`、`127.0.0.1:28189`、`127.0.0.1:28190`。强制命令阻止交互式 shell，但允许 `ssh -N` 反向转发。

总控读取本机私钥，编码为 `COLABBB_TUNNEL_KEY_B64`，连同宿主/端口/用户，经 `colab exec --env ...` 注入既有会话中的远端脚本（保留总控的会话复用语义；上游 `run` 会创建新虚拟机，不是 `exec` 的等价别名）；源码不携带固定私钥。远端当前使用自动接纳 SSH 主机密钥的模式，部署时应在受信网络核验宿主并按自身安全策略加固；只在可信 GPU 运行环境使用。私钥会出现在受信的本机子进程参数与远端环境中，勿开放宿主进程读取权限。

## 功能与配置

- **图片**：内部兼容键 `v5_int8`；三种画幅（竖/横/方），三档步数，默认二阶段 3K；支持参考图和工作流参数配置。
- **视频**：24fps 声画输出；`标准 832p / 超清 1088p / 2K 精修`；`2:3 / 3:2 / 1:1`；可选首帧参考图。
- **提示词大师**：专业写实摄影 / 电影感视觉描述，保持 `<提示词>...</提示词>` 输出提取约定；系统提示词与采样参数可在面板设置中编辑，保存在 `state/studio_settings.json`。
- **随机生图**：每次 1～10 张，从 16 组摄影主题不重复取样；已就绪的 LLM 优先扩写，否则立即使用完整内置提示词，直接提交同一 JOB 图像队列，无需任何外部业务模块。
- **模型配置**：显示名称用 `COLABBB_IMAGE_LABEL`、`COLABBB_VIDEO_LABEL`、`COLABBB_LLM_LABEL`；仓库、路径、文件名用配置模板中的 `COLABBB_IMAGE_*`、`COLABBB_VIDEO_*`、`COLABBB_LLM_*`。仅替换仓库不能保证任意模型架构兼容，需匹配现有 ComfyUI 节点与工作流；`INT8` / `27B` 是兼容界面标签，示例权重的实际量化与参数量以模型文件为准。默认生图公开示例为 FP8 权重，LLM 示例为 Qwen3-30B-A3B GGUF。下载/显存需求和权重许可须在首次生成前确认。
- **Token**：可选 `HF_TOKEN` / `CIVITAI_TOKEN`，仅在下载需要鉴权的权重时填写；不要提交真实 `*.env`。
- **闲置关机**：`COLAB_IDLE_MIN` 控制面板/会话的闲置分钟数，租约保护长任务；Bot 默认活跃续期使用更短窗口，可设置 `COLABBB_TG_IDLE_MIN`。

核心面板仅使用 Python 标准库。缩略图优化可选安装 Pillow，未安装时仍可读取原图。远端脚本会按需准备 ComfyUI、模型和推理依赖，首次加载可能较长；需要网络与合适 GPU。本仓库的离线测试不证明特定 GPU 性能、远端模型可下载性或上游服务可用性。

## 可选模块（默认不安装服务）

### Telegram Bot

`sudo ./install.sh --with-tgbot` 安装服务模板；按照 [模块文档](optional/tg-bot/README.md) 填写 Token 和授权用户。授权名单为空时全部拒绝。设置完成后 `sudo systemctl enable --now colabbb-tgbot`。Bot 通过本机 IPC 复用队列，不同步频道、不自动上传文件。

### ComfyUI TCP 代理

`sudo ./install.sh --with-comfy-proxy` 安装服务模板。`COLABBB_COMFY_PROXY_HOST/PORT` 控制监听，`COLABBB_COMFY_TARGET_HOST/PORT` 控制目标隧道；流量会刷新闲置活跃时间。**代理本身没有鉴权**，配置模板默认仅监听本机；如需公网访问，应先配置带认证的 HTTPS 反代、VPN 或防火墙白名单，再启动 `colabbb-comfy-proxy`。

面板右上角 ComfyUI 链接：`COLABBB_COMFY_URL` 为完整 `http(s)` URL 时使用指定地址；留空使用当前页面主机的 `18188` 端口；设为 `hide` 或 `off` 隐藏按钮。

禁用选装模块只需 `systemctl disable --now colabbb-tgbot` / `colabbb-comfy-proxy`；不影响核心创作与队列。安装脚本不会自动启动服务，也不会修改防火墙、sshd 或 `authorized_keys`。

## 本地验证

```bash
python3 tests/smoke_test.py
```

使用临时隔离目录与随机本机端口，执行全部 Python 编译、Bash 语法、内联 JS `node --check`、精确 CSP 哈希验证、随机灵感离线单元测试、管理员初始化、真实 HTTP 登录/Cookie/API/图标/热覆盖/退出测试。不发送 Telegram 请求、不执行 Colab GPU 任务，也不创建系统服务。

## 源码布局

```text
colab-ai                 总控、配置注入、会话与媒体执行
web_panel.py             登录、API、双通道 JOB 与随机灵感
idle_guard.py            闲置关机与租约保护
remote_*.py              图像/视频/LLM/指标/卸载远端执行器
ui/                      权威工作台模板与透明品牌图标
optional/                Bot 与 ComfyUI 代理
systemd/                 核心与可选模块的独立服务模板
tests/smoke_test.py       无 GPU 离线与 HTTP 端到端验证
config.env.example       不含真实凭据的统一配置模板
```

源码发布包不包括 `.git`、缓存、真实配置、`state/`、`outputs/` 或模型文件；运行数据始终留在部署机器上。
