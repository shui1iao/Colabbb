# 可选模块：Google Drive 加密镜像与 Telegram 频道同步

默认关闭（`COLABBB_ENABLE_DRIVE_SYNC=0`），面板不显示「谷歌同步」按钮，也不加载同步守护进程。

## 启用方式

在 `.env` 中配置以下参数后执行 `docker compose up -d --force-recreate`：

```dotenv
COLABBB_ENABLE_DRIVE_SYNC=1
# 可选：64 位十六进制 256-bit AES-GCM 密钥（留空则首次启动自动随机生成并持久化于 /data/state/drive-sync/config.json）
COLABBB_DRIVE_AES_KEY_HEX=
# 可选：启用了 Google Drive API 的 GCP Project ID（用于 x-goog-user-project 配额头）
COLABBB_DRIVE_GCP_PROJECT=
# 可选：固定绑定用于云盘同步的邮箱（不填默认使用首个已登录账号，切换算力号不影响云盘同步）
COLABBB_DRIVE_SYNC_EMAIL=
# 可选：云盘文件夹名称
COLABBB_DRIVE_FOLDER_NAME=Colabbb-Gallery

# 可选：若还需支持「同步到 TG 频道」（每 6 张合并相册 + 云盘删除联动撤回）
TG_BOT_TOKEN=
TG_SYNC_CHANNEL_ID=
TG_SYNC_CHANNEL_NAME=
```

## 安全与加密设计

- **全量密文上云（`AES-256-GCM`）**：每张 PNG 在本地使用独立 12B 随机 Nonce 与 256-bit 密钥加密后再上传为 `application/octet-stream`（文件名混淆为 `blob_<hmac20>.bin`），云端无明文图片头或文件名泄露。
- **请务必备份 `/data/state/drive-sync/config.json` 中的 `aes_key_hex`**：丢失主密钥将无法解密云盘中的 `.bin` 备份。
- **Web 面板双动作独立可控**：启用后作品库工具栏出现「谷歌同步 ▾」浮层，支持「仅同步谷歌云盘」或「同步到 TG 频道」分开触发。
