#!/usr/bin/env bash
set -euo pipefail
umask 077
SOURCE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PREFIX=${COLABBB_HOME:-/opt/colabbb}
WITH_TG=0; WITH_PROXY=0; SERVICES=1; BIN_LINK=1; SKIP_ADMIN=0; USERNAME=${COLABBB_ADMIN_USER:-}
INIT_ONLY=0
while (($#)); do
  case "$1" in
    --prefix) PREFIX=$2; shift 2;;
    --with-tgbot) WITH_TG=1; shift;;
    --with-comfy-proxy) WITH_PROXY=1; shift;;
    --no-services) SERVICES=0; shift;;
    --no-bin-link) BIN_LINK=0; shift;;
    --skip-admin) SKIP_ADMIN=1; shift;;
    init-admin|--init-admin) INIT_ONLY=1; shift;;
    --username) USERNAME=$2; shift 2;;
    -h|--help) echo 'Usage: install.sh [init-admin] [--username USER] [--prefix DIR] [--with-tgbot] [--with-comfy-proxy] [--no-services] [--no-bin-link] [--skip-admin]'; exit 0;;
    *) echo "未知参数: $1" >&2; exit 2;;
  esac
done
PREFIX=$(realpath -m -- "$PREFIX")
[[ "$PREFIX" != *$'\n'* && "$PREFIX" != *' '* && "$PREFIX" != *'%'* && "$PREFIX" != *'&'* && "$PREFIX" != *'|'* ]] || { echo '安装路径不能包含空格或特殊分隔符' >&2; exit 2; }
command -v python3 >/dev/null
export COLABBB_HOME="$PREFIX"
if ((INIT_ONLY)); then
  SCRIPT="$PREFIX/web_panel.py"
  [[ -f "$SCRIPT" ]] || SCRIPT="$SOURCE/web_panel.py"
  ARGS=(--init-admin); [[ -z "$USERNAME" ]] || ARGS+=(--username "$USERNAME")
  exec python3 "$SCRIPT" "${ARGS[@]}"
fi
command -v ssh-keygen >/dev/null
mkdir -p -- "$PREFIX"
if [[ "$SOURCE" != "$PREFIX" ]]; then
  python3 - "$SOURCE" "$PREFIX" <<'PY'
import pathlib, shutil, sys
src, dst = map(pathlib.Path, sys.argv[1:])
for item in src.rglob('*'):
    rel = item.relative_to(src)
    if any(x in ('.git', '__pycache__', 'state', 'outputs') for x in rel.parts): continue
    if item.is_file() and not item.name.endswith(('.env', '.pyc')):
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)
PY
fi
if [[ ! -f "$PREFIX/config.env" ]]; then
  sed "s|^COLABBB_HOME=.*|COLABBB_HOME=$PREFIX|" "$PREFIX/config.env.example" > "$PREFIX/config.env"
fi
chmod 600 "$PREFIX/config.env"
# 导入统一配置，但显式环境覆盖具有优先级。
STATE_DIR=$(python3 - "$PREFIX/config.env" <<'PY'
import os, sys
for line in open(sys.argv[1], encoding='utf-8'):
    line=line.strip()
    if line and not line.startswith('#') and '=' in line:
        k,v=line.split('=',1); os.environ.setdefault(k.strip(),v.strip().strip('"').strip("'"))
print(os.environ.get('COLABBB_STATE_DIR') or os.path.join(os.environ['COLABBB_HOME'],'state'))
PY
)
mkdir -p -- "$STATE_DIR"
chmod 700 "$STATE_DIR"
KEY="$STATE_DIR/tunnel_ed25519"
if [[ ! -f "$KEY" ]]; then
  ssh-keygen -q -t ed25519 -N '' -C 'colabbb-tunnel' -f "$KEY"
fi
chmod 600 "$KEY"
[[ -f "$KEY.pub" ]] || ssh-keygen -y -f "$KEY" > "$KEY.pub"
echo '将以下一整行加入反向隧道 SSH 宿主对应用户的 ~/.ssh/authorized_keys：'
printf 'restrict,port-forwarding,command="/bin/false" %s\n' "$(cat "$KEY.pub")"
echo '本脚本不修改 authorized_keys。请同时检查 sshd 的 AllowTcpForwarding 与 GatewayPorts 设置。'
chmod +x "$PREFIX/colab-ai" "$PREFIX/install.sh"
if ((BIN_LINK)); then
  [[ $EUID -eq 0 ]] || { echo '创建 CLI 系统链接需要 root，或使用 --no-bin-link'; exit 1; }
  if [[ -e /usr/local/bin/colab-ai || -L /usr/local/bin/colab-ai ]]; then
    [[ $(readlink -f /usr/local/bin/colab-ai) == "$PREFIX/colab-ai" ]] || { echo '现有 colab-ai 链接或程序不会被覆盖，请手动处理或使用 --no-bin-link'; exit 1; }
  else
    ln -s "$PREFIX/colab-ai" /usr/local/bin/colab-ai
  fi
fi
if ((!SKIP_ADMIN)) && [[ ! -f "$STATE_DIR/panel_auth.json" ]]; then
  ARGS=(--init-admin); [[ -z "$USERNAME" ]] || ARGS+=(--username "$USERNAME")
  python3 "$PREFIX/web_panel.py" "${ARGS[@]}"
fi
if ((SERVICES)); then
  [[ $EUID -eq 0 ]] || { echo '安装服务需要 root，或使用 --no-services'; exit 1; }
  for name in panel tgbot comfy-proxy; do
    [[ "$name" != tgbot || $WITH_TG == 1 ]] || continue
    [[ "$name" != comfy-proxy || $WITH_PROXY == 1 ]] || continue
    sed "s|/opt/colabbb|$PREFIX|g" "$PREFIX/systemd/colabbb-$name.service" > "/etc/systemd/system/colabbb-$name.service"
  done
  systemctl daemon-reload
  echo '服务模板已安装。填写配置并初始化管理员后，可运行 systemctl enable --now colabbb-panel'
fi
if ((WITH_TG)) && [[ ! -f "$PREFIX/optional/tg-bot/tgbot.env" ]]; then
  cp "$PREFIX/optional/tg-bot/tgbot.env.example" "$PREFIX/optional/tg-bot/tgbot.env"
  chmod 600 "$PREFIX/optional/tg-bot/tgbot.env"
fi
if ((WITH_PROXY)); then
  echo '代理无自带登录鉴权！请先配置防火墙或带鉴权的 HTTPS 反代，再启用 colabbb-comfy-proxy。'
fi
echo "完成。请编辑 $PREFIX/config.env，填写反向 SSH 宿主信息。"
