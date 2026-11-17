#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
export HOME=${HOME:-/data/home}
export COLABBB_HOME=${COLABBB_HOME:-/app}
export COLABBB_STATE_DIR=${COLABBB_STATE_DIR:-/data/state}
export COLABBB_OUTPUT_DIR=${COLABBB_OUTPUT_DIR:-/data/outputs}
export COLABBB_TUNNEL_PORT=${COLABBB_TUNNEL_PORT:-22222}
export COLABBB_TUNNEL_USER=${COLABBB_TUNNEL_USER:-tunnel}
export PYTHON_KEYRING_BACKEND=${PYTHON_KEYRING_BACKEND:-keyrings.alt.file.PlaintextKeyring}
mkdir -p "$HOME/.config/colab-cli" "$HOME/.local/share/python_keyring" "$COLABBB_STATE_DIR" "$COLABBB_OUTPUT_DIR"
chmod 700 /data "$HOME" "$COLABBB_STATE_DIR" "$COLABBB_OUTPUT_DIR"
# Includes credentials created during an earlier container lifetime.
find "$HOME" "$COLABBB_STATE_DIR" -type d -exec chmod 700 {} +
find "$HOME" "$COLABBB_STATE_DIR" -type f -exec chmod 600 {} +

key="$COLABBB_STATE_DIR/tunnel_ed25519"
if [[ ! -f "$key" ]]; then
    ssh-keygen -q -t ed25519 -N '' -C colabbb-tunnel -f "$key"
fi
chmod 600 "$key"
ssh-keygen -y -f "$key" > "$key.pub"
chmod 600 "$key.pub"

if [[ ! -f "$COLABBB_STATE_DIR/panel_auth.json" ]]; then
    if [[ -n ${COLABBB_ADMIN_USER:-} && -n ${COLABBB_ADMIN_PASS:-} ]]; then
        python3 "$COLABBB_HOME/web_panel.py" --init-admin --username "$COLABBB_ADMIN_USER"
        echo '管理员已初始化；请从 .env 清除 COLABBB_ADMIN_PASS，并重新创建容器以清除明文环境变量。'
    else
        echo '尚未初始化管理员：所有 API 保持拒绝访问；请用 docker compose exec 执行 --init-admin。'
    fi
fi
# Do not pass bootstrap credentials into the long-running application processes.
unset COLABBB_ADMIN_PASS COLABBB_ADMIN_USER

pids=()
shutdown() {
    trap - TERM INT
    if ((${#pids[@]})); then
        kill -TERM "${pids[@]}" 2>/dev/null || true
        wait "${pids[@]}" 2>/dev/null || true
    fi
}
trap 'shutdown; exit 143' TERM
trap 'shutdown; exit 130' INT

if [[ ${COLABBB_INTERNAL_SSHD:-1} != 0 ]]; then
    host_key="$COLABBB_STATE_DIR/ssh_host_ed25519_key"
    if [[ ! -f "$host_key" ]]; then
        ssh-keygen -q -t ed25519 -N '' -C colabbb-host -f "$host_key"
    fi
    chmod 600 "$host_key" "$host_key.pub"
    install -d -m 755 /run/sshd
    install -d -o tunnel -g tunnel -m 700 /home/tunnel/.ssh
    printf 'restrict,port-forwarding,permitlisten="127.0.0.1:28188",permitlisten="127.0.0.1:28189",permitlisten="127.0.0.1:28190",command="/bin/false" %s\n' "$(cat "$key.pub")" > /home/tunnel/.ssh/authorized_keys
    chown tunnel:tunnel /home/tunnel/.ssh/authorized_keys
    chmod 600 /home/tunnel/.ssh/authorized_keys
    cat > /etc/ssh/sshd_config_colabbb <<EOF
Port 2222
ListenAddress 0.0.0.0
HostKey $host_key
PidFile /run/sshd_colabbb.pid
AuthorizedKeysFile /home/tunnel/.ssh/authorized_keys
AllowUsers tunnel
PubkeyAuthentication yes
AuthenticationMethods publickey
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitEmptyPasswords no
UsePAM no
PermitRootLogin no
AllowTcpForwarding remote
AllowStreamLocalForwarding no
GatewayPorts no
PermitListen 127.0.0.1:28188 127.0.0.1:28189 127.0.0.1:28190
PermitOpen none
PermitTTY no
X11Forwarding no
PermitTunnel no
PermitUserRC no
ForceCommand /bin/false
MaxAuthTries 3
LogLevel VERBOSE
EOF
    /usr/sbin/sshd -t -f /etc/ssh/sshd_config_colabbb
    /usr/sbin/sshd -D -e -f /etc/ssh/sshd_config_colabbb &
    pids+=("$!")
fi
if [[ ${COLABBB_ENABLE_COMFY_PROXY:-0} == 1 ]]; then
    python3 "$COLABBB_HOME/optional/comfy-proxy/comfy_port_proxy.py" &
    pids+=("$!")
    echo "${pids[-1]}" > "$COLABBB_STATE_DIR/proxy.pid"
fi
if [[ ${COLABBB_ENABLE_TGBOT:-0} == 1 ]]; then
    python3 "$COLABBB_HOME/optional/tg-bot/tg_bot.py" &
    pids+=("$!")
fi
if [[ ${COLABBB_ENABLE_DRIVE_SYNC:-0} == 1 ]]; then
    python3 "$COLABBB_HOME/optional/drive-sync/drive_sync.py" &
    pids+=("$!")
fi
"$@" &
pids+=("$!")
# If any required service exits, restart the whole container via Compose policy.
status=0
wait -n "${pids[@]}" || status=$?
shutdown
((status != 0)) || status=1
exit "$status"
