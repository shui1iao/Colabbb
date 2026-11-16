#!/usr/bin/env python3
"""可选 Telegram Bot：仅通过本机面板 IPC 提交创作任务，不依赖外部业务模块。"""
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request

HOME = os.environ.get('COLABBB_HOME', '/opt/colabbb')
STATE_DIR = os.environ.get('COLABBB_STATE_DIR', os.path.join(HOME, 'state'))
PANEL_URL = os.environ.get('COLABBB_PANEL_URL', 'http://127.0.0.1:' + os.environ.get('COLABBB_PANEL_PORT', '17860')).rstrip('/')
_parsed_panel = urllib.parse.urlsplit(PANEL_URL)
if _parsed_panel.scheme != 'http' or _parsed_panel.hostname not in ('127.0.0.1', '::1', 'localhost') or _parsed_panel.username or _parsed_panel.password or _parsed_panel.path or _parsed_panel.query or _parsed_panel.fragment:
    raise RuntimeError('COLABBB_PANEL_URL 必须是本机 HTTP 地址')
LOG = logging.getLogger('colabbb-tgbot')


def allowed_users():
    return {x.strip() for x in os.environ.get('TG_ALLOWED_USERS', '').replace(';', ',').split(',') if x.strip()}


def panel_request(path, data=None):
    with open(os.path.join(STATE_DIR, 'panel_auth.json'), encoding='utf-8') as fh:
        token = json.load(fh).get('internal_ipc_token', '')
    if not token:
        raise RuntimeError('请先初始化面板管理员')
    req = urllib.request.Request(PANEL_URL + path, data=None if data is None else json.dumps(data, ensure_ascii=False).encode(), headers={'Content-Type': 'application/json', 'X-G4-Internal': token})
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def telegram(token, method, data=None):
    # 不记录 token 或异常中的 Telegram URL。
    req = urllib.request.Request('https://api.telegram.org/bot' + token + '/' + method, data=json.dumps(data or {}).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=40) as response:
        result = json.load(response)
    if not result.get('ok'):
        raise RuntimeError('Telegram API 请求失败')
    return result['result']


def handle_message(message, username):
    uid = str(message.get('from', {}).get('id', ''))
    # 未配置授权名单时 fail-closed；仅处理私人聊天，避免将状态发往群组。
    if uid not in allowed_users() or message.get('chat', {}).get('type') != 'private':
        return None
    text = str(message.get('text') or '').strip()
    command, _, body = text.partition(' ')
    command = command.split('@', 1)[0].lower()
    if command in ('/start', '/help'):
        return f'@{username}\n/image 摄影提示词\n/video 视频分镜\n/llm 创作要求\n/random 1~10\n/status\n任务结果与文件请在 Web 工作台查看。'
    if command == '/status':
        data = panel_request('/api/job')
        return json.dumps(data, ensure_ascii=False)[:3500]
    if command == '/random':
        try:
            count = int(body or '1')
            if not 1 <= count <= 10: raise ValueError()
        except ValueError:
            return '用法：/random 1~10'
        payload = {'module': 'random', 'count': count, 'source': 'tg'}
    elif command in ('/image', '/video', '/llm'):
        if not body.strip(): return '请在命令后输入创作要求。'
        payload = {'module': command[1:], 'text': body.strip(), 'source': 'tg'}
    else:
        return '输入 /help 查看命令。'
    result = panel_request('/api/run', payload)
    return str(result.get('message') or ('已提交' if result.get('ok') else '未提交'))


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    token = os.environ.get('TG_BOT_TOKEN', '').strip()
    if not token: raise SystemExit('请先配置 TG_BOT_TOKEN')
    if not allowed_users():
        LOG.warning('TG_ALLOWED_USERS 为空，所有请求均被拒绝。')
    username = telegram(token, 'getMe')['username']
    LOG.info('Bot 已启动；仅允许显式授权的私人聊天。')
    offset = 0
    while True:
        try:
            updates = telegram(token, 'getUpdates', {'offset': offset, 'timeout': 25, 'allowed_updates': ['message']})
            for update in updates:
                offset = update['update_id'] + 1
                message = update.get('message', {})
                try:
                    answer = handle_message(message, username)
                except Exception:
                    answer = '面板暂不可用，请检查本机服务与管理员初始化。'
                if answer:
                    telegram(token, 'sendMessage', {'chat_id': message['chat']['id'], 'text': answer[:4000]})
        except Exception:
            LOG.warning('连接失败，稍后重试（凭据不会写入日志）。')
            time.sleep(5)


if __name__ == '__main__':
    main()
