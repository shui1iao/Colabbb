#!/usr/bin/env python3
"""离线语法、模板、单元与真实 HTTP 冒烟测试；不连接 GPU、Telegram 或 SSH。"""
import ast
import base64
import hashlib
import http.cookiejar
import importlib.util
import json
import os
from pathlib import Path
import py_compile
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def check_html(name, html, temp):
    from html.parser import HTMLParser
    class Parser(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=False)
            self.scripts = []; self.csp = ''; self.current = None
        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == 'meta' and attrs.get('http-equiv', '').lower() == 'content-security-policy': self.csp = attrs.get('content', '')
            if tag == 'script' and 'src' not in attrs: self.current = ''
        def handle_data(self, data):
            if self.current is not None: self.current += data
        def handle_endtag(self, tag):
            if tag == 'script' and self.current is not None:
                self.scripts.append(self.current); self.current = None
    parser = Parser(); parser.feed(html)
    assert parser.scripts and parser.csp
    for i, script in enumerate(parser.scripts):
        digest = base64.b64encode(hashlib.sha256(script.encode()).digest()).decode()
        script_policy = next(x.strip() for x in parser.csp.split(';') if x.strip().startswith('script-src '))
        assert "'sha256-" + digest + "'" in script_policy, (name, digest)
        path = temp / f'{name}-{i}.js'; path.write_text(script)
        subprocess.run(['node', '--check', str(path)], check=True)
        print(f'PASS CSP/JS {name}: sha256-{digest}')


def main():
    with tempfile.TemporaryDirectory(prefix='colabbb-smoke-') as directory:
        temp = Path(directory); state = temp / 'state'; state.mkdir(mode=0o700)
        outputs = temp / 'outputs'; outputs.mkdir()
        files = sorted(ROOT.rglob('*.py')) + [ROOT / 'colab-ai']
        for i, path in enumerate(files):
            py_compile.compile(str(path), cfile=str(temp / f'{i}.pyc'), doraise=True)
        for script in ['entrypoint.sh', 'colab.sh']:
            subprocess.run(['bash', '-n', str(ROOT / 'docker' / script)], check=True)
        dockerfile = (ROOT / 'Dockerfile').read_text()
        compose = (ROOT / 'docker-compose.yml').read_text()
        for token in ['python:3.12-slim-bookworm', 'openssh-server', 'google-colab-cli', 'keyrings.alt', 'Pillow', 'HOME=/data/home', 'tini']:
            assert token in dockerfile, token
        for token in ['ghcr.io/shui1iao/colabbb:latest', 'build: .', './data:/data', 'env_file: .env', 'init: true', 'restart: unless-stopped', ':2222']:
            assert token in compose, token
        assert not (ROOT / 'install.sh').exists() and not (ROOT / 'systemd').exists()
        print(f'PASS Python compile: {len(files)} files; entrypoint bash -n; Docker/Compose contract')
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        password = secrets.token_urlsafe(24)
        env = dict(os.environ, COLABBB_HOME=str(ROOT), COLABBB_CONFIG_FILE=str(temp / 'absent.env'),
                   COLABBB_STATE_DIR=str(state), COLABBB_OUTPUT_DIR=str(outputs), COLABBB_CLI_BIN='/bin/false',
                   COLABBB_PANEL_HOST='127.0.0.1', COLABBB_PANEL_PORT=str(port),
                   COLABBB_ADMIN_USER='test-admin', COLABBB_ADMIN_PASS=password, PYTHONDONTWRITEBYTECODE='1')
        # 同进程导入的路径同样保持临时隔离。
        with mock.patch.dict(os.environ, env, clear=True):
            sys.path.insert(0, str(ROOT))
            spec = importlib.util.spec_from_file_location('panel_smoke', ROOT / 'web_panel.py')
            panel = importlib.util.module_from_spec(spec); spec.loader.exec_module(panel)
            assert panel._verify_login_credentials('test-admin', password) is False
            assert panel._verify_session_cookie('invalid') is False
            html = (ROOT / 'ui/panel_ui.html').read_text()
            assert panel.HTML_PAGE.encode() == (ROOT / 'ui/panel_ui.html').read_bytes()
            assert panel.get_live_html_bytes() == html.encode()
            for name, value in [('ui', html), ('HTML_PAGE', panel.HTML_PAGE), ('login', panel.LOGIN_VIEW_TEMPLATE)]: check_html(name, value, temp)
            assert len(panel.PanelRandomOrchestrator.SEEDS) == 16
            random = panel.PanelRandomOrchestrator.__new__(panel.PanelRandomOrchestrator)
            import collections, threading
            random.lock = threading.RLock(); random.cond = threading.Condition(random.lock)
            random.queue = collections.deque(); random.active_stage = ''; random.active_t0 = 0.; random.cancel_epoch = 0
            assert random.enqueue(10) == (0, 10)
            assert len({x[1] for x in random.queue}) == 10
            sample = random.queue[0]
            with mock.patch.object(panel.urllib.request, 'urlopen', side_effect=OSError('offline')), mock.patch.object(panel.JOB, 'start_job', return_value=(True, '#1')) as job:
                random._run_one(*sample)
                assert job.call_count == 1
                args, kwargs = job.call_args
                assert args[1] == 'image' and args[0][1] == 'image'
                assert len(kwargs['prompt_preview']) >= 120
            assert random.cancel_all() and not random.queue
            cfg = panel.load_studio_settings()
            assert '<提示词>' in cfg['llm']['system_prompt']
            assert set(cfg['image_workflow']['resolutions']) == {'portrait', 'landscape', 'square'}
            assert panel._validate_warmup_targets(['v5_int8', 'video', 'llm']) == ['v5_int8', 'video', 'llm']
            print('PASS offline auth fail-closed, random 10 unique seeds/fallback/JOB queue, workflow/warmup')
            from importlib.machinery import SourceFileLoader
            loader = SourceFileLoader("cli_smoke", str(ROOT / "colab-ai"))
            cli_spec = importlib.util.spec_from_loader(loader.name, loader)
            cli = importlib.util.module_from_spec(cli_spec); loader.exec_module(cli)
            with mock.patch.dict(os.environ, {"COLABBB_TUNNEL_HOST": "gateway.example.com", "COLABBB_TUNNEL_PORT": "22", "COLABBB_TUNNEL_USER": "tunnel", "COLABBB_IMAGE_LABEL": "Custom DiT"}):
                flags = cli._build_remote_env_flags({"COLAB_AI_ARGS_JSON": "[]"})
                assert all(flags[i] == "--env" for i in range(0, len(flags), 2))
                injected = dict(value.split("=", 1) for value in flags[1::2])
                key = state / "tunnel_ed25519"
                assert key.stat().st_mode & 0o777 == 0o600
                assert base64.b64decode(injected["COLABBB_TUNNEL_KEY_B64"]) == key.read_bytes()
                assert injected["COLABBB_TUNNEL_HOST"] == "gateway.example.com"
                assert injected["COLABBB_TUNNEL_USER"] == "tunnel"
                assert injected["COLABBB_IMAGE_LABEL"] == "Custom DiT"
            bot_spec = importlib.util.spec_from_file_location("bot_smoke", ROOT / "optional/tg-bot/tg_bot.py")
            bot = importlib.util.module_from_spec(bot_spec); bot_spec.loader.exec_module(bot)
            with mock.patch.dict(os.environ, {"TG_ALLOWED_USERS": ""}):
                assert bot.handle_message({"from": {"id": 1}, "chat": {"type": "private"}, "text": "/random 1"}, "test_bot") is None
            with mock.patch.dict(os.environ, {"TG_ALLOWED_USERS": "1"}), mock.patch.object(bot, "panel_request", return_value={"ok": True, "message": "queued"}) as ipc:
                assert bot.handle_message({"from": {"id": 1}, "chat": {"type": "private"}, "text": "/random 10"}, "test_bot") == "queued"
                assert ipc.call_args.args == ("/api/run", {"module": "random", "count": 10, "source": "tg"})
            print("PASS dynamic local SSH key/env injection; optional Bot empty-allowlist refusal and random IPC reuse")

        base = f'http://127.0.0.1:{port}'
        def request(opener, path, payload=None):
            data = None if payload is None else json.dumps(payload).encode()
            req = urllib.request.Request(base + path, data=data, headers={'Content-Type': 'application/json'})
            try:
                with opener.open(req, timeout=5) as response: return response.status, response.read(), response.headers
            except urllib.error.HTTPError as error: return error.code, error.read(), error.headers
        anonymous = urllib.request.build_opener()
        def start():
            log = open(temp / 'server.log', 'ab')
            process = subprocess.Popen([sys.executable, str(ROOT / 'web_panel.py')], env=env, stdout=log, stderr=log)
            log.close()
            for _ in range(100):
                if process.poll() is not None: raise RuntimeError((temp / 'server.log').read_text())
                try:
                    if request(anonymous, '/')[0] == 200: return process
                except OSError: pass
                time.sleep(.05)
            process.terminate(); process.wait(timeout=5)
            raise RuntimeError('启动超时')
        def stop(process):
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        process = start()
        try:
            assert '请先运行 --init-admin 初始化管理员账号' in request(anonymous, '/')[1].decode()
            assert request(anonymous, '/api/login', {'username': 'admin', 'password': 'admin'})[0] == 401
            assert request(anonymous, '/api/job')[0] == 401
        finally: stop(process)
        subprocess.run([sys.executable, str(ROOT / 'web_panel.py'), '--init-admin', '--username', 'test-admin'], env=env, check=True)
        assert (state / 'panel_auth.json').stat().st_mode & 0o777 == 0o600
        assert password not in (state / 'panel_auth.json').read_text()
        process = start()
        try:
            for path, filename in [('/g4-icon.svg', 'g4-icon.svg'), ('/favicon.ico', 'favicon.ico'), ('/apple-touch-icon.png', 'g4-icon-180.png')]:
                status, data, _ = request(anonymous, path)
                assert status == 200 and data == (ROOT / 'ui/icons' / filename).read_bytes()
            assert request(anonymous, '/')[0] == 200
            for path in ['/api/job', '/api/settings', '/api/outputs']:
                assert request(anonymous, path)[0] == 401
            cookies = http.cookiejar.CookieJar(); logged = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))
            status, data, headers = request(logged, '/api/login', {'username': 'test-admin', 'password': password})
            assert status == 200 and json.loads(data)['ok'] and cookies
            assert 'HttpOnly' in headers['Set-Cookie'] and 'SameSite=Lax' in headers['Set-Cookie']
            status, data, _ = request(logged, '/')
            assert status == 200 and data == (ROOT / 'ui/panel_ui.html').read_bytes()
            for path in ['/api/settings', '/api/outputs', '/api/job']:
                status, data, _ = request(logged, path)
                assert status == 200; json.loads(data)
            assert request(logged, '/api/run', {'module': 'random', 'count': 11})[0] == 400
            # 热覆盖是完整模板替换，不改动权威 HTML。
            override = b'<!doctype html><html><body>override-check</body></html>'
            (state / 'panel_ui.html').write_bytes(override)
            assert request(logged, '/')[1] == override
            (state / 'panel_ui.html').unlink()
            assert request(logged, '/')[1] == (ROOT / 'ui/panel_ui.html').read_bytes()
            assert request(logged, '/api/logout', {})[0] == 200
            assert request(logged, '/api/settings')[0] == 401
            print('PASS HTTP: uninitialized fail-closed; public icons byte-identical; anonymous login 200/API 401; cookie login; authenticated HTML/JSON 200; override; logout')
        finally: stop(process)
    print('ALL SMOKE TESTS PASSED')


if __name__ == '__main__':
    main()
