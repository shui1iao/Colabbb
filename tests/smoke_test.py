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
import shutil
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
        if shutil.which('node'):
            subprocess.run(['node', '--check', str(path)], check=True)
        elif shutil.which('docker'):
            subprocess.run(['docker', 'exec', '-i', 'sub-store', 'node', '-e', 'new Function(require("fs").readFileSync(0, "utf8"));'], input=script, text=True, check=True)
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
            for path in ['/api/job', '/api/settings', '/api/outputs', '/api/colab_accounts', '/api/drive_sync']:
                assert request(anonymous, path)[0] == 401
            cookies = http.cookiejar.CookieJar(); logged = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))
            status, data, headers = request(logged, '/api/login', {'username': 'test-admin', 'password': password})
            assert status == 200 and json.loads(data)['ok'] and cookies
            assert 'HttpOnly' in headers['Set-Cookie'] and 'SameSite=Lax' in headers['Set-Cookie']
            status, data, _ = request(logged, '/')
            assert status == 200 and data == (ROOT / 'ui/panel_ui.html').read_bytes()
            for path in ['/api/settings', '/api/outputs', '/api/job', '/api/colab_accounts', '/api/drive_sync']:
                status, data, _ = request(logged, path)
                assert status == 200
                parsed_json = json.loads(data)
                if path == '/api/colab_accounts':
                    assert isinstance(parsed_json.get('accounts'), list)
                if path == '/api/drive_sync':
                    assert parsed_json.get('enabled') is False
            assert request(logged, '/api/drive_sync', {'mode': 'drive'})[0] == 409
            assert request(logged, '/api/run', {'module': 'random', 'count': 11})[0] == 400
            # v0.2.3 安全与健壮性回归测试
            # 1. 跨源 Origin POST 返回 403
            bad_origin_req = urllib.request.Request(
                base + '/api/touch',
                data=b'{}',
                headers={'Content-Type': 'application/json', 'Origin': 'https://evil.example'},
            )
            try:
                logged.open(bad_origin_req, timeout=5)
                raise AssertionError('cross-origin POST should be rejected')
            except urllib.error.HTTPError as err:
                assert err.code == 403, err.code
            # 2. HEAD / 不返回响应体且携带安全头
            head_req = urllib.request.Request(base + '/', method='HEAD')
            with logged.open(head_req, timeout=5) as hr:
                assert hr.status == 200 and hr.read() == b''
                assert hr.headers.get('X-Frame-Options') == 'DENY'
                assert hr.headers.get('Server') == 'colabbb-panel'
            # 3. /api/run 拦截非白名单 colab-ai 子命令
            assert request(logged, '/api/run', {'module': 'image', 'text': 'colab-ai accounts'})[0] == 403
            # 4. /api/upload 拒绝 .html，接受合法 PNG 且 /files/ 返回 sandbox CSP + Range 206
            up_bad = urllib.request.Request(
                base + '/api/upload?filename=xss.html',
                data=b'<script>alert(1)</script>',
                headers={'Content-Type': 'application/octet-stream'},
            )
            try:
                logged.open(up_bad, timeout=5)
                raise AssertionError('html upload should be rejected')
            except urllib.error.HTTPError as err:
                assert err.code == 400, err.code
            png_1x1 = base64.b64decode(
                'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII='
            )
            up_ok = urllib.request.Request(
                base + '/api/upload?filename=ok.png',
                data=png_1x1,
                headers={'Content-Type': 'image/png'},
            )
            with logged.open(up_ok, timeout=5) as ur:
                assert ur.status == 200
                up_doc = json.loads(ur.read())
                assert up_doc['ok'] and up_doc['url'].startswith('/files/upload_image/')
            status, body, fhdrs = request(logged, up_doc['url'])
            assert status == 200 and body == png_1x1
            assert 'sandbox' in (fhdrs.get('Content-Security-Policy') or '')
            assert fhdrs.get('X-Content-Type-Options') == 'nosniff'
            range_req = urllib.request.Request(base + up_doc['url'], headers={'Range': 'bytes=0-7'})
            with logged.open(range_req, timeout=5) as rr:
                assert rr.status == 206 and rr.read() == png_1x1[:8]
                assert rr.headers.get('Content-Range') == f'bytes 0-7/{len(png_1x1)}'
            # 5. 设置页面与写入接口已移除：POST /api/settings 与 /api/settings/reset 返回 404，只读 GET /api/settings 保留
            assert request(logged, '/api/settings', {'llm': {'system_prompt': 'x'}})[0] == 404
            assert request(logged, '/api/settings/reset', {'section': 'all'})[0] == 404
            # 热覆盖是完整模板替换，不改动权威 HTML。
            override = b'<!doctype html><html><body>override-check</body></html>'
            (state / 'panel_ui.html').write_bytes(override)
            assert request(logged, '/')[1] == override
            (state / 'panel_ui.html').unlink()
            assert request(logged, '/')[1] == (ROOT / 'ui/panel_ui.html').read_bytes()
            # 5.5 公开 PWA manifest.webmanifest 返回 200 JSON
            m_st, m_body, m_hdrs = request(anonymous, '/manifest.webmanifest')
            assert m_st == 200 and json.loads(m_body).get('display') == 'standalone', (m_st, m_body)
            # 6. 登出后服务端递增 session_version，原 Cookie 复制重放立即失效（401）
            saved_cookie = '; '.join(f'{c.name}={c.value}' for c in cookies)
            assert request(logged, '/api/logout', {})[0] == 200
            assert request(logged, '/api/settings')[0] == 401
            replay_req = urllib.request.Request(base + '/api/settings', headers={'Cookie': saved_cookie})
            try:
                anonymous.open(replay_req, timeout=5)
                raise AssertionError('revoked cookie should be rejected after logout')
            except urllib.error.HTTPError as err:
                assert err.code == 401, err.code
            print('PASS HTTP: uninitialized fail-closed; public icons; CSRF 403; HEAD no-body; upload/sandbox/Range 206; regex validation; override; server-side logout revocation')
        finally: stop(process)

        # 离线校验 v0.2.3 colab_startup、colab_accounts fail-closed 与 idle_guard
        with mock.patch.dict(os.environ, env, clear=True):
            import colab_startup, colab_accounts, idle_guard
            lp = state / 'test-startup.lock'
            with open(lp, 'w+', encoding='utf-8') as lfh:
                assert colab_startup.read_pending(lfh, 'ai') is None
                rec = {
                    'version': 1, 'request_uuid': '11111111-1111-4111-8111-111111111111',
                    'account': {'email': 'a@example.com', 'sub': 'sub-1'},
                    'session': 'ai', 'gpu': 'G4', 'created_at': time.time() - 60,
                    'phase': 'unconfirmed', 'post_count': 1, 'last_post_at': time.time() - 30,
                }
                colab_startup.write_pending(lfh, rec)
                assert colab_startup.read_pending(lfh, 'ai', 'G4')['phase'] == 'unconfirmed'
            assert colab_accounts.get_drive_sync_token_json('missing@example.com') == ''
            g = idle_guard.Guard(str(state), 'ai-smoke')
            g.touch()
            with g.gate():
                d = g.read()
                d['attempts'] = 2; d['retry_at'] = time.time() + 100
                g.write(d)
            with g.lease():
                d2 = g.read()
                assert d2['attempts'] == 0 and d2['retry_at'] == 0
            print('PASS v0.2.3 colab_startup pending, colab_accounts Drive fail-closed, idle_guard lease reset')
            # v0.2.4 回归：JobManager llm 等 start 互斥、task_detail 返回 state、新版 5 栏侧栏+顶部状态卡+移动端底栏+退出登录处理
            import collections as _col, threading as _th
            jm = panel.JobManager.__new__(panel.JobManager)
            jm.lock = _th.RLock(); jm.cond = _th.Condition(jm.lock)
            jm.queues = {'media': _col.deque(), 'llm': _col.deque(), 'sys': _col.deque()}
            jm.active_procs = {1: {'jid': 1, 'module': 'start', 'cmd_list': [panel.COLAB_AI_BIN, 'start'], 'lane': 'media'}}
            jm.paused = {}; jm.recent_done = _col.deque(maxlen=10)
            assert jm._has_start_pending() is True
            det = jm.get_task_detail(1)
            assert det['ok'] and det['state'] == 'running', det
            ui_txt = (ROOT / 'ui/panel_ui.html').read_text(encoding='utf-8')
            for marker in (
                'id="topNav"', 'id="taskPill"', 'id="machinePill"', 'id="stageView"',
                'id="stagePrev"', 'id="stageNext"', 'id="viewerZoomSlider"', 'id="viewerMobBar"',
                'id="uiTip"', "'logout-panel':", 'data-tip=', 'bindScrollFade'
            ):
                assert marker in ui_txt, f'missing UI marker: {marker}'
            for removed in ('id="appSidebar"', 'id="mobileBottomNav"', 'POST /api/settings'):
                assert removed not in ui_txt, f'stale marker present: {removed}'
            import hashlib as _hl, re as _re
            _sm = _re.search(r'<script>(.*)</script>', ui_txt, _re.S)
            assert _sm, 'inline script missing in ui/panel_ui.html'
            _sha = base64.b64encode(_hl.sha256(_sm.group(1).encode('utf-8')).digest()).decode('ascii')
            assert f"script-src 'sha256-{_sha}'" in ui_txt, f'CSP script-src sha256 mismatch: expected {_sha}'
            assert ui_txt == panel.HTML_PAGE, 'ui/panel_ui.html out of sync with web_panel.HTML_PAGE'
            assert '_fatal_conn_err' in (ROOT / 'colab-ai').read_text(encoding='utf-8')
            print('PASS v0.2.10 redesigned UI, removed settings write API, exec hang guard & CSP SHA-256')

            # v0.2.8 回归：Colab Public API 规格解析、endpoint 提取、配额项目解析、候选回退链与显存自适应
            assert colab_startup.parse_gpu_spec('G4') == ('G4', False, 'G4')
            assert colab_startup.parse_gpu_spec('a100-hm') == ('A100', True, 'A100-HM')
            assert colab_startup.parse_gpu_spec('A100-80G') == ('A100', True, 'A100-HM')
            assert colab_startup.extract_endpoint_from_proxy_url(
                'https://8080-gpu-a100-s-abc123-a.us-west1-0.prod.colab.dev/', ''
            ) == 'gpu-a100-s-abc123'
            assert colab_startup.extract_endpoint_from_proxy_url('', 'runtimes/r-1234') == 'r-1234'
            assert colab_startup.normalize_proxy_url('https://x.colab.dev///') == 'https://x.colab.dev'
            with mock.patch.dict(os.environ, {'COLABBB_QUOTA_PROJECT': 'demo-proj-123'}):
                assert colab_accounts.resolve_quota_project('u@example.com') == 'demo-proj-123'
            assert cli._gpu_display_label('A100-HM') == 'A100-80GB'
            assert cli._gpu_display_label('A100') == 'A100-40GB'
            assert cli._gpu_candidate_chain('G4')[:4] == ['G4', 'A100-HM', 'A100', 'H100']
            assert cli._gpu_candidate_chain('H100') == ['H100']
            assert 'colaboratory.googleapis.com' in (ROOT / 'colab_startup.py').read_text(encoding='utf-8')
            assert 'list_effective_assignments' in (ROOT / 'web_panel.py').read_text(encoding='utf-8')
            assert '_yield_llm_vram_if_needed' in (ROOT / 'remote_image.py').read_text(encoding='utf-8')
            assert '"/unload"' in (ROOT / 'remote_llm.py').read_text(encoding='utf-8')
            print('PASS v0.2.8 Colab Public API, A100-HM fallback chain, quota project resolution & VRAM swap guard')

            # v0.2.10 回归：NO_RUNTIMES 重试、视频显存自适应、Kat 同源 UI 门禁（0 原生 title=、0 裸圆角、0 负边距、0 重复选择器、无设置写入接口）
            assert '"no_runtimes"' in (ROOT / 'colab_startup.py').read_text(encoding='utf-8')
            assert 'last_no_runtimes_at' in (ROOT / 'colab-ai').read_text(encoding='utf-8')
            assert '"http://127.0.0.1:8189/unload"' in (ROOT / 'remote_video.py').read_text(encoding='utf-8')
            assert 'closeZenoSelect' not in ui_txt, 'stale closeZenoSelect ReferenceError in UI'
            _body_txt = ui_txt[ui_txt.find('<body'):ui_txt.find('<script>')]
            assert not _re.findall(r'<[^>]*\btitle\s*=', _body_txt), 'native title= found in ui/panel_ui.html'
            assert not [b for b in _re.findall(r'<button\b[^>]*>', _body_txt) if not _re.search(r'\btype\s*=', b)], 'button missing type='
            _style_m = _re.search(r'<style>(.*)</style>', ui_txt, _re.S)
            _style_txt = _style_m.group(1)
            _clean_css = _re.sub(r'/\*.*?\*/', '', _style_txt, flags=_re.S)
            assert _clean_css.count('{') == _clean_css.count('}'), 'unbalanced CSS braces in ui/panel_ui.html'
            _css_body = _style_txt[_style_txt.find('/* ===== 基础'):]
            assert not [x for x in _re.findall(r'border-radius:\s*([^;]+);', _css_body) if _re.search(r'\d+px', x)], 'raw px border-radius in CSS'
            assert not [l for l in _style_txt.splitlines() if _re.search(r'margin[^;]*-[0-9]', l)], 'negative margin hack in CSS'
            assert 'output_name' in (ROOT / 'web_panel.py').read_text(encoding='utf-8')
            print('PASS v0.2.10 Kat UI gates (0 title=, 0 raw radii, 0 neg margin), stepless zoom & mobile immersive viewer')
    print('ALL SMOKE TESTS PASSED')


if __name__ == '__main__':
    main()
