#!/usr/bin/env python3
"""Real Docker/SSH/HTTP integration test; no GPU or external bot requests."""
import argparse
import functools
import hashlib
import http.cookiejar
import http.server
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'colabbb-test:v0.2.3'


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', action='store_true')
    args = parser.parse_args()
    if args.build:
        subprocess.run(['docker', 'build', '-t', IMAGE, '.'], cwd=ROOT, check=True)
    work = ROOT.parent / 'verification'
    work.mkdir(exist_ok=True)
    name = 'colabbb-e2e-' + secrets.token_hex(5)
    containers = set()
    tunnel = None
    server = None
    with tempfile.TemporaryDirectory(prefix='docker-e2e-', dir=work) as directory:
        temp = Path(directory)
        data = temp / 'data'; data.mkdir(mode=0o700)
        password = secrets.token_urlsafe(24)
        def launch(bootstrap=True, sshd=True, proxy=True):
            command = ['docker', 'run', '-d', '--name', name,
                       '-v', f'{data}:/data', '-p', '127.0.0.1::17860',
                       '-p', '127.0.0.1::2222', '-p', '127.0.0.1::18188',
                       '-e', f'COLABBB_INTERNAL_SSHD={int(sshd)}',
                       '-e', f'COLABBB_ENABLE_COMFY_PROXY={int(proxy)}']
            if bootstrap:
                command += ['-e', 'COLABBB_ADMIN_USER=test-admin', '-e', 'COLABBB_ADMIN_PASS=' + password]
            run(*command, IMAGE)
            containers.add(name)
            published = json.loads(run('docker', 'inspect', '--format', '{{json .NetworkSettings.Ports}}', name))
            return {int(k.split('/')[0]): int(v[0]['HostPort']) for k, v in published.items()}
        def remove():
            if name in containers:
                run('docker', 'rm', '-fv', name)
                containers.remove(name)
        def request(opener, port, path, payload=None):
            req = urllib.request.Request(f'http://127.0.0.1:{port}' + path,
                                         data=None if payload is None else json.dumps(payload).encode(),
                                         headers={'Content-Type': 'application/json'})
            try:
                with opener.open(req, timeout=3) as response:
                    return response.status, response.read(), response.headers
            except urllib.error.HTTPError as error:
                return error.code, error.read(), error.headers
        anonymous = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        def ready(port):
            for _ in range(150):
                try:
                    if request(anonymous, port, '/')[0] == 200:
                        return
                except (OSError, urllib.error.URLError):
                    pass
                if run('docker', 'inspect', '--format', '{{.State.Running}}', name) != 'true':
                    raise RuntimeError(run('docker', 'logs', name))
                time.sleep(.1)
            raise RuntimeError('panel readiness timeout: ' + run('docker', 'logs', name))
        def dex(code):
            return run('docker', 'exec', name, 'python3', '-c', code)
        def login_and_check(port):
            for path in ['/g4-icon.svg', '/favicon.ico', '/apple-touch-icon.png']:
                assert request(anonymous, port, path)[0] == 200
            for path in ['/api/settings', '/api/job', '/api/colab_accounts', '/api/drive_sync']:
                assert request(anonymous, port, path)[0] == 401
            cookies = http.cookiejar.CookieJar()
            logged = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(cookies))
            status, body, headers = request(logged, port, '/api/login', {'username': 'test-admin', 'password': password})
            assert status == 200 and json.loads(body)['ok'] and cookies
            assert 'HttpOnly' in headers['Set-Cookie'] and 'SameSite=Lax' in headers['Set-Cookie']
            for path in ['/api/settings', '/api/job', '/api/colab_accounts', '/api/drive_sync']:
                status, body, _ = request(logged, port, path)
                assert status == 200
                doc = json.loads(body)
                if path == '/api/colab_accounts':
                    assert isinstance(doc.get('accounts'), list)
                if path == '/api/drive_sync':
                    assert doc.get('enabled') is False
            return logged
        try:
            # Parse the real Compose YAML with the Docker Compose implementation.
            compose_dir = temp / 'compose'; compose_dir.mkdir()
            shutil.copy2(ROOT / 'config.env.example', compose_dir / '.env')
            cfg = json.loads(run('docker', 'compose', '--project-directory', str(compose_dir),
                                 '--env-file', str(compose_dir / '.env'), '-f', str(ROOT / 'docker-compose.yml'),
                                 'config', '--format', 'json'))['services']['colabbb']
            assert cfg['image'] == 'ghcr.io/shui1iao/colabbb:latest' and cfg['init'] is True
            assert cfg['environment']['COLABBB_PANEL_PORT'] == '17860'
            assert cfg['environment']['COLABBB_TUNNEL_USER'] == 'tunnel'
            print('PASS Compose YAML validation and container/host port separation', flush=True)
            ports = launch()
            ready(ports[17860])
            version = run('docker', 'exec', name, 'colab', '--version')
            assert 'google-colab-cli ' in version
            print('PASS colab --version: ' + version, flush=True)
            help_text = run('docker', 'exec', name, 'colab', 'auth', 'login', '--help')
            assert 'Persistent local OAuth login' in help_text
            # Technical fixture only: exercise the real upstream credential-save path
            # without contacting Google or claiming a real account was authorized.
            dex("import os,runpy,sys; from unittest.mock import patch; from google.oauth2.credentials import Credentials; sys.argv=['colab auth login']; c=Credentials(token='integration-only-token',refresh_token='integration-only-refresh',token_uri='https://oauth2.googleapis.com/token',client_id='integration-client',client_secret='integration-only-secret'); p=patch('colab_cli.auth._run_remote_flow',return_value=c); p.start(); runpy.run_path('/app/docker/colab_login.py',run_name='__main__'); p.stop()")
            assert dex("import keyring; from colab_cli.auth import KEYRING_SERVICE_NAME,KEYRING_USERNAME; print(bool(keyring.get_password(KEYRING_SERVICE_NAME,KEYRING_USERNAME)))") == 'True'
            print('PASS auth login adapter help and upstream OAuth keyring save path (mocked consent, no real Google login)', flush=True)
            result = dex("import keyring,os; print(os.environ['PYTHON_KEYRING_BACKEND']); print(keyring.get_keyring().file_path)")
            assert result.splitlines() == ['keyrings.alt.file.PlaintextKeyring', '/data/home/.local/share/python_keyring/keyring_pass.cfg']
            dex("import keyring; keyring.set_password('colabbb-integration', 'test', 'persistence-check')")
            assert dex("import keyring; print(keyring.get_password('colabbb-integration', 'test'))") == 'persistence-check'
            for path in [data, data / 'state', data / 'home', data / 'home/.config/colab-cli', data / 'home/.local/share/python_keyring']:
                assert path.stat().st_mode & 0o777 == 0o700, path
            for path in [data / 'state/tunnel_ed25519', data / 'state/ssh_host_ed25519_key', data / 'state/panel_auth.json', data / 'home/.local/share/python_keyring/keyring_pass.cfg']:
                assert path.stat().st_mode & 0o777 == 0o600, path
            assert password not in (data / 'state/panel_auth.json').read_text()
            print('PASS persistent keyring path, write/read, directory 0700 and credential 0600', flush=True)
            login_and_check(ports[17860])
            print('PASS real container HTTP: anonymous icons 200, API 401, Cookie login/settings/job 200', flush=True)

            # The simulated remote HTTP endpoint runs on the host, outside the container.
            mock_dir = temp / 'mock'; mock_dir.mkdir()
            (mock_dir / 'index.html').write_text('colabbb-reverse-http-ok\n')
            class QuietHandler(http.server.SimpleHTTPRequestHandler):
                def log_message(self, *args):
                    pass
            server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(QuietHandler, directory=str(mock_dir)))
            threading.Thread(target=server.serve_forever, daemon=True).start()
            known = temp / 'known_hosts'
            known.write_text(f'[127.0.0.1]:{ports[2222]} ' + (data / 'state/ssh_host_ed25519_key.pub').read_text())
            ssh = ['ssh', '-i', str(data / 'state/tunnel_ed25519'), '-p', str(ports[2222]),
                   '-o', 'IdentitiesOnly=yes', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
                   '-o', 'StrictHostKeyChecking=yes', '-o', f'UserKnownHostsFile={known}',
                   '-o', 'GlobalKnownHostsFile=/dev/null', '-o', 'ExitOnForwardFailure=yes']
            ssh_log = temp / 'ssh.log'
            with ssh_log.open('w') as log:
                tunnel = subprocess.Popen(ssh + ['-N', '-R', f'127.0.0.1:28188:127.0.0.1:{server.server_port}', 'tunnel@127.0.0.1'], stdout=log, stderr=log)
            http_code = "import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:28188/',timeout=3); assert r.status==200; print(r.read().decode().strip())"
            for _ in range(60):
                if tunnel.poll() is not None:
                    raise RuntimeError(ssh_log.read_text())
                try:
                    assert dex(http_code) == 'colabbb-reverse-http-ok'
                    break
                except subprocess.CalledProcessError:
                    time.sleep(.1)
            else:
                raise RuntimeError('reverse HTTP tunnel did not become ready')
            assert request(anonymous, ports[18188], '/')[1] == b'colabbb-reverse-http-ok\n'
            print('PASS real host ssh -N -R -> container 127.0.0.1:28188 -> host mock HTTP: 200; optional proxy HTTP 200', flush=True)
            rejected = subprocess.run(ssh + ['tunnel@127.0.0.1', 'id'], text=True, capture_output=True, timeout=10)
            assert rejected.returncode != 0 and 'uid=' not in rejected.stdout
            print(f'PASS same-key SSH command id denied: exit={rejected.returncode}, no shell output', flush=True)
            rejected = subprocess.run(ssh + ['-N', '-R', f'127.0.0.1:28191:127.0.0.1:{server.server_port}', 'tunnel@127.0.0.1'], text=True, capture_output=True, timeout=10)
            assert rejected.returncode != 0 and 'remote port forwarding failed' in rejected.stderr
            print(f'PASS non-allowlisted reverse port 28191 denied: exit={rejected.returncode}', flush=True)
            config = run('docker', 'exec', name, '/usr/sbin/sshd', '-T', '-f', '/etc/ssh/sshd_config_colabbb')
            for setting in ['allowtcpforwarding remote', 'gatewayports no', 'permittty no', 'passwordauthentication no', 'usepam no', 'allowusers tunnel']:
                assert setting in config
            assert dex("import spwd; print(spwd.getspnam('tunnel').sp_pwdp)") == '*'
            print('PASS effective sshd restriction config and password marker * (not locked !)', flush=True)
            # Preserve only non-sensitive evidence in the test log.
            log_result = subprocess.run(['docker', 'logs', name], check=True, text=True, capture_output=True)
            logs = log_result.stdout + log_result.stderr
            for line in logs.splitlines():
                if 'Accepted publickey' in line or 'Starting session: forced-command' in line:
                    print('SSHD evidence: ' + line, flush=True)
            tunnel.terminate(); tunnel.wait(timeout=10); tunnel = None
            before = {p: hashlib.sha256((data / 'state' / p).read_bytes()).hexdigest()
                      for p in ['tunnel_ed25519', 'ssh_host_ed25519_key', 'panel_auth.json']}
            run('docker', 'stop', '--time', '10', name)
            remove()
            ports = launch(bootstrap=False)
            ready(ports[17860])
            assert before == {p: hashlib.sha256((data / 'state' / p).read_bytes()).hexdigest() for p in before}
            assert dex("import keyring; print(keyring.get_password('colabbb-integration', 'test'))") == 'persistence-check'
            login_and_check(ports[17860])
            print('PASS container recreation preserves tunnel key, host key, admin auth and keyring login storage', flush=True)
            remove()
            shutil.rmtree(data); data.mkdir(mode=0o700)
            ports = launch(bootstrap=False, sshd=False, proxy=False)
            ready(ports[17860])
            assert '请先运行 --init-admin' in request(anonymous, ports[17860], '/')[1].decode()
            assert request(anonymous, ports[17860], '/api/job')[0] == 401
            assert request(anonymous, ports[17860], '/api/login', {'username': 'admin', 'password': 'admin'})[0] == 401
            assert not (data / 'state/ssh_host_ed25519_key').exists()
            assert dex("import socket; s=socket.socket(); print(s.connect_ex(('127.0.0.1',2222)))") != '0'
            # Exactly the documented exec route, with environment only for noninteractive test input.
            run('docker', 'exec', '-e', 'COLABBB_ADMIN_PASS=' + password, name,
                'python3', '/app/web_panel.py', '--init-admin', '--username', 'test-admin')
            login_and_check(ports[17860])
            print('PASS absent admin fails closed, internal sshd=0 closes SSH, compose-exec init-admin works live', flush=True)
            remove()
            shutil.rmtree(data); data.mkdir(mode=0o700)
            password = 'short'
            launch()
            for _ in range(100):
                if run('docker', 'inspect', '--format', '{{.State.Running}}', name) == 'false':
                    break
                time.sleep(.1)
            else:
                raise RuntimeError('short bootstrap password should stop startup')
            assert not (data / 'state/panel_auth.json').exists()
            print('PASS bootstrap password shorter than 12 is rejected without creating admin auth', flush=True)
        finally:
            if tunnel is not None:
                tunnel.terminate()
                try:
                    tunnel.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    tunnel.kill(); tunnel.wait()
            if server is not None:
                server.shutdown(); server.server_close()
            for container in list(containers):
                subprocess.run(['docker', 'rm', '-fv', container], check=True, capture_output=True)
                containers.remove(container)
            assert not run('docker', 'ps', '-aq', '--filter', f'name=^{name}$')
    assert not Path(directory).exists()
    print('PASS cleanup: all test containers, mounts, keys, HTTP/SSH processes and temporary data removed', flush=True)
    print('ALL DOCKER E2E TESTS PASSED', flush=True)


if __name__ == '__main__':
    main()
