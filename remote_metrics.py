#!/usr/bin/env python3
"""
Colab G4 极轻量常驻硬件探针 (/content/_g4_metrics_daemon.py -> 127.0.0.1:8190 -> 宿主机 127.0.0.1:28190)
每 2.5s 后台无阻塞采样 CPU% (/proc/stat)、内存 (/proc/meminfo)、整卡显存与利用率 (nvidia-smi) 及硬盘 (/content)，
HTTP /metrics 内存直返耗时 <0.2ms，零 Jupyter Kernel 占用。
"""
import base64, os, socket, subprocess, sys, time

LOCAL_PORT = 8190
TUNNEL_PORT = int(os.environ.get("COLABBB_METRICS_TUNNEL_PORT", "28190"))

DAEMON_CODE = r'''#!/usr/bin/env python3
import json, os, shutil, subprocess, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SNAP_LOCK = threading.Lock()
SNAP = {"ok": True, "ts": 0.0}

def _read_cpu_times():
    with open("/proc/stat", "r") as fh:
        parts = [int(x) for x in fh.readline().split()[1:]]
    idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
    return sum(parts), idle

def _read_mem_gb():
    tot_kb = avail_kb = 0
    with open("/proc/meminfo", "r") as fh:
        for ln in fh:
            if ln.startswith("MemTotal:"):
                tot_kb = int(ln.split()[1])
            elif ln.startswith("MemAvailable:"):
                avail_kb = int(ln.split()[1])
            if tot_kb and avail_kb:
                break
    tot_gb = tot_kb / (1024.0 * 1024.0)
    used_gb = max(0.0, (tot_kb - avail_kb) / (1024.0 * 1024.0))
    pct = round(100.0 * used_gb / tot_gb, 1) if tot_gb > 0 else 0.0
    return round(used_gb, 1), round(tot_gb, 1), pct

def _read_gpu():
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name,utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"],
            timeout=2.0, text=True
        ).strip().splitlines()[0]
        parts = [p.strip() for p in out.split(",")]
        gname = parts[0]
        gutil = float(parts[1])
        used_mib = float(parts[2])
        tot_mib = float(parts[3])
        used_gb = used_mib / 1024.0
        tot_gb = tot_mib / 1024.0
        pct = round(100.0 * used_mib / tot_mib, 1) if tot_mib > 0 else 0.0
        return gname, round(gutil, 1), round(used_gb, 1), round(tot_gb, 1), pct
    except Exception:
        return "", 0.0, 0.0, 0.0, 0.0

def _sampler():
    prev_tot, prev_idle = _read_cpu_times()
    cores = os.cpu_count() or 1
    time.sleep(0.25)
    while True:
        try:
            cur_tot, cur_idle = _read_cpu_times()
            dt_tot = max(1, cur_tot - prev_tot)
            dt_idle = max(0, cur_idle - prev_idle)
            prev_tot, prev_idle = cur_tot, cur_idle
            cpu_pct = round(max(0.0, min(100.0, 100.0 * (dt_tot - dt_idle) / dt_tot)), 1)

            ram_u, ram_t, ram_p = _read_mem_gb()
            gname, gutil, vram_u, vram_t, vram_p = _read_gpu()
            du = shutil.disk_usage("/content" if os.path.isdir("/content") else "/")
            disk_u = round(du.used / (1024.0 ** 3), 1)
            disk_t = round(du.total / (1024.0 ** 3), 1)
            disk_p = round(100.0 * du.used / du.total, 1) if du.total > 0 else 0.0

            payload = {
                "ok": True,
                "ts": round(time.time(), 2),
                "cpu_pct": cpu_pct,
                "cpu_cores": cores,
                "ram_used_gb": ram_u,
                "ram_total_gb": ram_t,
                "ram_pct": ram_p,
                "vram_used_gb": vram_u,
                "vram_total_gb": vram_t,
                "vram_pct": vram_p,
                "gpu_util_pct": gutil,
                "gpu_name": gname,
                "disk_used_gb": disk_u,
                "disk_total_gb": disk_t,
                "disk_pct": disk_p,
            }
            with SNAP_LOCK:
                SNAP.clear()
                SNAP.update(payload)
        except Exception:
            pass
        time.sleep(2.5)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass
    def do_GET(self):
        if self.path.startswith("/metrics") or self.path == "/health":
            with SNAP_LOCK:
                body = json.dumps(SNAP, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

if __name__ == "__main__":
    threading.Thread(target=_sampler, daemon=True).start()
    time.sleep(0.35)
    ThreadingHTTPServer(("127.0.0.1", 8190), Handler).serve_forever()
'''


def ensure_metrics_and_tunnel():
    daemon_path = "/content/_g4_metrics_daemon.py"
    tmp_d = f"{daemon_path}.{os.getpid()}"
    with open(tmp_d, "w", encoding="utf-8") as fh:
        fh.write(DAEMON_CODE)
    os.chmod(tmp_d, 0o755)
    os.replace(tmp_d, daemon_path)

    alive = False
    try:
        with socket.create_connection(("127.0.0.1", LOCAL_PORT), timeout=0.8):
            alive = True
    except OSError:
        pass
    if not alive:
        log_fh = open("/content/g4_metrics.log", "a")
        subprocess.Popen([sys.executable, daemon_path], stdout=log_fh, stderr=log_fh, start_new_session=True)
        for _ in range(15):
            time.sleep(0.2)
            try:
                with socket.create_connection(("127.0.0.1", LOCAL_PORT), timeout=0.5):
                    break
            except OSError:
                pass

    tunnel_key_b64 = os.environ["COLABBB_TUNNEL_KEY_B64"].strip()
    tunnel_host = os.environ["COLABBB_TUNNEL_HOST"].strip()
    tunnel_port = os.environ.get("COLABBB_TUNNEL_PORT", "22").strip() or "22"
    tunnel_user = os.environ.get("COLABBB_TUNNEL_USER", "root").strip() or "root"
    if not tunnel_key_b64 or not tunnel_host or tunnel_host.startswith("your-"):
        return

    key_path = "/content/.colabbb_tunnel_key_metrics"
    tmp_k = f"{key_path}.{os.getpid()}"
    with open(tmp_k, "wb") as fh:
        fh.write(base64.b64decode(tunnel_key_b64))
    os.chmod(tmp_k, 0o600)
    os.replace(tmp_k, key_path)

    if subprocess.run(["pgrep", "-f", f"{TUNNEL_PORT}:127.0.0.1:{LOCAL_PORT}"], capture_output=True).returncode != 0:
        subprocess.Popen([
            "ssh", "-N", "-T",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ServerAliveInterval=20",
            "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes",
            "-i", key_path,
            "-p", tunnel_port,
            "-R", f"127.0.0.1:{TUNNEL_PORT}:127.0.0.1:{LOCAL_PORT}",
            f"{tunnel_user}@{tunnel_host}",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


if __name__ == "__main__":
    ensure_metrics_and_tunnel()
    print("G4_METRICS_READY", flush=True)
