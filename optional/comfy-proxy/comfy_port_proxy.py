#!/usr/bin/env python3
"""可选 ComfyUI TCP 代理与活跃度续期；公网使用需自行增加鉴权与访问控制。"""
import asyncio, os, time

HOME = os.environ.get('COLABBB_HOME', '/opt/colabbb')
STATE_DIR = os.environ.get('COLABBB_STATE_DIR', os.path.join(HOME, 'state'))
PAIRS = [(os.environ.get('COLABBB_COMFY_PROXY_HOST', '127.0.0.1'),
          int(os.environ.get('COLABBB_COMFY_PROXY_PORT', '18188')),
          os.environ.get('COLABBB_COMFY_TARGET_HOST', '127.0.0.1'),
          int(os.environ.get('COLABBB_COMFY_TARGET_PORT', '28188')))]
IDLE_TOUCH = os.path.join(STATE_DIR, 'last_active.ts')
_last_touch = 0.0

def touch_activity():
    global _last_touch
    now = time.time()
    if now - _last_touch < 10.0:
        return
    _last_touch = now
    try:
        os.makedirs(os.path.dirname(IDLE_TOUCH), exist_ok=True)
        with open(IDLE_TOUCH, "w") as fh:
            fh.write(str(now))
    except Exception:
        pass

async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            touch_activity()
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass

async def handle_client(client_reader, client_writer, target_host, target_port):
    touch_activity()
    try:
        remote_reader, remote_writer = await asyncio.open_connection(target_host, target_port)
    except Exception:
        try:
            client_writer.close()
        except Exception:
            pass
        return
    await asyncio.gather(
        pipe(client_reader, remote_writer),
        pipe(remote_reader, client_writer),
    )

async def main():
    servers = []
    for lhost, lport, thost, tport in PAIRS:
        srv = await asyncio.start_server(
            lambda r, w, th=thost, tp=tport: handle_client(r, w, th, tp),
            lhost, lport, reuse_address=True
        )
        servers.append(srv)
    await asyncio.gather(*(s.serve_forever() for s in servers))

if __name__ == "__main__":
    asyncio.run(main())
