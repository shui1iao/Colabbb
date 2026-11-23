#!/usr/bin/env python3
"""Colabbb 可选模块：Google Drive 加密镜像 → Telegram 频道按需相册同步器（支持联动撤回删除）。"""
import fcntl
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

_HERE = Path(__file__).resolve().parent
_HOME = Path(os.path.abspath(os.environ.get("COLABBB_HOME") or _HERE.parents[1]))
for _p in (str(_HERE), str(_HOME)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from drive_sync import BASE, Cloud, ENC_MAGIC, decrypt_payload, sync_once
from drive_sync_store import gate, root_dir, source_dir

ALBUM_BATCH_SIZE = 6


def get_channel_config() -> tuple[str, int, str]:
    bot_token = (os.environ.get("TG_BOT_TOKEN") or "").strip()
    raw_cid = (os.environ.get("TG_SYNC_CHANNEL_ID") or "").strip()
    channel_id = int(raw_cid) if raw_cid and raw_cid.lstrip("-").isdigit() else 0
    channel_label = (os.environ.get("TG_SYNC_CHANNEL_NAME") or (raw_cid if raw_cid else "TG 频道")).strip()
    return bot_token, channel_id, channel_label


def tg_api(bot_token: str, method: str, payload: dict = None, timeout: int = 30) -> dict:
    url = f"https://api.telegram.org/bot{bot_token}/{method}"
    data = json.dumps(payload or {}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8", errors="ignore")
            try:
                err_json = json.loads(err)
                if e.code == 429:
                    retry_after = int((err_json.get("parameters") or {}).get("retry_after", 3)) + 1
                    time.sleep(retry_after)
                    continue
                return err_json
            except Exception:
                return {"ok": False, "error_code": e.code, "description": "Telegram HTTP error"}
        except Exception as e:
            if attempt < 3:
                time.sleep(2)
                continue
            return {"ok": False, "description": type(e).__name__}
    return {"ok": False, "description": "max retries exceeded"}


def prepare_tg_safe_image(file_path: str, force_jpg: bool = False) -> tuple[str, bool]:
    if not force_jpg and os.path.getsize(file_path) <= 9_500_000:
        return file_path, False
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
        tmp_jpg = tmp.name
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-i", file_path, "-q:v", "1", tmp_jpg],
        check=True,
        timeout=30,
    )
    return tmp_jpg, True


def tg_upload_photo(bot_token: str, chat_id: int, file_path: str, timeout: int = 120) -> dict:
    api_base = f"https://api.telegram.org/bot{bot_token}"

    def _do_upload(path_to_send: str) -> dict:
        boundary = "----TgSyncBoundary" + uuid.uuid4().hex
        body = bytearray()
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(b'Content-Disposition: form-data; name="chat_id"\r\n\r\n')
        body.extend(f"{chat_id}\r\n".encode("utf-8"))

        fname = os.path.basename(path_to_send)
        ctype = mimetypes.guess_type(path_to_send)[0] or "image/png"
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="photo"; filename="{fname}"\r\n'.encode("utf-8"))
        body.extend(f"Content-Type: {ctype}\r\n\r\n".encode())
        with open(path_to_send, "rb") as fh:
            body.extend(fh.read())
        body.extend(f"\r\n--{boundary}--\r\n".encode())

        req = urllib.request.Request(
            f"{api_base}/sendPhoto",
            data=bytes(body),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                err = e.read().decode("utf-8", errors="ignore")
                try:
                    err_json = json.loads(err)
                except Exception:
                    err_json = {"ok": False, "error_code": e.code, "description": "Telegram HTTP error"}
                if e.code == 429:
                    retry_after = int((err_json.get("parameters") or {}).get("retry_after", 4)) + 1
                    time.sleep(retry_after)
                    continue
                return err_json
            except Exception as e:
                if attempt < 4:
                    time.sleep(2)
                    continue
                return {"ok": False, "description": type(e).__name__}
        return {"ok": False, "description": "upload retries exceeded"}

    send_path, is_conv = prepare_tg_safe_image(file_path, force_jpg=False)
    try:
        res = _do_upload(send_path)
        if res.get("ok"):
            return res
        desc = (res.get("description") or "").upper()
        if not is_conv and any(
            k in desc for k in ("PHOTO_SAVE_FILE_INVALID", "IMAGE_PROCESS_FAILED", "FILE_PARTS_INVALID", "TOO LARGE")
        ):
            jpg_path, _ = prepare_tg_safe_image(file_path, force_jpg=True)
            try:
                return _do_upload(jpg_path)
            finally:
                try:
                    os.unlink(jpg_path)
                except Exception:
                    pass
        return res
    finally:
        if is_conv:
            try:
                os.unlink(send_path)
            except Exception:
                pass


def tg_upload_media_group(bot_token: str, chat_id: int, file_paths: list[str], timeout: int = 180) -> dict:
    if len(file_paths) == 1:
        single = tg_upload_photo(bot_token, chat_id, file_paths[0], timeout=timeout)
        if single.get("ok") and single.get("result"):
            return {"ok": True, "result": [single["result"]]}
        return single

    api_base = f"https://api.telegram.org/bot{bot_token}"

    def _do_group_upload(paths_to_send: list[str]) -> dict:
        boundary = "----TgGroupBoundary" + uuid.uuid4().hex
        body = bytearray()
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(b'Content-Disposition: form-data; name="chat_id"\r\n\r\n')
        body.extend(f"{chat_id}\r\n".encode("utf-8"))

        media_arr = [{"type": "photo", "media": f"attach://photo_{i}"} for i in range(len(paths_to_send))]
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(b'Content-Disposition: form-data; name="media"\r\n\r\n')
        body.extend(f"{json.dumps(media_arr, ensure_ascii=False)}\r\n".encode("utf-8"))

        for i, p in enumerate(paths_to_send):
            fname = os.path.basename(p)
            ctype = mimetypes.guess_type(p)[0] or "image/png"
            body.extend(f"--{boundary}\r\n".encode())
            body.extend(f'Content-Disposition: form-data; name="photo_{i}"; filename="{fname}"\r\n'.encode("utf-8"))
            body.extend(f"Content-Type: {ctype}\r\n\r\n".encode())
            with open(p, "rb") as fh:
                body.extend(fh.read())
            body.extend(b"\r\n")

        body.extend(f"--{boundary}--\r\n".encode())
        req = urllib.request.Request(
            f"{api_base}/sendMediaGroup",
            data=bytes(body),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                err = e.read().decode("utf-8", errors="ignore")
                try:
                    err_json = json.loads(err)
                except Exception:
                    err_json = {"ok": False, "error_code": e.code, "description": "Telegram HTTP error"}
                if e.code == 429:
                    retry_after = int((err_json.get("parameters") or {}).get("retry_after", 5)) + 1
                    time.sleep(retry_after)
                    continue
                return err_json
            except Exception as e:
                if attempt < 4:
                    time.sleep(2)
                    continue
                return {"ok": False, "description": type(e).__name__}
        return {"ok": False, "description": "group upload retries exceeded"}

    prepared = []
    try:
        for p in file_paths:
            sp, is_c = prepare_tg_safe_image(p, force_jpg=False)
            prepared.append((sp, is_c))
        res = _do_group_upload([x[0] for x in prepared])
        if res.get("ok"):
            return res
        desc = (res.get("description") or "").upper()
        if any(
            k in desc for k in ("PHOTO_SAVE_FILE_INVALID", "IMAGE_PROCESS_FAILED", "FILE_PARTS_INVALID", "TOO LARGE")
        ):
            for sp, is_c in prepared:
                if is_c:
                    try:
                        os.unlink(sp)
                    except Exception:
                        pass
            prepared = []
            for p in file_paths:
                sp, is_c = prepare_tg_safe_image(p, force_jpg=True)
                prepared.append((sp, is_c))
            return _do_group_upload([x[0] for x in prepared])
        return res
    finally:
        for sp, is_c in prepared:
            if is_c:
                try:
                    os.unlink(sp)
                except Exception:
                    pass


def ensure_table():
    with gate() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tg_channel_posts (
                drive_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                md5 TEXT NOT NULL DEFAULT '',
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                posted_at REAL NOT NULL,
                deleted_from_tg INTEGER NOT NULL DEFAULT 0
            )
            """
        )


def list_drive_active_images(cloud: Cloud, folder_id: str) -> list[dict]:
    with gate() as c:
        db_by_ver = {
            r["version"]: dict(r)
            for r in c.execute("SELECT version, name, md5, drive_id FROM files").fetchall()
        }
        db_by_did = {r["drive_id"]: r for r in db_by_ver.values() if r.get("drive_id")}

    files = []
    page_token = None
    while True:
        params = {
            "q": f"'{folder_id}' in parents and trashed = false",
            "fields": "nextPageToken,files(id,name,size,md5Checksum,createdTime,mimeType,appProperties)",
            "pageSize": 1000,
            "orderBy": "createdTime,name",
        }
        if page_token:
            params["pageToken"] = page_token
        res = cloud.req("GET", BASE, params=params)
        for f in res.get("files", []):
            props = f.get("appProperties") or {}
            if props.get("g4_owner") != cloud.cfg.get("owner"):
                continue
            ver = props.get("g4_version") or ""
            db_row = db_by_ver.get(ver) or db_by_did.get(f["id"]) or {}
            f["_orig_name"] = db_row.get("name") or f.get("name") or ""
            f["_orig_md5"] = db_row.get("md5") or f.get("md5Checksum") or ""
            f["_g4_version"] = ver
            files.append(f)
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    def _sort_key(item: dict):
        name = item.get("_orig_name") or item.get("name") or ""
        m = re.search(r"_(\d{10,})_", name)
        ts_num = int(m.group(1)) if m else 0
        return (ts_num if ts_num > 0 else 0, item.get("createdTime") or "", name)

    files.sort(key=_sort_key)
    return files


def get_image_local_or_download(cloud: Cloud, item: dict) -> tuple[str, bool]:
    src = source_dir()
    orig_name = os.path.basename(item.get("_orig_name") or item.get("name") or "")
    exp_md5 = item.get("_orig_md5") or ""
    if orig_name and orig_name.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
        cand = src / orig_name
        if cand.is_file():
            try:
                data = cand.read_bytes()
                if not exp_md5 or hashlib.md5(data).hexdigest() == exp_md5:
                    return str(cand), False
            except Exception:
                pass

    fid = item["id"]
    r = cloud.session.get(f"{BASE}/{fid}", params={"alt": "media"}, timeout=(15, 120))
    if not r.ok:
        raise RuntimeError(f"Drive download HTTP {r.status_code}")
    raw = r.content
    ver = item.get("_g4_version") or (item.get("appProperties") or {}).get("g4_version") or ""
    if raw[:4] == ENC_MAGIC:
        raw = decrypt_payload(raw, ver, cloud.cfg)
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp.write(raw)
        return tmp.name, True


def record_posted_item(channel_id: int, item: dict, mid: int):
    with gate() as c:
        c.execute(
            """
            INSERT INTO tg_channel_posts(drive_id, name, md5, channel_id, message_id, posted_at, deleted_from_tg)
            VALUES (?,?,?,?,?,?,0)
            ON CONFLICT(drive_id) DO UPDATE SET
                name=excluded.name,
                md5=excluded.md5,
                channel_id=excluded.channel_id,
                message_id=excluded.message_id,
                posted_at=excluded.posted_at,
                deleted_from_tg=0
            """,
            (
                item["id"],
                item.get("_orig_name") or item.get("name") or "",
                item.get("_orig_md5") or item.get("md5Checksum") or "",
                channel_id,
                mid,
                time.time(),
            ),
        )


def run_sync(notify_chat_id: int = 0, progress_msg_id: int = 0, progress_cb=None) -> dict:
    bot_token, channel_id, channel_label = get_channel_config()
    if not bot_token or not channel_id:
        return {"ok": False, "message": "未配置 TG_BOT_TOKEN 或 TG_SYNC_CHANNEL_ID，无法同步至频道"}

    def _report(text: str, **meta):
        if notify_chat_id and progress_msg_id:
            tg_api(bot_token, "editMessageText", {"chat_id": notify_chat_id, "message_id": progress_msg_id, "text": text})
        if callable(progress_cb):
            try:
                progress_cb(text, meta)
            except Exception:
                pass

    ensure_table()
    r_dir = root_dir()
    r_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_file = r_dir / "tg_channel_sync.lock"
    with open(lock_file, "a") as lf:
        os.chmod(lock_file, 0o600)
        try:
            fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _report("正在执行频道同步中，请稍候...", stage="busy")
            return {"ok": False, "busy": True, "message": "正在执行频道同步中，请稍候"}

        t0 = time.time()
        _report("正在连接 Google Drive 检查加密库与频道状态...", stage="checking")

        cloud = Cloud()
        folder_id = cloud.folder()
        lock_path = root_dir() / "worker.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(lock_path, "a+") as wlk:
                try:
                    fcntl.flock(wlk.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    print("[tg_channel_sync] drive_sync daemon holds worker.lock; skipping inline sync_once", flush=True)
                else:
                    sync_once(cloud, folder_id, force_retry=True)
        except Exception as exc:
            print(f"[tg_channel_sync] inline sync_once error: {exc}", flush=True)

        drive_files = list_drive_active_images(cloud, folder_id)
        active_map = {f["id"]: f for f in drive_files}

        with gate() as c:
            posted_rows = [
                dict(r)
                for r in c.execute(
                    "SELECT * FROM tg_channel_posts WHERE channel_id=? AND deleted_from_tg=0",
                    (channel_id,),
                ).fetchall()
            ]
        posted_map = {r["drive_id"]: r for r in posted_rows}

        to_delete = [r for did, r in posted_map.items() if did not in active_map]
        deleted_count = 0
        for row in to_delete:
            res = tg_api(bot_token, "deleteMessage", {"chat_id": row["channel_id"], "message_id": row["message_id"]})
            desc = (res.get("description") or "").lower()
            if res.get("ok") or "message to delete not found" in desc or "message can't be deleted" in desc:
                with gate() as c:
                    c.execute(
                        "UPDATE tg_channel_posts SET deleted_from_tg=1 WHERE drive_id=?",
                        (row["drive_id"],),
                    )
                deleted_count += 1
            time.sleep(0.25)

        to_post = [f for f in drive_files if f["id"] not in posted_map]
        posted_count = 0
        failed_count = 0
        total_to_post = len(to_post)

        batches = [to_post[i : i + ALBUM_BATCH_SIZE] for i in range(0, total_to_post, ALBUM_BATCH_SIZE)]
        for b_idx, batch in enumerate(batches, 1):
            done_so_far = (b_idx - 1) * ALBUM_BATCH_SIZE
            _report(
                f"正在同步图片到频道 {channel_label}（{min(done_so_far + len(batch), total_to_post)}/{total_to_post} 张）...",
                stage="uploading",
                done=min(done_so_far + len(batch), total_to_post),
                total=total_to_post,
            )

            local_files = []
            valid_items = []
            for item in batch:
                try:
                    p, is_tmp = get_image_local_or_download(cloud, item)
                    local_files.append((p, is_tmp))
                    valid_items.append(item)
                except Exception:
                    failed_count += 1

            if valid_items:
                try:
                    up = tg_upload_media_group(bot_token, channel_id, [x[0] for x in local_files])
                    if up.get("ok") and isinstance(up.get("result"), list) and len(up["result"]) == len(valid_items):
                        for item, msg_obj in zip(valid_items, up["result"]):
                            mid = msg_obj.get("message_id", 0)
                            if mid:
                                record_posted_item(channel_id, item, mid)
                                posted_count += 1
                            else:
                                failed_count += 1
                    else:
                        for item, (p, _) in zip(valid_items, local_files):
                            s_up = tg_upload_photo(bot_token, channel_id, p)
                            if s_up.get("ok"):
                                mid = (s_up.get("result") or {}).get("message_id", 0)
                                record_posted_item(channel_id, item, mid)
                                posted_count += 1
                            else:
                                failed_count += 1
                            time.sleep(0.8)
                except Exception:
                    failed_count += len(valid_items)
                finally:
                    for p, is_tmp in local_files:
                        if is_tmp:
                            try:
                                os.unlink(p)
                            except Exception:
                                pass

            if b_idx < len(batches):
                time.sleep(2.2)

        elapsed = time.time() - t0
        with gate() as c:
            total_in_channel = c.execute(
                "SELECT COUNT(*) FROM tg_channel_posts WHERE channel_id=? AND deleted_from_tg=0",
                (channel_id,),
            ).fetchone()[0]

        if posted_count == 0 and deleted_count == 0 and failed_count == 0:
            summary = f"频道 {channel_label} 已与云盘一致（云盘 {len(drive_files)} 张 · 频道 {total_in_channel} 张）"
        else:
            parts = [f"云盘 → 频道 {channel_label} 同步完成 ({elapsed:.1f}s)"]
            parts.append(f"新增发送：{posted_count} 张" + (f"，失败 {failed_count} 张" if failed_count else ""))
            if deleted_count:
                parts.append(f"联动撤回：{deleted_count} 张")
            parts.append(f"加密云盘：{len(drive_files)} 张 | 频道共计：{total_in_channel} 张")
            summary = "\n".join(parts)

        _report(summary, stage="done", posted=posted_count, deleted=deleted_count, failed=failed_count)
        return {
            "ok": True,
            "posted": posted_count,
            "deleted": deleted_count,
            "failed": failed_count,
            "drive_total": len(drive_files),
            "channel_total": total_in_channel,
            "channel_label": channel_label,
            "elapsed_sec": round(elapsed, 1),
            "summary": summary,
        }


if __name__ == "__main__":
    chat_arg = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].lstrip("-").isdigit() else 0
    msg_arg = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 0
    res = run_sync(chat_arg, msg_arg)
    print(json.dumps(res, ensure_ascii=False))
    sys.exit(0 if res.get("ok") else 1)
