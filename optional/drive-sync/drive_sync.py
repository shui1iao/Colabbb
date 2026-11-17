#!/usr/bin/env python3
"""Colabbb 可选 Google Drive AES-256-GCM 加密作品镜像守护进程。"""
import fcntl
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import stat
import struct
import sys
import time
import uuid
import zlib
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials

_HERE = Path(__file__).resolve().parent
_HOME = Path(os.path.abspath(os.environ.get("COLABBB_HOME") or _HERE.parents[1]))
for _p in (str(_HERE), str(_HOME)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from colab_accounts import get_drive_sync_token_json
from drive_sync_store import apply_deletions, config, ensure_initialized_from_env, gate, root_dir, signature, source_dir

LOG = logging.getLogger("colabbb-drive-sync")
BASE = "https://www.googleapis.com/drive/v3/files"
FIELDS = "id,name,parents,size,md5Checksum,trashed,mimeType,appProperties,webViewLink"
ENC_MAGIC = b"G4E1"
ENC_SCHEME = "aes256gcm_v1"
ENC_OVERHEAD = 4 + 12 + 16


class CloudError(RuntimeError):
    pass


def _aes_key(cfg=None) -> bytes:
    if cfg is None:
        cfg = config()
    hex_k = cfg.get("aes_key_hex") or ""
    if len(hex_k) != 64:
        raise CloudError("Missing or invalid 256-bit aes_key_hex in config.json")
    return bytes.fromhex(hex_k)


def remote_blob_name(version: str, cfg=None) -> str:
    key = _aes_key(cfg)
    digest = hmac.new(key, version.encode("utf-8"), hashlib.sha256).hexdigest()[:20]
    return f"blob_{digest}.bin"


def encrypt_payload(plaintext: bytes, version: str, cfg=None) -> bytes:
    key = _aes_key(cfg)
    nonce = os.urandom(12)
    ct = AESGCM(key).encrypt(nonce, plaintext, version.encode("utf-8"))
    return ENC_MAGIC + nonce + ct


def decrypt_payload(blob: bytes, version: str = "", cfg=None) -> bytes:
    if len(blob) < ENC_OVERHEAD or blob[:4] != ENC_MAGIC:
        raise ValueError("Not a valid encrypted payload")
    key = _aes_key(cfg)
    nonce = blob[4:16]
    ct = blob[16:]
    aad = version.encode("utf-8") if version else None
    try:
        return AESGCM(key).decrypt(nonce, ct, aad)
    except Exception:
        if aad is not None:
            return AESGCM(key).decrypt(nonce, ct, None)
        raise


def png_complete(data: bytes) -> bool:
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        return False
    off, image_data, header = 8, False, False
    while off + 12 <= len(data):
        n = struct.unpack(">I", data[off : off + 4])[0]
        if n > len(data) - off - 12:
            return False
        typ = data[off + 4 : off + 8]
        payload = data[off + 8 : off + 8 + n]
        crc = struct.unpack(">I", data[off + 8 + n : off + 12 + n])[0]
        if zlib.crc32(typ + payload) & 0xFFFFFFFF != crc:
            return False
        if off == 8:
            if typ != b"IHDR" or n != 13:
                return False
            header = True
        if typ == b"IDAT":
            image_data = True
        off += n + 12
        if typ == b"IEND":
            return n == 0 and header and image_data and off == len(data)
    return False


def snapshot(name: str):
    if name.startswith(".") or os.path.basename(name) != name:
        return None
    src = source_dir()
    try:
        fd = os.open(src / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= 100 * 1024 * 1024:
            return None
        with os.fdopen(fd, "rb", closefd=False) as f:
            data = f.read(before.st_size + 1)
        after = os.fstat(fd)
        if (
            signature(name, before) != signature(name, after)
            or len(data) != before.st_size
            or not png_complete(data)
        ):
            return None
        return signature(name, before), hashlib.md5(data).hexdigest(), data
    finally:
        os.close(fd)


class Cloud:
    def __init__(self):
        ensure_initialized_from_env()
        self.cfg = config()
        _aes_key(self.cfg)
        token = get_drive_sync_token_json(self.cfg.get("account_email"))
        if not token:
            raise CloudError("Colab OAuth 凭证不可用，请先登录 Google 账号")
        credentials = Credentials.from_authorized_user_info(json.loads(token))
        credentials.refresh(Request())
        self.session = AuthorizedSession(credentials)
        if self.cfg.get("project_id"):
            self.session.headers["x-goog-user-project"] = self.cfg["project_id"]

    def req(self, method, url, **kw):
        r = self.session.request(method, url, timeout=(15, 90), **kw)
        if not r.ok:
            raise CloudError("Drive HTTP %s" % r.status_code)
        return r.json() if r.content else {}

    def new_id(self):
        return self.req("GET", BASE + "/generateIds", params={"count": 1, "space": "drive"})["ids"][0]

    def get(self, fid):
        r = self.session.get(BASE + "/" + fid, params={"fields": FIELDS}, timeout=(15, 60))
        if r.status_code == 404:
            return None
        if not r.ok:
            raise CloudError("Drive metadata HTTP %s" % r.status_code)
        return r.json()

    def permanent_delete(self, fid):
        r = self.session.delete(BASE + "/" + fid, timeout=(15, 60))
        if r.status_code not in (200, 204, 404):
            raise CloudError("Drive delete HTTP %s" % r.status_code)

    def owned(self, remote, folder=None):
        if remote.get("appProperties", {}).get("g4_owner") != self.cfg["owner"]:
            raise CloudError("Drive object ownership mismatch; refusing mutation")
        if folder and folder not in remote.get("parents", []):
            raise CloudError("Drive object moved out of dedicated folder; refusing mutation")

    def folder(self):
        with gate() as c:
            row = c.execute("SELECT value FROM meta WHERE key='folder_id'").fetchone()
            fid = row[0] if row else None
        if not fid:
            new = self.new_id()
            with gate() as c:
                c.execute("INSERT OR IGNORE INTO meta VALUES ('folder_id',?)", (new,))
                fid = c.execute("SELECT value FROM meta WHERE key='folder_id'").fetchone()[0]
        remote = self.get(fid)
        if remote is None:
            folder_title = self.cfg.get("folder_name") or "Colabbb-Gallery"
            remote = self.req(
                "POST",
                BASE,
                params={"fields": FIELDS},
                json={
                    "id": fid,
                    "name": folder_title,
                    "mimeType": "application/vnd.google-apps.folder",
                    "appProperties": {"g4_owner": self.cfg["owner"]},
                },
            )
        self.owned(remote)
        if remote.get("trashed"):
            raise CloudError("Dedicated folder is in trash; sync halted")
        return fid

    def upload(self, row, plaintext_data, folder):
        expected_blob_name = remote_blob_name(row["version"], self.cfg)
        expected_enc_size = row["size"] + ENC_OVERHEAD
        remote = self.get(row["drive_id"])
        if remote is not None:
            self.owned(remote, folder)
            props = remote.get("appProperties", {})
            if props.get("g4_enc") != ENC_SCHEME or remote.get("name") != expected_blob_name:
                self.permanent_delete(row["drive_id"])
                remote = None

        if remote is None:
            enc_bytes = encrypt_payload(plaintext_data, row["version"], self.cfg)
            enc_md5 = hashlib.md5(enc_bytes).hexdigest()
            boundary = "colabbbsync_" + uuid.uuid4().hex
            meta = json.dumps(
                {
                    "id": row["drive_id"],
                    "name": expected_blob_name,
                    "mimeType": "application/octet-stream",
                    "parents": [folder],
                    "appProperties": {
                        "g4_owner": self.cfg["owner"],
                        "g4_version": row["version"],
                        "g4_enc": ENC_SCHEME,
                        "g4_enc_md5": enc_md5,
                    },
                },
                ensure_ascii=False,
            ).encode()
            body = (
                ("--" + boundary + "\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").encode()
                + meta
                + ("\r\n--" + boundary + "\r\nContent-Type: application/octet-stream\r\n\r\n").encode()
                + enc_bytes
                + ("\r\n--" + boundary + "--\r\n").encode()
            )
            self.req(
                "POST",
                "https://www.googleapis.com/upload/drive/v3/files",
                params={"uploadType": "multipart", "fields": FIELDS},
                data=body,
                headers={"Content-Type": "multipart/related; boundary=" + boundary},
            )
            remote = self.get(row["drive_id"])

        self.owned(remote, folder)
        props = remote.get("appProperties", {})
        if (
            remote.get("trashed")
            or remote.get("name") != expected_blob_name
            or props.get("g4_enc") != ENC_SCHEME
            or remote.get("md5Checksum") != props.get("g4_enc_md5")
            or int(remote.get("size", -1)) != expected_enc_size
            or props.get("g4_version") != row["version"]
        ):
            raise CloudError("Remote encrypted blob checksum/identity mismatch; refusing overwrite")

    def trash(self, row, folder):
        if not row["drive_id"]:
            return
        remote = self.get(row["drive_id"])
        if remote is None:
            return
        self.owned(remote, folder)
        if remote.get("appProperties", {}).get("g4_version") != row["version"]:
            raise CloudError("Deletion identity mismatch")
        if not remote.get("trashed"):
            self.req("PATCH", BASE + "/" + row["drive_id"], json={"trashed": True}, params={"fields": "id,trashed"})
            remote = self.get(row["drive_id"])
            if remote is not None and not remote.get("trashed"):
                raise CloudError("Trash operation not confirmed")


def discover():
    ensure_initialized_from_env()
    src = source_dir()
    src.mkdir(parents=True, exist_ok=True)
    with gate() as c:
        apply_deletions(c)
        names = [p.name for p in src.iterdir() if not p.name.startswith(".")]
        for name in names:
            p = src / name
            st = p.lstat()
            if not stat.S_ISREG(st.st_mode):
                continue
            ver = signature(name, st)
            if c.execute("SELECT 1 FROM files WHERE version=?", (ver,)).fetchone():
                continue
            if c.execute("SELECT 1 FROM deletions WHERE version=?", (ver,)).fetchone():
                continue
            if not name.lower().endswith(".png"):
                continue
            snap = snapshot(name)
            if snap and snap[0] == ver:
                c.execute(
                    "INSERT OR IGNORE INTO files(version,name,md5,size) VALUES (?,?,?,?)",
                    (ver, name, snap[1], len(snap[2])),
                )


def sync_once(cloud, folder, force_retry: bool = False) -> dict:
    discover()
    now_ts = time.time()
    with gate() as c:
        if force_retry:
            c.execute(
                "UPDATE files SET retry_at=0 WHERE trashed=0 AND (uploaded=0 OR delete_requested=1) AND retry_at>0"
            )
        rows = [
            dict(r)
            for r in c.execute(
                "SELECT * FROM files WHERE trashed=0 "
                "AND (uploaded=0 OR delete_requested=1) AND retry_at<=? "
                "ORDER BY delete_requested DESC,name",
                (now_ts,),
            )
        ]
    uploaded_now = 0
    trashed_now = 0
    failed_now = 0
    for row in rows:
        try:
            with gate() as c:
                apply_deletions(c)
                row = dict(c.execute("SELECT * FROM files WHERE version=?", (row["version"],)).fetchone())
            if row["delete_requested"]:
                cloud.trash(row, folder)
                with gate() as c:
                    c.execute("UPDATE files SET trashed=1,last_error='',attempts=0 WHERE version=?", (row["version"],))
                trashed_now += 1
                LOG.info("Gallery deletion synchronized to Drive trash")
                continue
            with gate() as c:
                snap = snapshot(row["name"])
            if not snap or snap[0] != row["version"] or snap[1] != row["md5"]:
                raise CloudError("Source changed/missing; awaiting explicit gallery intent")
            if not row["drive_id"]:
                fid = cloud.new_id()
                with gate() as c:
                    c.execute("UPDATE files SET drive_id=? WHERE version=? AND drive_id IS NULL", (fid, row["version"]))
                    row["drive_id"] = c.execute("SELECT drive_id FROM files WHERE version=?", (row["version"],)).fetchone()[0]
            cloud.upload(row, snap[2], folder)
            with gate() as c:
                apply_deletions(c)
                c.execute(
                    "UPDATE files SET uploaded=1,last_error='',attempts=0,retry_at=0 WHERE version=?",
                    (row["version"],),
                )
            uploaded_now += 1
            LOG.info("Encrypted blob uploaded; persisted size and MD5 matched")
        except Exception as exc:
            failed_now += 1
            code = str(exc) if isinstance(exc, CloudError) else type(exc).__name__
            with gate() as c:
                c.execute(
                    "UPDATE files SET attempts=attempts+1,retry_at=?,last_error=? WHERE version=?",
                    (time.time() + min(600, 10 * 2 ** min(row["attempts"], 6)), code, row["version"]),
                )
            LOG.warning("Image operation deferred: %s", code)

    r_dir = root_dir()
    with gate() as c:
        values = c.execute(
            "SELECT COUNT(*),COALESCE(SUM(uploaded=1 AND delete_requested=0 AND trashed=0),0),"
            "COALESCE(SUM(uploaded=0 AND delete_requested=0 AND trashed=0),0),"
            "COALESCE(SUM(delete_requested=1 AND trashed=0),0),"
            "COALESCE(SUM(trashed=1),0),COALESCE(SUM(last_error!='' AND trashed=0),0) FROM files"
        ).fetchone()
        status = dict(zip(("tracked", "uploaded", "pending_upload", "pending_delete", "trashed", "errors"), values))
        status.update(
            updated_at=time.time(),
            encryption=ENC_SCHEME,
            folder_url="https://drive.google.com/drive/folders/" + folder,
        )
        temp = r_dir / ".status.tmp"
        fd = os.open(temp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(status, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, r_dir / "status.json")
    return {
        "ok": True,
        "uploaded_now": uploaded_now,
        "trashed_now": trashed_now,
        "failed_now": failed_now,
        "status": status,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ensure_initialized_from_env()
    r_dir = root_dir()
    lock_file = r_dir / "worker.lock"
    with open(lock_file, "a") as lock:
        os.chmod(lock_file, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cloud, folder = None, None
        while True:
            try:
                if cloud is None:
                    cloud = Cloud()
                folder = cloud.folder()
                sync_once(cloud, folder)
            except Exception as exc:
                LOG.error("Sync cycle paused: %s", str(exc) if isinstance(exc, CloudError) else type(exc).__name__)
                cloud = None
            time.sleep(10)


if __name__ == "__main__":
    main()
