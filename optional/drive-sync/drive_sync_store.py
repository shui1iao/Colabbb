"""Colabbb 可选 Google Drive 加密同步状态存储与原子删除登记。"""
import contextlib
import fcntl
import hashlib
import json
import os
import secrets
import sqlite3
import stat
from pathlib import Path


def _base_dir() -> Path:
    return Path(os.path.abspath(os.environ.get("COLABBB_HOME") or Path(__file__).resolve().parents[2]))


def root_dir() -> Path:
    st = os.path.abspath(os.environ.get("COLABBB_STATE_DIR") or str(_base_dir() / "state"))
    return Path(st) / "drive-sync"


def source_dir() -> Path:
    out = os.path.abspath(os.environ.get("COLABBB_OUTPUT_DIR") or str(_base_dir() / "outputs"))
    return Path(out) / "image"


def config_path() -> Path:
    return root_dir() / "config.json"


def db_path() -> Path:
    return root_dir() / "sync.sqlite3"


def signature(name, st):
    fields = (name, st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
    return hashlib.sha256(json.dumps(fields, ensure_ascii=False).encode()).hexdigest()


def ensure_initialized_from_env() -> dict:
    """当管理员开启 COLABBB_ENABLE_DRIVE_SYNC=1 且尚未初始化时，从环境变量安全初始化加密同步库。"""
    r_dir = root_dir()
    cfg_f = config_path()
    db_f = db_path()
    src = source_dir()
    src.mkdir(parents=True, exist_ok=True)
    r_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    if cfg_f.is_file() and db_f.is_file():
        return config()

    project_id = (os.environ.get("COLABBB_DRIVE_GCP_PROJECT") or "").strip()
    aes_key_hex = (os.environ.get("COLABBB_DRIVE_AES_KEY_HEX") or "").strip().lower()
    if not aes_key_hex:
        aes_key_hex = secrets.token_hex(32)
    if len(aes_key_hex) != 64:
        raise RuntimeError("COLABBB_DRIVE_AES_KEY_HEX 必须为 64 位十六进制字符串（256-bit）")
    bytes.fromhex(aes_key_hex)

    owner_tag = (os.environ.get("COLABBB_DRIVE_OWNER_TAG") or "colabbb-gallery").strip() or "colabbb-gallery"
    folder_name = (os.environ.get("COLABBB_DRIVE_FOLDER_NAME") or "Colabbb-Gallery").strip() or "Colabbb-Gallery"
    folder_id = (os.environ.get("COLABBB_DRIVE_FOLDER_ID") or "").strip()
    acct_email = (os.environ.get("COLABBB_DRIVE_SYNC_EMAIL") or "").strip().lower()

    st = src.lstat()
    if not stat.S_ISDIR(st.st_mode):
        raise RuntimeError("Unsafe image source directory")

    if not db_f.exists():
        fd = os.open(db_f, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        c = sqlite3.connect(db_f)
        c.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS files (
              version TEXT PRIMARY KEY, name TEXT NOT NULL, drive_id TEXT UNIQUE,
              md5 TEXT NOT NULL, size INTEGER NOT NULL,
              uploaded INTEGER NOT NULL DEFAULT 0, delete_requested INTEGER NOT NULL DEFAULT 0,
              trashed INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
              retry_at REAL NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS deletions (version TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        if folder_id:
            c.execute("INSERT OR IGNORE INTO meta VALUES ('folder_id', ?)", (folder_id,))
        c.commit()
        c.close()

    if not cfg_f.exists():
        doc = {
            "project_id": project_id,
            "aes_key_hex": aes_key_hex,
            "owner": owner_tag,
            "folder_name": folder_name,
            "account_email": acct_email,
            "source_identity": [st.st_dev, st.st_ino],
        }
        fd = os.open(cfg_f, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())

    return config()


def config():
    cfg_f = config_path()
    src = source_dir()
    src.mkdir(parents=True, exist_ok=True)
    d = json.loads(cfg_f.read_text(encoding="utf-8"))
    st = src.lstat()
    if not stat.S_ISDIR(st.st_mode):
        raise RuntimeError("Source directory missing or invalid")
    # 容器挂载卷重建后设备号可能变化，若目录仍为受控常规目录且非符号链接，保持兼容或校验
    if "source_identity" in d and [st.st_dev, st.st_ino] != d["source_identity"]:
        if os.environ.get("COLABBB_DRIVE_STRICT_INODE", "0") == "1":
            raise RuntimeError("Source directory identity changed; sync halted")
    env_proj = (os.environ.get("COLABBB_DRIVE_GCP_PROJECT") or "").strip()
    if env_proj and not d.get("project_id"):
        d["project_id"] = env_proj
    env_em = (os.environ.get("COLABBB_DRIVE_SYNC_EMAIL") or "").strip().lower()
    if env_em:
        d["account_email"] = env_em
    return d


@contextlib.contextmanager
def gate():
    r_dir = root_dir()
    db_f = db_path()
    if not r_dir.is_dir() or not db_f.is_file():
        ensure_initialized_from_env()
    lock_f = r_dir / "state.lock"
    with open(lock_f, "a") as f:
        os.chmod(lock_f, 0o600)
        fcntl.flock(f, fcntl.LOCK_EX)
        c = sqlite3.connect("file:" + str(db_f) + "?mode=rw", uri=True, timeout=15)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA synchronous=FULL")
        try:
            config()
            yield c
            c.commit()
        finally:
            c.close()


def remove_image(path):
    """若已初始化 Google Drive 同步库，则在删除本地原图前先持久化删除意图，否则直接删除本地文件。"""
    p = Path(os.path.abspath(path))
    src = source_dir()
    if p.parent != src or p.name.startswith("."):
        os.remove(path)
        return
    if not config_path().is_file() or not db_path().is_file():
        os.remove(path)
        return
    with gate() as c:
        st = p.lstat()
        if not stat.S_ISREG(st.st_mode):
            raise ValueError("Refusing symlink or nonregular image deletion")
        ver = signature(p.name, st)
        c.execute("INSERT OR IGNORE INTO deletions VALUES (?,?)", (ver, p.name))
        c.commit()
        try:
            os.unlink(p)
        except Exception:
            c.execute("DELETE FROM deletions WHERE version=?", (ver,))
            c.commit()
            raise


def apply_deletions(c):
    src = source_dir()
    for r in c.execute("SELECT version,name FROM deletions").fetchall():
        p = src / r["name"]
        try:
            st = p.lstat()
        except FileNotFoundError:
            st = None
        if st is not None and signature(p.name, st) == r["version"]:
            c.execute("DELETE FROM deletions WHERE version=?", (r["version"],))
        else:
            c.execute(
                "UPDATE files SET delete_requested=1,retry_at=0 WHERE version=? AND delete_requested=0",
                (r["version"],),
            )
