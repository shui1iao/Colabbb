#!/usr/bin/env python3
"""Colabbb Google Colab 多账号共存、余额并发查询、安全切换与面板在线授权管理。"""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import glob
import hashlib
from importlib import resources
import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.abspath(os.environ.get("COLABBB_HOME") or BASE_DIR)

_ACCOUNTS_CACHE_LOCK = threading.Lock()
_ACCOUNTS_CACHE = {"ts": 0.0, "data": None}
ACCOUNTS_CACHE_TTL = 30.0


def _state_dir() -> str:
    return os.path.abspath(os.environ.get("COLABBB_STATE_DIR") or os.path.join(BASE_DIR, "state"))


def _accounts_dir() -> str:
    return os.path.join(_state_dir(), "colab-accounts")


def _registry_file() -> str:
    return os.path.join(_accounts_dir(), "accounts.json")


def _switch_lock_file() -> str:
    return os.path.join(_accounts_dir(), ".switch.lock")


def _pending_oauth_file() -> str:
    return os.path.join(_accounts_dir(), ".pending_oauth.json")


def _sessions_path() -> str:
    return os.path.expanduser("~/.config/colab-cli/sessions.json")


def _default_primary_email() -> str:
    return (os.environ.get("COLABBB_DRIVE_SYNC_EMAIL") or "").strip().lower()


def _ensure_dir():
    d = _accounts_dir()
    os.makedirs(d, mode=0o700, exist_ok=True)
    try:
        os.chmod(d, 0o700)
    except OSError:
        pass


def _atomic_write_json(path: str, payload, mode: int = 0o600):
    parent = os.path.dirname(path)
    os.makedirs(parent, mode=0o700, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent, prefix=".tmp_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _atomic_write_text(path: str, text: str, mode: int = 0o600):
    parent = os.path.dirname(path)
    os.makedirs(parent, mode=0o700, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent, prefix=".tmp_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _norm_email(email: str) -> str:
    return (email or "").strip().lower()


def token_path_for(email: str) -> str:
    return os.path.join(_accounts_dir(), f"{_norm_email(email)}.token.json")


def sessions_path_for(email: str) -> str:
    return os.path.join(_accounts_dir(), f"{_norm_email(email)}.sessions.json")


def compute_short_labels(emails: list[str]) -> dict[str, tuple[str, str]]:
    """生成桌面名与手机端短标识（同前缀多账号自动取差异尾缀）。"""
    normed = [_norm_email(e) for e in emails if _norm_email(e)]
    locals_map = {e: (e.split("@", 1)[0] if "@" in e else e) for e in normed}
    if len(normed) <= 1:
        out = {}
        for e, loc in locals_map.items():
            short = loc if len(loc) <= 7 else loc[-5:]
            out[e] = (loc, short)
        return out

    loc_list = list(locals_map.values())
    common = os.path.commonprefix(loc_list)
    out = {}
    for e, loc in locals_map.items():
        if len(common) >= 4 and len(loc) > len(common):
            suffix = loc[len(common):]
            short = suffix if len(suffix) >= 2 else loc[-3:]
        else:
            short = loc if len(loc) <= 6 else loc[-5:]
        out[e] = (loc, short)
    return out


def _adopt_keyring_account_if_empty():
    """若通过 colab auth login 在 keyring 中登录了首个账号且多账号目录为空，自动纳管。"""
    if glob.glob(os.path.join(_accounts_dir(), "*.token.json")):
        return
    try:
        from colab_cli.auth import PUBLIC_SCOPES, _get_token_from_keyring
        from google.auth.transport.requests import AuthorizedSession, Request
        from google.oauth2.credentials import Credentials

        raw = _get_token_from_keyring()
        if not raw:
            return
        creds = Credentials.from_authorized_user_info(json.loads(raw), PUBLIC_SCOPES)
        if (not creds.valid) and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            raw = creds.to_json()
        sess = AuthorizedSession(creds)
        r_info = sess.get("https://www.googleapis.com/oauth2/v2/userinfo", timeout=10)
        if not r_info.ok:
            return
        em = _norm_email(r_info.json().get("email") or "")
        if em and "@" in em:
            _atomic_write_text(token_path_for(em), raw, 0o600)
    except Exception:
        pass


def load_registry() -> dict:
    _ensure_dir()
    reg_file = _registry_file()
    reg = None
    if os.path.isfile(reg_file):
        try:
            with open(reg_file, "r", encoding="utf-8") as fh:
                parsed = json.load(fh)
            if isinstance(parsed, dict):
                reg = parsed
        except Exception:
            reg = None

    discovered = []
    for p in sorted(glob.glob(os.path.join(_accounts_dir(), "*.token.json"))):
        bn = os.path.basename(p)
        if bn.endswith(".token.json"):
            em = _norm_email(bn[:-len(".token.json")])
            if em and "@" in em:
                discovered.append(em)

    pref_primary = _default_primary_email()

    if reg is None:
        accounts = []
        if pref_primary and pref_primary in discovered:
            accounts.append({"email": pref_primary, "added_at": int(time.time())})
        for em in discovered:
            if em != pref_primary:
                accounts.append({"email": em, "added_at": int(time.time())})
        active = pref_primary if (pref_primary and any(a["email"] == pref_primary for a in accounts)) else (accounts[0]["email"] if accounts else "")
        drive_em = pref_primary if (pref_primary and any(a["email"] == pref_primary for a in accounts)) else active
        reg = {
            "active_email": active,
            "drive_sync_email": drive_em,
            "accounts": accounts,
        }
        if accounts:
            _atomic_write_json(reg_file, reg, 0o600)
        return reg

    raw_accts = reg.get("accounts") if isinstance(reg.get("accounts"), list) else []
    seen = set()
    clean_accts = []
    for item in raw_accts:
        if isinstance(item, dict):
            em = _norm_email(item.get("email") or "")
            if em and os.path.isfile(token_path_for(em)) and em not in seen:
                seen.add(em)
                clean_accts.append({
                    "email": em,
                    "added_at": int(item.get("added_at") or time.time()),
                })
    for em in discovered:
        if em not in seen:
            seen.add(em)
            clean_accts.append({"email": em, "added_at": int(time.time())})

    active = _norm_email(reg.get("active_email") or "")
    if active not in seen:
        active = pref_primary if (pref_primary and pref_primary in seen) else (clean_accts[0]["email"] if clean_accts else "")
    drive_em = _norm_email(reg.get("drive_sync_email") or pref_primary)
    if drive_em not in seen and seen:
        drive_em = pref_primary if (pref_primary and pref_primary in seen) else active

    out = {
        "active_email": active,
        "drive_sync_email": drive_em,
        "accounts": clean_accts,
    }
    if out != reg and clean_accts:
        _atomic_write_json(reg_file, out, 0o600)
    return out


def load_accounts_doc() -> dict:
    return load_registry()


def get_active_account_summary() -> dict:
    reg = load_registry()
    emails = [a["email"] for a in reg["accounts"]]
    labels = compute_short_labels(emails)
    active = reg.get("active_email") or ""
    loc, short = labels.get(active, (active.split("@")[0] if active else "", active[:4] if active else ""))
    return {
        "email": active,
        "local_name": loc,
        "short_label": short,
        "account_count": len(emails),
        "drive_sync_email": reg.get("drive_sync_email") or "",
    }


def get_drive_sync_token_json(preferred_email: str = None) -> str:
    """获取固定绑定于 Google Drive 加密同步的账号 OAuth 凭证，不受切换算力账号影响。"""
    reg = load_registry()
    target = _norm_email(preferred_email or reg.get("drive_sync_email") or _default_primary_email() or reg.get("active_email"))
    if target:
        tpath = token_path_for(target)
        if os.path.isfile(tpath):
            with open(tpath, "r", encoding="utf-8") as fh:
                data = fh.read().strip()
            if data:
                return data
    from colab_cli.auth import _get_token_from_keyring
    return _get_token_from_keyring() or ""


def _load_credentials_for_email(email: str):
    from colab_cli.auth import PUBLIC_SCOPES
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials

    em = _norm_email(email)
    tpath = token_path_for(em)
    if not os.path.isfile(tpath):
        raise FileNotFoundError(f"账号凭证不存在: {em}")
    with open(tpath, "r", encoding="utf-8") as fh:
        raw = fh.read()
    creds = Credentials.from_authorized_user_info(json.loads(raw), PUBLIC_SCOPES)
    if (not creds.valid) and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        new_json = creds.to_json()
        _atomic_write_text(tpath, new_json, 0o600)
        reg = load_registry()
        if _norm_email(reg.get("active_email") or "") == em:
            try:
                from colab_cli.auth import _save_token_to_keyring
                _save_token_to_keyring(new_json)
            except Exception:
                pass
    return creds


def query_account_billing(email: str) -> dict:
    from colab_cli.client import Client, Prod
    from google.auth.transport.requests import AuthorizedSession

    em = _norm_email(email)
    out = {
        "email": em,
        "balance": "未知",
        "balance_num": None,
        "rate": "未知",
        "active": None,
        "assignments_count": None,
    }
    try:
        creds = _load_credentials_for_email(em)
        cl = Client(Prod(), AuthorizedSession(creds))
        info = cl.get_consumption_user_info()
        bal = float(info.paid_compute_units_balance)
        rate = float(info.consumption_rate_hourly)
        cnt = int(info.assignments_count)
        if math.isfinite(bal) and math.isfinite(rate) and rate >= 0 and cnt >= 0:
            out.update({
                "balance": f"{bal:.2f}",
                "balance_num": round(bal, 2),
                "rate": f"{rate:.2f}/hr",
                "active": cnt > 0,
                "assignments_count": cnt,
            })
    except Exception:
        pass
    return out


def invalidate_accounts_cache():
    with _ACCOUNTS_CACHE_LOCK:
        _ACCOUNTS_CACHE["ts"] = 0.0
        _ACCOUNTS_CACHE["data"] = None


def list_accounts_with_status(force: bool = False, drive_sync_enabled: bool = False) -> dict:
    with _ACCOUNTS_CACHE_LOCK:
        now = time.time()
        if (not force) and _ACCOUNTS_CACHE["data"] is not None and (now - _ACCOUNTS_CACHE["ts"] < ACCOUNTS_CACHE_TTL):
            return _ACCOUNTS_CACHE["data"]

    if force:
        _adopt_keyring_account_if_empty()

    reg = load_registry()
    active_em = _norm_email(reg.get("active_email") or "")
    drive_em = _norm_email(reg.get("drive_sync_email") or "") if drive_sync_enabled else ""
    emails = [a["email"] for a in reg.get("accounts", [])]
    labels = compute_short_labels(emails)

    billing_map = {}
    if emails:
        with ThreadPoolExecutor(max_workers=min(4, len(emails))) as pool:
            for item in pool.map(query_account_billing, emails):
                billing_map[item["email"]] = item

    rows = []
    for em in emails:
        loc, short = labels.get(em, (em.split("@")[0], em[:4]))
        b = billing_map.get(em) or {
            "balance": "未知",
            "balance_num": None,
            "rate": "未知",
            "active": None,
            "assignments_count": None,
        }
        rows.append({
            "email": em,
            "local_name": loc,
            "short_label": short,
            "is_active": em == active_em,
            "is_drive_sync": bool(drive_sync_enabled and drive_em and em == drive_em),
            "balance": b["balance"],
            "balance_num": b["balance_num"],
            "rate": b["rate"],
            "active": b["active"],
            "assignments_count": b["assignments_count"],
        })

    payload = {
        "ok": True,
        "active_email": active_em,
        "drive_sync_email": drive_em,
        "accounts": rows,
    }
    with _ACCOUNTS_CACHE_LOCK:
        _ACCOUNTS_CACHE["ts"] = time.time()
        _ACCOUNTS_CACHE["data"] = payload
    return payload


def _session_start_lock_path(session: str = None) -> str:
    sess = (session or os.environ.get("COLAB_SESSION", "ai")).strip() or "ai"
    key = hashlib.sha256(sess.encode("utf-8")).hexdigest()
    return os.path.join(_state_dir(), f"session-start-{key}.lock")


def _clear_ephemeral_kernel_stamps():
    st_dir = _state_dir()
    for pat in ("kernel-ready-*.py", "kernel-keepalive-*.json"):
        for fp in glob.glob(os.path.join(st_dir, pat)):
            try:
                os.remove(fp)
            except OSError:
                pass


def switch_active_account(
    target_email: str,
    stop_current: bool = False,
    colab_ai_bin: str = None,
) -> tuple[bool, str, dict]:
    from colab_cli.auth import PUBLIC_SCOPES, _get_token_from_keyring, _save_token_to_keyring
    from colab_cli.client import Client, Prod
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2.credentials import Credentials

    _ensure_dir()
    cli_bin = colab_ai_bin or os.environ.get("COLABBB_CLI_BIN") or os.path.join(BASE_DIR, "colab-ai")
    target = _norm_email(target_email)
    if not target or "@" not in target:
        return False, "目标账号邮箱无效", {}

    with open(_switch_lock_file(), "a+", encoding="utf-8") as lk:
        try:
            fcntl.flock(lk.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False, "另一个账号切换操作正在进行，请稍后再试", {}

        reg = load_registry()
        known = {a["email"] for a in reg.get("accounts", [])}
        if target not in known or not os.path.isfile(token_path_for(target)):
            return False, f"未找到已登录的账号凭证: {target}", {}

        try:
            target_creds = _load_credentials_for_email(target)
            target_token_json = target_creds.to_json()
        except Exception:
            return False, f"目标账号 ({target}) 授权已失效，请重新授权登录", {"reauth_required": True}

        current = _norm_email(reg.get("active_email") or "")
        if current == target:
            _save_token_to_keyring(target_token_json)
            invalidate_accounts_cache()
            return True, f"当前已在使用 {target}", {"active_email": target, "already_active": True}

        os.makedirs(_state_dir(), exist_ok=True)
        s_lock_path = _session_start_lock_path()
        with open(s_lock_path, "a+") as slk:
            try:
                fcntl.flock(slk.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False, "当前有正在进行的开机或关机操作，请稍后再试", {"busy": True}

            cur_confirmed_empty = False
            if current and os.path.isfile(token_path_for(current)):
                try:
                    cur_creds = _load_credentials_for_email(current)
                    cur_client = Client(Prod(), AuthorizedSession(cur_creds))
                    cur_assignments = cur_client.list_assignments()
                    if not isinstance(cur_assignments, list):
                        raise RuntimeError("invalid assignments")
                except Exception:
                    return (
                        False,
                        f"无法确认当前账号 ({current}) 是否有运行中的实例；为防后台漏关机扣费已中止切换，请稍后重试",
                        {"check_failed": True},
                    )

                if len(cur_assignments) > 0:
                    if not stop_current:
                        return (
                            False,
                            f"当前账号 ({current}) 仍有运行中的 Colab 实例，请确认先关机再切换，以免原账号后台持续计费",
                            {"need_stop_confirm": True, "current_email": current},
                        )
                    fcntl.flock(slk.fileno(), fcntl.LOCK_UN)
                    res = subprocess.run(
                        [sys.executable, cli_bin, "stop"],
                        capture_output=True,
                        text=True,
                        timeout=115,
                    )
                    try:
                        fcntl.flock(slk.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        return False, "关机后有并发电源操作正在进行，已取消切换，请稍后重试", {"busy": True}
                    if res.returncode != 0:
                        return (
                            False,
                            f"自动关闭当前账号 ({current}) 实例未确认成功，已取消切换以防双边计费",
                            {"stop_failed": True},
                        )
                    try:
                        rem = cur_client.list_assignments()
                        if not isinstance(rem, list) or len(rem) > 0:
                            raise RuntimeError("still running")
                    except Exception:
                        return (
                            False,
                            f"关闭后仍未确认当前账号 ({current}) 实例释放，已取消切换",
                            {"stop_failed": True},
                        )
                cur_confirmed_empty = True
            elif not current:
                cur_confirmed_empty = True

            sess_path = _sessions_path()
            if current:
                try:
                    kr_tok = _get_token_from_keyring()
                    if kr_tok:
                        _ = Credentials.from_authorized_user_info(json.loads(kr_tok), PUBLIC_SCOPES)
                        _atomic_write_text(token_path_for(current), kr_tok, 0o600)
                except Exception:
                    pass
                if os.path.isfile(sess_path):
                    try:
                        with open(sess_path, "r", encoding="utf-8") as sf:
                            s_raw = sf.read()
                        json.loads(s_raw or "{}")
                        _atomic_write_text(sessions_path_for(current), s_raw, 0o600)
                    except Exception:
                        pass

            target_sess_file = sessions_path_for(target)
            restored_sessions = "{}\n"
            if os.path.isfile(target_sess_file):
                try:
                    with open(target_sess_file, "r", encoding="utf-8") as tsf:
                        cand = tsf.read()
                    if isinstance(json.loads(cand or "{}"), dict):
                        restored_sessions = cand
                except Exception:
                    restored_sessions = "{}\n"

            os.makedirs(os.path.dirname(sess_path), mode=0o700, exist_ok=True)
            _atomic_write_text(sess_path, restored_sessions, 0o600)
            _save_token_to_keyring(target_token_json)
            _clear_ephemeral_kernel_stamps()
            if cur_confirmed_empty:
                slk.seek(0)
                slk.truncate()
                slk.flush()
                os.fsync(slk.fileno())

            reg["active_email"] = target
            _atomic_write_json(_registry_file(), reg, 0o600)
            invalidate_accounts_cache()

        target_bill = query_account_billing(target)
        return True, f"已切换算力账号至 {target}", {"active_email": target, "billing": target_bill}


def begin_oauth_login() -> dict:
    from colab_cli.auth import PUBLIC_SCOPES, REMOTE_REDIRECT_URI
    from google_auth_oauthlib.flow import InstalledAppFlow

    _ensure_dir()
    cfg = json.loads(resources.files("colab_cli").joinpath("oauth_config.json").read_text())
    flow = InstalledAppFlow.from_client_config(cfg, PUBLIC_SCOPES)
    flow.redirect_uri = REMOTE_REDIRECT_URI
    auth_url, state = flow.authorization_url(prompt="consent", access_type="offline", token_usage="remote")
    _atomic_write_json(
        _pending_oauth_file(),
        {
            "code_verifier": flow.code_verifier,
            "state": state,
            "redirect_uri": REMOTE_REDIRECT_URI,
            "created_at": time.time(),
        },
        0o600,
    )
    return {"ok": True, "auth_url": auth_url}


def complete_oauth_login(code: str) -> tuple[bool, str, dict]:
    from colab_cli.auth import PUBLIC_SCOPES, _save_token_to_keyring
    from google.auth.transport.requests import AuthorizedSession
    from google_auth_oauthlib.flow import InstalledAppFlow

    clean_code = (code or "").strip()
    if not clean_code:
        return False, "请输入 Google 授权码", {}
    p_file = _pending_oauth_file()
    if not os.path.isfile(p_file):
        return False, "授权会话不存在或已过期，请重新点击获取授权链接", {}

    try:
        with open(p_file, "r", encoding="utf-8") as fh:
            pending = json.load(fh)
        if time.time() - float(pending.get("created_at") or 0) > 1800:
            return False, "授权链接已超过 30 分钟有效期，请重新获取", {}

        cfg = json.loads(resources.files("colab_cli").joinpath("oauth_config.json").read_text())
        flow = InstalledAppFlow.from_client_config(cfg, PUBLIC_SCOPES, state=pending.get("state"))
        flow.redirect_uri = pending["redirect_uri"]
        flow.code_verifier = pending["code_verifier"]
        flow.fetch_token(code=clean_code)
        creds = flow.credentials

        sess = AuthorizedSession(creds)
        r_info = sess.get("https://www.googleapis.com/oauth2/v2/userinfo", timeout=15)
        r_info.raise_for_status()
        email = _norm_email(r_info.json().get("email") or "")
        if not email or "@" not in email:
            return False, "未能从 Google 获取有效账号邮箱", {}
    except Exception:
        return False, "授权码无效或已过期，请重新打开授权链接获取新授权码", {}

    tok_json = creds.to_json()
    _atomic_write_text(token_path_for(email), tok_json, 0o600)
    if not os.path.isfile(sessions_path_for(email)):
        _atomic_write_text(sessions_path_for(email), "{}\n", 0o600)

    try:
        os.remove(p_file)
    except OSError:
        pass

    reg = load_registry()
    accts = reg.get("accounts") if isinstance(reg.get("accounts"), list) else []
    if not any(_norm_email(a.get("email") or "") == email for a in accts):
        accts.append({"email": email, "added_at": int(time.time())})
    reg["accounts"] = accts
    if not reg.get("active_email"):
        reg["active_email"] = email
    if not reg.get("drive_sync_email"):
        reg["drive_sync_email"] = email
    if _norm_email(reg.get("active_email") or "") == email:
        _save_token_to_keyring(tok_json)
    _atomic_write_json(_registry_file(), reg, 0o600)
    invalidate_accounts_cache()

    bill = query_account_billing(email)
    return True, f"已成功登录 {email}（余额 {bill['balance']} CU）", {"email": email, "billing": bill}
