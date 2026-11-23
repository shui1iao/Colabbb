"""Bounded Colab allocation recovery. No credentials are persisted here."""
import json
import math
import os
import sys
import time
import uuid


class PendingError(ValueError):
    pass


def _state_dir():
    base = os.path.abspath(os.environ.get("COLABBB_HOME", os.path.dirname(os.path.abspath(__file__))))
    return os.path.abspath(os.environ.get("COLABBB_STATE_DIR", os.path.join(base, "state")))


def _accounts_registry_path():
    return os.path.join(_state_dir(), "colab-accounts", "accounts.json")


def read_pending(lock, session, gpu=None, allow_legacy=False):
    lock.seek(0)
    raw = lock.read().strip()
    if not raw:
        return None
    if raw == "NEW_UNCONFIRMED":
        if allow_legacy:
            return {"legacy": True}
        raise PendingError("旧启动保护没有请求身份，须先核实并一次性清理；不会按时间自动放行。")
    try:
        records = [json.loads(line) for line in raw.splitlines()]
        identity = None
        for p in records:
            if not (isinstance(p, dict)
                    and set(p) == {"version", "request_uuid", "account", "session", "gpu", "created_at", "phase", "post_count", "last_post_at"}
                    and type(p["version"]) is int and p["version"] == 1
                    and str(uuid.UUID(p["request_uuid"])) == p["request_uuid"]
                    and p["session"] == session
                    and isinstance(p["gpu"], str) and p["gpu"]
                    and (gpu is None or p["gpu"].lower() == gpu.lower())
                    and isinstance(p["account"], dict)
                    and set(p["account"]) == {"email", "sub"}
                    and all(isinstance(v, str) and v for v in p["account"].values())
                    and p["phase"] in ("prepared", "posting", "unconfirmed", "bound")
                    and type(p["post_count"]) is int and p["post_count"] >= 0
                    and type(p["created_at"]) in (int, float) and math.isfinite(p["created_at"]) and p["created_at"] > 0
                    and type(p["last_post_at"]) in (int, float) and math.isfinite(p["last_post_at"]) and p["last_post_at"] >= 0):
                raise ValueError("Invalid pending")
            current_identity = {k: p[k] for k in ("version", "request_uuid", "account", "session", "gpu", "created_at")}
            if identity is not None and current_identity != identity:
                raise ValueError("Mixed allocation identities")
            identity = current_identity
        return records[-1]
    except (ValueError, TypeError, KeyError, AssertionError, AttributeError):
        raise PendingError("启动保护损坏、格式未知或会话/硬件身份不匹配；保留原记录并拒绝创建。") from None


def write_pending(lock, pending):
    # Append on the same inode. Never truncate an uncertain identity during a
    # phase update: a crash in truncate/write could otherwise look like empty.
    # Incomplete/mixed records fail closed; only confirmed attachment/reconciliation may clear.
    lock.seek(0, os.SEEK_END)
    lock.write(json.dumps(pending, ensure_ascii=False, sort_keys=True) + "\n")
    lock.flush()
    os.fsync(lock.fileno())


def verified_client(pending=None):
    """Read current keyring; refresh only in memory; verify actual Google identity."""
    from colab_cli.auth import PUBLIC_SCOPES, _get_token_from_keyring
    from colab_cli.client import Client, Prod
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import AuthorizedSession, Request

    email = ""
    reg_path = _accounts_registry_path()
    if os.path.isfile(reg_path):
        with open(reg_path, encoding="utf-8") as fh:
            email = str(json.load(fh).get("active_email") or "").strip().lower()
    raw = _get_token_from_keyring()
    if not raw:
        raise PendingError("当前账号授权不可用；保留启动保护。")
    creds = Credentials.from_authorized_user_info(json.loads(raw), PUBLIC_SCOPES)
    if not creds.valid:
        creds.refresh(Request())
    # Never replay an allocation POST automatically after an authentication error.
    auth = AuthorizedSession(creds, max_refresh_attempts=0)
    response = auth.get("https://www.googleapis.com/oauth2/v3/userinfo", timeout=15)
    response.raise_for_status()
    info = response.json()
    account = {"email": str(info.get("email", "")).strip().lower(), "sub": info.get("sub")}
    if (not account["email"] or info.get("email_verified") is not True
            or not isinstance(account["sub"], str) or not account["sub"]):
        raise PendingError("当前凭据与算力账号身份不一致；拒绝创建或清除启动保护。")
    if email and account["email"] != email:
        raise PendingError("当前凭据与算力账号身份不一致；拒绝创建或清除启动保护。")
    if pending and not pending.get("legacy") and pending["account"] != account:
        raise PendingError("当前凭据与上次创建身份不一致；保留启动保护。")
    return Client(Prod(), auth), account


def check_generation(path, expected):
    try:
        with open(path, encoding="utf-8") as fh:
            current = fh.read().strip()
    except FileNotFoundError:
        current = "0"
    if current != expected:
        raise PendingError("旧任务已因关机暂停；禁止继续分配，请手动继续。")


def bind_assignment(session, result, variant, accelerator, shape):
    from colab_cli.common import state
    from colab_cli.state import SessionState
    from colab_cli.client import Shape
    s = SessionState(
        name=session, endpoint=result.endpoint,
        token=result.runtime_proxy_info.token, url=result.runtime_proxy_info.url,
        token_expires_at=result.runtime_proxy_info.expires_at(),
        variant=variant.value, accelerator=accelerator.value,
        machine_shape=(Shape.HIGH_RAM.name if shape == Shape.HIGH_RAM else Shape.STANDARD.name),
    )
    state.store.add(s)
    state.history.log_event(session, "session_created", {
        "endpoint": s.endpoint, "variant": s.variant,
        "accelerator": s.accelerator, "machine_shape": s.machine_shape,
    })


def child(action, fd, session, gpu, generation_path, generation):
    from colab_cli.client import Assignment, GetAssignmentResponse, ColabRequestError
    from colab_cli.commands.session import resolve_runtime_options
    import requests

    # Inherited FD keeps parent's flock alive even if parent dies mid-POST.
    with os.fdopen(os.dup(fd), "r+", encoding="utf-8") as lock:
        pending = read_pending(lock, session, gpu)
        client, account = verified_client(pending)
        check_generation(generation_path, generation)
        variant, accelerator, shape = resolve_runtime_options(gpu=gpu, tpu=None, high_mem=False)
        if action == "prepare":
            if pending is None:
                pending = dict(version=1, request_uuid=str(uuid.uuid4()), account=account,
                               session=session, gpu=gpu, created_at=time.time(),
                               phase="prepared", post_count=0, last_post_at=0)
                write_pending(lock, pending)
            return {"status": "prepared"}
        if pending is None:
            raise PendingError("创建身份缺失；拒绝分配。")
        request_id = uuid.UUID(pending["request_uuid"])
        # GET failure is never an empty result, and can never reach POST.
        result = client._get_assignment(request_id, variant, accelerator, shape)
        if isinstance(result, Assignment):
            bind_assignment(session, result, variant, accelerator, shape)
            pending["phase"] = "bound"
            write_pending(lock, pending)
            return {"status": "allocated"}
        if not isinstance(result, GetAssignmentResponse):
            raise PendingError("固定请求查询响应未知；保留保护，拒绝分配。")
        if action == "query":
            return {"status": "empty"}
        if action != "attempt":
            raise PendingError("未知启动操作。")
        # Backoff applies across later start invocations too. Parent waits cancellably.
        if time.time() - pending["last_post_at"] < 15:
            return {"status": "wait"}
        check_generation(generation_path, generation)
        pending.update(phase="posting", post_count=pending["post_count"] + 1, last_post_at=time.time())
        write_pending(lock, pending)
        check_generation(generation_path, generation)
        try:
            # Exactly one POST, using the same persisted UUID and current GET token.
            result = client._post_assignment(request_id, result.token, variant, accelerator, shape)
        except Exception as exc:
            # Retry spacing starts at failure completion, not at POST submission.
            pending.update(phase="unconfirmed", last_post_at=time.time())
            write_pending(lock, pending)
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = isinstance(exc, requests.exceptions.Timeout) or (
                isinstance(exc, ColabRequestError) and status is not None and 500 <= status <= 599)
            rejected = isinstance(exc, ColabRequestError) and status is not None and 400 <= status <= 499
            if retryable:
                return {"status": "retryable"}
            if rejected:
                return {"status": "rejected", "http_status": status}
            return {"status": "blocked"}
        bind_assignment(session, result, variant, accelerator, shape)
        pending["phase"] = "bound"
        write_pending(lock, pending)
        return {"status": "allocated"}


if __name__ == "__main__":
    try:
        os.environ["COLAB_CLI_ASSIGN_RETRIES"] = "0"
        os.environ["COLAB_CLI_REQUEST_TIMEOUT"] = "60"
        result = child(sys.argv[1], int(sys.argv[2]), *sys.argv[3:])
    except PendingError as exc:
        result = {"status": "blocked", "message": str(exc)}
    except BaseException:
        # Never emit raw exceptions / response bodies / credentials.
        result = {"status": "blocked", "message": "账号认证、网络查询、响应或本地绑定异常；未确认无实例，保留启动保护。"}
    print("COLAB_AI_STARTUP=" + json.dumps(result, ensure_ascii=False), flush=True)
