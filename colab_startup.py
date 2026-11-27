"""Bounded Colab allocation recovery via Colab Public API (with Tunnel API compatibility).
No credentials are persisted here.
"""
from datetime import datetime, timezone
import json
import math
import os
import re
import sys
import time
from urllib.parse import urljoin
import uuid

PUBLIC_API_BASE = "https://colaboratory.googleapis.com"
_URL_ENDPOINT_RE = re.compile(r"^https://\d+-((?:gpu|cpu|tpu|m)-[a-z0-9-]+)-[a-z]\.", re.IGNORECASE)


class PendingError(ValueError):
    pass


def _state_dir():
    base = os.path.abspath(os.environ.get("COLABBB_HOME", os.path.dirname(os.path.abspath(__file__))))
    return os.path.abspath(os.environ.get("COLABBB_STATE_DIR", os.path.join(base, "state")))


def _accounts_registry_path():
    return os.path.join(_state_dir(), "colab-accounts", "accounts.json")


def parse_gpu_spec(spec: str) -> tuple[str, bool, str]:
    """Parse GPU spec like 'G4', 'A100-HM', 'A100_HM' -> (base_gpu, high_mem, canonical_spec)."""
    raw = (spec or "").strip().upper()
    high_mem = False
    for suf in ("-HM", "_HM", ":HM", "-80G", "-HIGHMEM"):
        if raw.endswith(suf):
            raw = raw[:-len(suf)].strip()
            high_mem = True
            break
    canonical = f"{raw}-HM" if high_mem else raw
    return raw, high_mem, canonical


def normalize_proxy_url(url: object) -> str:
    return url.rstrip("/") if isinstance(url, str) else ""


def extract_endpoint_from_proxy_url(url: str, fallback_name: str = "") -> str:
    m = _URL_ENDPOINT_RE.match(url or "")
    if m:
        return m.group(1)
    clean = (fallback_name or "").removeprefix("runtimes/").strip()
    return clean


def read_pending(lock, session, gpu=None, allow_legacy=False):
    lock.seek(0)
    raw = lock.read().strip()
    if not raw:
        return None
    if raw == "NEW_UNCONFIRMED":
        if allow_legacy:
            return {"legacy": True}
        raise PendingError("旧启动保护没有请求身份，须先核实并一次性清理；不会按时间自动放行。")
    expected_spec = parse_gpu_spec(gpu)[2] if gpu is not None else None
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
                    and (expected_spec is None or parse_gpu_spec(p["gpu"])[2] == expected_spec)
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
    lock.seek(0, os.SEEK_END)
    lock.write(json.dumps(pending, ensure_ascii=False, sort_keys=True) + "\n")
    lock.flush()
    os.fsync(lock.fileno())


def _public_headers(quota_project: str) -> dict:
    hdrs = {
        "Accept": "application/json",
        "X-Colab-Client-Agent": "colab-cli",
    }
    if quota_project:
        hdrs["x-goog-user-project"] = quota_project
    return hdrs


def verified_client(pending=None):
    """Read current keyring; refresh only in memory; verify actual Google identity."""
    from colab_cli.auth import PUBLIC_SCOPES, _get_token_from_keyring
    from colab_cli.client import Client, Prod
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import AuthorizedSession, Request

    email = ""
    reg_path = _accounts_registry_path()
    if os.path.isfile(reg_path):
        try:
            with open(reg_path, encoding="utf-8") as fh:
                email = str(json.load(fh).get("active_email") or "").strip().lower()
        except Exception:
            email = ""
    if email and pending and not pending.get("legacy") and pending["account"]["email"] != email:
        raise PendingError("当前账号与上次创建身份不一致；请在原账号核对，不会覆盖 pending 或自动切号。")

    raw = _get_token_from_keyring()
    if not raw:
        raise PendingError("当前账号授权不可用；保留启动保护。")
    creds = Credentials.from_authorized_user_info(json.loads(raw), PUBLIC_SCOPES)
    if not creds.valid:
        creds.refresh(Request())
    # Do NOT attach quota_project globally on `creds`: `www.googleapis.com/oauth2/v3/userinfo`
    # rejects `x-goog-user-project` with 403 if People/OAuth2 API isn't enabled on that project.
    # Pass `x-goog-user-project` ONLY to `colaboratory.googleapis.com` via `_public_headers()`.
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


def _creds_from_client(client):
    sess = getattr(client, "session", None)
    return getattr(sess, "credentials", None)


def _runtime_dict_to_listed_assignment(rt: dict, tunnel_by_url: dict = None):
    from colab_cli.client import Accelerator, AssignmentVariant, ListedAssignment, RuntimeProxyInfo, Shape

    spec = rt.get("runtimeSpec") or {}
    conn = rt.get("connectionInfo") or {}
    raw_var = spec.get("variant") or ""
    if raw_var == "VARIANT_GPU":
        mapped_var = AssignmentVariant.GPU
    elif raw_var == "VARIANT_TPU":
        mapped_var = AssignmentVariant.TPU
    else:
        mapped_var = AssignmentVariant.DEFAULT

    raw_shape = spec.get("shape") or ""
    mapped_shape = Shape.HIGH_RAM if raw_shape == "SHAPE_HIGHMEM" else Shape.STANDARD

    raw_acc = (spec.get("accelerator") or "NONE").upper()
    try:
        mapped_acc = Accelerator(raw_acc)
    except ValueError:
        mapped_acc = Accelerator.NONE

    url = conn.get("url") or ""
    norm_url = normalize_proxy_url(url)
    tun_match = (tunnel_by_url or {}).get(norm_url) if norm_url else None

    ep = (conn.get("endpoint") or "").strip()
    if not ep and tun_match is not None:
        ep = tun_match.endpoint
    if not ep:
        ep = extract_endpoint_from_proxy_url(url, rt.get("name") or "")

    expires_in = 3600
    exp_str = conn.get("expireTime") or ""
    if exp_str:
        try:
            dt = datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
            expires_in = max(60, int((dt - datetime.now(timezone.utc)).total_seconds()))
        except Exception:
            pass

    proxy_info = RuntimeProxyInfo(
        token=conn.get("token") or "",
        tokenExpiresInSeconds=expires_in,
        url=url,
    )
    return ListedAssignment(
        accelerator=mapped_acc,
        endpoint=ep,
        variant=mapped_var,
        machineShape=mapped_shape,
        runtimeProxyInfo=proxy_info,
    )


def list_effective_assignments(client, quota_project: str = "") -> list:
    """List active assignments across BOTH Colab Public API and Legacy Tunnel API.

    Fails closed if neither backend responds validly; merges by proxy URL so both
    Public-API-created and browser-created sessions are visible with fresh tokens.
    """
    tunnel_ok = False
    tunnel_list = []
    try:
        t_res = client.list_assignments()
        if isinstance(t_res, list):
            tunnel_list = t_res
            tunnel_ok = True
    except Exception:
        tunnel_ok = False

    tunnel_by_url = {
        normalize_proxy_url(a.runtime_proxy_info.url): a
        for a in tunnel_list
        if normalize_proxy_url(a.runtime_proxy_info.url)
    }

    pub_ok = False
    pub_runtimes = []
    try:
        r = client.session.get(
            f"{PUBLIC_API_BASE}/v1beta/runtimes",
            headers=_public_headers(quota_project),
            timeout=15,
        )
        if r.status_code == 200:
            body = r.json() if r.text.strip() else {}
            if isinstance(body, dict):
                raw_rts = body.get("runtimes") or []
                if isinstance(raw_rts, list):
                    pub_runtimes = raw_rts
                    pub_ok = True
    except Exception:
        pub_ok = False

    if not pub_ok and not tunnel_ok:
        raise RuntimeError("Failed to query active runtimes from both Public and Tunnel APIs")

    merged = []
    seen_urls = set()
    seen_eps = set()

    for rt in pub_runtimes:
        if not isinstance(rt, dict):
            continue
        la = _runtime_dict_to_listed_assignment(rt, tunnel_by_url)
        nu = normalize_proxy_url(la.runtime_proxy_info.url)
        if nu:
            seen_urls.add(nu)
        if la.endpoint:
            seen_eps.add(la.endpoint)
        merged.append(la)

    for a in tunnel_list:
        nu = normalize_proxy_url(a.runtime_proxy_info.url)
        if (nu and nu in seen_urls) or (a.endpoint and a.endpoint in seen_eps):
            continue
        merged.append(a)

    return merged


def query_pending_by_uuid(client, request_id: uuid.UUID, variant, accelerator, shape, quota_project: str = ""):
    """Authoritatively check whether `request_id` has an active runtime on Public or Tunnel API.

    Returns ("allocated", ListedAssignment_or_Assignment) or ("empty", None).
    Raises on network/auth/protocol uncertainty (fail-closed).
    """
    from colab_cli.client import Assignment, GetAssignmentResponse

    runtime_id = f"r-{request_id}"
    r = client.session.get(
        f"{PUBLIC_API_BASE}/v1beta/runtimes/{runtime_id}",
        headers=_public_headers(quota_project),
        timeout=20,
    )
    if r.status_code == 200:
        rt = r.json()
        tunnel_by_url = {}
        try:
            for a in client.list_assignments():
                nu = normalize_proxy_url(a.runtime_proxy_info.url)
                if nu:
                    tunnel_by_url[nu] = a
        except Exception:
            pass
        la = _runtime_dict_to_listed_assignment(rt, tunnel_by_url)
        return "allocated", la

    if r.status_code != 404:
        if r.status_code == 403 and not quota_project:
            raise PendingError("未找到可用的 GCP 配额项目；请在 Google Cloud 控制台创建一个免费空项目或在 .env 中设置 COLABBB_QUOTA_PROJECT。")
        raise PendingError(f"Public API 请求身份核对异常 (HTTP {r.status_code})；保留启动保护。")

    # Also verify legacy tunnel endpoint in case an older pending record came from tunnel assign
    try:
        tres = client._get_assignment(request_id, variant, accelerator, shape)
        if isinstance(tres, Assignment):
            return "allocated", tres
        if not isinstance(tres, GetAssignmentResponse):
            raise PendingError("隧道请求查询响应未知；保留保护。")
    except PendingError:
        raise
    except Exception:
        # Public API already authoritatively returned 404 for r-{request_id}
        pass

    return "empty", None


def stop_active_runtimes_everywhere(session: str = "ai") -> None:
    """Shut down kernel and release all active runtimes for the active account on BOTH Public and Tunnel APIs."""
    from colab_cli.common import state
    from colab_cli.runtime import ColabRuntime
    import colab_accounts

    s = state.store.get(session)
    if s and s.url and s.token:
        try:
            rt = ColabRuntime(s.url, s.token, kernel_id=s.kernel_id)
            rt.stop(shutdown_kernel=True)
        except Exception:
            pass

    client, acct = verified_client()
    qp = colab_accounts.resolve_quota_project(acct["email"], _creds_from_client(client))

    # 1. Delete all runtimes visible on Colab Public API
    pub_ok = False
    try:
        r_list = client.session.get(
            f"{PUBLIC_API_BASE}/v1beta/runtimes",
            headers=_public_headers(qp),
            timeout=20,
        )
        if r_list.status_code == 200:
            pub_ok = True
            body = r_list.json() if r_list.text.strip() else {}
            for rt in (body.get("runtimes") or []):
                rname = (rt.get("name") or "").strip()
                if rname:
                    if not rname.startswith("runtimes/"):
                        rname = f"runtimes/{rname}"
                    del_r = client.session.delete(
                        f"{PUBLIC_API_BASE}/v1beta/{rname}",
                        params={"allowMissing": "true"},
                        headers=_public_headers(qp),
                        timeout=30,
                    )
                    if not del_r.ok and del_r.status_code != 404:
                        del_r.raise_for_status()
        elif r_list.status_code not in (403, 404):
            r_list.raise_for_status()
    except Exception:
        if qp:
            raise

    if s and s.endpoint and s.endpoint.startswith("r-"):
        client.session.delete(
            f"{PUBLIC_API_BASE}/v1beta/runtimes/{s.endpoint}",
            params={"allowMissing": "true"},
            headers=_public_headers(qp),
            timeout=20,
        )

    # 2. Also unassign any remaining runtime visible on Tunnel API (e.g. browser-created)
    tunnel_ok = False
    try:
        t_asgs = client.list_assignments()
        tunnel_ok = isinstance(t_asgs, list)
    except Exception:
        t_asgs = []
    for a in (t_asgs or []):
        if a.endpoint:
            try:
                client.unassign(a.endpoint)
            except Exception:
                pass

    if not pub_ok and not tunnel_ok:
        raise RuntimeError("Failed to query or release active runtimes from both Public and Tunnel APIs")

    if s:
        state.store.remove(session)
        state.history.log_event(session, "session_terminated", {"reason": "user_requested"})


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
        name=session,
        endpoint=result.endpoint,
        token=result.runtime_proxy_info.token,
        url=result.runtime_proxy_info.url,
        token_expires_at=result.runtime_proxy_info.expires_at(),
        variant=variant.value,
        accelerator=accelerator.value,
        machine_shape=(Shape.HIGH_RAM.name if shape == Shape.HIGH_RAM else Shape.STANDARD.name),
    )
    state.store.add(s)
    state.history.log_event(session, "session_created", {
        "endpoint": s.endpoint,
        "variant": s.variant,
        "accelerator": s.accelerator,
        "machine_shape": s.machine_shape,
    })


def _extract_op_error_reason(err_obj: dict) -> str:
    for d in (err_obj.get("details") or []):
        if isinstance(d, dict) and isinstance(d.get("reason"), str) and d["reason"]:
            return d["reason"]
    return ""


def _create_via_public_api(client, quota_project: str, request_id: uuid.UUID, variant, accelerator, shape, generation_path: str, generation: str):
    from colab_cli.client import Shape, Variant
    import requests

    runtime_id = f"r-{request_id}"
    if variant == Variant.GPU:
        pub_variant = "VARIANT_GPU"
    elif variant == Variant.TPU:
        pub_variant = "VARIANT_TPU"
    else:
        pub_variant = "VARIANT_CPU"
    pub_acc = accelerator.value if accelerator else "NONE"
    pub_shape = "SHAPE_HIGHMEM" if shape == Shape.HIGH_RAM else "SHAPE_STANDARD"

    resp = client.session.post(
        f"{PUBLIC_API_BASE}/v1beta/runtimes",
        params={"runtimeId": runtime_id, "requestId": str(request_id)},
        json={
            "runtimeSpec": {
                "variant": pub_variant,
                "accelerator": pub_acc,
                "shape": pub_shape,
            }
        },
        headers=_public_headers(quota_project),
        timeout=60,
    )

    if resp.status_code == 409:
        st, la = query_pending_by_uuid(client, request_id, variant, accelerator, shape, quota_project)
        if st == "allocated":
            return {"status": "allocated_obj", "obj": la}

    if not resp.ok:
        code = resp.status_code
        reason = ""
        try:
            err_body = resp.json().get("error") or {}
            reason = _extract_op_error_reason(err_body)
        except Exception:
            pass
        kind = f"HTTP {code}" + (f":{reason}" if reason else "")
        if code == 403 and not quota_project:
            return {
                "status": "blocked",
                "http_status": code,
                "err_kind": kind,
                "message": "未找到可用的 GCP 配额项目；请在 Google Cloud 控制台创建一个免费空项目或在 .env 中设置 COLABBB_QUOTA_PROJECT。",
            }
        if 500 <= code <= 599:
            return {"status": "retryable", "http_status": code, "err_kind": kind}
        if 400 <= code <= 499:
            return {"status": "rejected", "http_status": code, "err_kind": kind}
        return {"status": "blocked", "http_status": code, "err_kind": kind}

    op = resp.json()
    op_name = (op.get("name") or "").strip()
    polls = 0
    while not op.get("done"):
        if not op_name or polls >= 6:
            return {"status": "retryable", "http_status": 504, "err_kind": "LRO WaitTimeout"}
        polls += 1
        check_generation(generation_path, generation)
        try:
            w_resp = client.session.get(
                urljoin(f"{PUBLIC_API_BASE}/", f"v1/{op_name}:wait"),
                params={"timeout": "25s"},
                headers=_public_headers(quota_project),
                timeout=35,
            )
            if w_resp.status_code in (502, 503, 504):
                w_resp = client.session.get(
                    urljoin(f"{PUBLIC_API_BASE}/", f"v1/{op_name}"),
                    headers=_public_headers(quota_project),
                    timeout=15,
                )
            if not w_resp.ok:
                return {"status": "retryable", "http_status": w_resp.status_code, "err_kind": f"HTTP {w_resp.status_code}"}
            op = w_resp.json()
        except requests.exceptions.Timeout:
            continue

    if op.get("error"):
        err_obj = op["error"] if isinstance(op["error"], dict) else {}
        rpc_code = err_obj.get("code")
        reason = _extract_op_error_reason(err_obj)
        if rpc_code == 6:
            st, la = query_pending_by_uuid(client, request_id, variant, accelerator, shape, quota_project)
            if st == "allocated":
                return {"status": "allocated_obj", "obj": la}
        label = reason or (f"RPC {rpc_code}" if rpc_code is not None else "Unavailable")
        # FAILED_PRECONDITION (9) / INVALID_ARGUMENT (3) / PERMISSION_DENIED (7) -> definitive non-creation for this spec/account
        if rpc_code in (3, 7, 9):
            return {"status": "rejected", "http_status": 412 if rpc_code == 9 else 400, "err_kind": label}
        if rpc_code in (4, 8, 13, 14):
            return {"status": "retryable", "http_status": 503, "err_kind": label}
        return {"status": "blocked", "http_status": 500, "err_kind": label}

    rt = op.get("response")
    if not isinstance(rt, dict) or not rt.get("connectionInfo"):
        st, la = query_pending_by_uuid(client, request_id, variant, accelerator, shape, quota_project)
        if st == "allocated":
            return {"status": "allocated_obj", "obj": la}
        raise PendingError("Public API 返回完成但缺少连接信息；保留启动保护。")

    tunnel_by_url = {}
    try:
        for a in client.list_assignments():
            nu = normalize_proxy_url(a.runtime_proxy_info.url)
            if nu:
                tunnel_by_url[nu] = a
    except Exception:
        pass
    la = _runtime_dict_to_listed_assignment(rt, tunnel_by_url)
    return {"status": "allocated_obj", "obj": la}


def child(action, fd, session, gpu, generation_path, generation):
    from colab_cli.commands.session import resolve_runtime_options
    import colab_accounts
    import requests

    base_gpu, high_mem, canonical_gpu = parse_gpu_spec(gpu)
    with os.fdopen(os.dup(fd), "r+", encoding="utf-8") as lock:
        pending = read_pending(lock, session, canonical_gpu)
        client, account = verified_client(pending)
        quota_project = colab_accounts.resolve_quota_project(account["email"], _creds_from_client(client))
        check_generation(generation_path, generation)
        variant, accelerator, shape = resolve_runtime_options(gpu=base_gpu, tpu=None, high_mem=high_mem)
        if action == "prepare":
            if pending is None:
                pending = dict(version=1, request_uuid=str(uuid.uuid4()), account=account,
                               session=session, gpu=canonical_gpu, created_at=time.time(),
                               phase="prepared", post_count=0, last_post_at=0)
                write_pending(lock, pending)
            return {"status": "prepared"}
        if pending is None:
            raise PendingError("创建身份缺失；拒绝分配。")
        request_id = uuid.UUID(pending["request_uuid"])
        q_status, q_obj = query_pending_by_uuid(client, request_id, variant, accelerator, shape, quota_project)
        if q_status == "allocated":
            bind_assignment(session, q_obj, variant, accelerator, shape)
            pending["phase"] = "bound"
            write_pending(lock, pending)
            return {"status": "allocated"}
        if action == "query":
            return {"status": "empty"}
        if action != "attempt":
            raise PendingError("未知启动操作。")
        if time.time() - pending["last_post_at"] < 12:
            return {"status": "wait"}
        check_generation(generation_path, generation)
        pending.update(phase="posting", post_count=pending["post_count"] + 1, last_post_at=time.time())
        write_pending(lock, pending)
        check_generation(generation_path, generation)
        try:
            outcome = _create_via_public_api(
                client, quota_project, request_id, variant, accelerator, shape, generation_path, generation
            )
        except Exception as exc:
            pending.update(phase="unconfirmed", last_post_at=time.time())
            write_pending(lock, pending)
            is_timeout = isinstance(exc, requests.exceptions.Timeout)
            if is_timeout:
                return {"status": "retryable", "http_status": None, "err_kind": "Timeout"}
            if isinstance(exc, PendingError):
                return {"status": "blocked", "message": str(exc)}
            return {"status": "retryable", "http_status": 503, "err_kind": type(exc).__name__}

        if outcome["status"] == "allocated_obj":
            bind_assignment(session, outcome["obj"], variant, accelerator, shape)
            pending["phase"] = "bound"
            write_pending(lock, pending)
            return {"status": "allocated"}

        pending.update(phase="unconfirmed", last_post_at=time.time())
        write_pending(lock, pending)
        return outcome


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
