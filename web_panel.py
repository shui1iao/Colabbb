#!/usr/bin/env python3
"""Colabbb Web 创作工作台，媒体与提示词双通道 FIFO 队列。"""
import hmac, ipaddress, zipfile
from concurrent.futures import ThreadPoolExecutor
import base64, collections, copy, glob, hashlib, math, json, mimetypes, tempfile, os, re, shlex, signal, subprocess, sys, threading, time, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from stat import S_ISREG

_WEB_PANEL_MTIME_NS = 0
try:
    _WEB_PANEL_MTIME_NS = os.stat(os.path.realpath(__file__)).st_mtime_ns
except OSError:
    pass

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
# 与总控 CLI 相同：显式环境变量优先于本机配置文件。
for _path in (os.environ.get('COLABBB_CONFIG_FILE', ''), os.path.join(os.environ.get('COLABBB_HOME', BASE_DIR), 'config.env')):
    if _path and os.path.isfile(_path):
        for _line in open(_path, encoding='utf-8'):
            _line = _line.strip()
            if not _line or _line.startswith('#') or '=' not in _line: continue
            _k, _v = _line.split('=', 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
        break
BASE_DIR = os.path.abspath(os.environ.get('COLABBB_HOME') or BASE_DIR)
PORT = int(os.environ.get('COLABBB_PANEL_PORT', '17860'))
OUT_ROOT = os.path.abspath(os.environ.get('COLABBB_OUTPUT_DIR') or os.path.join(BASE_DIR, 'outputs'))
STATE_DIR = os.path.abspath(os.environ.get('COLABBB_STATE_DIR') or os.path.join(BASE_DIR, 'state'))
SETTINGS_FILE = os.path.join(STATE_DIR, 'studio_settings.json')
LLM_SYS_FILE = os.path.join(STATE_DIR, 'llm_default_system.txt')
IDLE_TOUCH = os.path.join(STATE_DIR, 'last_active.ts')
CHAT_FILE = os.path.join(STATE_DIR, 'llm_chat_history.json')
COLAB_AI_BIN = os.environ.get('COLABBB_CLI_BIN') or os.path.join(BASE_DIR, 'colab-ai')
COLAB_BIN = os.environ.get('COLAB_BIN', '/usr/local/bin/colab')
COMFY_TUNNEL_PORT = int(os.environ.get('COLABBB_COMFY_TUNNEL_PORT', '28188'))
LLM_TUNNEL_PORT = int(os.environ.get('COLABBB_LLM_TUNNEL_PORT', '28189'))
METRICS_TUNNEL_PORT = int(os.environ.get('COLABBB_METRICS_TUNNEL_PORT', '28190'))

import colab_accounts

_OPT_DRIVE_DIR = os.path.join(BASE_DIR, 'optional', 'drive-sync')
if os.path.isdir(_OPT_DRIVE_DIR) and _OPT_DRIVE_DIR not in sys.path:
    sys.path.insert(0, _OPT_DRIVE_DIR)


def is_drive_sync_enabled() -> bool:
    return os.environ.get('COLABBB_ENABLE_DRIVE_SYNC', '0').strip() == '1'


def is_channel_sync_enabled() -> bool:
    if not is_drive_sync_enabled():
        return False
    tok = (os.environ.get('TG_BOT_TOKEN') or '').strip()
    cid = (os.environ.get('TG_SYNC_CHANNEL_ID') or '').strip()
    return bool(tok and cid and cid.lstrip('-').isdigit())


def remove_synced_image(path):
    if is_drive_sync_enabled():
        try:
            from drive_sync_store import remove_image as _ds_remove
            _ds_remove(path)
            return
        except Exception:
            pass
    os.remove(path)

SETTINGS_LOCK = threading.RLock()
VALID_SAMPLERS = ("euler", "euler_ancestral", "dpmpp_2m", "dpmpp_2m_sde", "uni_pc", "heun", "deis")
VALID_SCHEDULERS = ("simple", "normal", "karras", "exponential", "sgm_uniform", "beta", "ddim_uniform")
VALID_UPSCALE_METHODS = ("lanczos", "bicubic", "bilinear", "area", "nearest-exact")

DEFAULT_STUDIO_SETTINGS = {
    "llm": {
        "system_prompt": (
            "你是一位专业的写实摄影与电影感视觉创作提示词生成助手，擅长将用户的简短创意、主题或分镜要求，转化为结构清晰、细节扎实、光影考究且零前后冲突的纯中文画面提示词，同时具备多轮原位精准改写能力。\n\n"
            "=== 输出格式规范 ===\n"
            "每次回复请先在 <核对>...</核对> 标签内做 3 行简短构图与一致性自检（系统会自动剥离核对草稿），随后在 <提示词>...</提示词> 标签内输出一段约 130～180 字、自然连贯、句号收尾的纯中文视觉提示词：\n"
            "<核对>\n"
            "1. 构图景别与环境光影：[画幅景别 + 具体空间场景 + 主光源方向、色温与氛围光影]\n"
            "2. 主体造型与动态神情：[人物或核心主体的外观造型、神态眼神、动作姿态与服装材质纹理]\n"
            "3. 镜头质感与原位改写检查：[焦段景深与胶片/数码摄影质感；若是修改轮，指明在原句对应位置直接替换且未改动部分原样保留]\n"
            "</核对>\n"
            "<提示词>\n"
            "[输出一整段约 130～180 字细腻写实的纯中文画面描述，不要标题、不要列表编号、不要英文与多余解释，以句号完整收尾]\n"
            "</提示词>\n\n"
            "=== 创作与多轮改写准则 ===\n"
            "1. 细节具象且符合真实光学物理：主动补全合理的环境纵深、自然光线过渡、织物与器物表面肌理、镜头焦段及浅景深虚化层次，避免空洞形容词堆砌。\n"
            "2. 单一位置集中描写、拒绝重复冲突：同一要素（景别环境、人物神态、手部动作、服饰造型、光影色调）在段落中仅集中描写一次，严禁前后矛盾。\n"
            "3. 多轮对话原位替换：当用户提出修改意见时，以上一轮 <提示词> 为底稿，仅在对应句子的原位置直接替换修改项，其余未提及的场景与造型细节保持原位不变。"
        ),
        "multi_turn_hint": "（多轮修改要求：请以上一版提示词为底稿，仅在对应元素原本所在的句子位置直接做原位替换修改，未要求改动的部分保持原有细节不变，确保全篇连贯自然、无重复与前后冲突。）",
        "check_prefix": "<think>\n\n</think>\n\n<提示词>\n",
        "temperature": 0.6,
        "max_tokens": 1024,
        "top_p": 0.9,
        "top_k": 20,
        "repeat_penalty": 1.05,
        "strip_terms": [],
    },
    "image_prompt": {
        "enable_clean": True,
        "strip_regexes": [],
        "replace_pairs": [],
        "enable_prefix": False,
        "zh_prefix": "",
        "en_prefix": "",
        "enable_suffix": True,
        "zh_suffix": "自然真实的环境光影层次与细腻光学景深，画面主体结构准确、材质纹理清晰自然，高解析度摄影画质。",
        "en_suffix": "Natural ambient lighting and subtle optical depth of field, accurate subject structure and authentic surface textures, high-resolution photographic quality.",
        "ref_face_prompt": "Keep the exact same subject's facial features and identity from Picture 1, while strictly following this new outfit, pose, expression, and environment (do NOT keep any clothes or background from Picture 1): {prompt}",
        "ref_scale_w": 768,
        "ref_scale_h": 1152,
        "ref_crop_w": 512,
        "ref_crop_h": 448,
        "ref_crop_x": 128,
        "ref_crop_y": 0,
    },
    "image_workflow": {
        "enable_hires": True,
        "hires_denoise": 0.16,
        "cfg": 1.0,
        "sampler_name": "euler",
        "scheduler": "simple",
        "upscale_method": "lanczos",
        "resolutions": {
            "portrait":  {"s1_w": 1024, "s1_h": 1536, "s2_w": 2048, "s2_h": 3072},
            "landscape": {"s1_w": 1536, "s1_h": 1024, "s2_w": 3072, "s2_h": 2048},
            "square":    {"s1_w": 1280, "s1_h": 1280, "s2_w": 2560, "s2_h": 2560},
        },
        "step_tiers": {
            "tier1": {"s1": 8,  "ref": 5},
            "tier2": {"s1": 12, "ref": 6},
            "tier3": {"s1": 16, "ref": 8},
        },
    },
}

def _align16(v, lo=256, hi=4096):
    return max(lo, min(hi, int(round(float(v) / 16.0)) * 16))

def _deep_merge_settings(base: dict, extra: dict) -> dict:
    out = copy.deepcopy(base)
    if not isinstance(extra, dict):
        return out
    for k, v in extra.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge_settings(out[k], v)
        elif k in out:
            out[k] = copy.deepcopy(v)
    return out

def _settings_path(state_dir=None) -> str:
    if state_dir is not None:
        return os.path.join(state_dir, "studio_settings.json")
    if STATE_DIR != "/opt/colabbb/state":
        return os.path.join(STATE_DIR, "studio_settings.json")
    return SETTINGS_FILE

def _atomic_write_text(target_path: str, content: str):
    parent = os.path.dirname(os.path.abspath(target_path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-" + os.path.basename(target_path) + "-", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, target_path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass
        raise

def load_studio_settings(state_dir=None) -> dict:
    with SETTINGS_LOCK:
        cfg = copy.deepcopy(DEFAULT_STUDIO_SETTINGS)
        spath = _settings_path(state_dir)
        if os.path.exists(spath):
            try:
                with open(spath, "r", encoding="utf-8") as fh:
                    parsed = json.load(fh)
                if isinstance(parsed, dict):
                    cfg = _deep_merge_settings(cfg, parsed)
            except Exception:
                pass
        llm = cfg["llm"]
        if not isinstance(llm.get("system_prompt"), str) or not llm["system_prompt"].strip():
            llm["system_prompt"] = DEFAULT_STUDIO_SETTINGS["llm"]["system_prompt"]
        else:
            llm["system_prompt"] = llm["system_prompt"].strip()
        if not isinstance(llm.get("multi_turn_hint"), str):
            llm["multi_turn_hint"] = DEFAULT_STUDIO_SETTINGS["llm"]["multi_turn_hint"]
        if not isinstance(llm.get("check_prefix"), str):
            llm["check_prefix"] = DEFAULT_STUDIO_SETTINGS["llm"]["check_prefix"]
        for fk, lo, hi in (("temperature", 0.0, 2.0), ("top_p", 0.05, 1.0), ("repeat_penalty", 0.8, 2.0)):
            try:
                v = llm[fk]
                if isinstance(v, bool) or not math.isfinite(float(v)):
                    raise ValueError()
                llm[fk] = round(max(lo, min(hi, float(v))), 4)
            except Exception:
                llm[fk] = DEFAULT_STUDIO_SETTINGS["llm"][fk]
        for ik, lo, hi in (("max_tokens", 128, 8192), ("top_k", 1, 200)):
            try:
                v = llm[ik]
                if isinstance(v, bool):
                    raise ValueError()
                llm[ik] = max(lo, min(hi, int(v)))
            except Exception:
                llm[ik] = DEFAULT_STUDIO_SETTINGS["llm"][ik]
        if isinstance(llm.get("strip_terms"), list):
            llm["strip_terms"] = [
                x.replace("\r", "").replace("\n", "")
                for x in llm["strip_terms"]
                if isinstance(x, str) and x.replace("\r", "").replace("\n", "") != ""
            ]
        else:
            llm["strip_terms"] = list(DEFAULT_STUDIO_SETTINGS["llm"]["strip_terms"])

        ip = cfg["image_prompt"]
        ip["enable_clean"] = bool(ip.get("enable_clean", True))
        ip["enable_prefix"] = bool(ip.get("enable_prefix", False))
        ip["enable_suffix"] = bool(ip.get("enable_suffix", True))
        for sk in ("zh_prefix", "en_prefix", "zh_suffix", "en_suffix", "ref_face_prompt"):
            if not isinstance(ip.get(sk), str):
                ip[sk] = DEFAULT_STUDIO_SETTINGS["image_prompt"][sk]
        if isinstance(ip.get("strip_regexes"), list):
            ip["strip_regexes"] = [x.strip() for x in ip["strip_regexes"] if isinstance(x, str) and x.strip()]
        else:
            ip["strip_regexes"] = list(DEFAULT_STUDIO_SETTINGS["image_prompt"]["strip_regexes"])
        if isinstance(ip.get("replace_pairs"), list):
            ip["replace_pairs"] = [
                {"from": str(it["from"]), "to": str(it.get("to", ""))}
                for it in ip["replace_pairs"]
                if isinstance(it, dict) and isinstance(it.get("from"), str) and it["from"] != ""
            ]
        else:
            ip["replace_pairs"] = copy.deepcopy(DEFAULT_STUDIO_SETTINGS["image_prompt"]["replace_pairs"])
        for pk in ("ref_scale_w", "ref_scale_h", "ref_crop_w", "ref_crop_h"):
            try:
                if isinstance(ip[pk], bool):
                    raise ValueError()
                ip[pk] = _align16(ip[pk])
            except Exception:
                ip[pk] = DEFAULT_STUDIO_SETTINGS["image_prompt"][pk]
        for ok in ("ref_crop_x", "ref_crop_y"):
            try:
                if isinstance(ip[ok], bool):
                    raise ValueError()
                ip[ok] = max(0, min(4096, int(ip[ok])))
            except Exception:
                ip[ok] = DEFAULT_STUDIO_SETTINGS["image_prompt"][ok]

        wf = cfg["image_workflow"]
        wf["enable_hires"] = bool(wf.get("enable_hires", True))
        for fk, lo, hi in (("hires_denoise", 0.01, 1.0), ("cfg", 0.1, 20.0)):
            try:
                v = wf[fk]
                if isinstance(v, bool) or not math.isfinite(float(v)):
                    raise ValueError()
                wf[fk] = round(max(lo, min(hi, float(v))), 4)
            except Exception:
                wf[fk] = DEFAULT_STUDIO_SETTINGS["image_workflow"][fk]
        if wf.get("sampler_name") not in VALID_SAMPLERS:
            wf["sampler_name"] = "euler"
        if wf.get("scheduler") not in VALID_SCHEDULERS:
            wf["scheduler"] = "simple"
        if wf.get("upscale_method") not in VALID_UPSCALE_METHODS:
            wf["upscale_method"] = "lanczos"
        if not isinstance(wf.get("resolutions"), dict):
            wf["resolutions"] = copy.deepcopy(DEFAULT_STUDIO_SETTINGS["image_workflow"]["resolutions"])
        else:
            for rk, rdef in DEFAULT_STUDIO_SETTINGS["image_workflow"]["resolutions"].items():
                rcur = wf["resolutions"].get(rk)
                if not isinstance(rcur, dict):
                    wf["resolutions"][rk] = dict(rdef)
                else:
                    for dk in ("s1_w", "s1_h", "s2_w", "s2_h"):
                        try:
                            if isinstance(rcur.get(dk), bool):
                                raise ValueError()
                            rcur[dk] = _align16(rcur.get(dk, rdef[dk]))
                        except Exception:
                            rcur[dk] = rdef[dk]
        if not isinstance(wf.get("step_tiers"), dict):
            wf["step_tiers"] = copy.deepcopy(DEFAULT_STUDIO_SETTINGS["image_workflow"]["step_tiers"])
        else:
            for tk, tdef in DEFAULT_STUDIO_SETTINGS["image_workflow"]["step_tiers"].items():
                tcur = wf["step_tiers"].get(tk)
                if not isinstance(tcur, dict):
                    wf["step_tiers"][tk] = dict(tdef)
                else:
                    try:
                        if isinstance(tcur.get("s1"), bool):
                            raise ValueError()
                        tcur["s1"] = max(1, min(60, int(tcur.get("s1", tdef["s1"]))))
                    except Exception:
                        tcur["s1"] = tdef["s1"]
                    try:
                        if isinstance(tcur.get("ref"), bool):
                            raise ValueError()
                        tcur["ref"] = max(0, min(40, int(tcur.get("ref", tdef["ref"]))))
                    except Exception:
                        tcur["ref"] = tdef["ref"]
        return cfg

from idle_guard import Guard, IdleError

IMAGE_KEYS = ("v5_int8",)
WARMUP_KEYS = IMAGE_KEYS + ("video", "llm")
MODEL_LABELS = {'v5_int8': os.environ.get('COLABBB_IMAGE_LABEL', 'Flux / DiT INT8'), 'video': os.environ.get('COLABBB_VIDEO_LABEL', '24fps 声画视频'), 'llm': os.environ.get('COLABBB_LLM_LABEL', 'Qwen 27B GGUF (llama-server)')}

def _validate_warmup_targets(targets):
    if not isinstance(targets, list) or not 1 <= len(targets) <= 3 or any(not isinstance(t, str) or t not in WARMUP_KEYS for t in targets):
        raise ValueError("预热 targets 必须是1至3项明确模型key的非空列表；不接受未知项目")
    return list(dict.fromkeys(targets))

def _parse_warmup_args(args):
    args = list(args)
    if args and args[0] == "--targets":
        return _validate_warmup_targets(args[1:])
    if any(a == "--targets" or a.startswith("--targets=") for a in args):
        raise ValueError("--targets 必须单独指定，不能与旧预热参数混用")
    model = "v5_int8"
    mods = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--image-model":
            i += 1
            if i >= len(args): raise ValueError("缺少 --image-model 值")
            model = args[i]
        elif arg.startswith("--image-model="):
            model = arg.split("=", 1)[1]
        elif arg in ("image", "video", "llm"):
            mods.append(arg)
        else:
            raise ValueError("未知预热参数")
        i += 1
    if model not in IMAGE_KEYS: raise ValueError("未知预热图像模型")
    return _validate_warmup_targets([model if m == "image" else m for m in (mods or ["image", "llm"])])

PANEL_IDLE_MIN = int(os.environ.get('COLAB_IDLE_MIN', '30'))
TG_IDLE_MIN = int(os.environ.get('COLABBB_TG_IDLE_MIN', '2'))

def idle_guard():
    return Guard(STATE_DIR, os.environ.get("COLAB_SESSION", "ai"))

def _resolve_source_idle_minutes(source: str = None) -> int:
    s = str(source or "panel").strip().lower()
    if s == "tg":
        return TG_IDLE_MIN
    if s == "autopush_timer":
        try:
            g = idle_guard()
            with g.gate():
                data = g.read(repair=False)
                if data.get("phase") == "running" and int(data.get("limit", PANEL_IDLE_MIN * 60)) > TG_IDLE_MIN * 60:
                    return PANEL_IDLE_MIN
        except Exception:
            pass
        return TG_IDLE_MIN
    return PANEL_IDLE_MIN

def touch_activity(idle_minutes: int = None):
    # Errors must not be silently presented as healthy activity.
    g = idle_guard()
    g.touch()
    if idle_minutes is not None:
        g.ensure_watcher(COLAB_AI_BIN, int(idle_minutes))

def touch_if_running(idle_minutes: int):
    try:
        g = idle_guard()
        is_running = False
        with g.gate():
            try:
                data = g.read(repair=False)
                is_running = (data.get("phase") == "running")
            except IdleError:
                is_running = False
        if is_running:
            g.touch()
            g.ensure_watcher(COLAB_AI_BIN, int(idle_minutes))
    except Exception:
        pass

_CHAT_LOCK = threading.Lock()

def load_chat_history():
    with _CHAT_LOCK:
        try:
            if os.path.exists(CHAT_FILE):
                with open(CHAT_FILE, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
    return []

def save_chat_history(msgs):
    with _CHAT_LOCK:
        os.makedirs(STATE_DIR, exist_ok=True)
        _atomic_write_text(CHAT_FILE, json.dumps(msgs[-40:], ensure_ascii=False, indent=2))

class ComfyProgressTracker:
    """只追踪由任务 stdout 明确绑定的 prompt；不推测无 ID 事件的归属。"""
    FRESH_SEC = 20.0
    ETA_FRESH_SEC = 10.0

    def __init__(self):
        self.lock = threading.Lock()
        self.prompts = {}
        self.connected = False
        self.recv_buffer = bytearray()
        threading.Thread(target=self._ws_loop, daemon=True).start()

    @staticmethod
    def _empty_snapshot():
        return {"node_id": "", "step_val": None, "step_max": None,
                "last_update": 0.0, "event": "", "samples": collections.deque(maxlen=8)}

    def bind(self, prompt_id):
        with self.lock:
            self.prompts[prompt_id] = self._empty_snapshot()

    def discard(self, prompt_id):
        if prompt_id:
            with self.lock:
                self.prompts.pop(prompt_id, None)

    def get_snapshot(self, prompt_id):
        with self.lock:
            snap = self.prompts.get(prompt_id)
            if not snap or not self.connected:
                return None
            return {**snap, "samples": list(snap["samples"])}

    def _recv_exact(self, sock, n):
        # HTTP upgrade recv may also contain the first websocket frame.
        while len(self.recv_buffer) < n:
            chunk = sock.recv(max(4096, n - len(self.recv_buffer)))
            if not chunk:
                raise ConnectionError("ws closed")
            self.recv_buffer.extend(chunk)
        result = bytes(self.recv_buffer[:n])
        del self.recv_buffer[:n]
        return result

    def _read_ws_frame(self, sock):
        b1, b2 = self._recv_exact(sock, 2)
        opcode = b1 & 0x0F
        masked = bool(b2 & 0x80)
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack(">H", self._recv_exact(sock, 2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self._recv_exact(sock, 8))[0]
        mask_key = self._recv_exact(sock, 4) if masked else b""
        payload = self._recv_exact(sock, length)
        if masked:
            payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        return opcode, payload

    def _record_event(self, msg):
        mtype = msg.get("type", "")
        data = msg.get("data") or {}
        pid = data.get("prompt_id")
        # No cross-prompt/node fallback: old, unbound or ID-less events are unknown.
        with self.lock:
            if not isinstance(pid, str) or pid not in self.prompts:
                return
            snap = self.prompts[pid]
            if snap["event"] in ("execution_success", "execution_error", "execution_interrupted", "execution_end"):
                return  # A late frame cannot resurrect a finished prompt.
            now = time.time()
            if mtype in ("execution_start", "execution_success", "execution_error", "execution_interrupted"):
                snap = self._empty_snapshot()
                snap.update(event=mtype, last_update=now)
                self.prompts[pid] = snap
            elif mtype == "executing":
                snap = self._empty_snapshot()
                nid = str(data.get("node") or "")
                snap.update(node_id=nid, event=mtype if nid else "execution_end", last_update=now)
                self.prompts[pid] = snap
            elif mtype == "progress":
                nid = str(data.get("node") or snap["node_id"] or "")
                val, mx = data.get("value"), data.get("max")
                if (not nid or isinstance(val, bool) or isinstance(mx, bool)
                        or not isinstance(val, (int, float)) or not isinstance(mx, (int, float))
                        or not math.isfinite(val) or not math.isfinite(mx)
                        or not 0 <= val <= mx or mx <= 0):
                    return
                if (nid != snap["node_id"] or mx != snap["step_max"]
                        or snap["step_val"] is None or val < snap["step_val"]
                        or now - snap["last_update"] > self.FRESH_SEC):
                    snap = self._empty_snapshot()
                    self.prompts[pid] = snap
                if not snap["samples"] or val > snap["samples"][-1][1]:
                    snap["samples"].append((now, val))
                snap.update(node_id=nid, step_val=val, step_max=mx, event=mtype, last_update=now)

    def _ws_loop(self):
        import socket
        while True:
            sock = None
            self.recv_buffer = bytearray()
            try:
                sock = socket.create_connection(("127.0.0.1", COMFY_TUNNEL_PORT), timeout=10)
                key = base64.b64encode(os.urandom(16)).decode()
                req = (
                    "GET /ws?clientId=host_panel_watcher HTTP/1.1\r\n"
                    f"Host: 127.0.0.1:{COMFY_TUNNEL_PORT}\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Key: {key}\r\n"
                    "Sec-WebSocket-Version: 13\r\n\r\n"
                ).encode()
                sock.sendall(req)
                hdr = b""
                while b"\r\n\r\n" not in hdr:
                    chunk = sock.recv(4096)
                    if not chunk:
                        raise ConnectionError("handshake EOF")
                    hdr += chunk
                head, tail = hdr.split(b"\r\n\r\n", 1)
                if not head.split(b"\r\n", 1)[0].startswith(b"HTTP/1.1 101 "):
                    raise ConnectionError("upgrade failed")
                self.recv_buffer.extend(tail)
                with self.lock:
                    self.connected = True
                sock.settimeout(60)
                while True:
                    opcode, payload = self._read_ws_frame(sock)
                    if opcode == 8:
                        break
                    if opcode == 9:
                        # Client control frames are masked and pong echoes ping data.
                        mask = os.urandom(4)
                        sock.sendall(bytes((0x8a, 0x80 | len(payload))) + mask
                                     + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))
                        continue
                    if opcode == 1:
                        self._record_event(json.loads(payload.decode("utf-8")))
            except Exception:
                pass
            finally:
                with self.lock:
                    self.connected = False
                    for pid in self.prompts:
                        self.prompts[pid] = self._empty_snapshot()
                if sock:
                    try:
                        sock.close()
                    except Exception:
                        pass
            time.sleep(2.0)

import struct
COMFY_TRACKER = ComfyProgressTracker()

def _edit_disabled_request(module, cmd_list):
    aliases = ("edit", "krea2edit", "legacyedit", "qwen", "qwen2.1")
    if (module or "").lower() in aliases:
        return True
    argv = list(cmd_list)[1:]
    if argv and argv[0] == "run": argv = argv[1:]
    if argv and argv[0].lower() in aliases: return True
    if not argv or argv[0] != "image": return False
    # Match CLI argparse semantics, including --model=value, -mVALUE and abbreviations.
    import argparse
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--model", "-m", default="v5_int8")
    try:
        parsed, _ = p.parse_known_args(argv[1:])
        return parsed.model.lower().strip() in aliases
    except SystemExit:
        return False  # Malformed argv is refused by CLI; it is never converted into generation.

REMOVED_IMAGE_KEYS = ("v4_int8", "v4-int8", "kreahd", "legacy_image", "v4_bf16", "v4-bf16", "v4_full", "v6_int8", "v6-int8", "v6_int8_convrot", "v6", "v6b_int8_convrot")

def _removed_model_request(module, cmd_list):
    argv = list(cmd_list or ())[1:]
    if argv and argv[0] == "run":
        argv = argv[1:]
    if not argv:
        return False
    sub = argv[0].lower()
    rest = argv[1:]
    if sub == "warmup":
        for i, a in enumerate(rest):
            low = a.lower().strip()
            if low in REMOVED_IMAGE_KEYS:
                return True
            if a == "--image-model" and i + 1 < len(rest) and rest[i + 1].lower().strip() in REMOVED_IMAGE_KEYS:
                return True
            if a.startswith("--image-model=") and a.split("=", 1)[1].lower().strip() in REMOVED_IMAGE_KEYS:
                return True
        return False
    if sub != "image":
        return False
    import argparse
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--model", "-m", default="v5_int8")
    try:
        parsed, _ = p.parse_known_args(rest)
        return parsed.model.lower().strip() in REMOVED_IMAGE_KEYS
    except SystemExit:
        return False

class JobManager:
    """
    支持连续提交任务 + 任务级阶段/采样进度 + 单任务终止/继续/展开详情：
    - media (image / video) 与 llm 分属两条独立并行通道
    - 同一通道内连续提交的任务自动进入 FIFO 队列顺序执行
    """
    def __init__(self):
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.queues = {
            "media": collections.deque(),
            "llm": collections.deque(),
            "sys": collections.deque(),
        }
        self.active_procs = {}
        self.recent_done = collections.deque()
        self._history_present = False
        self.paused = {}
        self.next_id = 1
        self.cmd_str = ""
        self.module = ""
        self.start_time = 0.0
        self.end_time = 0.0
        self.exit_code = None
        self.logs = []
        self.completed_seq = 0
        self._restore_unfinished()
        self._restore_history()
        threading.Thread(target=self._generation_monitor, daemon=True).start()
        for lane in ("media", "llm", "sys"):
            threading.Thread(target=self._lane_worker, args=(lane,), daemon=True).start()

    def _history_path(self):
        return os.path.join(STATE_DIR, "task-history.json")

    def _load_history(self):
        path = self._history_path()
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            return None
        if (st.st_mode & 0o170000 != 0o100000 or st.st_uid != 0
                or st.st_mode & 0o777 != 0o600):
            raise RuntimeError("任务历史必须为 root 所有的 0600 普通文件，已停止以保全记录")
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if (not isinstance(data, dict) or type(data.get("version")) is not int or data["version"] != 1
                or type(data.get("next_id")) is not int or data["next_id"] < 1
                or not isinstance(data.get("tasks"), list)):
            raise RuntimeError("任务历史格式损坏，已停止以保全记录")
        seen = set()
        for it in data["tasks"]:
            if (not isinstance(it, dict) or type(it.get("jid")) is not int or it["jid"] < 1
                    or it["jid"] in seen or it.get("status") not in ("done", "error", "stopped")
                    or not isinstance(it.get("cmd_list"), list) or not it["cmd_list"]
                    or not all(isinstance(x, str) for x in it["cmd_list"])
                    or not all(isinstance(it.get(k), str) for k in
                               ("cmd_s", "module", "label", "preview", "full_prompt", "status_text", "time_str"))
                    or type(it.get("elapsed")) not in (int, float)
                    or not math.isfinite(it["elapsed"]) or it["elapsed"] < 0
                    or type(it.get("resume_supported", True)) is not bool
                    or (it.get("resume_supported", True) and
                        (type(it.get("steps")) is not int or it["steps"] < 1))):
                raise RuntimeError("任务历史条目不完整或重复，已停止以保全记录")
            seen.add(it["jid"])
        return data

    def _persist_history(self):
        # Single writer under self.cond. Keep the previous inode for post-replace errors.
        previous = self._load_history()
        if self._history_present and previous is None:
            raise RuntimeError("任务历史文件意外缺失，拒绝重建或清空")
        if os.geteuid() != 0:
            raise RuntimeError("任务历史必须由 root 私有写入")
        os.makedirs(STATE_DIR, exist_ok=True)
        path = self._history_path()
        fd, tmp = tempfile.mkstemp(prefix=".task-history-", dir=STATE_DIR)
        backup = None
        committed = False
        cleanup_backup = True
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                os.fchmod(fh.fileno(), 0o600)
                old_stat = os.stat(path) if previous is not None else None
                os.fchown(fh.fileno(), old_stat.st_uid if old_stat else 0, old_stat.st_gid if old_stat else 0)
                json.dump({"version": 1, "next_id": self.next_id, "tasks": list(self.recent_done)},
                          fh, ensure_ascii=False, allow_nan=False)
                fh.flush()
                os.fsync(fh.fileno())
            if previous is not None:
                bfd, backup = tempfile.mkstemp(prefix=".task-history-old-", dir=STATE_DIR)
                os.close(bfd)
                os.unlink(backup)
                os.link(path, backup)
            os.replace(tmp, path)
            committed = True
            dfd = os.open(STATE_DIR, os.O_RDONLY)
            try: os.fsync(dfd)
            finally: os.close(dfd)
            self._history_present = True
        except Exception:
            if committed:
                cleanup_backup = False
                if previous is not None:
                    os.replace(backup, path)
                else:
                    os.unlink(path)  # Undo only this failed first creation, never an existing history.
                cleanup_backup = True
                dfd = os.open(STATE_DIR, os.O_RDONLY)
                try:
                    os.fsync(dfd)
                except OSError:
                    pass  # Original pathname/content has already been restored.
                finally:
                    os.close(dfd)
            raise
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
            if cleanup_backup and backup and os.path.exists(backup): os.unlink(backup)

    def _restore_history(self):
        data = self._load_history()
        if data is None:
            self._persist_history()  # Fresh installation; legacy seed must precede first startup.
            return
        self._history_present = True
        self.recent_done = collections.deque(data["tasks"])
        self.next_id = max([self.next_id, data["next_id"]] + [it["jid"] + 1 for it in self.recent_done])
        terminal_ids = {it["jid"] for it in self.recent_done}
        duplicates = terminal_ids.intersection(self.paused)
        if duplicates:
            # History is committed before removal from unfinished journal. A crash
            # between these writes must restore the REAL terminal state, not paused.
            for jid in duplicates: del self.paused[jid]
            self._persist_unfinished()  # Durable normalization before deletes or workers are admitted.

    def _record_history(self, record):
        if any(it["jid"] == record["jid"] for it in self.recent_done):
            raise RuntimeError("重复的终态任务编号，拒绝覆盖历史")
        old = self.recent_done
        self.recent_done = collections.deque([record, *old])
        try:
            self._persist_history()
        except Exception:
            self.recent_done = old
            raise

    def _finish_task(self, item, rc, started_at):
        # Caller holds the same cond as admission/stop/delete. Commit terminal
        # history BEFORE dropping the active record or its unfinished journal entry.
        jid = item["jid"]
        self._clear_task_prompt(item)
        if item.get("generation") != self._generation(): self._pause_item(item)
        ended_at = time.time()
        dt = round(ended_at - started_at, 1)
        if item.get("stopped_by_user"):
            st_code, status_txt = "stopped", "⏹️ 已终止"
        elif rc == 0:
            st_code, status_txt = "done", "✅ 已完成"
        else:
            st_code, status_txt = "error", f"❌ 异常(码{rc})"
        if jid not in self.paused:
            record = {**self._saved_item(item),
                      "progress_complete": self._completion_confirmed(item, rc),
                      "status": st_code, "status_text": status_txt, "elapsed": dt,
                      "time_str": time.strftime("%H:%M:%S"), "started_at": started_at,
                      "ended_at": ended_at, "exit_code": rc, "resume_supported": True}
            try:
                self._record_history(record)
            except Exception:
                item["phase"] = "任务已退出，但历史持久化失败；原记录保留，通道已停止派发"
                self.logs.append(f"[history] 任务 #{jid} 历史持久化失败，未移除原记录；请检查状态目录\n")
                raise  # Stop this worker, keep active/journal/lease; never silently discard and continue.
        self.active_procs.pop(jid, None)
        self._release_idle(item)
        self.exit_code = rc
        self.end_time = ended_at
        self.completed_seq += 1
        if jid in self.paused:
            self.paused[jid].update(elapsed=dt, time_str=time.strftime("%H:%M:%S"))
        self._persist_unfinished()
        self.cond.notify_all()
        return st_code, status_txt, dt

    def _unfinished_path(self):
        return os.path.join(STATE_DIR, "paused-tasks.json")

    @staticmethod
    def _saved_item(it):
        return {k: v for k, v in it.items() if k in (
            "jid", "cmd_list", "cmd_s", "module", "lane", "user_msg", "label",
            "steps", "preview", "full_prompt", "enqueued_at", "generation")}

    def _persist_unfinished(self):
        # Journal only unfinished work; terminal history is stored separately.
        items = dict(self.paused)
        for it in list(self.active_procs.values()) + [it for q in self.queues.values() for it in q]:
            if it["module"] not in ("start", "stop"):
                items[it["jid"]] = it
        os.makedirs(STATE_DIR, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".paused-", dir=STATE_DIR)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"next_id": self.next_id, "tasks": [self._saved_item(it) for it in items.values()]}, fh, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._unfinished_path())
            dfd = os.open(STATE_DIR, os.O_RDONLY)
            try: os.fsync(dfd)
            finally: os.close(dfd)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)

    def _restore_unfinished(self):
        try:
            with open(self._unfinished_path(), encoding="utf-8") as fh: data = json.load(fh)
        except FileNotFoundError:
            return
        # Invalid/unreadable state aborts startup; never replace it with an empty queue.
        if not isinstance(data, dict) or not isinstance(data.get("tasks"), list):
            raise RuntimeError("暂停状态损坏，已停止启动以保全原记录")
        for it in data["tasks"]:
            if not isinstance(it, dict) or not all(k in it for k in ("jid", "cmd_list", "module", "label", "preview", "cmd_s", "steps")):
                raise RuntimeError("暂停任务记录不完整，已停止启动")
            it.update(status="paused", status_text="⏸️ 已暂停（手动继续）", elapsed=0.0, time_str="", stopped_by_user=True)
            it["lane"] = self._lane_for(it["module"])
            self.paused[int(it["jid"])] = it
        self.next_id = max([int(data.get("next_id", 1))] + [jid + 1 for jid in self.paused])

    @staticmethod
    def _generation_path():
        key = hashlib.sha256(os.environ.get("COLAB_SESSION", "ai").encode()).hexdigest()
        return os.path.join(STATE_DIR, "power-generation-" + key)

    @staticmethod
    def _generation():
        try:
            with open(JobManager._generation_path()) as fh: return fh.read().strip()
        except FileNotFoundError:
            return "0"

    @staticmethod
    def _release_idle(it):
        lease = it.pop("idle_lease", None)
        if lease is not None:
            try:
                lease.close()
            except OSError:
                print("[idle] busy lease released but completion write failed", file=sys.stderr, flush=True)

    def _generation_monitor(self):
        # External CLI/automatic stop uses the same fence, not a second task system.
        while True:
            try:
                with self.cond:
                    self._sync_generation()
            except Exception as exc:
                print("[idle] panel pause coordination failed: " + type(exc).__name__, file=sys.stderr, flush=True)
            time.sleep(0.5)

    def _sync_generation(self):
        current = self._generation()
        changed = False
        for q in self.queues.values():
            for it in list(q):
                if it.get("generation") != current:
                    q.remove(it)
                    self._pause_item(it)
                    changed = True
        for it in self.active_procs.values():
            if it.get("generation") != current:
                self._pause_item(it)
                it["stopped_by_user"] = True
                changed = True  # retry journal failure before delivering any signal
        if changed:
            self._persist_unfinished()
            for it in self.active_procs.values():
                if it.get("generation") != current and not it.get("fence_signal_sent"):
                    p = it["proc"]
                    if p.poll() is None:
                        try: os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                        except ProcessLookupError: pass
                    it["fence_signal_sent"] = True
            self.cond.notify_all()

    def _pause_item(self, it):
        if "proc" not in it:
            self._release_idle(it)
        if it["module"] in ("start", "stop"):
            return
        it.update(stopped_by_user=True, status="paused", status_text="⏸️ 已暂停（手动继续）", elapsed=0.0, time_str="")
        self._clear_task_prompt(it)
        self.paused[it["jid"]] = it

    def _stop_barrier(self):
        # Claim the same short gate as CLI admission/automatic idle stop.
        idle_guard().claim_stop()
        for q in self.queues.values():
            while q:
                it = q.popleft()
                self._pause_item(it)
        for it in self.active_procs.values():
            self._pause_item(it)
            # Start is also interrupted, but is never restored as a resumable task.
            it["stopped_by_user"] = True
        self._persist_unfinished()
        for it in self.active_procs.values():
            p = it["proc"]
            try: os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except ProcessLookupError: pass
        self.logs.append("[lifecycle] stop barrier: old generation paused; no automatic resume\n")

    def _lane_for(self, module_name: str) -> str:
        if module_name in ("image", "edit", "video", "start", "warmup"):
            return "media"
        if module_name == "llm":
            return "llm"
        return "sys"

    @staticmethod
    def _is_start_job(module_name, cmd_list):
        return module_name == "start" and list(cmd_list) == [COLAB_AI_BIN, "start"]

    @staticmethod
    def _is_stop_job(module_name, cmd_list):
        return module_name == "stop" and list(cmd_list) == [COLAB_AI_BIN, "stop"]

    @staticmethod
    def _is_warmup_job(module_name, cmd_list):
        if module_name != "warmup" or list(cmd_list[:2]) != [COLAB_AI_BIN, "warmup"]:
            return False
        try:
            _parse_warmup_args(cmd_list[2:])
            return True
        except ValueError:
            return False
    def _has_start_pending(self):
        # Called only while holding self.lock / self.cond.
        return any(
            self._is_start_job(it["module"], it["cmd_list"])
            for it in list(self.active_procs.values()) + [it for q in self.queues.values() for it in q]
        )

    def start_job(self, cmd_list, module_name, user_msg=None, meta_label="", steps=13, prompt_preview="", resume_from=None, idle_minutes=PANEL_IDLE_MIN):
        if _edit_disabled_request(module_name, cmd_list):
            return False, "改图暂未启用；记录和引用保留，可手动删除记录"
        if _removed_model_request(module_name, cmd_list):
            return False, "该模型已移除；当前仅保留 V5 INT8"
        if module_name == "warmup" and not self._is_warmup_job(module_name, cmd_list):
            return False, "无效预热项目或命令"
        lane = self._lane_for(module_name)
        is_start = self._is_start_job(module_name, cmd_list)
        is_stop = self._is_stop_job(module_name, cmd_list)
        is_warmup = self._is_warmup_job(module_name, cmd_list)
        if is_warmup:
            with self.cond:
                if self._has_start_pending():
                    return False, "请先等待开机完成"
            # Authoritative read-only check, never the UI cache or sync_sessions.
            # Do not hold the queue lock during the network query.
            try:
                if not has_colab_instance():
                    return False, "请先开机"
            except Exception:
                return False, "检查实例状态失败，请稍后重试"
        with self.cond:
            self._sync_generation()
            stop_pending = any(self._is_stop_job(it["module"], it["cmd_list"]) for it in list(self.active_procs.values()) + [it for q in self.queues.values() for it in q])
            if stop_pending and not is_stop:
                return False, "关机正在进行，请等待完成后再提交或继续"
            if is_warmup and self._has_start_pending():
                return False, "请先等待开机完成"
            if is_start or is_stop or is_warmup:
                meta_label = "开机（仅启动实例）" if is_start else ("关机" if is_stop else (meta_label or "预热设备"))
                for it in self.active_procs.values():
                    if it["module"] == module_name and it["cmd_list"] == list(cmd_list):
                        if not is_stop:
                            it["idle_minutes"] = int(idle_minutes)
                            try: idle_guard().ensure_watcher(COLAB_AI_BIN, int(idle_minutes))
                            except Exception: pass
                        return True, f"{meta_label}任务 #{it['jid']} 已在进行中，未重复添加"
                for q in self.queues.values():
                    for it in q:
                        if it["module"] == module_name and it["cmd_list"] == list(cmd_list):
                            if not is_stop:
                                it["idle_minutes"] = int(idle_minutes)
                                try: idle_guard().ensure_watcher(COLAB_AI_BIN, int(idle_minutes))
                                except Exception: pass
                            return True, f"{meta_label}任务 #{it['jid']} 已提交、正在排队，未重复添加"
            if is_stop:
                try:
                    self._stop_barrier()
                except IdleError:
                    return False, "关机已由其他入口发起，请等待完成或核实状态"
            try:
                lease = None if is_stop else idle_guard().lease()
                if not is_stop:
                    idle_guard().ensure_watcher(COLAB_AI_BIN, int(idle_minutes))
            except (IdleError, OSError):
                return False, "关机未完成或闲置保护状态不可写，未派发；请核实并重试关机"
            jid = self.next_id
            self.next_id += 1
            cmd_s = " ".join(shlex.quote(x) for x in cmd_list)
            now = time.time()
            if not self.active_procs and sum(len(q) for q in self.queues.values()) == 0:
                self.start_time = now
                self.logs = []
            self.cmd_str = cmd_s
            self.module = module_name
            q_len = len(self.queues[lane])
            is_lane_busy = any(v["lane"] == lane for v in self.active_procs.values())
            full_p = (prompt_preview or user_msg or cmd_s).strip()
            short_p = full_p.replace("\n", " ")
            if len(short_p) > 42:
                short_p = short_p[:42] + "..."
            item = {
                "jid": jid,
                "cmd_list": list(cmd_list),
                "cmd_s": cmd_s,
                "module": module_name,
                "lane": lane,
                "user_msg": user_msg,
                "label": meta_label or module_name,
                "steps": int(steps or 13),
                "preview": short_p,
                "full_prompt": full_p,
                "enqueued_at": now,
                "stopped_by_user": False,
                "generation": lease.generation if lease else self._generation(),
                "idle_lease": lease,
                "idle_minutes": int(idle_minutes),
            }
            old_paused = self.paused.pop(resume_from, None)
            self.queues[lane].append(item)
            try:
                self._persist_unfinished()
            except OSError:
                self.queues[lane].remove(item)
                self._release_idle(item)
                if old_paused is not None: self.paused[resume_from] = old_paused
                return False, "无法保全任务状态，未派发；请检查状态目录后重试"
            if is_lane_busy or q_len > 0:
                self.logs.append(f"[{time.strftime('%H:%M:%S')}] 📥 任务 #{jid} ({item['label']}) 已加入队列 (前方等待 {q_len + (1 if is_lane_busy else 0)} 个)\n")
            self.cond.notify_all()
            return True, f"任务 #{jid} 已提交"

    def _lane_worker(self, lane: str):
        while True:
            with self.cond:
                while (not self.queues[lane]) or (lane == "llm" and self._has_start_pending()):
                    self.cond.wait()
                self._sync_generation()
                if not self.queues[lane] or (lane == "llm" and self._has_start_pending()):
                    continue
                item = self.queues[lane].popleft()
                jid = item["jid"]
                if item.get("generation") != self._generation():
                    self._pause_item(item)
                    self._persist_unfinished()
                    continue
                cmd_list = item["cmd_list"]
                cmd_s = item["cmd_s"]
                module_name = item["module"]
                user_msg = item["user_msg"]
                now = time.time()
                if not self.active_procs and self.start_time == 0.0:
                    self.start_time = now
                self.cmd_str = cmd_s
                self.module = module_name
                self.logs.append(f"[{time.strftime('%H:%M:%S')}] 🚀 开始执行任务 #{jid} ({item['label']})\n")
                job_idle_min = int(item.get("idle_minutes", PANEL_IDLE_MIN))
                env = dict(
                    os.environ,
                    COLAB_AI_GENERATION=item["generation"],
                    COLAB_IDLE_MIN=str(job_idle_min),
                )
                try:
                    proc = subprocess.Popen(
                        cmd_list,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                        start_new_session=True,
                        env=env,
                    )
                except OSError:
                    self._pause_item(item)
                    self._persist_unfinished()
                    self.logs.append(f"[任务 #{jid}] 无法启动，已保留为暂停\n")
                    continue
                item["proc"] = proc
                item["t0"] = now
                item["phase"] = self._initial_phase(module_name)
                item["prompt_id"] = None
                item["output_received"] = False
                item["log_read_failed"] = False
                item["warmup_ready"] = set()
                self.active_procs[jid] = item

            try:
                for line in proc.stdout:
                    touch_activity()
                    with self.lock:
                        self._observe_task_line(item, line)
                        self.logs.append(line)
                        if len(self.logs) > 2500:
                            self.logs = self.logs[-1800:]
            except Exception as e:
                with self.lock:
                    item["log_read_failed"] = True
                    self._clear_task_prompt(item)
                    item["phase"] = "任务日志读取中断，进度未知"
                    self.logs.append(f"\n[任务 #{jid} 读取日志异常: {e}]\n")
            finally:
                rc = proc.wait()
                proc.stdout.close()
                with self.cond:
                    other_mins = [
                        int(v.get("idle_minutes", PANEL_IDLE_MIN))
                        for v in list(self.active_procs.values()) + [x for q in self.queues.values() for x in q]
                        if v.get("jid") != jid and v.get("module") != "stop"
                    ]
                eff_idle_min = None if module_name == "stop" else max([job_idle_min] + other_mins)
                try:
                    touch_activity(eff_idle_min)
                except OSError:
                    print("[idle] activity write failed at task completion", file=sys.stderr, flush=True)
                with self.cond:
                    st_code, status_txt, dt = self._finish_task(item, rc, now)
                    self.logs.append(f"[{time.strftime('%H:%M:%S')}] [#{jid} {item['label']}] {status_txt} · 耗时 {dt:.1f}s\n")
                    if rc == 0 and module_name == "llm" and user_msg:
                        try:
                            llm_dir = os.path.join(OUT_ROOT, "llm")
                            if os.path.isdir(llm_dir):
                                mds = [
                                    os.path.join(llm_dir, f)
                                    for f in os.listdir(llm_dir)
                                    if f.endswith(".md") and os.path.getmtime(os.path.join(llm_dir, f)) >= now - 2
                                ]
                                if mds:
                                    mds.sort(key=os.path.getmtime)
                                    reply_txt = open(mds[-1], "r", encoding="utf-8", errors="ignore").read().strip()
                                    if reply_txt:
                                        hist = load_chat_history()
                                        hist.append({"role": "user", "content": user_msg, "time": time.strftime("%H:%M")})
                                        hist.append({"role": "assistant", "content": reply_txt, "time": time.strftime("%H:%M"), "model_label": item.get("label", "Qwen 27B GGUF")})
                                        save_chat_history(hist)
                        except Exception:
                            pass

    def stop_task(self, jid: int):
        jid = int(jid)
        need_interrupt = False
        with self.cond:
            for q in self.queues.values():
                for idx, it in enumerate(q):
                    if it["jid"] == jid:
                        del q[idx]
                        self._pause_item(it)
                        self._persist_unfinished()
                        self.completed_seq += 1
                        return True, f"任务 #{jid} 已暂停，可手动继续"
            it = self.active_procs.get(jid)
            if not it:
                return False, f"未找到运行或排队中的任务 #{jid}"
            if it["module"] in ("start", "stop"):
                return False, "电源操作进行中，请等待完成"
            self._pause_item(it)
            self._persist_unfinished()
            p = it["proc"]
            try: os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except ProcessLookupError: pass
            need_interrupt = it["module"] in ("image", "edit", "video")
        if need_interrupt:
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{COMFY_TUNNEL_PORT}/interrupt", data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
                urllib.request.urlopen(req, timeout=2).read()
            except Exception: pass
        return True, f"任务 #{jid} 已暂停，可手动继续"

    def resume_task(self, jid: int, idle_minutes: int = PANEL_IDLE_MIN):
        jid = int(jid)
        # RLock permits atomic lookup/requeue/removal and rejects duplicate continue clicks.
        with self.cond:
            if jid in self.active_procs:
                return False, "任务仍在退出，请稍后继续"
            found = self.paused.get(jid)
            if not found:
                found = next((it for it in self.recent_done if it["jid"] == jid), None)
            if not found:
                return False, f"未找到可继续任务 #{jid}"
            if _edit_disabled_request(found["module"], found["cmd_list"]):
                return False, "改图暂未启用；原记录保留，不能继续"
            if _removed_model_request(found["module"], found["cmd_list"]):
                return False, "该模型已移除；当前仅保留 V5 INT8"
            if not found.get("resume_supported", True):
                return False, "旧历史仅有 API 展示字段，缺少完整继续元数据；未派发"
            ok, msg = self.start_job(found["cmd_list"], found["module"], user_msg=found.get("user_msg"), meta_label=found["label"], steps=found.get("steps",13), prompt_preview=found.get("full_prompt", ""), resume_from=jid, idle_minutes=idle_minutes)
            if ok:
                self.paused.pop(jid, None)
                self._persist_unfinished()
            return ok, msg

    def _can_delete_task(self, jid):
        # Caller holds self.lock / self.cond, shared with stop/resume/workers.
        if (jid in self.active_procs
                or any(it["jid"] == jid for it in self.active_procs.values())
                or any(it["jid"] == jid for q in self.queues.values() for it in q)
                or any(self._is_stop_job(it["module"], it["cmd_list"])
                       for it in list(self.active_procs.values()) + [it for q in self.queues.values() for it in q])):
            return False
        it = self.paused.get(jid)
        if it is not None:
            return it.get("status") in ("paused", "stopped")
        return any(it["jid"] == jid and it.get("status") in ("done", "error", "stopped")
                   for it in self.recent_done)

    def delete_task(self, jid: int):
        if type(jid) is not int:
            return False, "任务编号必须为整数"
        with self.cond:
            if (jid not in self.paused and not any(it["jid"] == jid for it in self.recent_done)
                    and jid not in self.active_procs
                    and not any(it["jid"] == jid for q in self.queues.values() for it in q)):
                return False, f"未找到任务 #{jid}"
            if not self._can_delete_task(jid):
                return False, "任务运行、排队、退出或关机处理中，不能删除；请先停止并等待结束后单独删除"
            if jid in self.paused:
                # Keep the exact previous journal inode until the atomic writer has
                # fully returned: even a directory-fsync error after replace must
                # roll back both the journal and memory, not just the dict entry.
                backup = None
                backup_ready = False
                cleanup_backup = True
                old_paused = dict(self.paused)
                try:
                    fd, backup = tempfile.mkstemp(prefix=".paused-delete-", dir=STATE_DIR)
                    os.close(fd)
                    os.unlink(backup)
                    os.link(self._unfinished_path(), backup)
                    backup_ready = True
                    del self.paused[jid]
                    self._persist_unfinished()
                except Exception:
                    self.paused.clear()
                    self.paused.update(old_paused)
                    if backup_ready:
                        if not os.path.samefile(backup, self._unfinished_path()):
                            cleanup_backup = False  # Keep recovery inode if rollback itself fails.
                            os.replace(backup, self._unfinished_path())
                            cleanup_backup = True
                            dfd = os.open(STATE_DIR, os.O_RDONLY)
                            try:
                                os.fsync(dfd)
                            except OSError:
                                pass  # Original journal is already restored.
                            finally:
                                os.close(dfd)
                    return False, "无法持久化删除，原任务已保留；请检查状态目录后重试"
                finally:
                    if cleanup_backup and backup and os.path.exists(backup):
                        os.unlink(backup)
            else:
                # Flush the current unfinished view first: a previous completion
                # may have committed history before its journal cleanup failed.
                # Never delete the authoritative terminal record over a stale paused copy.
                old = self.recent_done
                try:
                    self._persist_unfinished()
                    self.recent_done = collections.deque(it for it in old if it["jid"] != jid)
                    self._persist_history()
                except Exception:
                    self.recent_done = old
                    return False, "无法持久化删除，原历史已保留；请检查状态目录后重试"
            return True, f"任务 #{jid} 的记录已删除，作品和素材未删除"

    def clear_tasks(self):
        with self.cond:
            if any(
                self._is_stop_job(it["module"], it["cmd_list"])
                for it in list(self.active_procs.values()) + [it for q in self.queues.values() for it in q]
            ):
                return False, "关机处理中，请等待关机完成后再清空任务列表"
            del_paused_ids = [jid for jid in list(self.paused.keys()) if self._can_delete_task(jid)]
            del_done_ids = {it["jid"] for it in self.recent_done if self._can_delete_task(it["jid"])}
            total_del = len(del_paused_ids) + len(del_done_ids)
            active_or_queued = len(self.active_procs) + sum(len(q) for q in self.queues.values())
            if total_del == 0:
                if active_or_queued > 0:
                    return False, "当前仅有运行或排队中的任务，无可清空的记录"
                return True, "任务列表已为空"
            old_paused = dict(self.paused)
            old_done = collections.deque(self.recent_done)
            try:
                for jid in del_paused_ids:
                    self.paused.pop(jid, None)
                self.recent_done = collections.deque(it for it in old_done if it["jid"] not in del_done_ids)
                self._persist_unfinished()
                self._persist_history()
            except Exception:
                self.paused.clear()
                self.paused.update(old_paused)
                self.recent_done = old_done
                return False, "无法持久化清空状态，原任务记录已保留"
            if active_or_queued > 0:
                return True, f"已清空 {total_del} 条记录（{active_or_queued} 个运行/排队任务及作品保留）"
            return True, f"已清空全部 {total_del} 条任务记录（作品和素材未删除）"

    def cancel_job(self):
        rand_cancelled = PANEL_RANDOM.cancel_all() if "PANEL_RANDOM" in globals() else False
        with self.cond:
            ids = [it["jid"] for q in self.queues.values() for it in q if it["module"] not in ("start", "stop")]
            ids += [jid for jid,it in self.active_procs.items() if it["module"] not in ("start", "stop")]
            for jid in ids: self.stop_task(jid)
            ok_any = bool(ids) or rand_cancelled
            return ok_any, "任务已中止/暂停" if ok_any else "当前没有可中止的任务"

    @staticmethod
    def _initial_phase(module_name):
        return {"image": "准备模型与提示词编码", "edit": "准备原图与改图编码",
                "video": "准备视频模型与首帧", "llm": "连接提示词引擎",
                "warmup": "预热所选模型（尚未就绪）",
                "start": "启动实例（不加载模型）", "stop": "关闭实例"}.get(module_name, "准备执行")

    @staticmethod
    def _clear_task_prompt(item):
        COMFY_TRACKER.discard(item.get("prompt_id"))
        item["prompt_id"] = None

    @staticmethod
    def _completion_confirmed(item, rc):
        if rc != 0 or item.get("stopped_by_user") or item.get("log_read_failed"):
            return False
        if item["module"] in ("image", "edit", "video", "llm", "asr"):
            return item.get("output_received", False)
        if item["module"] == "warmup":
            try:
                targets = _parse_warmup_args(item["cmd_list"][2:])
            except (ValueError, KeyError):
                return False
            expected = {"target:" + t for t in targets} if "--targets" in item["cmd_list"] else {t if t in ("llm", "video") else "image" for t in targets}
            return expected.issubset(item.get("warmup_ready", set()))
        return True  # Successful start/stop has no model output to transfer.

    def _observe_task_line(self, item, line):
        # Inspect only technical markers, never reflect log/prompt text into stage labels.
        if item.get("stopped_by_user"):
            return
        text = line.strip()
        target_event = re.fullmatch(r"COLAB_AI_WARMUP_TARGET_(BEGIN|OK)=(v5_int8|video|llm)", text)
        if target_event and item["module"] == "warmup":
            kind, target = target_event.groups()
            self._clear_task_prompt(item)
            if kind == "OK": item["warmup_ready"].add("target:" + target)
            item["phase"] = ("正在预热 " if kind == "BEGIN" else "已完成预热 ") + MODEL_LABELS.get(target, "提示词 LLM") + "（整批等待主命令确认）"
            return
        ready = re.fullmatch(r"COLAB_AI_WARMUP_OK=(image|video|llm)", text)
        if ready and item["module"] == "warmup":
            item["warmup_ready"].add(ready.group(1))
            self._clear_task_prompt(item)
            item["phase"] = "单项服务就绪，等待本批所选项目及主命令确认"
            return
        if text in (
                "[lifecycle] model service started once; waiting for readiness",
                "[lifecycle] existing service loading/unhealthy; waiting without kill/restart"):
            # Only these service-wait markers; unrelated lifecycle events stay ignored.
            # Do not change prompt binding or any sampling tracker state.
            item["phase"] = "正在启动/等待模型服务就绪"
            return
        if not re.match(r"^\[(?:colab-ai|image|edit|video|llm|llm_server)\]", text):
            return
        if any(k in text for k in ("自动重连", "自动重试", "自愈重试", "回退至标准通道")):
            self._clear_task_prompt(item)
            item["output_received"] = False
            item["warmup_ready"].clear()
            item["phase"] = "连接重试，等待新的任务进度"
            return
        submitted = re.search(r"\bprompt_id=([A-Za-z0-9_-]+)", text)
        if submitted and ("已提交" in text or "隧道提交" in text):
            self._clear_task_prompt(item)
            item["prompt_id"] = submitted.group(1)
            COMFY_TRACKER.bind(item["prompt_id"])
            item["phase"] = "已提交工作流，等待模型执行"
            return
        if text.startswith("[colab-ai] 已回传至 Host 本地:"):
            item["output_received"] = True
            self._clear_task_prompt(item)
            item["phase"] = "结果已回传，等待主命令结束"
            return
        phase = None
        if "正在开启 Colab" in text:
            item["output_received"] = False
            item["warmup_ready"].clear()
            phase = "正在启动或重新连接实例（尚未就绪）"
        elif "正在预热模块:" in text:
            phase = "预热图像模型（尚未就绪）" if text.endswith("image ...") else "预热 LLM 模型（尚未就绪）"
        elif "正在拉取" in text or "极速拉取" in text or "正在下载" in text:
            phase = "下载模型或推理组件（总进度未知）"
        elif "安装" in text or "正在克隆" in text:
            phase = "安装运行环境与依赖"
        elif "正在后台启动" in text or "正在启动专职" in text:
            phase = "启动模型服务"
        elif "常驻进" in text or "全层加载" in text:
            phase = "加载模型至显存（尚未就绪）"
        elif "正在通过极速隧道调用" in text:
            phase = "提示词模型推理生成中（无 token 进度）"
        elif "全层常驻 GPU" in text or "引擎已就绪" in text:
            phase = "提示词模型推理生成中（无 token 进度）" if item["module"] == "llm" else "模型服务已就绪，等待预热完成确认"
        elif "已就绪:" in text or "服务已启动" in text:
            phase = "模型文件或服务已就绪，等待执行"
        elif "正在上传" in text:
            phase = "上传输入文件"
        elif "生成完成" in text:
            phase = "生成完成，等待结果回传"
        if phase:
            self._clear_task_prompt(item)
            item["phase"] = phase

    def _task_live_progress(self, item, now):
        phase = item.get("phase", self._initial_phase(item["module"]))
        result = {"percent": None, "eta_sec": None, "queue_eta_sec": None,
                  "indeterminate": True, "scope": "phase", "phase_text": phase,
                  "step_value": None, "step_max": None, "node_id": None,
                  "jid": item["jid"], "lane": item["lane"]}
        if item.get("stopped_by_user"):
            result["phase_text"] = "正在终止，等待主命令结束"
            return result
        pid = item.get("prompt_id")
        if not pid:
            return result
        snap = COMFY_TRACKER.get_snapshot(pid)
        if not snap or now - snap["last_update"] > COMFY_TRACKER.FRESH_SEC:
            result["phase_text"] = "等待当前工作流进度（连接中断或数据尚未更新）"
            return result
        nid = snap["node_id"]
        mod = item["module"]
        samplers = {
            "image": {"3": "构图 1/2", "22": "精绘 2/2"},
            "warmup": {"3": "图像预热采样（非整体预热进度）", "14": "视频预热采样（非整体预热进度）"},
            "edit": {"6": "原尺寸改图", "22": "可选同尺寸精修"},
            "video": {"14": "声画构图采样", "214": "2K 细节精修 2/2"},
        }.get(mod, {})
        nodes = {
            "image": {"8": "解码构图结果", "20": "调整精绘潜空间尺寸", "21": "编码精绘潜空间",
                      "23": "解码最终图像", "29": "保存图像，等待回传"},
            "edit": {"7": "解码改图结果", "20": "准备同尺寸精修", "21": "编码精修潜空间",
                     "23": "解码精修结果", "8": "保存改图结果，等待回传", "96": "恢复原图尺寸"},
            "video": {"104": "编码首帧与提示词", "180": "3D 潜空间神经超分", "105": "注入 2K 首帧锚点", "8": "解码高清视频帧", "23": "解码立体声音轨", "60": "合成有声视频", "61": "保存视频，等待回传"},
            "warmup": {"3": "执行图像预热工作流（尚未就绪）", "8": "解码预热样本", "14": "执行视频预热工作流（尚未就绪）", "29": "确认图像预热结果", "61": "确认视频预热结果"},
        }.get(mod, {})
        result["node_id"] = nid or None
        if snap["event"] in ("execution_error", "execution_interrupted"):
            result["phase_text"] = "工作流异常或中断，等待主命令处理"
        elif snap["event"] in ("execution_success", "execution_end"):
            result["phase_text"] = "工作流执行结束，等待结果确认与回传"
        elif nid in samplers:
            result["phase_text"] = samplers[nid] + " · 当前采样阶段"
            val, mx = snap["step_val"], snap["step_max"]
            if val is not None and mx is not None and mx > 0:
                result.update(percent=min(99, int(100 * val / mx)), indeterminate=False,
                              step_value=val, step_max=mx)
                result["phase_text"] += f" ({val:g}/{mx:g} 步)"
                # At least three actual increasing samples; no historical/global duration.
                samples = snap["samples"]
                if (len(samples) >= 3 and now - samples[-1][0] <= COMFY_TRACKER.ETA_FRESH_SEC
                        and val < mx and samples[-1][0] - samples[0][0] >= 0.5):
                    delta = samples[-1][1] - samples[0][1]
                    if delta > 0:
                        step_sec = (samples[-1][0] - samples[0][0]) / delta
                        remaining = (mx - val) * step_sec - (now - samples[-1][0])
                        if remaining > 0:
                            result["eta_sec"] = round(remaining, 1)
        else:
            result["phase_text"] = nodes.get(nid, "工作流准备与模型/提示词编码（无可测百分比）")
        return result

    def _compute_live_progress(self, now: float) -> dict:
        base = {"percent": None, "eta_sec": None, "queue_eta_sec": None,
                "indeterminate": False, "scope": "task", "step_value": None, "step_max": None}
        queued_n = sum(len(q) for q in self.queues.values())
        if not self.active_procs:
            if queued_n:
                return {**base, "indeterminate": True, "stage_text": f"等待任务开始 · 排队 {queued_n} 个（耗时未知）"}
            if self.recent_done:
                last = self.recent_done[0]
                complete = last.get("progress_complete", False) and last["status"] == "done"
                note = "" if complete or last["status"] != "done" else " · 完成/回传信号未确认"
                clean_last_st = re.sub(r"^[🟢⏳⏸️⏹️✅❌\ufe0f\s]+", "", last.get("status_text") or "").strip() or last.get("status_text", "")
                return {**base, "percent": 100 if complete else None,
                        "stage_text": f"最近任务: #{last['jid']} {last['label']} ({clean_last_st} · 耗时 {last['elapsed']}s){note}"}
            return {**base, "stage_text": "空闲待命 · 随时可提交任务"}
        # Preserve the existing primary media-first display; other lanes remain parallel.
        cur = min(self.active_procs.values(), key=lambda x: (0 if x["lane"] == "media" else 1, x["jid"]))
        result = self._task_live_progress(cur, now)
        result["stage_text"] = f"#{cur['jid']} {cur['label']} · {result['phase_text']}"
        return result

    def get_task_detail(self, jid: int):
        jid = int(jid)
        with self.lock:
            for st_name, src in (
                ("running", list(self.active_procs.values())),
                ("queued", [it for q in self.queues.values() for it in q]),
                ("paused", list(self.paused.values())),
                (None, list(self.recent_done)),
            ):
                for it in src:
                    if it.get("jid") == jid:
                        return {
                            "ok": True,
                            "jid": jid,
                            "state": st_name if st_name is not None else (it.get("status") or "done"),
                            "full_prompt": it.get("full_prompt") or it.get("preview") or "",
                            "cmd_s": it.get("cmd_s") or "",
                        }
        return {"ok": False, "message": f"未找到任务 #{jid}"}

    def get_state(self, limit_done: int = 80, slim: bool = True):
        with self.lock:
            now = time.time()
            active_n = len(self.active_procs)
            queued_n = sum(len(q) for q in self.queues.values())
            rand_busy, rand_text, rand_started = PANEL_RANDOM.snapshot() if "PANEL_RANDOM" in globals() else (False, "", 0.0)
            running = (active_n > 0 or queued_n > 0 or rand_busy)
            base_t0 = self.start_time or (rand_started if rand_busy else 0.0)
            elapsed = (now - base_t0) if (running and base_t0) else (self.end_time - self.start_time if self.start_time else 0.0)
            prog = self._compute_live_progress(now)
            if rand_busy and active_n == 0 and queued_n == 0:
                prog = {
                    "percent": None, "eta_sec": None, "queue_eta_sec": None,
                    "indeterminate": True, "scope": "task",
                    "step_value": None, "step_max": None,
                    "stage_text": rand_text,
                }

            task_rows = []
            for jid, it in sorted(self.active_procs.items()):
                task_rows.append({
                    "jid": jid,
                    "state": "running",
                    "module": it["module"],
                    "user_msg": it.get("user_msg") or "",
                    "sent_time": time.strftime("%H:%M", time.localtime(it.get("enqueued_at") or now)),
                    "elapsed_sec": round(max(0.0, now - it["t0"]), 1),
                    "badge": "运行中",
                    "label": it["label"],
                    "preview": it["preview"],
                    "full_prompt": it.get("full_prompt", ""),
                    "cmd_s": it.get("cmd_s", ""),
                    "time_info": f"已用 {round(now - it['t0'], 1)}s · {self._task_live_progress(it, now)['phase_text']}",
                    "can_stop": True,
                    "can_resume": False,
                    "can_delete": False,
                })
            for lane in ("media", "llm", "sys"):
                for idx, it in enumerate(self.queues[lane], 1):
                    task_rows.append({
                        "jid": it["jid"],
                        "state": "queued",
                        "module": it["module"],
                        "user_msg": it.get("user_msg") or "",
                        "sent_time": time.strftime("%H:%M", time.localtime(it.get("enqueued_at") or now)),
                        "elapsed_sec": 0.0,
                        "badge": f"排队 #{idx}",
                        "label": it["label"],
                        "preview": it["preview"],
                        "full_prompt": it.get("full_prompt", ""),
                        "cmd_s": it.get("cmd_s", ""),
                        "time_info": f"{lane} 通道前方 {idx - 1 + sum(v['lane'] == lane for v in self.active_procs.values())} 个 · 耗时未知",
                        "can_stop": True,
                        "can_resume": False,
                        "can_delete": False,
                    })
            paused_list = [v for k, v in self.paused.items() if k not in self.active_procs]
            done_list = list(self.recent_done)
            total_history_count = len(paused_list) + len(done_list)
            deletable_count = sum(1 for it in paused_list + done_list if self._can_delete_task(it["jid"]))
            if limit_done and limit_done > 0:
                history_slice = (paused_list + done_list)[:limit_done]
            else:
                history_slice = paused_list + done_list
            for idx_h, it in enumerate(history_slice):
                edit_disabled = _edit_disabled_request(it["module"], it["cmd_list"])
                model_removed = _removed_model_request(it["module"], it["cmd_list"])
                keep_full = (not slim) or (idx_h < 25)
                raw_badge = it.get("status_text") or ""
                clean_badge = re.sub(r"^[🟢⏳⏸️⏹️✅❌️\s]+", "", raw_badge).strip() or raw_badge
                task_rows.append({
                    "jid": it["jid"],
                    "state": it["status"],
                    "module": it.get("module", ""),
                    "badge": clean_badge,
                    "label": it["label"],
                    "preview": it["preview"],
                    "full_prompt": it.get("full_prompt", "") if keep_full else "",
                    "cmd_s": it.get("cmd_s", "") if keep_full else "",
                    "has_detail": True,
                    "time_info": f"{it['time_str']} · {it['elapsed']}s" if it.get("time_str") else f"{it['elapsed']}s",
                    "can_stop": False,
                    "can_resume": it["jid"] not in self.active_procs and it.get("resume_supported", True) and not edit_disabled and not model_removed,
                    "resume_disabled_reason": "改图暂未启用" if edit_disabled else ("模型已移除" if model_removed else ""),
                    "can_delete": self._can_delete_task(it["jid"]),
                })

            # v0.2.10 只读追加：任务对应输出文件名（仅 basename，无则为空串）
            _out_map = {
                it["jid"]: it for it in (
                    list(self.active_procs.values())
                    + list(self.paused.values())
                    + list(self.recent_done)
                )
            }
            for _row in task_rows:
                _it = _out_map.get(_row["jid"], {})
                _op = _it.get("output_path") or _it.get("output_image") or ""
                _row["output_name"] = os.path.basename(_op) if _op else ""
            return {
                "running": running,
                "active_count": active_n,
                "queued_count": queued_n,
                "total_history_count": total_history_count,
                "deletable_count": deletable_count,
                "completed_seq": self.completed_seq,
                "start_pending": self._has_start_pending(),
                "stop_pending": any(
                    self._is_stop_job(it["module"], it["cmd_list"])
                    for it in list(self.active_procs.values()) + [it for q in self.queues.values() for it in q]
                ),
                "warmup_pending": any(
                    self._is_warmup_job(it["module"], it["cmd_list"])
                    for it in list(self.active_procs.values()) + [it for q in self.queues.values() for it in q]
                ),
                "start_completed_jid": next((
                    it["jid"] for it in self.recent_done
                    if self._is_start_job(it["module"], it["cmd_list"])
                ), 0),
                "stop_completed_jid": next((
                    it["jid"] for it in self.recent_done
                    if self._is_stop_job(it["module"], it["cmd_list"])
                ), 0),
                "warmup_completed_jid": next((
                    it["jid"] for it in self.recent_done
                    if self._is_warmup_job(it["module"], it["cmd_list"])
                ), 0),
                "cmd": self.cmd_str,
                "module": self.module,
                "elapsed": round(max(0.0, elapsed), 1),
                "exit_code": self.exit_code,
                "progress": prog,
                "tasks": task_rows,
                "paused_count": len(self.paused),
                "log_text": "".join(self.logs[-400:]),
            }

JOB = JobManager()


class PanelRandomOrchestrator:
    """随机摄影灵感：就绪的 LLM 优先扩写，否则使用本地完整提示词。"""
    SEEDS = (
        ('城市霓虹街拍', '雨后城市街道，霓虹灯映在湿润路面，身穿深色风衣的行人走过玻璃橱窗，橙蓝冷暖光交错'),
        ('海边日落逆光', '日落时分的海岸，金色逆光穿过浪花，浅色亚麻外套随海风轻摆，地平线平静开阔'),
        ('森林晨雾胶片', '清晨森林小径，薄雾与斜射的阳光分割空间，背包旅行者停在蕨类植物旁，树叶露珠晶莹'),
        ('极简建筑光影', '白色极简建筑中庭，午后阳光投下几何形状的阴影，单人剪影沿楼梯向上，石材肌理清晰'),
        ('赛博朋克雨夜', '未来城市雨夜，高架桥下电车驶过，青蓝光与暖红灯牌照亮街边的透明雨伞，水面倒影层次丰富'),
        ('古典油画静物', '深色木桌上的陶罐与梨子，窗边柔光从左侧落下，粗麻桌布褶皱自然，背景低调温暖'),
        ('高山蓝调时刻', '山脊上的蓝调黎明，远处雪峰映着淡紫天光，登山者站在岩石边缘，云海柔和翻涌'),
        ('咖啡馆窗边', '咖啡馆窗边的阅读者身穿针织衫，暖阳穿过纱帘，咖啡杯蒸汽缓缓上升，木桌纹理柔润'),
        ('沙漠旅途', '沙漠夕阳下的蜿蜒沙丘，旅行者穿着轻便长袖外套行走，风吹出细小沙纹，天空呈浅金色'),
        ('雪地木屋', '雪后松林中的木屋，暖色窗光照亮新雪，红色围巾的旅人沿脚印前行，远景静谧清澈'),
        ('花园午后', '春日花园里一位穿棉质衬衫的园艺师整理花盆，淡粉花瓣散落石径，柔和侧光勾勒叶片边缘'),
        ('海港纪实', '清晨海港码头，工装渔民整理绳索，海鸥掠过蓝灰色海面，旧船油漆与缆绳纤维真实可辨'),
        ('剧场肖像', '空旷剧场的一束顶光照亮穿深色礼服的表演者，丝绒幕布形成深红背景，神态专注自然'),
        ('雨窗静物', '窗前透明玻璃杯与一本翻开的书，雨滴沿玻璃滑落，远处城市光斑柔和虚化，灰蓝色调安静克制'),
        ('田野丰收', '秋日田野中农人穿着帆布夹克收集麦穗，金色夕阳照亮稻草，远处丘陵层次渐淡'),
        ('太空建筑概念', '月球基地的极简观察舱，宇航员身穿完整白色工作服望向远处星球，金属表面反射冷光，构图庄重'),
    )

    def __init__(self):
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.queue = collections.deque()
        self.active_stage = ''
        self.active_t0 = 0.0
        self.cancel_epoch = 0
        threading.Thread(target=self._worker, daemon=True).start()

    def enqueue(self, count=1, step_tier='tier2', idle_minutes=PANEL_IDLE_MIN):
        import random
        count = max(1, min(10, int(count)))
        tier = step_tier if step_tier in ('tier1', 'tier2', 'tier3') else 'tier2'
        with self.cond:
            ahead = len(self.queue) + bool(self.active_stage)
            for title, scene in random.sample(self.SEEDS, count):
                self.queue.append((self.cancel_epoch, title, scene, tier, int(idle_minutes)))
            self.cond.notify_all()
            return int(ahead), count

    def cancel_all(self):
        with self.cond:
            had = bool(self.queue or self.active_stage)
            self.cancel_epoch += 1
            self.queue.clear()
            self.active_stage = ''
            self.active_t0 = 0.0
            self.cond.notify_all()
            return had

    def snapshot(self):
        with self.lock:
            return bool(self.active_stage or self.queue), self.active_stage + (f'（待构思 {len(self.queue)} 张）' if self.queue else ''), self.active_t0

    def _wait(self, jid, epoch):
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            with self.lock:
                if epoch != self.cancel_epoch: return False
            with JOB.lock:
                done = next((it for it in JOB.recent_done if it['jid'] == jid), None)
                if done: return done.get('status') == 'done'
                if jid in JOB.paused: return False
            time.sleep(0.3)
        return False

    def _worker(self):
        while True:
            with self.cond:
                while not self.queue:
                    self.active_stage = ''; self.active_t0 = 0.0
                    self.cond.wait()
                item = self.queue.popleft()
                self.active_stage = '随机灵感 · ' + item[1]
                self.active_t0 = time.time()
            try:
                self._run_one(*item)
            except Exception as exc:
                with JOB.lock: JOB.logs.append(f'[随机灵感] {exc}\n')

    def _run_one(self, epoch, title, scene, tier, idle_min):
        os.makedirs(STATE_DIR, mode=0o700, exist_ok=True)
        prompt = ('电影感写实摄影，采用三分法构图，主体与环境形成清晰的前中后景层次。' + scene +
                  '。以50毫米镜头平视拍摄，浅景深自然虚化远景，光线过渡细腻，保留服装与器物表面真实纹理。色彩克制协调，画面细节清晰，主体结构准确，呈现富有叙事感的专业摄影作品。')
        suffix = str(time.time_ns()) + '_' + os.urandom(4).hex()
        req = os.path.join(STATE_DIR, 'llm_req_' + suffix + '.json')
        out = os.path.join(STATE_DIR, 'random_llm_' + suffix + '.txt')
        try:
            # 不因未加载模型而阻塞随机按钮；完整内置提示词始终可用。
            try:
                port = int(os.environ.get('COLABBB_LLM_TUNNEL_PORT', '28189'))
                with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=1) as response:
                    health = json.load(response)
                ready = bool(health.get('ok') and health.get('loaded'))
            except Exception:
                ready = False
            if ready:
                with open(req, 'w', encoding='utf-8') as fh:
                    json.dump([{'role': 'user', 'content': '请扩写为专业摄影提示词：' + scene}], fh, ensure_ascii=False)
                ok, message = JOB.start_job([COLAB_AI_BIN, 'llm', '--model', 'qwen27b', '--messages-file', req, '--out-file', out], 'llm', meta_label='随机构思 · ' + title, idle_minutes=idle_min)
                found = re.search(r'#(\d+)', message)
                if ok and found and self._wait(int(found.group(1)), epoch) and os.path.isfile(out):
                    raw = open(out, encoding='utf-8').read()
                    match = re.search(r'<提示词>(.*?)</提示词>', raw, re.S)
                    candidate = match.group(1).strip() if match else ''
                    if len(candidate) >= 60: prompt = candidate
            with self.lock:
                if epoch != self.cancel_epoch: return
                cfg = load_studio_settings()['image_workflow']
                dims = cfg['resolutions']['portrait']; steps = cfg['step_tiers'][tier]
                total = steps['s1'] + (steps['ref'] if cfg.get('enable_hires') else 0)
                pfile = os.path.join(STATE_DIR, 'img_req_prompt_' + suffix + '.txt')
                with open(pfile, 'w', encoding='utf-8') as fh: fh.write(prompt)
                JOB.start_job([COLAB_AI_BIN, 'image', '--prompt-file', pfile, '--model', 'v5_int8', '--width', str(dims['s2_w']), '--height', str(dims['s2_h']), '--steps', str(total), '--res-mode', 'portrait', '--step-tier', tier], 'image', meta_label='随机3K · ' + title, steps=total, prompt_preview=prompt, idle_minutes=idle_min)
        finally:
            # req 可能仍由超时的队列任务使用，交由已有的过期清理器回收。
            if os.path.isfile(out): os.unlink(out)



PANEL_RANDOM = PanelRandomOrchestrator()

def has_colab_instance():
    """Query server assignments without syncing state, creating or trusting caches."""
    query = (
        "import json, sys\n"
        f"sys.path.insert(0, {BASE_DIR!r})\n"
        "import colab_accounts\n"
        "from colab_startup import _creds_from_client, list_effective_assignments, verified_client\n"
        "try:\n"
        "    cl, acct = verified_client()\n"
        "    qp = colab_accounts.resolve_quota_project(acct['email'], _creds_from_client(cl))\n"
        "    assignments = list_effective_assignments(cl, qp)\n"
        "    if not isinstance(assignments, list): raise RuntimeError('Invalid assignments')\n"
        "    print(json.dumps({'active': bool(assignments)}))\n"
        "except (Exception, SystemExit):\n"
        "    raise SystemExit(1)\n"
    )
    # Use the already installed Colab CLI interpreter, not panel's stdlib Python.
    with open(COLAB_BIN, "r", encoding="utf-8") as fh:
        shebang = fh.readline().strip()
    if not shebang.startswith("#!"):
        raise RuntimeError("检查实例状态失败")
    interpreter = shlex.split(shebang[2:])
    if not interpreter:
        raise RuntimeError("检查实例状态失败")
    result = subprocess.run(interpreter + ["-c", query], capture_output=True, text=True, timeout=15)
    # Never expose client stdout/stderr, auth details or exception contents.
    if result.returncode != 0:
        raise RuntimeError("检查实例状态失败")
    data = json.loads(result.stdout)
    if not isinstance(data, dict) or type(data.get("active")) is not bool:
        raise RuntimeError("检查实例状态失败")
    return data["active"]


_STATUS_CACHE = {"ts": 0.0, "reg_mtime": 0.0, "data": None}
_STATUS_LOCK = threading.Lock()
STATUS_TTL = 30.0
STATUS_FAIL_TTL = 5.0

def get_colab_status(force: bool = False):
    # One authoritative billing read provides balance, hourly rate and active count.
    # Never infer power/billing from a live tunnel or extend stale data with it.
    cur_reg_mtime = colab_accounts._registry_mtime()
    with _STATUS_LOCK:
        now = time.time()
        cached_ttl = STATUS_TTL if (_STATUS_CACHE["data"] and _STATUS_CACHE["data"].get("active") is not None) else STATUS_FAIL_TTL
        if (
            not force
            and _STATUS_CACHE["data"] is not None
            and _STATUS_CACHE.get("reg_mtime") == cur_reg_mtime
            and (now - _STATUS_CACHE["ts"] < cached_ttl)
        ):
            return _STATUS_CACHE["data"]
        data = {"active": None, "sessions_raw": "状态检查失败", "balance": "未知", "rate": "未知"}
        query = (
            "import json\n"
            "from colab_cli.common import state\n"
            "try:\n"
            "    info = state.client.get_consumption_user_info()\n"
            "    print(json.dumps({'balance': info.paid_compute_units_balance, 'rate': info.consumption_rate_hourly, 'count': info.assignments_count}, allow_nan=False))\n"
            "except (Exception, SystemExit):\n"
            "    raise SystemExit(1)\n"
        )
        try:
            import math
            with open(COLAB_BIN, "r", encoding="utf-8") as fh:
                shebang = fh.readline().strip()
            if not shebang.startswith("#!"):
                raise ValueError("invalid interpreter")
            interpreter = shlex.split(shebang[2:])
            if not interpreter:
                raise ValueError("missing interpreter")
            result = subprocess.run(interpreter + ["-c", query], capture_output=True, text=True, timeout=15)
            if result.returncode != 0:
                raise ValueError("status unavailable")
            info = json.loads(result.stdout)
            balance, rate, count = info["balance"], info["rate"], info["count"]
            if (type(balance) not in (int, float) or not math.isfinite(balance)
                    or type(rate) not in (int, float) or not math.isfinite(rate) or rate < 0
                    or type(count) is not int or count < 0):
                raise ValueError("invalid status")
            data = {
                "active": count > 0,
                "sessions_raw": f"Active assignments: {count}",
                "balance": f"{balance:.2f}",
                "rate": f"{rate:.2f}/hr",
            }
        except Exception:
            # Keep unknowns; do not expose captured output, credentials or errors.
            pass
        try:
            acct_info = colab_accounts.get_active_account_summary()
            data.update({
                "email": acct_info.get("email") or "",
                "local_name": acct_info.get("local_name") or "",
                "short_label": acct_info.get("short_label") or "",
                "account_count": acct_info.get("account_count") or 0,
            })
        except Exception:
            pass
        _STATUS_CACHE["ts"] = time.time()
        _STATUS_CACHE["reg_mtime"] = colab_accounts._registry_mtime()
        _STATUS_CACHE["data"] = data
        return data

THUMB_ROOT = os.path.join(STATE_DIR, "thumbs")
META_CACHE_FILE = os.path.join(STATE_DIR, "png_meta_cache.json")
_PNG_META_LOCK = threading.Lock()
_PNG_META_CACHE = {}
_PNG_META_DIRTY = False

def _load_png_meta_cache():
    global _PNG_META_CACHE
    try:
        if os.path.isfile(META_CACHE_FILE):
            with open(META_CACHE_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                _PNG_META_CACHE = data
    except Exception:
        _PNG_META_CACHE = {}

def _save_png_meta_cache_if_dirty():
    global _PNG_META_DIRTY
    with _PNG_META_LOCK:
        if not _PNG_META_DIRTY:
            return
        try:
            _atomic_write_text(META_CACHE_FILE, json.dumps(_PNG_META_CACHE, ensure_ascii=False))
            _PNG_META_DIRTY = False
        except Exception:
            pass

_load_png_meta_cache()

def extract_png_comfy_info(fpath: str) -> dict:
    global _PNG_META_DIRTY
    try:
        st = os.stat(fpath)
        mtime = round(st.st_mtime, 3)
        fsize = st.st_size
        with _PNG_META_LOCK:
            cached = _PNG_META_CACHE.get(fpath)
            if isinstance(cached, dict) and cached.get("mtime") == mtime and cached.get("size") == fsize and isinstance(cached.get("info"), dict):
                return cached["info"]
        info = {"model_label": "未知模型", "model_id": "unknown", "steps": 0, "res": "", "prompt": ""}
        unets = set()
        has_22 = False
        with open(fpath, "rb") as fh:
            if fh.read(8) == b"\x89PNG\r\n\x1a\n":
                while True:
                    hdr = fh.read(8)
                    if len(hdr) < 8:
                        break
                    length, ctype = struct.unpack(">I4s", hdr)
                    if ctype == b"IHDR" and length >= 8:
                        data = fh.read(length)
                        fh.seek(4, 1)
                        pw, ph = struct.unpack(">II", data[:8])
                        info["res"] = f"{pw}x{ph}"
                    elif ctype == b"tEXt" and 7 < length < 524288:
                        data = fh.read(length)
                        fh.seek(4, 1)
                        if data.startswith(b"prompt\x00"):
                            wf = json.loads(data[7:].decode("utf-8", errors="ignore"))
                            for node in wf.values():
                                if not isinstance(node, dict):
                                    continue
                                ct = node.get("class_type", "")
                                inp = node.get("inputs", {})
                                if ct == "UNETLoader":
                                    unets.add(inp.get("unet_name", ""))
                                elif ct == "KSampler":
                                    if inp.get("steps"):
                                        info["steps"] = info.get("steps", 0) + int(inp["steps"])
                                elif ct == "Krea2EditGroundedEncode":
                                    txt = (inp.get("prompt") or "").strip()
                                    if txt:
                                        info["prompt"] = txt
                                        info["model_id"] = "krea2edit"
                                elif ct == "TextEncodeQwenImage21":
                                    txt = (inp.get("prompt") or "").strip()
                                    if txt:
                                        info["prompt"] = txt
                                        info["model_id"] = "krea2edit"
                                elif ct in ("CLIPTextEncode", "TextEncodeQwenImageEditPlus"):
                                    txt = (inp.get("text") or inp.get("prompt") or "").strip()
                                    if txt and not info["prompt"]:
                                        info["prompt"] = txt
                            has_22 = "22" in wf
                            break
                    elif ctype in (b"IDAT", b"IEND"):
                        # tEXt in ComfyUI PNGs always precedes IDAT; stop immediately at IDAT/IEND without reading 7MB pixel data.
                        break
                    else:
                        fh.seek(length + 4, 1)
        has_legacyedit = any("NoctQ" in u for u in unets)
        has_v5_int8 = "Flux / DiT_V5_int8_convrot.safetensors" in unets
        has_v5_bf16 = any("V5_bf16" in u for u in unets)
        if info["model_id"] == "krea2edit" and not has_legacyedit:
            if has_22:
                info["model_label"] = "Flux / DiT V5 改图+3K"
                info["model_id"] = "krea2edit3k"
            else:
                info["model_label"] = "Flux / DiT V5 命令改图"
        elif has_legacyedit:
            info["model_label"] = "旧 Noct-Q 改图"
        elif "Flux / DiT_V6B_int8_convrot.safetensors" in unets:
            info["model_label"], info["model_id"] = "Flux / DiT V6B INT8", "v6b_int8_convrot"
        elif "Flux / DiT_V6_int8_convrot.safetensors" in unets:
            info["model_label"], info["model_id"] = "Flux / DiT V6 INT8", "v6_int8"
        elif has_v5_bf16:
            info["model_label"], info["model_id"] = "Flux / DiT V5 BF16", "v5_bf16"
        elif has_v5_int8:
            info["model_label"], info["model_id"] = "Flux / DiT V5 INT8", "v5_int8"
        elif "Flux / DiT_V4_bf16.safetensors" in unets:
            info["model_label"], info["model_id"] = "Flux / DiT V4 满血", "v4_bf16"
        elif "Flux / DiT_V4_int8_convrot.safetensors" in unets:
            info["model_label"], info["model_id"] = "Flux / DiT V4 INT8", "v4_int8"
        with _PNG_META_LOCK:
            _PNG_META_CACHE[fpath] = {"mtime": mtime, "size": fsize, "info": info}
            _PNG_META_DIRTY = True
        return info
    except Exception:
        return {"model_label": "未知模型", "model_id": "unknown", "steps": 0, "res": "", "prompt": ""}

_THUMB_GEN_LOCK = threading.Lock()
_THUMB_INFLIGHT = set()
_THUMB_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="thumb")

def ensure_webp_thumb(mod: str, fname: str, sync: bool = False) -> str:
    if mod not in ("image", "upload_image"):
        return ""
    src = os.path.join(OUT_ROOT, mod, fname)
    if not os.path.isfile(src):
        return ""
    dst_dir = os.path.join(THUMB_ROOT, mod)
    dst = os.path.join(dst_dir, fname + ".webp")
    try:
        st_s = os.stat(src)
        if os.path.isfile(dst):
            st_d = os.stat(dst)
            if st_d.st_size > 100 and abs(st_d.st_mtime - st_s.st_mtime) < 2.0:
                return dst
    except Exception:
        return ""
    if not sync:
        key = (mod, fname)
        with _THUMB_GEN_LOCK:
            if key not in _THUMB_INFLIGHT:
                _THUMB_INFLIGHT.add(key)
                _THUMB_EXECUTOR.submit(_bg_gen_thumb, mod, fname)
        return dst if os.path.isfile(dst) else ""
    return _do_gen_thumb(mod, fname, src, dst_dir, dst)

def _bg_gen_thumb(mod: str, fname: str):
    try:
        src = os.path.join(OUT_ROOT, mod, fname)
        dst_dir = os.path.join(THUMB_ROOT, mod)
        dst = os.path.join(dst_dir, fname + ".webp")
        _do_gen_thumb(mod, fname, src, dst_dir, dst)
    finally:
        with _THUMB_GEN_LOCK:
            _THUMB_INFLIGHT.discard((mod, fname))

def _do_gen_thumb(mod: str, fname: str, src: str, dst_dir: str, dst: str) -> str:
    try:
        os.makedirs(dst_dir, exist_ok=True)
        st_s = os.stat(src)
        tmp = os.path.join(dst_dir, f".tmp.{os.getpid()}.{threading.get_ident()}.{fname}.webp")
        res = subprocess.run([
            "ffmpeg", "-y", "-v", "error", "-i", src,
            "-vf", "scale='min(640,iw)':-2:flags=lanczos",
            "-frames:v", "1", "-f", "webp", "-c:v", "libwebp", "-quality", "82", tmp
        ], timeout=25)
        if res.returncode == 0 and os.path.isfile(tmp) and os.path.getsize(tmp) > 100:
            os.utime(tmp, (st_s.st_atime, st_s.st_mtime))
            os.replace(tmp, dst)
            return dst
        if os.path.exists(tmp):
            os.unlink(tmp)
    except Exception:
        pass
    return ""

def cleanup_stale_temp_files():
    """Clean up old ephemeral request files in state/ older than 6 hours that are not referenced by paused/active/queued tasks."""
    try:
        now = time.time()
        referenced = set()
        with JOB.lock:
            for src in (list(JOB.active_procs.values()),
                        [it for q in JOB.queues.values() for it in q],
                        list(JOB.paused.values())):
                for it in src:
                    for arg in it.get("cmd_list") or ():
                        if isinstance(arg, str) and arg.startswith(STATE_DIR):
                            referenced.add(os.path.abspath(arg))
        for fn in os.listdir(STATE_DIR):
            if not (fn.startswith(("img_req_prompt_", "edit_req_prompt_", "vid_req_prompt_", "llm_req_", "llm_sys_"))
                    or (fn.startswith("idle-") and ".watch-" in fn and fn.endswith(".lock"))):
                continue
            if fn in ("img_req_prompt.txt", "llm_req_messages.json"):
                continue
            fp = os.path.abspath(os.path.join(STATE_DIR, fn))
            if fp in referenced:
                continue
            try:
                st = os.stat(fp)
                if now - st.st_mtime > 21600:
                    os.unlink(fp)
            except OSError:
                pass
    except Exception:
        pass

def _cleanup_loop():
    while True:
        cleanup_stale_temp_files()
        time.sleep(3600)

threading.Thread(target=_cleanup_loop, daemon=True).start()


METRICS_CACHE_LOCK = threading.Lock()
METRICS_CACHE = {"ts": 0.0, "data": None}
METRICS_BOOT_LOCK = threading.Lock()
LAST_METRICS_BOOT_TRY = 0.0


def _ensure_remote_metrics_async():
    global LAST_METRICS_BOOT_TRY
    now = time.time()
    with METRICS_BOOT_LOCK:
        if now - LAST_METRICS_BOOT_TRY < 45.0:
            return
        LAST_METRICS_BOOT_TRY = now
    mp = os.path.join(BASE_DIR, "remote_metrics.py")
    if os.path.isfile(mp):
        try:
            subprocess.Popen(
                ["/usr/local/bin/colab", "exec", "-s", os.environ.get("COLAB_SESSION", "ai"), "--timeout", "20", "-f", mp],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
            )
        except Exception:
            pass


def read_g4_hw_metrics() -> dict:
    now = time.time()
    with METRICS_CACHE_LOCK:
        if METRICS_CACHE["data"] is not None and (now - METRICS_CACHE["ts"]) < 2.0:
            return METRICS_CACHE["data"]

    res = None
    # Primary: dedicated 28190 G4 hardware probe (<15ms, full CPU + RAM + nvidia-smi VRAM + Disk)
    try:
        raw = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{METRICS_TUNNEL_PORT}/metrics", timeout=1.2).read().decode("utf-8"))
        if raw.get("ok") and raw.get("ram_total_gb"):
            res = {"ok": True, "online": True, **raw}
    except Exception:
        pass

    # Fallback while 28190 is spinning up: read RAM & VRAM from 28188 ComfyUI /system_stats and trigger async 28190 start
    if res is None:
        try:
            st = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{COMFY_TUNNEL_PORT}/system_stats", timeout=1.2).read().decode("utf-8"))
            sys_d = st.get("system") or {}
            devs = st.get("devices") or [{}]
            d0 = devs[0] if devs else {}
            rt = float(sys_d.get("ram_total") or 0) / (1024.0 ** 3)
            rf = float(sys_d.get("ram_free") or 0) / (1024.0 ** 3)
            ru = max(0.0, rt - rf)
            vt = float(d0.get("vram_total") or 0) / (1024.0 ** 3)
            vf = float(d0.get("vram_free") or 0) / (1024.0 ** 3)
            vu = max(0.0, vt - vf)
            _ensure_remote_metrics_async()
            res = {
                "ok": True,
                "online": True,
                "partial": True,
                "cpu_pct": None,
                "cpu_cores": None,
                "ram_used_gb": round(ru, 1),
                "ram_total_gb": round(rt, 1),
                "ram_pct": round(100.0 * ru / rt, 1) if rt > 0 else 0.0,
                "vram_used_gb": round(vu, 1),
                "vram_total_gb": round(vt, 1),
                "vram_pct": round(100.0 * vu / vt, 1) if vt > 0 else 0.0,
                "disk_used_gb": None,
                "disk_total_gb": None,
                "disk_pct": None,
            }
        except Exception:
            # Check cached status: if instance is known active but no tunnel yet, spin up 28190 async
            if (_STATUS_CACHE.get("data") or {}).get("active") is True:
                _ensure_remote_metrics_async()
            res = {"ok": True, "online": False}

    with METRICS_CACHE_LOCK:
        METRICS_CACHE["ts"] = time.time()
        METRICS_CACHE["data"] = res
    return res


def list_outputs():
    result = {
        "image": [],
        "video": [],
        "upload_image": [],
        "llm": [],
        "counts": {"image": 0, "video": 0, "upload_image": 0, "llm": 0, "total": 0},
    }
    for mod in ("image", "video", "upload_image", "llm"):
        mod_dir = os.path.join(OUT_ROOT, mod)
        if not os.path.isdir(mod_dir):
            continue
        for fname in os.listdir(mod_dir):
            fpath = os.path.join(mod_dir, fname)
            if fname.startswith("."):
                continue
            low = fname.lower()
            target_mod = mod
            if mod == "upload_image":
                if low.endswith((".mp4", ".webm", ".mov")):
                    target_mod = "video"
                elif not low.endswith((".png", ".jpg", ".jpeg", ".webp")):
                    continue
            elif mod == "image" and not low.endswith((".png", ".jpg", ".jpeg", ".webp")):
                continue
            elif mod == "video" and not low.endswith((".mp4", ".webm", ".mov")):
                continue
            elif mod == "llm" and not low.endswith((".md", ".json", ".txt")):
                continue

            try:
                st = os.stat(fpath)
            except OSError:
                continue
            if not S_ISREG(st.st_mode):
                continue

            if target_mod == "llm" and st.st_size <= 10:
                try:
                    raw_tiny = open(fpath, "r", encoding="utf-8", errors="ignore").read().strip()
                    if raw_tiny in ("", "..."):
                        continue
                except Exception:
                    continue

            qname = urllib.parse.quote(fname)
            entry = {
                "name": fname,
                "source_mod": mod,
                "path": fpath,
                "url": f"/files/{mod}/{qname}",
                "size_kb": round(st.st_size / 1024.0, 1),
                "mtime": st.st_mtime,
                "time_str": time.strftime("%m-%d %H:%M", time.localtime(st.st_mtime)),
            }
            if target_mod in ("image", "upload_image"):
                entry["thumb_url"] = f"/thumbs/{mod}/{qname}?v={int(st.st_mtime)}"
                ensure_webp_thumb(mod, fname, sync=False)
                if mod == "image" and low.endswith(".png"):
                    entry.update(extract_png_comfy_info(fpath))
                else:
                    entry["model_label"] = "上传素材"
                    entry["model_id"] = "upload"
            elif target_mod == "video":
                is_legacy_wan = low.startswith("wan22")
                entry["model_label"] = "上传视频" if mod == "upload_image" else ("Wan 2.2 14B HD" if is_legacy_wan else "24fps 声画视频 HD")
                entry["model_id"] = "upload_video" if mod == "upload_image" else ("wan22" if is_legacy_wan else "h3")
            elif target_mod == "llm":
                if st.st_size < 500_000:
                    try:
                        with open(fpath, "r", encoding="utf-8", errors="ignore") as fh:
                            entry["preview_text"] = fh.read(6000)
                    except Exception:
                        pass
            result[target_mod].append(entry)

    _save_png_meta_cache_if_dirty()
    total = 0
    for mod in ("image", "video", "upload_image", "llm"):
        result[mod].sort(key=lambda x: x["mtime"], reverse=True)
        cnt = len(result[mod])
        result["counts"][mod] = cnt
        total += cnt
    result["counts"]["total"] = total
    return result


# ==================== 可选：Google Drive 加密云盘与 Telegram 频道同步控制 ====================
_DRIVE_SYNC_LOCK = threading.Lock()
_DRIVE_SYNC_TASK = {
    "running": False,
    "mode": "",
    "stage": "",
    "progress": "",
    "started_at": 0.0,
    "finished_at": 0.0,
    "last_ok": None,
    "last_message": "",
    "last_summary": "",
}


def get_drive_sync_overview() -> dict:
    if not is_drive_sync_enabled():
        return {"ok": True, "enabled": False, "channel_enabled": False, "task": {"running": False}}

    from drive_sync_store import apply_deletions as ds_apply_deletions, config as ds_config, ensure_initialized_from_env, gate as ds_gate, root_dir as ds_root_dir
    from drive_sync import discover as ds_discover
    import tg_channel_sync as tcs

    try:
        ensure_initialized_from_env()
        ds_discover()
    except Exception:
        pass

    status_doc = {}
    status_file = ds_root_dir() / "status.json"
    try:
        if status_file.is_file():
            status_doc = json.loads(status_file.read_text(encoding="utf-8"))
    except Exception:
        status_doc = {}

    drive_uploaded = int(status_doc.get("uploaded") or 0)
    pending_upload = int(status_doc.get("pending_upload") or 0)
    pending_delete = int(status_doc.get("pending_delete") or 0)
    trashed_cnt = int(status_doc.get("trashed") or 0)
    err_cnt = int(status_doc.get("errors") or 0)
    folder_url = str(status_doc.get("folder_url") or "")
    updated_at = float(status_doc.get("updated_at") or 0.0)
    channel_synced = 0
    drive_email = ""
    ch_enabled = is_channel_sync_enabled()
    _, ch_id, ch_label = tcs.get_channel_config()

    try:
        cfg_d = ds_config()
        drive_email = str(cfg_d.get("account_email") or "")
    except Exception:
        pass
    if not drive_email:
        try:
            drive_email = colab_accounts.load_accounts_doc().get("drive_sync_email") or ""
        except Exception:
            pass

    try:
        tcs.ensure_table()
        with ds_gate() as c:
            ds_apply_deletions(c)
            vals = c.execute(
                "SELECT COALESCE(SUM(uploaded=1 AND delete_requested=0 AND trashed=0),0), "
                "COALESCE(SUM(uploaded=0 AND delete_requested=0 AND trashed=0),0), "
                "COALESCE(SUM(delete_requested=1 AND trashed=0),0), "
                "COALESCE(SUM(trashed=1),0), "
                "COALESCE(SUM(last_error!='' AND trashed=0),0) FROM files"
            ).fetchone()
            if vals:
                drive_uploaded, pending_upload, pending_delete, trashed_cnt, err_cnt = [int(x or 0) for x in vals]
            if not folder_url:
                frow = c.execute("SELECT value FROM meta WHERE key='folder_id'").fetchone()
                if frow and frow[0]:
                    folder_url = "https://drive.google.com/drive/folders/" + str(frow[0])
            if ch_id:
                crow = c.execute(
                    "SELECT COUNT(*) FROM tg_channel_posts WHERE channel_id=? AND deleted_from_tg=0",
                    (ch_id,),
                ).fetchone()
                if crow:
                    channel_synced = int(crow[0] or 0)
    except Exception:
        pass

    with _DRIVE_SYNC_LOCK:
        task_snap = dict(_DRIVE_SYNC_TASK)

    return {
        "ok": True,
        "enabled": True,
        "channel_enabled": ch_enabled,
        "account_email": drive_email,
        "encryption": str(status_doc.get("encryption") or "aes256gcm_v1"),
        "folder_url": folder_url,
        "updated_at": updated_at,
        "updated_str": time.strftime("%m-%d %H:%M:%S", time.localtime(updated_at)) if updated_at > 0 else "",
        "drive_uploaded": drive_uploaded,
        "pending_upload": pending_upload,
        "pending_delete": pending_delete,
        "trashed": trashed_cnt,
        "errors": err_cnt,
        "channel_username": ch_label if ch_enabled else "",
        "channel_synced": channel_synced,
        "task": task_snap,
    }


def _run_drive_sync_worker(mode: str):
    import drive_sync as ds
    import tg_channel_sync as tcs

    def _on_prog(text: str, meta: dict):
        first_line = (text or "").strip().splitlines()[0] if text else ""
        with _DRIVE_SYNC_LOCK:
            _DRIVE_SYNC_TASK["stage"] = str((meta or {}).get("stage") or "working")
            _DRIVE_SYNC_TASK["progress"] = first_line[:160]

    t0 = time.time()
    try:
        if mode == "drive":
            with _DRIVE_SYNC_LOCK:
                _DRIVE_SYNC_TASK["stage"] = "connecting"
                _DRIVE_SYNC_TASK["progress"] = "正在连接 Google Drive 校验加密库..."
            cloud = ds.Cloud()
            folder_id = cloud.folder()
            with _DRIVE_SYNC_LOCK:
                _DRIVE_SYNC_TASK["stage"] = "syncing"
                _DRIVE_SYNC_TASK["progress"] = "正在加密上传新增图片并同步删除项..."
            res = ds.sync_once(cloud, folder_id, force_retry=True)
            el_s = round(time.time() - t0, 1)
            up_n = int(res.get("uploaded_now") or 0)
            tr_n = int(res.get("trashed_now") or 0)
            fl_n = int(res.get("failed_now") or 0)
            st_d = res.get("status") or {}
            tot_u = int(st_d.get("uploaded") or 0)
            if up_n == 0 and tr_n == 0 and fl_n == 0:
                msg = f"谷歌云加密库已是最新（共 {tot_u} 张）"
            else:
                parts = []
                if up_n:
                    parts.append(f"新增上传 {up_n} 张")
                if tr_n:
                    parts.append(f"移入回收站 {tr_n} 张")
                if fl_n:
                    parts.append(f"失败 {fl_n} 张")
                msg = f"谷歌云同步完成（{'，'.join(parts)} · 共 {tot_u} 张 · {el_s}s）"
            with _DRIVE_SYNC_LOCK:
                _DRIVE_SYNC_TASK.update({
                    "running": False,
                    "stage": "done",
                    "progress": "",
                    "finished_at": time.time(),
                    "last_ok": fl_n == 0,
                    "last_message": msg,
                    "last_summary": msg,
                })
        else:
            res = tcs.run_sync(0, 0, progress_cb=_on_prog)
            if not res.get("ok"):
                busy_msg = res.get("message") or ("频道同步正在进行中，请稍候" if res.get("busy") else "频道同步未完成")
                with _DRIVE_SYNC_LOCK:
                    _DRIVE_SYNC_TASK.update({
                        "running": False,
                        "stage": "error",
                        "progress": "",
                        "finished_at": time.time(),
                        "last_ok": False,
                        "last_message": busy_msg,
                        "last_summary": busy_msg,
                    })
                return
            p_n = int(res.get("posted") or 0)
            d_n = int(res.get("deleted") or 0)
            f_n = int(res.get("failed") or 0)
            ch_tot = int(res.get("channel_total") or 0)
            ch_name = res.get("channel_label") or "TG 频道"
            if p_n == 0 and d_n == 0 and f_n == 0:
                msg = f"频道 {ch_name} 已与谷歌云一致（共 {ch_tot} 张）"
            else:
                seg = []
                if p_n:
                    seg.append(f"新增发送 {p_n} 张")
                if d_n:
                    seg.append(f"联动撤回 {d_n} 张")
                if f_n:
                    seg.append(f"失败 {f_n} 张")
                msg = f"已同步至频道 {ch_name}（{'，'.join(seg)} · 共 {ch_tot} 张）"
            with _DRIVE_SYNC_LOCK:
                _DRIVE_SYNC_TASK.update({
                    "running": False,
                    "stage": "done",
                    "progress": "",
                    "finished_at": time.time(),
                    "last_ok": f_n == 0,
                    "last_message": msg,
                    "last_summary": str(res.get("summary") or msg),
                })
    except Exception as exc:
        err_txt = f"同步失败：{exc}"
        with _DRIVE_SYNC_LOCK:
            _DRIVE_SYNC_TASK.update({
                "running": False,
                "stage": "error",
                "progress": "",
                "finished_at": time.time(),
                "last_ok": False,
                "last_message": err_txt[:220],
                "last_summary": err_txt[:400],
            })


def trigger_panel_drive_sync(mode: str) -> tuple[bool, str, dict]:
    if not is_drive_sync_enabled():
        return False, "当前未启用 Google Drive 同步模块（COLABBB_ENABLE_DRIVE_SYNC=0）", {}
    if mode not in ("drive", "channel"):
        return False, "无效的同步模式", {}
    if mode == "channel" and not is_channel_sync_enabled():
        return False, "未配置 TG_BOT_TOKEN 或 TG_SYNC_CHANNEL_ID，无法同步至频道", {}
    with _DRIVE_SYNC_LOCK:
        if _DRIVE_SYNC_TASK["running"]:
            cur_m = "同步到频道" if _DRIVE_SYNC_TASK["mode"] == "channel" else "同步谷歌云盘"
            return False, f"正在执行「{cur_m}」，请稍候完成", dict(_DRIVE_SYNC_TASK)
        _DRIVE_SYNC_TASK.update({
            "running": True,
            "mode": mode,
            "stage": "starting",
            "progress": "正在启动同步任务..." if mode == "drive" else "正在连接谷歌云与 Telegram 频道...",
            "started_at": time.time(),
        })
        snap = dict(_DRIVE_SYNC_TASK)
    threading.Thread(target=_run_drive_sync_worker, args=(mode,), daemon=True).start()
    start_msg = "已开始同步本地作品到谷歌加密云盘" if mode == "drive" else "已开始将谷歌云作品同步至 Telegram 频道"
    return True, start_msg, snap


LIVE_UI_HTML_PATH = os.path.join(STATE_DIR, "panel_ui.html")
_LIVE_UI_LOCK = threading.Lock()
_LIVE_UI_CACHE = {"mtime": -1, "bytes": None}


def get_live_html_bytes() -> bytes:
    with _LIVE_UI_LOCK:
        try:
            if os.path.isfile(LIVE_UI_HTML_PATH):
                st = os.stat(LIVE_UI_HTML_PATH)
                if st.st_mtime_ns >= _WEB_PANEL_MTIME_NS:
                    if st.st_mtime_ns != _LIVE_UI_CACHE["mtime"] or _LIVE_UI_CACHE["bytes"] is None:
                        with open(LIVE_UI_HTML_PATH, "rb") as fh:
                            data = fh.read()
                        if data.startswith(b"<!doctype html>"):
                            _LIVE_UI_CACHE["mtime"] = st.st_mtime_ns
                            _LIVE_UI_CACHE["bytes"] = data
                    if _LIVE_UI_CACHE["bytes"]:
                        return _LIVE_UI_CACHE["bytes"]
        except Exception:
            pass
    return HTML_PAGE.encode("utf-8")


# ==================== Web 面板认证与本机内部 IPC 鉴权 ====================
PANEL_AUTH_FILE = os.path.join(STATE_DIR, "panel_auth.json")
SESSION_COOKIE_NAME = "g4_session"
SESSION_TTL_SEC = 30 * 86400
_AUTH_LOCK = threading.Lock()
_AUTH_CACHE = {"mtime": -1, "doc": None}
_LOGIN_ATTEMPTS = {}
_LOGIN_LOCK = threading.Lock()
_LOGIN_MAX_KEYS = 2048
_ALLOWED_UPLOAD_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".mp4", ".webm", ".mov"}
_SAFE_INLINE_MEDIA_MIMES = {
    "image/png", "image/jpeg", "image/webp", "image/gif",
    "video/mp4", "video/webm", "video/quicktime",
    "text/plain", "text/markdown",
}


def _get_auth_config() -> dict:
    with _AUTH_LOCK:
        try:
            st = os.stat(PANEL_AUTH_FILE)
            if st.st_mtime_ns != _AUTH_CACHE["mtime"] or _AUTH_CACHE["doc"] is None:
                with open(PANEL_AUTH_FILE, "r", encoding="utf-8") as fh:
                    _AUTH_CACHE["doc"] = json.load(fh)
                    _AUTH_CACHE["mtime"] = st.st_mtime_ns
            return dict(_AUTH_CACHE["doc"] or {})
        except Exception:
            return {}


def _bump_session_version():
    with _AUTH_LOCK:
        try:
            if not os.path.isfile(PANEL_AUTH_FILE):
                return
            with open(PANEL_AUTH_FILE, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            if not isinstance(doc, dict):
                return
            doc["session_version"] = int(doc.get("session_version") or 0) + 1
            fd, tmp = tempfile.mkstemp(prefix=".auth-", dir=STATE_DIR)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as out:
                    os.fchmod(out.fileno(), 0o600)
                    json.dump(doc, out, ensure_ascii=False)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(tmp, PANEL_AUTH_FILE)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            st = os.stat(PANEL_AUTH_FILE)
            _AUTH_CACHE["doc"] = doc
            _AUTH_CACHE["mtime"] = st.st_mtime_ns
        except Exception:
            pass


def _make_session_cookie(username: str) -> str:
    cfg = _get_auth_config()
    sec = bytes.fromhex(cfg.get("session_secret_hex") or "00" * 32)
    exp = int(time.time()) + SESSION_TTL_SEC
    ver = int(cfg.get("session_version") or 0)
    payload = f"{username}|{exp}|v{ver}"
    sig = hmac.new(sec, payload.encode("utf-8"), hashlib.sha256).hexdigest()
    raw = f"{payload}|{sig}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _verify_session_cookie(token: str) -> bool:
    if not token or len(token) > 512:
        return False
    cfg = _get_auth_config()
    sec_hex = cfg.get("session_secret_hex") or ""
    expected_user = cfg.get("username") or "admin"
    expected_ver = int(cfg.get("session_version") or 0)
    if not sec_hex:
        return False
    try:
        pad = "=" * ((4 - len(token) % 4) % 4)
        decoded = base64.urlsafe_b64decode((token + pad).encode("ascii")).decode("utf-8")
        parts = decoded.split("|")
        if len(parts) == 4:
            user, exp_s, ver_s, sig = parts
            if ver_s != f"v{expected_ver}":
                return False
            payload = f"{user}|{exp_s}|{ver_s}"
        elif len(parts) == 3 and expected_ver == 0:
            user, exp_s, sig = parts
            payload = f"{user}|{exp_s}"
        else:
            return False
        if not hmac.compare_digest(user, expected_user):
            return False
        if int(exp_s) < int(time.time()):
            return False
        expected_sig = hmac.new(bytes.fromhex(sec_hex), payload.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(sig, expected_sig)
    except Exception:
        return False


def _verify_login_credentials(username: str, password: str) -> bool:
    cfg = _get_auth_config()
    expected_user = cfg.get("username") or "admin"
    salt_hex = cfg.get("salt_hex") or ""
    pwd_hash_hex = cfg.get("pwd_hash_hex") or ""
    iters = int(cfg.get("iterations") or 240000)
    if not salt_hex or not pwd_hash_hex:
        return False
    user_ok = hmac.compare_digest((username or "").strip(), expected_user)
    cand_hash = hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), bytes.fromhex(salt_hex), iters).hex()
    pwd_ok = hmac.compare_digest(cand_hash, pwd_hash_hex)
    return bool(user_ok and pwd_ok)


def _prune_login_attempts_locked(now: float):
    stale = [k for k, v in _LOGIN_ATTEMPTS.items() if not any(now - ts < 300 for ts in v)]
    for k in stale:
        _LOGIN_ATTEMPTS.pop(k, None)
    if len(_LOGIN_ATTEMPTS) > _LOGIN_MAX_KEYS:
        ordered = sorted(_LOGIN_ATTEMPTS.items(), key=lambda kv: max(kv[1]) if kv[1] else 0.0)
        for k, _ in ordered[: len(_LOGIN_ATTEMPTS) - _LOGIN_MAX_KEYS]:
            _LOGIN_ATTEMPTS.pop(k, None)


def _check_login_rate_limit(ip_key: str) -> bool:
    now = time.time()
    with _LOGIN_LOCK:
        _prune_login_attempts_locked(now)
        hist = [ts for ts in _LOGIN_ATTEMPTS.get(ip_key, []) if now - ts < 300]
        if hist:
            _LOGIN_ATTEMPTS[ip_key] = hist
        else:
            _LOGIN_ATTEMPTS.pop(ip_key, None)
        return len(hist) < 10


def _record_login_failure(ip_key: str, success: bool):
    now = time.time()
    with _LOGIN_LOCK:
        _prune_login_attempts_locked(now)
        if success:
            _LOGIN_ATTEMPTS.pop(ip_key, None)
        else:
            hist = [ts for ts in _LOGIN_ATTEMPTS.get(ip_key, []) if now - ts < 300]
            hist.append(now)
            _LOGIN_ATTEMPTS[ip_key] = hist


def _is_trusted_proxy_ip(peer_ip: str) -> bool:
    if not peer_ip:
        return False
    raw = peer_ip[7:] if peer_ip.startswith("::ffff:") else peer_ip
    try:
        addr = ipaddress.ip_address(raw)
        return addr.is_loopback or addr.is_private
    except ValueError:
        return False


def _resolve_safe_out_file(raw_path: str) -> str:
    if not isinstance(raw_path, str) or not raw_path.strip():
        return ""
    real_root = os.path.realpath(OUT_ROOT)
    real_p = os.path.realpath(raw_path.strip())
    try:
        if os.path.commonpath([real_root, real_p]) == real_root and os.path.isfile(real_p):
            return real_p
    except ValueError:
        pass
    return ""


def _validate_uploaded_media(full_path: str, ext: str) -> bool:
    try:
        with open(full_path, "rb") as fh:
            head = fh.read(32)
        if len(head) < 8:
            return False
        if ext in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
            try:
                from PIL import Image
            except ImportError:
                if ext == ".png":
                    return head[:8] == b"\x89PNG\r\n\x1a\n"
                if ext in (".jpg", ".jpeg"):
                    return head[:3] == b"\xff\xd8\xff"
                if ext == ".gif":
                    return head[:6] in (b"GIF87a", b"GIF89a")
                if ext == ".webp":
                    return head[:4] == b"RIFF" and head[8:12] == b"WEBP"
                return False
            with Image.open(full_path) as im:
                im.verify()
            return True
        if ext in (".mp4", ".mov"):
            return len(head) >= 12 and head[4:8] in (b"ftyp", b"moov", b"mdat", b"wide", b"free")
        if ext == ".webm":
            return head[:4] == b"\x1a\x45\xdf\xa3"
    except Exception:
        return False
    return False

LOGIN_VIEW_TEMPLATE = '<!doctype html>\n<html lang="zh-CN">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n  <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'sha256-KLNzgjEDQkQ7X/QMaSvAIjv1o+9VUoretBVEl7irTQk=\'; style-src \'unsafe-inline\'; img-src \'self\' data:; connect-src \'self\'; form-action \'self\'; base-uri \'none\'">\n  <title>登录 · G4 工作台</title>\n  <link rel="icon" type="image/svg+xml" href="/g4-icon.svg?v=g5">\n  <link rel="icon" type="image/png" sizes="64x64" href="/favicon.png?v=g5">\n  <link rel="shortcut icon" href="/favicon.ico?v=g5">\n  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v=g5">\n  <style>\n    :root {\n      color-scheme: light dark;\n      --bg: #ECEEF2;\n      --surface-1: #FFFFFF;\n      --surface-2: #F2F4F8;\n      --surface-2-hover: #E9ECF2;\n      --surface-3: #E1E5ED;\n      --surface-focus: #FFFFFF;\n      --hairline: rgba(15,23,42,.06);\n      --ring-focus: rgba(15,23,42,.20);\n      --shadow-focus: 0 2px 10px rgba(15,23,42,.06);\n      --shadow-sheet: 0 1px 2px rgba(15,23,42,.04), 0 12px 32px rgba(15,23,42,.06);\n      --text: #181A1F;\n      --text-2: #586070;\n      --text-3: #8891A0;\n      --accent: #0071E3;\n      --accent-hover: #0062C7;\n      --accent-ring: rgba(0,113,227,.32);\n      --danger: #D70015;\n      --danger-soft: rgba(215,0,21,.09);\n      --on-accent: #FFFFFF;\n      --fs-xs: 12px;\n      --fs-sm: 13px;\n      --fs-md: 15px;\n      --r-ctrl: 10px;\n      --r-inner: calc(var(--r-ctrl) - 3px);\n      --r-panel: 16px;\n      --r-pill: 999px;\n      --ease: cubic-bezier(.2,.8,.2,1);\n    }\n    @media (prefers-color-scheme: dark) {\n      :root {\n        --bg: #181A1F;\n        --surface-1: #22252C;\n        --surface-2: #2B2F38;\n        --surface-2-hover: #323742;\n        --surface-3: #3B414E;\n        --surface-focus: #2E333E;\n        --hairline: rgba(255,255,255,.07);\n        --ring-focus: rgba(255,255,255,.20);\n        --shadow-focus: 0 4px 16px rgba(0,0,0,.26);\n        --shadow-sheet: inset 0 1px 0 rgba(255,255,255,.05), 0 20px 52px rgba(0,0,0,.42);\n        --text: #F2F4F8;\n        --text-2: #A6AEBC;\n        --text-3: #727B8A;\n        --accent: #0A84FF;\n        --accent-hover: #3395FF;\n        --accent-ring: rgba(10,132,255,.38);\n        --danger: #FF453A;\n        --danger-soft: rgba(255,69,58,.15);\n      }\n    }\n    *, *::before, *::after { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }\n    :focus:not(:focus-visible) { outline: none; }\n    :focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }\n    html, body { height: 100%; margin: 0; }\n    body {\n      min-height: 100dvh;\n      padding: 24px 16px calc(24px + env(safe-area-inset-bottom, 0px));\n      background: var(--bg);\n      color: var(--text);\n      font-family: "Segoe UI Variable Text", "Segoe UI", ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei UI", sans-serif;\n      font-size: var(--fs-sm);\n      line-height: 1.5;\n      display: flex;\n      align-items: center;\n      justify-content: center;\n      -webkit-font-smoothing: antialiased;\n    }\n    .login-card {\n      width: 100%;\n      max-width: 384px;\n      background: var(--surface-1);\n      border: 0;\n      border-radius: var(--r-panel);\n      padding: 24px;\n      box-shadow: var(--shadow-sheet);\n      display: flex;\n      flex-direction: column;\n      gap: 20px;\n    }\n    @keyframes shakeCard {\n      0%, 100% { transform: translateX(0); }\n      25% { transform: translateX(-4px); }\n      50% { transform: translateX(4px); }\n      75% { transform: translateX(-2px); }\n    }\n    .login-card.shake { animation: shakeCard 180ms var(--ease); }\n    .login-head {\n      display: flex;\n      align-items: center;\n      justify-content: space-between;\n      gap: 12px;\n    }\n    .brand-row { display: flex; align-items: center; gap: 12px; min-width: 0; }\n    .brand-mark { width: 34px; height: 34px; flex: 0 0 34px; display: inline-flex; align-items: center; justify-content: center; user-select: none; }\n    .brand-mark-svg { width: 100%; height: 100%; display: block; overflow: visible; }\n    .brand-info { display: flex; flex-direction: column; gap: 2px; min-width: 0; }\n    .brand-title { font-size: var(--fs-md); font-weight: 600; color: var(--text); line-height: 1.25; white-space: nowrap; }\n    .brand-sub { font-size: var(--fs-xs); color: var(--text-3); line-height: 1.25; white-space: nowrap; }\n    .guard-pill {\n      height: 24px;\n      padding: 0 9px;\n      border-radius: var(--r-inner);\n      background: var(--surface-2);\n      color: var(--text-2);\n      font-size: var(--fs-xs);\n      font-weight: 500;\n      display: inline-flex;\n      align-items: center;\n      gap: 4px;\n      flex-shrink: 0;\n      user-select: none;\n    }\n    .ico { width: 16px; height: 16px; flex-shrink: 0; stroke: currentColor; stroke-width: 1.75; stroke-linecap: round; stroke-linejoin: round; fill: none; }\n    .ico-13 { width: 14px; height: 14px; }\n    .login-form { display: flex; flex-direction: column; gap: 14px; margin: 0; }\n    .field-group { display: flex; flex-direction: column; gap: 6px; }\n    .field-label { font-size: var(--fs-sm); font-weight: 500; color: var(--text); user-select: none; }\n    .input-shell {\n      height: 36px;\n      padding: 0 4px 0 12px;\n      border-radius: var(--r-ctrl);\n      background: var(--surface-2);\n      display: flex;\n      align-items: center;\n      gap: 8px;\n      transition: background-color 150ms var(--ease), box-shadow 150ms var(--ease);\n    }\n    .input-shell .field-ico { color: var(--text-3); }\n    .input-shell:focus-within {\n      background: var(--surface-focus);\n      box-shadow: inset 0 0 0 1px var(--ring-focus), var(--shadow-focus);\n    }\n    .input-shell:has(.field-input:focus-visible) {\n      box-shadow: inset 0 0 0 1.5px var(--accent-ring), var(--shadow-focus);\n    }\n    .input-shell:focus-within .field-ico { color: var(--text-2); }\n    .field-input {\n      flex: 1;\n      min-width: 0;\n      width: 100%;\n      height: 100%;\n      border: 0;\n      background: transparent;\n      color: var(--text);\n      caret-color: var(--accent);\n      font-family: inherit;\n      font-size: var(--fs-sm);\n      padding: 0;\n    }\n    .field-input:focus, .field-input:focus-visible { outline: none; }\n    .field-input::placeholder { color: var(--text-3); }\n    .field-input:-webkit-autofill,\n    .field-input:-webkit-autofill:hover,\n    .field-input:-webkit-autofill:focus {\n      -webkit-text-fill-color: var(--text);\n      -webkit-box-shadow: 0 0 0 1000px var(--surface-2) inset;\n      transition: background-color 9999s ease-out 0s;\n    }\n    .eye-btn {\n      width: 28px;\n      height: 28px;\n      flex: 0 0 28px;\n      padding: 0;\n      border: 0;\n      border-radius: var(--r-inner);\n      background: transparent;\n      color: var(--text-3);\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      cursor: pointer;\n      transition: background-color 150ms var(--ease), color 150ms var(--ease);\n    }\n    .error-box {\n      display: none;\n      align-items: center;\n      gap: 8px;\n      padding: 8px 12px;\n      border-radius: var(--r-ctrl);\n      background: var(--danger-soft);\n      color: var(--danger);\n      font-size: var(--fs-xs);\n      font-weight: 500;\n      line-height: 1.5;\n    }\n    .error-box.visible { display: flex; }\n    .submit-btn {\n      margin-top: 6px;\n      width: 100%;\n      height: 40px;\n      padding: 0 16px;\n      border: 0;\n      border-radius: var(--r-ctrl);\n      background: var(--accent);\n      color: var(--on-accent);\n      font-family: inherit;\n      font-size: var(--fs-sm);\n      font-weight: 600;\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      gap: 8px;\n      cursor: pointer;\n      user-select: none;\n      transition: background-color 150ms var(--ease), opacity 150ms var(--ease);\n    }\n    .submit-btn:active:not(:disabled) { background: var(--accent-hover); }\n    .submit-btn:disabled { opacity: .4; cursor: not-allowed; }\n    .login-foot { font-size: var(--fs-xs); color: var(--text-3); text-align: center; user-select: none; }\n    @keyframes spin { to { transform: rotate(360deg); } }\n    .spin { animation: spin 600ms linear infinite; }\n    @media (hover: hover) and (pointer: fine) {\n      .input-shell:hover:not(:focus-within) { background: var(--surface-2-hover); }\n      .eye-btn:hover { background: var(--surface-3); color: var(--text); }\n      .submit-btn:hover:not(:disabled) { background: var(--accent-hover); }\n    }\n    @media (prefers-reduced-motion: reduce) {\n      .login-card.shake { animation: none; }\n    }\n    .hint { font-size: var(--fs-xs); color: var(--text-3); text-align: center; margin: 0; }\n    .hint:empty { display: none; }\n  </style>\n</head>\n<body>\n  <svg xmlns="http://www.w3.org/2000/svg" style="display:none" aria-hidden="true">\n    <symbol id="i-lock" viewBox="0 0 24 24"><rect x="4" y="11" width="16" height="10" rx="2.5"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></symbol>\n    <symbol id="i-user" viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"/><path d="M4.5 20a7.5 7.5 0 0 1 15 0"/></symbol>\n    <symbol id="i-eye" viewBox="0 0 24 24"><path d="M2 12s3.6-6.5 10-6.5S22 12 22 12s-3.6 6.5-10 6.5S2 12 2 12z"/><circle cx="12" cy="12" r="2.8"/></symbol>\n    <symbol id="i-eye-off" viewBox="0 0 24 24"><path d="M17.94 17.94A10.4 10.4 0 0 1 12 18.5C5.6 18.5 2 12 2 12a18.8 18.8 0 0 1 4.4-5.26M9.9 5.73A9.8 9.8 0 0 1 12 5.5c6.4 0 10 6.5 10 6.5a18.8 18.8 0 0 1-2.55 3.68"/><line x1="3" y1="3" x2="21" y2="21"/></symbol>\n    <symbol id="i-alert" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><line x1="12" y1="8" x2="12" y2="12.5"/><circle cx="12" cy="16" r="0.6" fill="currentColor"/></symbol>\n    <symbol id="i-spin" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/></symbol>\n  </svg>\n\n  <main class="login-card" id="loginCard">\n    <div class="login-head">\n      <div class="brand-row">\n        <span class="brand-mark" aria-hidden="true"><svg class="brand-mark-svg" viewBox="68 48 896 904" aria-hidden="true"><defs><linearGradient id="bm_bright" gradientUnits="userSpaceOnUse" x1="150" y1="100" x2="880" y2="900"><stop offset="0%" stop-color="#9BD4FF"/><stop offset="46%" stop-color="#4A90FF"/><stop offset="100%" stop-color="#1D54E0"/></linearGradient><linearGradient id="bm_main" gradientUnits="userSpaceOnUse" x1="180" y1="120" x2="880" y2="920"><stop offset="0%" stop-color="#75BEFF"/><stop offset="50%" stop-color="#377DF6"/><stop offset="100%" stop-color="#1745BE"/></linearGradient><linearGradient id="bm_deep" gradientUnits="userSpaceOnUse" x1="180" y1="140" x2="860" y2="940"><stop offset="0%" stop-color="#4B92FF"/><stop offset="55%" stop-color="#2260DF"/><stop offset="100%" stop-color="#123597"/></linearGradient><linearGradient id="bm_gold" gradientUnits="userSpaceOnUse" x1="350" y1="120" x2="850" y2="850"><stop offset="0%" stop-color="#FFF0A8"/><stop offset="48%" stop-color="#FACC3E"/><stop offset="100%" stop-color="#E59213"/></linearGradient><linearGradient id="bm_cyan" gradientUnits="userSpaceOnUse" x1="200" y1="120" x2="820" y2="850"><stop offset="0%" stop-color="#B8ECFF"/><stop offset="50%" stop-color="#58B4FF"/><stop offset="100%" stop-color="#256AF0"/></linearGradient><mask id="bm_knock" maskUnits="userSpaceOnUse" x="-100" y="-100" width="1224" height="1224"><rect x="-100" y="-100" width="1224" height="1224" fill="#fff"/><path d="M 786 352 L 558 688 L 904 688 M 786 352 L 786 888" fill="none" stroke="#000" stroke-width="174" stroke-linecap="round" stroke-linejoin="round"/></mask></defs><g mask="url(#bm_knock)"><path d="M 642 238 A 308 308 0 1 0 464 786" fill="none" stroke="url(#bm_main)" stroke-width="138" stroke-linecap="round"/><path d="M 304 338 A 182 182 0 0 0 284 568" fill="none" stroke="url(#bm_cyan)" stroke-width="44" stroke-linecap="round" opacity="0.9"/></g><path d="M 456 336 C 469 402, 490 423, 556 436 C 490 449, 469 470, 456 536 C 443 470, 422 449, 356 436 C 422 423, 443 402, 456 336 Z" fill="url(#bm_gold)"/><path d="M 786 352 L 558 688 L 904 688" fill="none" stroke="url(#bm_bright)" stroke-width="116" stroke-linecap="round" stroke-linejoin="round"/><path d="M 786 352 L 786 888" fill="none" stroke="url(#bm_deep)" stroke-width="116" stroke-linecap="round"/></svg></span>\n        <div class="brand-info">\n          <span class="brand-title">G4 工作台</span>\n          <span class="brand-sub">请输入账号密码验证身份</span>\n        </div>\n      </div>\n      <span class="guard-pill">\n        <svg class="ico ico-13" aria-hidden="true"><use href="#i-lock"/></svg>\n        <span>访问保护</span>\n      </span>\n    </div>\n\n    <form class="login-form" id="loginForm" autocomplete="on" novalidate>\n      <div class="field-group">\n        <label class="field-label" for="loginUser">账号</label>\n        <div class="input-shell">\n          <svg class="ico field-ico" aria-hidden="true"><use href="#i-user"/></svg>\n          <input class="field-input" id="loginUser" name="username" type="text" autocomplete="username" autocapitalize="none" spellcheck="false" placeholder="输入管理账号" required>\n        </div>\n      </div>\n\n      <div class="field-group">\n        <label class="field-label" for="loginPass">密码</label>\n        <div class="input-shell">\n          <svg class="ico field-ico" aria-hidden="true"><use href="#i-lock"/></svg>\n          <input class="field-input" id="loginPass" name="password" type="password" autocomplete="current-password" placeholder="输入访问密码" required>\n          <button type="button" class="eye-btn" id="togglePwdBtn" aria-label="显示密码">\n            <svg class="ico" aria-hidden="true"><use id="eyeUse" href="#i-eye"/></svg>\n          </button>\n        </div>\n      </div>\n\n      <div class="error-box" id="loginErr" role="alert">\n        <svg class="ico" aria-hidden="true"><use href="#i-alert"/></svg>\n        <span id="loginErrText">账号或密码错误</span>\n      </div>\n\n      <button type="submit" class="submit-btn" id="loginSubmitBtn">\n        <span id="submitBtnLabel">进入工作台</span>\n      </button>\n    </form><p class="hint"><!--INIT_NOTICE--></p>\n\n    <div class="login-foot">登录凭证加密校验 · 当前设备保持 30 天免重复登录</div>\n  </main>\n\n  <script>\'use strict\';\nconst sysDark = window.matchMedia(\'(prefers-color-scheme: dark)\');\nfunction syncTheme() {\n  const dark = sysDark.matches;\n  document.documentElement.setAttribute(\'data-theme\', dark ? \'dark\' : \'light\');\n}\nsysDark.addEventListener(\'change\', syncTheme);\nsyncTheme();\n\nconst userInp = document.getElementById(\'loginUser\');\nconst passInp = document.getElementById(\'loginPass\');\nconst eyeBtn = document.getElementById(\'togglePwdBtn\');\nconst eyeUse = document.getElementById(\'eyeUse\');\nconst errBox = document.getElementById(\'loginErr\');\nconst errTxt = document.getElementById(\'loginErrText\');\nconst card = document.getElementById(\'loginCard\');\nconst subBtn = document.getElementById(\'loginSubmitBtn\');\n\nif (window.matchMedia(\'(hover: hover) and (pointer: fine)\').matches) {\n  userInp?.focus();\n}\n\neyeBtn?.addEventListener(\'click\', () => {\n  const show = passInp.type === \'password\';\n  passInp.type = show ? \'text\' : \'password\';\n  eyeUse?.setAttribute(\'href\', show ? \'#i-eye-off\' : \'#i-eye\');\n  eyeBtn.setAttribute(\'aria-label\', show ? \'隐藏密码\' : \'显示密码\');\n});\n\nfunction showErr(msg) {\n  errTxt.textContent = msg || \'账号或密码错误\';\n  errBox.classList.add(\'visible\');\n  card.classList.remove(\'shake\');\n  void card.offsetWidth;\n  card.classList.add(\'shake\');\n}\n\n[userInp, passInp].forEach(el => el?.addEventListener(\'input\', () => {\n  errBox.classList.remove(\'visible\');\n}));\n\nlet busy = false;\ndocument.getElementById(\'loginForm\')?.addEventListener(\'submit\', async (e) => {\n  e.preventDefault();\n  if (busy) return;\n  const username = (userInp.value || \'\').trim();\n  const password = passInp.value || \'\';\n  if (!username || !password) {\n    showErr(\'请输入账号和密码\');\n    (!username ? userInp : passInp).focus();\n    return;\n  }\n  busy = true;\n  subBtn.disabled = true;\n  errBox.classList.remove(\'visible\');\n  const ns = \'http://www.w3.org/2000/svg\';\n  const sp = document.createElementNS(ns, \'svg\');\n  sp.setAttribute(\'class\', \'ico spin\');\n  const u = document.createElementNS(ns, \'use\');\n  u.setAttribute(\'href\', \'#i-spin\');\n  sp.appendChild(u);\n  const st = document.createElement(\'span\');\n  st.textContent = \'正在验证...\';\n  subBtn.replaceChildren(sp, st);\n\n  try {\n    const res = await fetch(\'/api/login\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ username, password })\n    });\n    const data = await res.json().catch(() => ({}));\n    if (!res.ok || !data.ok) {\n      showErr(data.message || \'账号或密码不正确\');\n      busy = false;\n      subBtn.disabled = false;\n      const lbl = document.createElement(\'span\');\n      lbl.textContent = \'进入工作台\';\n      subBtn.replaceChildren(lbl);\n      passInp.select();\n      return;\n    }\n    window.location.replace(\'/\');\n  } catch (_) {\n    showErr(\'网络连接异常，请重试\');\n    busy = false;\n    subBtn.disabled = false;\n    const lbl = document.createElement(\'span\');\n    lbl.textContent = \'进入工作台\';\n    subBtn.replaceChildren(lbl);\n  }\n});\n</script>\n</body>\n</html>'

# 权威 UI 只存储一份；运行时保持与磁盘模板字节一致。
HTML_PAGE = '<!doctype html>\n<html lang="zh-CN">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n  <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'sha256-jGZET8QFWbUj/cTslh9wwFjFG84+pKjPdWeLoWjqMMQ=\'; style-src \'unsafe-inline\'; img-src \'self\' data: blob:; connect-src \'self\'; form-action \'self\'; object-src \'none\'; base-uri \'none\'; frame-src \'none\'; media-src \'self\'; font-src \'none\'; worker-src \'none\'; manifest-src \'self\'">\n  <meta name="color-scheme" content="light dark">\n  <meta name="theme-color" media="(prefers-color-scheme: dark)" content="#181A1F">\n  <meta name="theme-color" media="(prefers-color-scheme: light)" content="#ECEEF2">\n  <meta name="apple-mobile-web-app-capable" content="yes">\n  <meta name="mobile-web-app-capable" content="yes">\n  <meta name="apple-mobile-web-app-status-bar-style" content="default">\n  <title>G4 工作台</title>\n  <link rel="icon" type="image/svg+xml" href="/g4-icon.svg?v=g5">\n  <link rel="icon" type="image/png" sizes="64x64" href="/favicon.png?v=g5">\n  <link rel="shortcut icon" href="/favicon.ico?v=g5">\n  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v=g5">\n  <link rel="manifest" href="/manifest.webmanifest">\n  <style>\n/* ===== G4 工作台 · Kat 同源质感与统一原语设计系统 ===== */\n:root {\n  color-scheme: light dark;\n  --bg: #ECEEF2;\n  --surface-1: #FFFFFF;\n  --surface-2: #F2F4F8;\n  --surface-2-hover: #E9ECF2;\n  --surface-3: #E1E5ED;\n  --surface-subtle: #F6F7FA;\n  --surface-focus: #FFFFFF;\n  --hairline: rgba(15,23,42,.07);\n  --ring-focus: rgba(15,23,42,.20);\n  --shadow-focus: 0 2px 10px rgba(15,23,42,.06);\n  --shadow-sheet: 0 1px 2px rgba(15,23,42,.04), 0 6px 20px rgba(15,23,42,.035);\n  --shadow-pop: 0 18px 48px rgba(15,23,42,.14), 0 2px 8px rgba(15,23,42,.06), 0 0 0 1px rgba(15,23,42,.06);\n  --edge-pop: rgba(15,23,42,.08);\n\n  --text: #181A1F;\n  --text-2: #586070;\n  --text-3: #8891A0;\n  --accent: #0071E3;\n  --accent-hover: #0062C7;\n  --accent-soft: rgba(0,113,227,.10);\n  --accent-ring: rgba(0,113,227,.32);\n  --ok: #1F9D49;\n  --warn: #B26A00;\n  --danger: #D70015;\n  --danger-soft: rgba(215,0,21,.09);\n  --overlay: rgba(15,23,42,.34);\n\n  --seg-on: var(--surface-1);\n  --seg-on-bg-track: rgba(15,23,42,.055);\n  --seg-shadow: 0 1px 3px rgba(15,23,42,.09);\n  --toast-bg: #1E2128;\n  --tip-bg: rgba(24,26,31,.88);\n  --tip-ring: rgba(255,255,255,.12);\n  --on-accent: #FFFFFF;\n  --on-media: #FFFFFF;\n  --scrim: rgba(0,0,0,.62);\n  --scrim-0: rgba(0,0,0,0);\n  --media-soft: rgba(0,0,0,.28);\n  --viewer-bg: #111317;\n  --viewer-btn: rgba(24,26,31,.72);\n  --viewer-btn-hover: rgba(34,37,44,.92);\n  --viewer-btn-ring: rgba(255,255,255,.14);\n  --stage-glow: radial-gradient(circle at 50% 42%, rgba(0,113,227,.09), transparent 64%);\n  --shimmer: rgba(255,255,255,.55);\n\n  /* 字号（仅 4 档） */\n  --fs-xs: 12px;\n  --fs-sm: 13px;\n  --fs-md: 15px;\n  --fs-lg: 17px;\n\n  /* 圆角（全站仅此 3 档 + 同心内圆角） */\n  --r-ctrl: 10px;\n  --r-inner: calc(var(--r-ctrl) - 3px);\n  --r-check: calc(var(--r-ctrl) - 5px);\n  --r-panel: 16px;\n  --r-pill: 999px;\n\n  /* 控件高度（全站仅此 3 档） */\n  --h-sm: 28px;\n  --h-md: 34px;\n  --h-lg: 40px;\n\n  --gap: 12px;\n  --ease: cubic-bezier(.2,.8,.2,1);\n  --dur: 150ms;\n  --dur-out: 120ms;\n  --font: "Segoe UI Variable Text","Segoe UI",ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"PingFang SC","Hiragino Sans GB","Microsoft YaHei UI",sans-serif;\n  --mono: ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;\n  --safe-b: env(safe-area-inset-bottom, 0px);\n  --safe-t: env(safe-area-inset-top, 0px);\n}\n\n@media (prefers-color-scheme: dark) {\n  :root {\n    --bg: #181A1F;\n    --surface-1: #22252C;\n    --surface-2: #2B2F38;\n    --surface-2-hover: #323742;\n    --surface-3: #3B414E;\n    --surface-subtle: rgba(255,255,255,.038);\n    --surface-focus: #2E333E;\n    --hairline: rgba(255,255,255,.07);\n    --ring-focus: rgba(255,255,255,.20);\n    --shadow-focus: 0 4px 16px rgba(0,0,0,.26);\n    --shadow-sheet: 0 1px 2px rgba(0,0,0,.20), inset 0 1px 0 rgba(255,255,255,.045);\n    --shadow-pop: 0 20px 52px rgba(0,0,0,.48), 0 0 0 1px rgba(255,255,255,.08);\n    --edge-pop: rgba(255,255,255,.09);\n\n    --text: #F2F4F8;\n    --text-2: #A6AEBC;\n    --text-3: #727B8A;\n    --accent: #0A84FF;\n    --accent-hover: #3395FF;\n    --accent-soft: rgba(10,132,255,.16);\n    --accent-ring: rgba(10,132,255,.38);\n    --ok: #30D158;\n    --warn: #FFB340;\n    --danger: #FF453A;\n    --danger-soft: rgba(255,69,58,.15);\n    --overlay: rgba(8,10,14,.56);\n\n    --seg-on: #363B47;\n    --seg-on-bg-track: #22252C;\n    --seg-shadow: 0 1px 2px rgba(0,0,0,.24), inset 0 1px 0 rgba(255,255,255,.06);\n    --toast-bg: #2E333E;\n    --tip-bg: rgba(34,37,44,.94);\n    --tip-ring: rgba(255,255,255,.12);\n    --stage-glow: radial-gradient(circle at 50% 42%, rgba(10,132,255,.12), transparent 64%);\n    --shimmer: rgba(255,255,255,.05);\n  }\n}\n\n.force-dark {\n  color-scheme: dark;\n  --bg: #181A1F;\n  --surface-1: #22252C;\n  --surface-2: #2B2F38;\n  --surface-2-hover: #323742;\n  --surface-3: #3B414E;\n  --surface-subtle: rgba(255,255,255,.04);\n  --surface-focus: #2E333E;\n  --hairline: rgba(255,255,255,.08);\n  --ring-focus: rgba(255,255,255,.20);\n  --shadow-sheet: inset 0 1px 0 rgba(255,255,255,.05);\n  --shadow-pop: 0 20px 52px rgba(0,0,0,.52), 0 0 0 1px rgba(255,255,255,.08);\n  --edge-pop: rgba(255,255,255,.09);\n  --text: #F2F4F8;\n  --text-2: #A6AEBC;\n  --text-3: #727B8A;\n  --accent: #0A84FF;\n  --accent-hover: #3395FF;\n  --accent-soft: rgba(10,132,255,.16);\n  --accent-ring: rgba(10,132,255,.38);\n  --ok: #30D158;\n  --danger: #FF453A;\n  --danger-soft: rgba(255,69,58,.15);\n  --seg-on: #363B47;\n  --seg-shadow: none;\n}\n\n/* ===== 基础与键鼠焦点分离 ===== */\n*, *::before, *::after { box-sizing: border-box; }\nhtml, body { height: 100%; margin: 0; overflow: hidden; }\nbody {\n  background: var(--bg);\n  color: var(--text);\n  font-family: var(--font);\n  font-size: var(--fs-sm);\n  line-height: 1.5;\n  -webkit-font-smoothing: antialiased;\n  -webkit-tap-highlight-color: transparent;\n  text-rendering: optimizeLegibility;\n}\nbutton, input, textarea, select { font: inherit; color: inherit; }\nbutton { cursor: pointer; }\nh1, h2, p { margin: 0; }\n[hidden] { display: none !important; }\n:focus:not(:focus-visible) { outline: none; }\n:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }\n[tabindex="-1"]:focus { outline: none; }\n.num { font-variant-numeric: tabular-nums; }\n.sprite { position: absolute; width: 0; height: 0; overflow: hidden; pointer-events: none; }\n.visually-hidden { position: absolute !important; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); white-space: nowrap; border: 0; padding: 0; margin: 0; }\n.ico { width: 16px; height: 16px; flex: 0 0 auto; fill: none; stroke: currentColor; stroke-width: 1.75; stroke-linecap: round; stroke-linejoin: round; }\n.ico-20 { width: 20px; height: 20px; }\n.ico-32 { width: 32px; height: 32px; }\n.row-spacer { flex: 1 1 auto; }\n.hint, .hint-3 { font-size: var(--fs-xs); color: var(--text-3); }\n.hint-2 { font-size: var(--fs-xs); color: var(--text-2); }\n.dot { width: 8px; height: 8px; border-radius: var(--r-pill); background: var(--text-3); flex: 0 0 auto; }\n.dot-ok { background: var(--ok); }\n.dot-warn { background: var(--warn); }\n.dot-breath { animation: breath 1600ms ease-in-out infinite; }\n@keyframes breath { 0%, 100% { opacity: 1; } 50% { opacity: .35; } }\n@keyframes spin { to { transform: rotate(360deg); } }\n@keyframes shimmer { from { transform: translateX(-100%); } to { transform: translateX(100%); } }\n@keyframes stripes { from { transform: translateX(-50%); } to { transform: translateX(0); } }\n@keyframes typing { 0%, 80%, 100% { opacity: .25; } 40% { opacity: 1; } }\n\n* { scrollbar-width: thin; scrollbar-color: var(--surface-3) transparent; }\n::-webkit-scrollbar { width: 6px; height: 6px; }\n::-webkit-scrollbar-track { background: transparent; }\n::-webkit-scrollbar-thumb { background: var(--surface-3); border-radius: var(--r-pill); }\n\n/* 动态滚动边缘渐隐（短内容无属性，不加遮罩） */\n.fade-x[data-fade-start="1"]:not([data-fade-end="1"]) { -webkit-mask-image: linear-gradient(90deg, transparent, #000 22px, #000); mask-image: linear-gradient(90deg, transparent, #000 22px, #000); }\n.fade-x[data-fade-end="1"]:not([data-fade-start="1"]) { -webkit-mask-image: linear-gradient(90deg, #000, #000 calc(100% - 26px), transparent); mask-image: linear-gradient(90deg, #000, #000 calc(100% - 26px), transparent); }\n.fade-x[data-fade-start="1"][data-fade-end="1"] { -webkit-mask-image: linear-gradient(90deg, transparent, #000 22px, #000 calc(100% - 26px), transparent); mask-image: linear-gradient(90deg, transparent, #000 22px, #000 calc(100% - 26px), transparent); }\n.fade-y[data-fade-start="1"]:not([data-fade-end="1"]) { -webkit-mask-image: linear-gradient(180deg, transparent, #000 20px, #000); mask-image: linear-gradient(180deg, transparent, #000 20px, #000); }\n.fade-y[data-fade-end="1"]:not([data-fade-start="1"]) { -webkit-mask-image: linear-gradient(180deg, #000, #000 calc(100% - 20px), transparent); mask-image: linear-gradient(180deg, #000, #000 calc(100% - 20px), transparent); }\n.fade-y[data-fade-start="1"][data-fade-end="1"] { -webkit-mask-image: linear-gradient(180deg, transparent, #000 20px, #000 calc(100% - 20px), transparent); mask-image: linear-gradient(180deg, transparent, #000 20px, #000 calc(100% - 20px), transparent); }\n\n/* ===== 共用容器原语 ===== */\n.surface {\n  background: var(--surface-1);\n  color: var(--text);\n  box-shadow: var(--shadow-sheet);\n}\n.panel {\n  background: var(--surface-1);\n  color: var(--text);\n  border-radius: var(--r-panel);\n  box-shadow: var(--shadow-sheet);\n}\n.well {\n  background: var(--surface-2);\n  border-radius: var(--r-ctrl);\n  padding: 12px;\n}\n.ref-zone {\n  background: var(--surface-subtle);\n  border-radius: var(--r-ctrl);\n  box-shadow: inset 0 0 0 1px var(--hairline);\n  padding: 10px 12px;\n}\n\n/* ===== 统一按钮原语 ===== */\n.btn {\n  position: relative;\n  height: var(--h-md);\n  padding: 0 14px;\n  border: 0;\n  border-radius: var(--r-ctrl);\n  display: inline-flex;\n  align-items: center;\n  justify-content: center;\n  gap: 6px;\n  font-size: var(--fs-sm);\n  font-weight: 500;\n  white-space: nowrap;\n  text-decoration: none;\n  background: transparent;\n  color: var(--text-2);\n  user-select: none;\n  flex-shrink: 0;\n  transition: background-color var(--dur) var(--ease), color var(--dur) var(--ease), opacity var(--dur) var(--ease), box-shadow var(--dur) var(--ease);\n}\n.btn-primary { background: var(--accent); color: var(--on-accent); font-weight: 600; }\n.btn-secondary { background: var(--surface-2); color: var(--text); }\n.btn-ghost { background: transparent; color: var(--text-2); }\n.btn-danger { background: var(--danger); color: var(--on-accent); font-weight: 600; }\n.btn.surface { background: var(--surface-1); color: var(--text); box-shadow: var(--shadow-sheet); }\n.btn.is-danger { color: var(--danger); }\n.btn-secondary.is-danger { background: var(--surface-2); }\n.btn-sm { height: var(--h-sm); padding: 0 10px; font-size: var(--fs-xs); }\n.btn-lg { height: var(--h-lg); font-weight: 600; }\n.btn:disabled { opacity: .4; cursor: not-allowed; }\n.btn.is-copied, .btn-link.is-copied { color: var(--ok); }\n.btn .chev { width: 15px; height: 15px; color: var(--text-3); }\n.btn .spinner { display: none; width: 14px; height: 14px; border: 2px solid var(--surface-3); border-top-color: var(--accent); border-radius: var(--r-pill); animation: spin 700ms linear infinite; }\n.btn.is-busy .spinner { display: inline-block; }\n.btn.is-busy > .ico:first-child { display: none; }\n\n/* 固定宽度修饰（防止文字切换引发位移） */\n.fix-idea { width: 92px; padding: 0 10px; }\n.fix-rand { width: 90px; padding: 0 10px; }\n.fix-sync { width: 104px; padding: 0 10px; }\n.fix-batch { width: 82px; padding: 0 10px; }\n.fix-switch { width: 56px; padding: 0; }\n.fix-power { width: 96px; padding: 0 10px; }\n\n@media (hover: hover) {\n  .btn-primary:not(:disabled):hover { background: var(--accent-hover); }\n  .btn-secondary:not(:disabled):hover { background: var(--surface-3); }\n  .btn.surface:not(:disabled):hover, .icon-btn.surface:not(:disabled):hover { background: var(--surface-2-hover); color: var(--text); }\n  .btn-ghost:not(:disabled):hover { background: var(--surface-2); color: var(--text); }\n  .btn.is-danger:not(:disabled):hover, .btn.is-danger-hover:not(:disabled):hover { background: var(--danger-soft); color: var(--danger); }\n  .btn-danger:not(:disabled):hover { opacity: .9; }\n}\n\n.icon-btn {\n  position: relative;\n  width: var(--h-md);\n  height: var(--h-md);\n  padding: 0;\n  border: 0;\n  border-radius: var(--r-ctrl);\n  background: transparent;\n  color: var(--text-2);\n  display: inline-flex;\n  align-items: center;\n  justify-content: center;\n  flex: 0 0 auto;\n  text-decoration: none;\n  transition: background-color var(--dur) var(--ease), color var(--dur) var(--ease);\n}\n.icon-btn-fill { background: var(--surface-2); color: var(--text); }\n.icon-btn.surface { background: var(--surface-1); color: var(--text); box-shadow: var(--shadow-sheet); }\n.icon-btn-sm { width: var(--h-sm); height: var(--h-sm); }\n.icon-btn:disabled { opacity: .4; cursor: not-allowed; }\n@media (hover: hover) {\n  .icon-btn:not(:disabled):not(.surface):not(.icon-btn-fill):hover { background: var(--surface-2); color: var(--text); }\n  .icon-btn-fill:not(:disabled):hover { background: var(--surface-3); color: var(--text); }\n}\n\n.btn-link {\n  display: inline-flex;\n  align-items: center;\n  gap: 4px;\n  height: 24px;\n  padding: 0;\n  border: 0;\n  background: transparent;\n  color: var(--text-2);\n  font-size: var(--fs-xs);\n  font-weight: 500;\n  text-decoration: none;\n  white-space: nowrap;\n  flex-shrink: 0;\n  transition: color var(--dur) var(--ease);\n}\n.btn-link .ico { width: 14px; height: 14px; }\n@media (hover: hover) { .btn-link:hover { color: var(--text); } }\n\n/* ===== 统一分段控件原语 ===== */\n.seg {\n  display: inline-flex;\n  padding: 3px;\n  gap: 2px;\n  border-radius: var(--r-ctrl);\n  background: var(--surface-2);\n  min-width: 0;\n  flex-shrink: 0;\n}\n.seg-on-bg { background: var(--seg-on-bg-track); box-shadow: var(--shadow-sheet); }\n.seg-item {\n  position: relative;\n  height: var(--h-sm);\n  padding: 0 12px;\n  border: 0;\n  border-radius: var(--r-inner);\n  background: transparent;\n  color: var(--text-2);\n  font-size: var(--fs-sm);\n  font-weight: 500;\n  white-space: nowrap;\n  display: inline-flex;\n  align-items: center;\n  justify-content: center;\n  gap: 5px;\n  transition: background-color var(--dur) var(--ease), color var(--dur) var(--ease), box-shadow var(--dur) var(--ease);\n}\n.seg-item.is-active { background: var(--seg-on); color: var(--text); box-shadow: var(--seg-shadow); }\n@media (hover: hover) { .seg-item:not(.is-active):hover { color: var(--text); } }\n.seg-fill { display: flex; width: 100%; }\n.seg-fill .seg-item { flex: 1 1 0; min-width: 0; padding: 0 8px; }\n.seg-2line .seg-item { height: 42px; flex-direction: column; gap: 1px; line-height: 1.28; }\n.seg-val { font-size: var(--fs-sm); font-weight: 500; max-width: 100%; overflow: hidden; text-overflow: ellipsis; }\n.seg-hint { font-size: var(--fs-xs); font-weight: 400; color: var(--text-3); max-width: 100%; overflow: hidden; text-overflow: ellipsis; }\n.seg-count { font-size: var(--fs-xs); color: var(--text-3); font-weight: 400; }\n.seg-item.is-active .seg-count { color: var(--text-2); }\n\n/* ===== 统一输入与柔和焦点原语 ===== */\n.field {\n  border: 0;\n  color: var(--text);\n  caret-color: var(--accent);\n  transition: background-color var(--dur) var(--ease), box-shadow var(--dur) var(--ease);\n}\n.field-on-bg { background: var(--surface-1); box-shadow: var(--shadow-sheet); }\n@media (hover: hover) {\n  .field:hover:not(:focus):not(:focus-within) { background: var(--surface-2-hover); }\n  .field-on-bg:hover:not(:focus):not(:focus-within) { background: var(--surface-1); box-shadow: var(--shadow-sheet), inset 0 0 0 1px var(--edge-pop); }\n}\n.field:focus,\n.field:focus-within {\n  outline: none;\n  background: var(--surface-focus);\n  box-shadow: inset 0 0 0 1px var(--ring-focus), var(--shadow-focus);\n}\n.field:focus-visible,\n.field:has(:focus-visible) {\n  outline: none;\n  box-shadow: inset 0 0 0 1.5px var(--accent-ring), var(--shadow-focus);\n}\n\n.input, .select-trigger {\n  height: var(--h-md);\n  width: 100%;\n  padding: 0 12px;\n  font-size: var(--fs-sm);\n  min-width: 0;\n}\n.textarea {\n  display: block;\n  width: 100%;\n  padding: 13px 14px;\n  font-size: var(--fs-sm);\n  line-height: 1.65;\n  resize: none;\n}\n.input::placeholder, .textarea::placeholder, .search-input::placeholder, .composer-input::placeholder { color: var(--text-3); }\n\n.search {\n  height: var(--h-md);\n  display: flex;\n  align-items: center;\n  gap: 8px;\n  padding: 0 12px;\n  color: var(--text-3);\n  min-width: 220px;\n  max-width: 340px;\n  flex: 1 1 auto;\n}\n.search-input { flex: 1; min-width: 0; height: 100%; border: 0; background: transparent; color: var(--text); font-size: var(--fs-sm); padding: 0; }\n.search-input:focus { outline: none; }\n.search-input::-webkit-search-cancel-button { opacity: .5; }\n\n/* 自绘下拉 */\n.select { position: absolute; opacity: 0; pointer-events: none; width: 1px; height: 1px; }\n.zselect { position: relative; display: block; min-width: 0; }\n.zselect-sm .select-trigger { height: var(--h-sm); padding: 0 10px; font-size: var(--fs-xs); min-width: 96px; }\n.select-trigger {\n  border: 0;\n  border-radius: var(--r-ctrl);\n  background: var(--surface-2);\n  color: var(--text);\n  display: flex;\n  align-items: center;\n  justify-content: space-between;\n  gap: 8px;\n  text-align: left;\n  transition: background-color var(--dur) var(--ease);\n}\n@media (hover: hover) { .select-trigger:not(:disabled):hover { background: var(--surface-3); } }\n.select-trigger .ico { color: var(--text-3); }\n.select-trigger:disabled { opacity: .4; }\n.zselect-label { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n\n/* 自绘复选框（全站替代原生 checkbox） */\n.check {\n  position: relative;\n  display: flex;\n  align-items: center;\n  gap: 9px;\n  height: 30px;\n  font-size: var(--fs-sm);\n  color: var(--text);\n  cursor: pointer;\n  user-select: none;\n}\n.check-input {\n  position: absolute;\n  opacity: 0;\n  width: 1px;\n  height: 1px;\n  margin: 0;\n  pointer-events: none;\n}\n.check-box {\n  width: 16px;\n  height: 16px;\n  flex: 0 0 16px;\n  border-radius: var(--r-check);\n  background: var(--surface-2);\n  box-shadow: inset 0 0 0 1px var(--ring-focus);\n  color: var(--on-accent);\n  display: inline-flex;\n  align-items: center;\n  justify-content: center;\n  transition: background-color var(--dur) var(--ease), box-shadow var(--dur) var(--ease);\n}\n.check-box .ico { width: 12px; height: 12px; stroke-width: 2.5; opacity: 0; transform: scale(.75); transition: opacity var(--dur) var(--ease), transform var(--dur) var(--ease); }\n.check-input:checked + .check-box { background: var(--accent); box-shadow: none; }\n.check-input:checked + .check-box .ico { opacity: 1; transform: scale(1); }\n.check-input:focus-visible + .check-box { outline: 2px solid var(--accent); outline-offset: 2px; }\n\n.progress { position: relative; display: block; height: 4px; border-radius: var(--r-pill); background: var(--surface-3); overflow: hidden; }\n.progress-thin { height: 3px; }\n.progress-fill { position: absolute; left: 0; top: 0; bottom: 0; width: 100%; border-radius: var(--r-pill); background: var(--accent); transform-origin: left center; transform: scaleX(0); transition: transform 300ms var(--ease); }\n.progress-fill.is-warn { background: var(--warn); }\n.progress.is-indeterminate .progress-fill { width: 200%; transform: none; transition: none; background: repeating-linear-gradient(90deg, var(--accent) 0 16px, var(--accent-soft) 16px 32px); animation: stripes 1200ms linear infinite; }\n\n.empty { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; color: var(--text-3); font-size: var(--fs-sm); text-align: center; padding: 32px 16px; }\n\n/* ===== 极简框架：单一固定顶栏 + 全宽内容 ===== */\n.app {\n  height: 100vh;\n  height: 100dvh;\n  padding: var(--gap);\n  gap: var(--gap);\n  display: grid;\n  grid-template-rows: var(--h-md) minmax(0, 1fr);\n  overflow: hidden;\n}\n.topbar {\n  display: flex;\n  align-items: center;\n  gap: 10px;\n  min-width: 0;\n}\n.brand-btn {\n  width: var(--h-md);\n  height: var(--h-md);\n  padding: 5px;\n  border: 0;\n  border-radius: var(--r-ctrl);\n  background: transparent;\n  display: inline-flex;\n  align-items: center;\n  justify-content: center;\n  flex: 0 0 auto;\n  transition: background-color var(--dur) var(--ease);\n}\n@media (hover: hover) { .brand-btn:hover { background: var(--surface-1); } }\n.brand-btn .brand-mark-svg { width: 24px; height: 24px; display: block; overflow: visible; }\n.top-nav .seg-item { width: 76px; padding: 0; }\n.topbar-right { display: flex; align-items: center; gap: 8px; flex: 0 0 auto; }\n\n.pages { position: relative; min-height: 0; min-width: 0; }\n.page { position: absolute; inset: 0; display: none; min-height: 0; }\n.page.is-active { display: flex; }\n\n/* 顶栏统一圆角固定尺寸控件（与 top-nav / desk-logout 同为 --r-ctrl，零抖动） */\n.top-chip {\n  position: relative;\n  height: var(--h-md);\n  padding: 0 11px;\n  border: 0;\n  border-radius: var(--r-ctrl);\n  display: inline-flex;\n  align-items: center;\n  gap: 6px;\n  font-size: var(--fs-sm);\n  font-weight: 500;\n  white-space: nowrap;\n  overflow: hidden;\n  flex: 0 0 auto;\n  transition: background-color var(--dur) var(--ease), color var(--dur) var(--ease);\n}\n@media (hover: hover) { .top-chip:hover { background: var(--surface-2-hover); } }\n.top-chip .ico { color: var(--text-2); }\n\n.mini-ring { width: 16px; height: 16px; flex: 0 0 16px; transform: rotate(-90deg); }\n.ring-track { fill: none; stroke: var(--accent-soft); stroke-width: 2.2; }\n.ring-fill { fill: none; stroke: var(--accent); stroke-width: 2.2; stroke-linecap: round; stroke-dasharray: 40.84; stroke-dashoffset: 40.84; transition: stroke-dashoffset 280ms var(--ease); }\n.mini-ring.is-spin { animation: spin 1200ms linear infinite; }\n.mini-ring.is-spin .ring-fill { stroke-dashoffset: 28; }\n\n.task-pill { width: 172px; justify-content: flex-start; }\n.chip-text { color: var(--text-2); overflow: hidden; text-overflow: ellipsis; }\n.task-pill.is-running { background: var(--accent-soft); color: var(--accent); box-shadow: inset 0 0 0 1px var(--accent-ring); }\n.task-pill.is-running .chip-text { color: var(--accent); }\n.chip-badge { margin-left: auto; font-size: var(--fs-xs); color: var(--accent); background: var(--accent-soft); border-radius: var(--r-inner); padding: 0 6px; line-height: 18px; flex: 0 0 auto; }\n.chip-paused { margin-left: auto; display: inline-flex; align-items: center; gap: 4px; font-size: var(--fs-xs); color: var(--warn); flex: 0 0 auto; }\n.chip-progress { position: absolute; left: 6px; right: 6px; bottom: 2px; height: 2px; border-radius: var(--r-pill); overflow: hidden; }\n.chip-progress-fill { position: absolute; inset: 0; background: var(--accent); border-radius: var(--r-pill); transform-origin: left center; transform: scaleX(0); transition: transform 300ms var(--ease); }\n\n.machine-pill { width: 232px; justify-content: flex-start; }\n.chip-state { overflow: hidden; text-overflow: ellipsis; }\n.chip-right { margin-left: auto; display: inline-flex; align-items: center; gap: 5px; flex: 0 0 auto; }\n.chip-rate { color: var(--text-3); font-weight: 400; font-size: var(--fs-xs); }\n\n/* ===== 创作页：左编辑面板 + 右舞台面板 ===== */\n.page-create { display: none; }\n.page-create.is-active { display: grid; grid-template-columns: 440px minmax(0, 1fr); gap: var(--gap); }\n.editor { display: flex; flex-direction: column; min-height: 0; overflow: hidden; }\n.editor-head { flex: 0 0 auto; display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 14px 16px 4px; }\n.mode-seg .seg-item { width: 76px; padding: 0; }\n.mode-seg .ico { width: 15px; height: 15px; }\n.back-write-btn { display: none; }\n.page-create[data-editor="chat"] .back-write-btn { display: inline-flex; }\n.editor-scroll { flex: 1 1 auto; min-height: 0; overflow: auto; padding: 10px 16px 12px; display: flex; flex-direction: column; gap: 16px; }\n.editor-chat { flex: 1 1 auto; min-height: 0; display: none; flex-direction: column; }\n.page-create[data-editor="chat"] .editor-scroll, .page-create[data-editor="chat"] .editor-foot { display: none; }\n.page-create[data-editor="chat"] .editor-chat { display: flex; }\n.page-create[data-mode="image"] [data-mode-only="video"], .page-create[data-mode="video"] [data-mode-only="image"] { display: none !important; }\n\n.block { display: flex; flex-direction: column; gap: 8px; flex-shrink: 0; }\n.block-prompt { flex: 1 1 auto; min-height: 196px; }\n.block-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 0 2px; }\n.block-title { font-size: var(--fs-sm); font-weight: 600; color: var(--text); }\n.block-aside { font-size: var(--fs-xs); color: var(--text-3); min-width: 48px; text-align: right; }\n.prompt-input { flex: 1 1 auto; min-height: 142px; overflow-y: auto; }\n.prompt-tools { display: flex; align-items: center; gap: 6px; flex-wrap: nowrap; }\n\n.ref-drop {\n  display: flex;\n  align-items: center;\n  justify-content: space-between;\n  gap: 8px;\n  min-height: 50px;\n  transition: background-color var(--dur) var(--ease), box-shadow var(--dur) var(--ease);\n}\n.ref-drop.is-dragover { background: var(--accent-soft); box-shadow: inset 0 0 0 1.5px var(--accent); }\n.ref-drop-btns { display: flex; gap: 6px; flex-shrink: 0; }\n.ref-drop-note { white-space: nowrap; }\n.ref-mounted { display: flex; align-items: center; gap: 10px; padding: 8px 10px; min-width: 0; }\n.ref-thumb { width: 44px; height: 44px; flex: 0 0 44px; border-radius: var(--r-inner); object-fit: cover; background: var(--surface-3); }\n.ref-info { flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; gap: 1px; }\n.ref-name { font-size: var(--fs-sm); font-weight: 500; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n\n.params { display: flex; flex-direction: column; gap: 7px; }\n.param-row { display: flex; align-items: center; gap: 8px; }\n.param-label { flex: 0 0 48px; font-size: var(--fs-xs); color: var(--text-2); padding-left: 2px; }\n.param-row .seg { flex: 1 1 auto; }\n\n.editor-foot {\n  flex: 0 0 auto;\n  display: flex;\n  align-items: center;\n  justify-content: space-between;\n  gap: 12px;\n  padding: 10px 16px 14px;\n  background: var(--surface-1);\n}\n.foot-summary {\n  font-size: var(--fs-xs);\n  color: var(--text-3);\n  overflow: hidden;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n  min-width: 0;\n}\n.run-btn { min-width: 132px; height: 38px; padding: 0 18px; gap: 8px; }\n.kbd { font-size: var(--fs-xs); font-weight: 500; opacity: .72; }\n\n/* AI 帮写 */\n.chat-bar { flex: 0 0 auto; display: flex; align-items: center; gap: 8px; padding: 10px 16px 2px; }\n.chat-ctx { flex: 0 0 auto; display: flex; align-items: center; gap: 8px; padding: 4px 16px 2px; }\n.chip { display: inline-flex; align-items: center; height: 24px; padding: 0 9px; border-radius: var(--r-inner); background: var(--surface-2); color: var(--text-2); font-size: var(--fs-xs); }\n.chip.is-on { background: var(--accent-soft); color: var(--accent); }\n.chat-list { flex: 1 1 auto; min-height: 0; overflow: auto; padding: 12px 16px 16px; display: flex; flex-direction: column; gap: 16px; }\n.chat-empty { margin: auto; color: var(--text-3); font-size: var(--fs-sm); text-align: center; max-width: 260px; }\n.msg { display: flex; flex-direction: column; gap: 8px; max-width: 100%; }\n.msg-user { align-self: flex-end; max-width: 85%; padding: 9px 13px; background: var(--accent-soft); color: var(--text); border-radius: var(--r-ctrl); white-space: pre-wrap; word-break: break-word; }\n.msg-ai { align-self: flex-start; }\n.msg-ai-text { white-space: pre-wrap; word-break: break-word; line-height: 1.65; color: var(--text); }\n.msg-actions { display: flex; gap: 6px; }\n.typing { display: inline-flex; align-items: center; gap: 8px; color: var(--text-2); font-size: var(--fs-xs); }\n.typing-dots { display: inline-flex; gap: 4px; }\n.typing-dots i { width: 6px; height: 6px; border-radius: var(--r-pill); background: var(--text-3); animation: typing 1200ms infinite; }\n.typing-dots i:nth-child(2) { animation-delay: 150ms; }\n.typing-dots i:nth-child(3) { animation-delay: 300ms; }\n.chat-composer { flex: 0 0 auto; padding: 8px 16px 14px; }\n.composer-box { display: flex; align-items: flex-end; gap: 8px; padding: 8px 8px 8px 14px; }\n.composer-input { flex: 1 1 auto; min-width: 0; border: 0; padding: 4px 0; background: transparent; color: var(--text); font-size: var(--fs-sm); line-height: 1.65; resize: none; max-height: 140px; overflow-y: auto; }\n.composer-input:focus { outline: none; }\n.send-btn { width: 32px; height: 32px; flex: 0 0 32px; border: 0; border-radius: var(--r-inner); background: var(--accent); color: var(--on-accent); display: inline-flex; align-items: center; justify-content: center; transition: background-color var(--dur) var(--ease), opacity var(--dur) var(--ease); }\n.send-btn:disabled { opacity: .4; cursor: not-allowed; }\n@media (hover: hover) { .send-btn:not(:disabled):hover { background: var(--accent-hover); } }\n\n/* 舞台（无卡套卡：最近条直接落在舞台底部） */\n.stage { display: flex; flex-direction: column; min-height: 0; min-width: 0; padding: 16px 18px 14px; gap: 12px; overflow: hidden; }\n.stage:focus { outline: none; }\n.stage-view { position: relative; flex: 1 1 auto; min-height: 0; display: flex; align-items: center; justify-content: center; }\n.stage-media { width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; touch-action: pan-y; }\n.stage-media img, .stage-media video { display: block; max-width: 100%; max-height: 100%; object-fit: contain; border-radius: var(--r-ctrl); background: var(--surface-subtle); cursor: zoom-in; }\n.stage-media video { cursor: auto; }\n.stage-nav { position: absolute; top: 50%; transform: translateY(-50%); width: 36px; height: 36px; padding: 0; border: 0; border-radius: var(--r-ctrl); background: var(--viewer-btn); box-shadow: inset 0 0 0 1px var(--viewer-btn-ring); color: var(--on-media); display: flex; align-items: center; justify-content: center; z-index: 3; opacity: .76; transition: opacity var(--dur) var(--ease), background-color var(--dur) var(--ease); }\n.stage-nav.prev { left: 10px; }\n.stage-nav.next { right: 10px; }\n.stage-nav:disabled { opacity: .24; cursor: not-allowed; }\n@media (hover: hover) { .stage-nav:not(:disabled):hover { opacity: 1; background: var(--viewer-btn-hover); } }\n.stage-box { position: relative; overflow: hidden; border-radius: var(--r-ctrl); background: var(--surface-subtle); box-shadow: inset 0 0 0 1px var(--hairline); display: flex; align-items: center; justify-content: center; }\n.stage-run-box { background-image: var(--stage-glow); }\n.skeleton { position: absolute; inset: 0; overflow: hidden; pointer-events: none; }\n.skeleton::after { content: ""; position: absolute; inset: 0; background: linear-gradient(90deg, transparent, var(--shimmer), transparent); animation: shimmer 2400ms ease-in-out infinite; }\n.stage-run-card {\n  position: relative;\n  z-index: 1;\n  display: flex;\n  flex-direction: column;\n  align-items: center;\n  gap: 5px;\n  text-align: center;\n  padding: 16px 24px;\n  border-radius: var(--r-ctrl);\n  background: var(--surface-1);\n  box-shadow: var(--shadow-sheet), inset 0 0 0 1px var(--hairline);\n  min-width: 156px;\n}\n.stage-run-pct { font-size: var(--fs-lg); font-weight: 600; color: var(--accent); line-height: 1.15; }\n.stage-run-pct:empty { display: none; }\n.stage-box-center { position: relative; display: flex; flex-direction: column; align-items: center; gap: 8px; text-align: center; padding: 16px; }\n.stage-phase { font-size: var(--fs-sm); font-weight: 500; color: var(--text); }\n.stage-abort { position: absolute; top: 12px; right: 12px; z-index: 2; }\n.stage-progress { position: absolute; left: 16px; right: 16px; bottom: 14px; height: 4px; border-radius: var(--r-pill); z-index: 2; }\n.fail-ico { color: var(--text-3); }\n.fail-title { font-size: var(--fs-md); font-weight: 600; color: var(--text); }\n.stage-empty { height: 100%; }\n\n.stage-batch { flex: 0 0 auto; display: flex; align-items: center; justify-content: center; gap: 10px; font-size: var(--fs-xs); color: var(--text-2); }\n.stage-batch-track { width: 72px; height: 4px; border-radius: var(--r-pill); background: var(--surface-3); overflow: hidden; position: relative; }\n.stage-batch-fill { position: absolute; inset: 0; border-radius: var(--r-pill); background: var(--accent); transform-origin: left center; }\n\n.stage-actions { flex: 0 0 var(--h-md); height: var(--h-md); display: flex; align-items: center; justify-content: space-between; gap: 10px; min-width: 0; }\n.stage-meta { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; }\n.stage-act-group { display: flex; align-items: center; gap: 6px; flex: 0 0 auto; }\n\n.recent { flex: 0 0 auto; display: flex; flex-direction: column; gap: 8px; min-width: 0; padding-top: 2px; }\n.recent-head { display: flex; align-items: center; justify-content: space-between; height: 22px; }\n.recent-list { display: flex; gap: 8px; overflow-x: auto; overflow-y: hidden; height: 80px; scrollbar-width: none; }\n.recent-list::-webkit-scrollbar { display: none; }\n.recent-item { position: relative; flex: 0 0 54px; width: 54px; height: 80px; padding: 0; border: 0; border-radius: var(--r-inner); overflow: hidden; background: var(--surface-2); }\n.recent-item::after { content: ""; position: absolute; inset: 0; border-radius: inherit; pointer-events: none; }\n.recent-item img, .recent-item video { width: 100%; height: 100%; object-fit: cover; display: block; pointer-events: none; }\n.recent-item.is-current::after { box-shadow: inset 0 0 0 2px var(--accent); }\n.recent-empty { color: var(--text-3); font-size: var(--fs-xs); align-self: center; }\n\n/* ===== 作品页 ===== */\n.page-works { flex-direction: column; gap: var(--gap); }\n.works-toolbar { flex: 0 0 var(--h-md); height: var(--h-md); display: flex; align-items: center; gap: 10px; min-width: 0; }\n.works-seg { flex: 0 0 auto; }\n.works-tools { flex: 1 1 auto; display: flex; align-items: center; gap: 8px; min-width: 0; }\n.works-sheet { flex: 1 1 auto; min-height: 0; display: flex; flex-direction: column; overflow: hidden; }\n.pager-top { flex: 0 0 36px; height: 36px; display: flex; align-items: center; justify-content: space-between; padding: 0 14px; }\n.pager-top-ctl { display: flex; align-items: center; gap: 4px; }\n.pager-top-label { min-width: 48px; text-align: center; }\n.works-scroll { flex: 1 1 auto; min-height: 0; overflow: auto; padding: 2px 14px 16px; }\n.page-works.is-batch .works-scroll { padding-bottom: 90px; }\n\n.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(176px, 1fr)); gap: 10px; }\n.grid.is-list { display: flex; flex-direction: column; gap: 6px; }\n.card { position: relative; aspect-ratio: 2 / 3; border-radius: var(--r-ctrl); overflow: hidden; background: var(--surface-2); cursor: pointer; }\n.card > img, .card > video { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: cover; display: block; }\n.card-overlay { position: absolute; left: 0; right: 0; bottom: 0; height: 42%; display: flex; align-items: flex-end; justify-content: space-between; gap: 4px; padding: 8px; background: linear-gradient(var(--scrim-0), var(--scrim)); opacity: 0; transition: opacity var(--dur) var(--ease); }\n.card-date { font-size: var(--fs-xs); color: var(--on-media); padding: 0 0 4px 4px; white-space: nowrap; }\n.card-btns { display: flex; gap: 4px; }\n.card-btns .icon-btn { color: var(--on-media); }\n@media (hover: hover) {\n  .card:hover .card-overlay, .card:focus-within .card-overlay { opacity: 1; }\n  .card-btns .icon-btn:hover { background: var(--media-soft); color: var(--on-media); }\n}\n.page-works.is-batch .card-overlay { display: none; }\n.card-duration { position: absolute; top: 8px; left: 8px; padding: 0 6px; border-radius: var(--r-inner); background: var(--scrim); color: var(--on-media); font-size: var(--fs-xs); line-height: 20px; }\n.card-check { position: absolute; top: 8px; left: 8px; width: 22px; height: 22px; border-radius: var(--r-pill); border: 1.5px solid var(--on-media); background: var(--media-soft); color: var(--on-media); display: none; align-items: center; justify-content: center; }\n.card-check .ico { width: 14px; height: 14px; stroke-width: 2.5; opacity: 0; }\n.page-works.is-batch .card-check { display: flex; }\n.page-works.is-batch .card-duration { left: 38px; }\n.card.is-selected { box-shadow: 0 0 0 2px var(--accent); }\n.card.is-selected .card-check { background: var(--accent); border-color: var(--accent); }\n.card.is-selected .card-check .ico { opacity: 1; }\n\n.prow { display: flex; align-items: center; gap: 12px; padding: 12px 14px; border-radius: var(--r-ctrl); background: var(--surface-2); cursor: pointer; min-width: 0; transition: background-color var(--dur) var(--ease); }\n@media (hover: hover) { .prow:hover { background: var(--surface-2-hover); } }\n.prow-main { flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; gap: 4px; }\n.prow-text { font-size: var(--fs-sm); color: var(--text); display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; word-break: break-word; }\n.prow-acts { display: flex; gap: 4px; flex: 0 0 auto; }\n.prow-check { width: 22px; height: 22px; flex: 0 0 22px; border-radius: var(--r-pill); border: 1.5px solid var(--text-3); display: none; align-items: center; justify-content: center; color: var(--on-accent); }\n.prow-check .ico { width: 14px; height: 14px; stroke-width: 2.5; opacity: 0; }\n.page-works.is-batch .prow-check { display: flex; }\n.page-works.is-batch .prow-acts { display: none; }\n.prow.is-selected { box-shadow: inset 0 0 0 1.5px var(--accent); }\n.prow.is-selected .prow-check { background: var(--accent); border-color: var(--accent); }\n.prow.is-selected .prow-check .ico { opacity: 1; }\n.grid-empty { grid-column: 1 / -1; }\n\n.pager-bottom { display: flex; align-items: center; justify-content: center; flex-wrap: wrap; gap: 8px; padding: 18px 0 4px; }\n.pager-pages { display: flex; align-items: center; gap: 4px; }\n.page-btn { min-width: var(--h-sm); height: var(--h-sm); padding: 0 8px; border: 0; border-radius: var(--r-ctrl); background: transparent; color: var(--text-2); font-size: var(--fs-xs); font-variant-numeric: tabular-nums; position: relative; transition: background-color var(--dur) var(--ease), color var(--dur) var(--ease); }\n.page-btn.is-current { background: var(--accent-soft); color: var(--accent); font-weight: 600; }\n.page-btn:disabled { opacity: .4; cursor: not-allowed; }\n@media (hover: hover) { .page-btn:not(.is-current):not(:disabled):hover { background: var(--surface-2); color: var(--text); } }\n.page-ellipsis { color: var(--text-3); font-size: var(--fs-xs); padding: 0 4px; }\n\n.batch-bar {\n  position: absolute;\n  left: 50%;\n  bottom: 20px;\n  transform: translateX(-50%);\n  height: 50px;\n  padding: 0 10px;\n  display: flex;\n  align-items: center;\n  gap: 6px;\n  box-shadow: var(--shadow-pop);\n  white-space: nowrap;\n  z-index: 20;\n}\n.batch-count { font-size: var(--fs-sm); font-weight: 600; padding: 0 8px; min-width: 68px; }\n\n/* ===== 浮层（进出对称过渡） ===== */\n.pop {\n  position: fixed;\n  z-index: 50;\n  top: 0;\n  left: 0;\n  box-shadow: var(--shadow-pop);\n  opacity: 0;\n  transform: translateY(4px) scale(.985);\n  transition: opacity var(--dur-out) var(--ease), transform var(--dur-out) var(--ease);\n  max-height: calc(100dvh - 72px);\n  overflow: auto;\n}\n.pop.is-open { opacity: 1; transform: none; transition-duration: var(--dur); }\n.pop-panel { padding: 14px; display: flex; flex-direction: column; gap: 10px; }\n.pop-title { font-size: var(--fs-md); font-weight: 600; }\n.menu-pop { padding: 6px; display: flex; flex-direction: column; gap: 2px; min-width: 168px; }\n.menu-item {\n  position: relative;\n  display: flex;\n  align-items: center;\n  gap: 8px;\n  width: 100%;\n  height: var(--h-md);\n  padding: 0 10px;\n  border: 0;\n  border-radius: var(--r-ctrl);\n  background: transparent;\n  color: var(--text);\n  font-size: var(--fs-sm);\n  text-align: left;\n  white-space: nowrap;\n  text-decoration: none;\n  transition: background-color var(--dur) var(--ease), color var(--dur) var(--ease);\n}\n.menu-item .ico { color: var(--text-2); }\n.menu-item.is-danger, .menu-item.is-danger .ico { color: var(--danger); }\n.menu-item.is-selected { color: var(--accent); }\n.menu-item.is-focused { background: var(--surface-2); }\n.menu-item:disabled { opacity: .4; cursor: not-allowed; }\n@media (hover: hover) {\n  .menu-item:not(:disabled):hover { background: var(--surface-2); }\n  .menu-item.is-danger:not(:disabled):hover { background: var(--danger-soft); }\n}\n.menu-item-2line { height: auto; min-height: 46px; padding: 8px 10px; align-items: flex-start; }\n.menu-item-2line .ico { margin-top: 2px; }\n.mi-text { display: flex; flex-direction: column; gap: 2px; min-width: 0; white-space: normal; }\n.zselect-menu { max-height: 268px; overflow: auto; }\n.zselect-menu .menu-item { justify-content: space-between; }\n\n/* 机器浮层（账号 + 电源预热为主，硬件指标收轻置底） */\n.machine-pop { width: 356px; }\n.acct-box { display: flex; flex-direction: column; gap: 10px; }\n.acct-row { display: flex; align-items: center; gap: 10px; min-width: 0; }\n.avatar { width: var(--h-md); height: var(--h-md); flex: 0 0 var(--h-md); border-radius: var(--r-inner); background: var(--surface-3); color: var(--text); display: inline-flex; align-items: center; justify-content: center; font-size: var(--fs-sm); font-weight: 600; text-transform: uppercase; }\n.acct-main { flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; }\n.acct-email { font-size: var(--fs-sm); font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n.acct-panel { display: flex; flex-direction: column; gap: 8px; }\n.acct-list { display: flex; flex-direction: column; gap: 4px; }\n.acct-item {\n  display: flex;\n  align-items: center;\n  gap: 8px;\n  min-height: 42px;\n  padding: 6px 10px;\n  border: 0;\n  border-radius: var(--r-inner);\n  background: var(--surface-1);\n  text-align: left;\n  width: 100%;\n  transition: background-color var(--dur) var(--ease);\n}\n.acct-item:disabled { opacity: .4; cursor: not-allowed; }\n@media (hover: hover) { .acct-item:not(:disabled):hover { background: var(--surface-3); } }\n.acct-item-main { flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; }\n.acct-item-email { font-size: var(--fs-sm); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n.acct-item-bal { font-size: var(--fs-xs); color: var(--text-2); white-space: nowrap; }\n.acct-item .ico-check { color: var(--accent); }\n.acct-panel-foot { display: flex; gap: 6px; }\n.oauth { display: flex; flex-direction: column; gap: 8px; padding-top: 4px; }\n.oauth-row { display: flex; gap: 8px; align-items: center; }\n.oauth-row .input { flex: 1 1 auto; background: var(--surface-1); }\n.power-row { display: flex; align-items: center; gap: 8px; }\n.warmup { display: flex; flex-direction: column; gap: 4px; }\n.warmup-foot { display: flex; align-items: center; gap: 8px; margin-top: 4px; }\n.hw-strip {\n  display: grid;\n  grid-template-columns: 1fr 1fr;\n  gap: 8px 14px;\n  padding: 8px 10px;\n  border-radius: var(--r-ctrl);\n  background: var(--surface-subtle);\n}\n.hw-cell { display: flex; flex-direction: column; gap: 4px; min-width: 0; }\n.hw-top { display: flex; align-items: baseline; justify-content: space-between; gap: 6px; min-width: 0; }\n.hw-val { font-size: var(--fs-xs); font-weight: 500; color: var(--text-2); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n.mob-logout-row { display: none; justify-content: center; padding-top: 2px; }\n\n/* 随机生成浮层（单行清晰数量格，无误导默认蓝底，悬停高亮） */\n.random-pop { width: 318px; gap: 10px; }\n.rand-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 6px; }\n.rand-item {\n  height: var(--h-md);\n  padding: 0 4px;\n  border: 0;\n  border-radius: var(--r-ctrl);\n  background: var(--surface-2);\n  color: var(--text);\n  font-size: var(--fs-sm);\n  font-weight: 500;\n  font-variant-numeric: tabular-nums;\n  display: inline-flex;\n  align-items: center;\n  justify-content: center;\n  white-space: nowrap;\n  transition: background-color var(--dur) var(--ease), color var(--dur) var(--ease);\n}\n@media (hover: hover) { .rand-item:hover { background: var(--accent-soft); color: var(--accent); } }\n\n.sync-pop { width: 324px; gap: 10px; }\n.sync-head { display: flex; align-items: center; justify-content: space-between; padding: 0 2px; }\n.sync-box { display: flex; flex-direction: column; gap: 8px; }\n.sync-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }\n.sync-stat { display: flex; flex-direction: column; gap: 2px; }\n.sync-num { font-size: var(--fs-md); font-weight: 600; }\n.sync-note.is-warn { color: var(--warn); }\n.sync-actions { display: flex; flex-direction: column; gap: 2px; }\n\n/* ===== 悬浮任务抽屉（桌面位于顶栏下方，不遮挡顶栏控件） ===== */\n.backdrop { position: fixed; inset: 0; z-index: 60; background: var(--overlay); opacity: 0; transition: opacity var(--dur-out) var(--ease); }\n.backdrop.is-open { opacity: 1; transition-duration: var(--dur); }\n.drawer {\n  position: fixed;\n  z-index: 61;\n  top: calc(var(--gap) + var(--h-md) + 8px);\n  right: var(--gap);\n  bottom: var(--gap);\n  width: 400px;\n  max-width: calc(100vw - 24px);\n  box-shadow: var(--shadow-pop);\n  display: flex;\n  flex-direction: column;\n  overflow: hidden;\n  transform: translateX(14px);\n  opacity: 0;\n  transition: transform var(--dur-out) var(--ease), opacity var(--dur-out) var(--ease);\n}\n.drawer.is-open { transform: none; opacity: 1; transition-duration: 180ms; }\n.drawer-grip { display: none; }\n.drawer-head { flex: 0 0 auto; display: flex; align-items: center; justify-content: space-between; padding: 14px 12px 8px 18px; }\n.drawer-title { font-size: var(--fs-md); font-weight: 600; }\n.drawer-seg-wrap { padding: 0 16px 10px; }\n.drawer-body { flex: 1 1 auto; min-height: 0; overflow: auto; padding: 4px 16px 12px; display: flex; flex-direction: column; gap: 10px; }\n.run-block { padding: 12px 14px; background: var(--surface-2); border-radius: var(--r-ctrl); display: flex; flex-direction: column; gap: 8px; }\n.run-line { display: flex; align-items: center; gap: 8px; min-width: 0; }\n.task-name { font-size: var(--fs-sm); font-weight: 500; flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n.run-sub { font-size: var(--fs-xs); color: var(--text-2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n.tlist { display: flex; flex-direction: column; gap: 4px; }\n.trow { display: flex; align-items: center; gap: 8px; min-height: 42px; padding: 4px 10px; border-radius: var(--r-ctrl); background: var(--surface-2); min-width: 0; }\n.trow-id { font-size: var(--fs-xs); color: var(--text-3); flex: 0 0 auto; }\n.trow-time { font-size: var(--fs-xs); color: var(--text-3); white-space: nowrap; flex: 0 0 auto; }\n.trow-acts { display: flex; gap: 4px; flex: 0 0 auto; }\n.hrow { border-radius: var(--r-ctrl); background: var(--surface-2); overflow: hidden; }\n.hrow-head { display: flex; align-items: center; gap: 8px; width: 100%; min-height: 40px; padding: 0 10px; border: 0; background: transparent; text-align: left; min-width: 0; transition: background-color var(--dur) var(--ease); }\n@media (hover: hover) { .hrow-head:hover { background: var(--surface-2-hover); } }\n.st-ico { flex: 0 0 auto; }\n.st-ico.is-ok { color: var(--ok); }\n.st-ico.is-err { color: var(--danger); }\n.st-ico.is-off { color: var(--text-3); }\n.hrow-detail { padding: 2px 12px 12px 34px; display: flex; flex-direction: column; gap: 8px; }\n.hrow-prompt { font-size: var(--fs-xs); color: var(--text-2); white-space: pre-wrap; word-break: break-word; max-height: 200px; overflow: auto; line-height: 1.65; }\n.hrow-cmd { font-family: var(--mono); font-size: var(--fs-xs); color: var(--text-3); word-break: break-all; }\n.hrow-acts { display: flex; gap: 6px; }\n.drawer-note { display: flex; align-items: center; gap: 8px; padding: 0 2px; }\n.drawer-note .hint-2 { flex: 1 1 auto; }\n.drawer-foot-row { display: flex; justify-content: space-between; gap: 8px; padding-top: 4px; }\n.drawer-empty { padding: 36px 0; text-align: center; color: var(--text-3); font-size: var(--fs-sm); }\n.log-wrap { flex: 0 0 auto; padding: 8px 14px 12px; display: flex; flex-direction: column; min-height: 0; }\n.log-wrap.is-open { flex: 0 0 42%; }\n.log {\n  border-radius: var(--r-ctrl);\n  background: var(--surface-subtle);\n  box-shadow: inset 0 0 0 1px var(--hairline);\n  overflow: hidden;\n  display: flex;\n  flex-direction: column;\n  min-height: 0;\n  height: 100%;\n}\n.log-head { flex: 0 0 36px; height: 36px; display: flex; align-items: center; gap: 8px; padding: 0 12px; border: 0; background: transparent; width: 100%; text-align: left; min-width: 0; transition: background-color var(--dur) var(--ease); }\n@media (hover: hover) { .log-head:hover { background: var(--surface-2); } }\n.log-title { font-size: var(--fs-xs); font-weight: 600; color: var(--text-2); flex: 0 0 auto; }\n.log-preview { flex: 1 1 auto; min-width: 0; font-family: var(--mono); font-size: var(--fs-xs); color: var(--text-3); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n.log.is-open .log-preview { visibility: hidden; }\n.log-head .chev { color: var(--text-3); transform: rotate(180deg); transition: transform var(--dur) var(--ease); }\n.log.is-open .log-head .chev { transform: none; }\n.log-box { flex: 1 1 auto; min-height: 0; margin: 0; overflow: auto; padding: 8px 12px 12px; background: var(--bg); color: var(--text-2); font-family: var(--mono); font-size: var(--fs-xs); line-height: 1.5; white-space: pre-wrap; word-break: break-all; }\n\n/* ===== 弹窗（进出对称动画） ===== */\n.dialog { padding: 22px; border: 0; box-shadow: var(--shadow-pop); max-width: calc(100vw - 32px); }\n.dialog::backdrop { background: var(--overlay); transition: opacity var(--dur-out) var(--ease); }\n.dialog[open] { animation: dlg-in var(--dur) var(--ease); }\n.dialog[open]::backdrop { animation: bd-in var(--dur) var(--ease); }\n.dialog.is-closing { animation: dlg-out var(--dur-out) var(--ease) forwards; pointer-events: none; }\n.dialog.is-closing::backdrop { animation: bd-out var(--dur-out) var(--ease) forwards; }\n@keyframes dlg-in { from { opacity: 0; transform: translateY(6px) scale(.985); } to { opacity: 1; transform: none; } }\n@keyframes dlg-out { from { opacity: 1; transform: none; } to { opacity: 0; transform: translateY(6px) scale(.985); } }\n@keyframes bd-in { from { opacity: 0; } to { opacity: 1; } }\n@keyframes bd-out { from { opacity: 1; } to { opacity: 0; } }\n\n.confirm { width: 400px; }\n.dialog-title { font-size: var(--fs-md); font-weight: 600; }\n.dialog-text { margin-top: 8px; font-size: var(--fs-sm); color: var(--text-2); white-space: pre-wrap; }\n.dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 20px; }\n.prompt-dialog { width: 640px; max-height: 80vh; flex-direction: column; }\n.prompt-dialog[open] { display: flex; }\n.pd-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; }\n.pd-titles { display: flex; flex-direction: column; gap: 4px; min-width: 0; }\n.pd-body { margin-top: 14px; flex: 1 1 auto; min-height: 0; overflow: auto; white-space: pre-wrap; word-break: break-word; line-height: 1.65; font-size: var(--fs-sm); color: var(--text); user-select: text; }\n\n.ref-picker { width: 720px; height: 80vh; padding: 0; flex-direction: column; overflow: hidden; }\n.ref-picker[open] { display: flex; }\n.rp-head { flex: 0 0 auto; display: flex; align-items: center; gap: 10px; padding: 14px 16px 10px; }\n.rp-search { min-width: 0; max-width: none; }\n.rp-scroll { flex: 1 1 auto; min-height: 0; overflow: auto; padding: 4px 16px 16px; }\n.rp-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); gap: 10px; }\n.rp-item { position: relative; aspect-ratio: 2 / 3; padding: 0; border: 0; border-radius: var(--r-ctrl); overflow: hidden; background: var(--surface-2); }\n.rp-item img { width: 100%; height: 100%; object-fit: cover; display: block; pointer-events: none; }\n.rp-item.is-current { box-shadow: 0 0 0 2px var(--accent); }\n.rp-badge { position: absolute; top: 6px; right: 6px; width: 22px; height: 22px; border-radius: var(--r-pill); background: var(--accent); color: var(--on-accent); display: flex; align-items: center; justify-content: center; }\n.rp-badge .ico { width: 14px; height: 14px; stroke-width: 2.5; }\n.rp-more { grid-column: 1 / -1; height: 1px; }\n\n/* ===== 详情查看器（无级缩放滑杆 + 滚轮/双指捏合） ===== */\n.viewer {\n  position: fixed;\n  inset: 0;\n  z-index: 70;\n  background: var(--viewer-bg);\n  color: var(--text);\n  display: grid;\n  grid-template-columns: minmax(0, 1fr) 368px;\n  opacity: 0;\n  transition: opacity var(--dur-out) var(--ease);\n}\n.viewer.is-open { opacity: 1; transition-duration: var(--dur); }\n.viewer-mob-top, .viewer-mob-bar, .viewer-sheet-backdrop { display: none; }\n.viewer-canvas {\n  position: relative;\n  min-width: 0;\n  min-height: 0;\n  overflow: hidden;\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  padding: 24px 24px 64px;\n  touch-action: none;\n}\n.viewer-canvas img { max-width: 100%; max-height: 100%; object-fit: contain; display: block; border-radius: var(--r-ctrl); cursor: grab; user-select: none; -webkit-user-drag: none; transform-origin: center center; will-change: transform; }\n.viewer-canvas img.is-anim { transition: transform 180ms var(--ease); }\n.viewer-canvas video { max-width: 100%; max-height: 100%; display: block; border-radius: var(--r-ctrl); }\n.viewer-canvas.is-dragging, .viewer-canvas.is-dragging img { cursor: grabbing; }\n\n.viewer-zoom-bar {\n  position: absolute;\n  left: 50%;\n  bottom: 16px;\n  transform: translateX(-50%);\n  z-index: 3;\n  height: var(--h-md);\n  padding: 0 6px;\n  border-radius: var(--r-ctrl);\n  background: var(--viewer-btn);\n  box-shadow: inset 0 0 0 1px var(--viewer-btn-ring);\n  backdrop-filter: blur(12px);\n  -webkit-backdrop-filter: blur(12px);\n  display: inline-flex;\n  align-items: center;\n  gap: 4px;\n}\n.zoom-step-btn {\n  width: 26px;\n  height: 26px;\n  padding: 0;\n  border: 0;\n  border-radius: var(--r-inner);\n  background: transparent;\n  color: var(--on-media);\n  display: inline-flex;\n  align-items: center;\n  justify-content: center;\n  transition: background-color var(--dur) var(--ease);\n}\n.zoom-step-btn .ico { width: 14px; height: 14px; }\n.zoom-slider {\n  width: 112px;\n  height: 24px;\n  margin: 0 4px;\n  accent-color: var(--accent);\n  cursor: pointer;\n}\n.zoom-pct {\n  min-width: 46px;\n  height: 26px;\n  padding: 0 6px;\n  border: 0;\n  border-radius: var(--r-inner);\n  background: transparent;\n  color: var(--on-media);\n  font-size: var(--fs-xs);\n  font-weight: 500;\n  transition: background-color var(--dur) var(--ease);\n}\n@media (hover: hover) {\n  .zoom-step-btn:hover, .zoom-pct:hover { background: var(--media-soft); }\n}\n.viewer-zoom-pill { display: none; position: absolute; left: 16px; bottom: 16px; z-index: 3; height: var(--h-sm); padding: 0 10px; border: 0; border-radius: var(--r-ctrl); background: var(--viewer-btn); box-shadow: inset 0 0 0 1px var(--viewer-btn-ring); color: var(--on-media); font-size: var(--fs-xs); font-weight: 500; }\n\n.viewer-nav { position: absolute; top: 50%; transform: translateY(-50%); width: 40px; height: 40px; border: 0; border-radius: var(--r-ctrl); background: var(--viewer-btn); box-shadow: inset 0 0 0 1px var(--viewer-btn-ring); color: var(--on-media); display: flex; align-items: center; justify-content: center; z-index: 2; transition: background-color var(--dur) var(--ease); }\n.viewer-nav.prev { left: 16px; }\n.viewer-nav.next { right: 384px; }\n@media (hover: hover) { .viewer-nav:hover { background: var(--viewer-btn-hover); } }\n\n.viewer-info {\n  position: relative;\n  margin: var(--gap) var(--gap) var(--gap) 0;\n  display: flex;\n  flex-direction: column;\n  gap: 10px;\n  padding: 16px;\n  min-height: 0;\n  overflow: hidden;\n}\n.viewer-grip { display: none; }\n.viewer-info-head { display: flex; align-items: center; gap: 8px; min-width: 0; }\n.viewer-name { flex: 1 1 auto; min-width: 0; font-size: var(--fs-sm); font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }\n#viewerMeta { display: flex; flex-wrap: wrap; gap: 2px 0; line-height: 1.45; }\n.meta-tok { white-space: nowrap; }\n.meta-tok[data-sep]::before { content: "\\00A0·\\00A0"; opacity: .6; }\n.viewer-prompt-box { flex: 1 1 auto; min-height: 0; display: flex; flex-direction: column; gap: 8px; }\n.viewer-prompt-head { display: flex; align-items: center; justify-content: space-between; }\n.viewer-prompt { flex: 1 1 auto; min-height: 0; overflow: auto; font-size: var(--fs-sm); line-height: 1.65; color: var(--text); white-space: pre-wrap; word-break: break-word; user-select: text; }\n.viewer-prompt.is-empty { color: var(--text-3); }\n.viewer-actions { display: flex; flex-wrap: wrap; gap: 8px; padding-top: 2px; }\n.viewer-actions .btn { flex: 1 1 0; padding: 0 8px; }\n.viewer-actions .btn-primary,\n.viewer-actions .viewer-del-btn { flex: 1 0 100%; }\n\n/* 桌面自绘磨砂提示 + Toast */\n.ui-tip {\n  position: fixed;\n  z-index: 95;\n  top: 0;\n  left: 0;\n  max-width: 260px;\n  padding: 5px 9px;\n  border-radius: var(--r-inner);\n  background: var(--tip-bg);\n  color: var(--on-accent);\n  box-shadow: 0 8px 24px rgba(0,0,0,.28), inset 0 0 0 1px var(--tip-ring);\n  backdrop-filter: blur(12px);\n  -webkit-backdrop-filter: blur(12px);\n  font-size: var(--fs-xs);\n  line-height: 1.4;\n  white-space: nowrap;\n  overflow: hidden;\n  text-overflow: ellipsis;\n  pointer-events: none;\n  opacity: 0;\n  transform: translateY(3px);\n  transition: opacity var(--dur-out) var(--ease), transform var(--dur-out) var(--ease);\n}\n.ui-tip.is-open { opacity: 1; transform: none; transition-duration: 120ms; }\n@media (hover: none), (pointer: coarse) { .ui-tip { display: none !important; } }\n\n.toast { position: fixed; left: 50%; bottom: 24px; z-index: 90; max-width: calc(100vw - 32px); padding: 9px 16px; border-radius: var(--r-ctrl); background: var(--toast-bg); color: var(--on-accent); font-size: var(--fs-sm); box-shadow: var(--shadow-pop); opacity: 0; transform: translate(-50%, 6px); pointer-events: none; transition: opacity var(--dur-out) var(--ease), transform var(--dur-out) var(--ease); }\n.toast.is-visible { opacity: 1; transform: translate(-50%, 0); transition-duration: var(--dur); }\n\n/* ===== 1600+ ===== */\n@media (min-width: 1600px) {\n  .page-create.is-active { grid-template-columns: 480px minmax(0, 1fr); }\n}\n\n/* ===== 平板 & 手机：与桌面完全相同的双面板结构，竖排堆叠 ===== */\n@media (max-width: 1099px) {\n  .page-create.is-active { display: flex; flex-direction: column; gap: var(--gap); overflow-y: auto; overflow-x: hidden; padding-bottom: 12px; }\n  .editor, .stage { flex: 0 0 auto; overflow: visible; }\n  .editor-scroll { flex: 0 0 auto; overflow: visible; }\n  .block-prompt { flex: 0 0 auto; min-height: 0; }\n  .prompt-input { flex: 0 0 auto; min-height: 136px; max-height: 40vh; }\n  .editor-chat { height: 520px; }\n  .stage { padding: 14px; }\n  .stage-view { flex: 0 0 auto; height: auto; }\n  .page-create.is-blank .stage-view { height: 140px; }\n  .stage-view.is-fit .stage-media { height: auto; }\n  .stage-view.is-fit .stage-media img, .stage-view.is-fit .stage-media video { max-height: 48vh; }\n  .kbd { display: none; }\n  .stage-actions { justify-content: center; }\n  .stage-actions .wide-only { display: none; }\n  .stage-meta { display: none; }\n  .stage-act-group .btn { padding: 0 12px; }\n}\n\n/* ===== 手机 < 768（原生页面自然滚动，顶部与底部零固定块，与桌面主页完全一致） ===== */\n@media (max-width: 767px) {\n  :root { --gap: 10px; }\n  html, body { height: auto; overflow-x: hidden; overflow-y: auto; }\n  .app {\n    height: auto;\n    min-height: 100dvh;\n    overflow: visible;\n    display: flex;\n    flex-direction: column;\n    gap: var(--gap);\n    padding: calc(var(--safe-t) + 8px) 8px calc(16px + var(--safe-b));\n  }\n  .topbar { gap: 6px; }\n  .brand-btn, .desk-logout { display: none; }\n  .top-nav .ico { display: none; }\n  .top-nav .seg-item { width: 56px; }\n  .pages { position: static; flex: 1 0 auto; }\n  .page { position: static; }\n\n  .top-chip { padding: 0 8px; gap: 5px; }\n  .task-pill { width: 78px; justify-content: center; }\n  .task-pill .chip-text { overflow: visible; text-overflow: clip; flex: 0 0 auto; }\n  .task-pill .chip-text.is-compact-hide { display: none; }\n  .task-pill .chip-badge, .task-pill .chip-paused { margin-left: 0; }\n  .machine-pill { width: 92px; justify-content: center; }\n  .machine-pill .chip-state, .machine-pill .chip-rate { display: none; }\n  .machine-pill .chip-right { margin-left: 0; }\n  .mob-logout-row { display: flex; }\n\n  .page-create.is-active { overflow: visible; padding-bottom: 0; }\n  .mode-seg .seg-item { width: 68px; }\n  .editor-head { padding: 12px 12px 4px; }\n  .editor-scroll { padding: 8px 12px 10px; gap: 14px; }\n  .editor-foot { padding: 6px 12px 12px; }\n  .editor-chat { height: calc(100dvh - 130px); min-height: 360px; }\n  .prompt-input { min-height: 132px; }\n  .ref-drop-note { display: none; }\n  .ref-drop-btns { width: 100%; }\n  .ref-drop-btns .btn { flex: 1 1 0; }\n  .param-label { flex-basis: 44px; }\n  .seg-2line .seg-item { height: 42px; }\n  .stage { padding: 12px; }\n  .stage-nav { width: 32px; height: 32px; }\n  .recent-list { height: 74px; }\n  .recent-item { flex: 0 0 50px; width: 50px; height: 74px; }\n\n  .works-toolbar { flex: 0 0 auto; height: auto; flex-direction: column; align-items: stretch; gap: 8px; }\n  .works-seg { overflow-x: auto; max-width: 100%; }\n  .works-tools .search { min-width: 0; max-width: none; }\n  .tool-btn { width: var(--h-md); padding: 0; }\n  .tool-btn .tool-label { display: none; }\n  .tool-btn.is-on { background: var(--accent-soft); color: var(--accent); box-shadow: none; }\n  .works-sheet { overflow: visible; }\n  .pager-top { height: 30px; flex-basis: 30px; padding: 0 10px; }\n  .works-scroll { overflow: visible; padding: 0 10px 12px; }\n  .page-works.is-batch .works-scroll { padding-bottom: 68px; }\n  .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }\n  .batch-bar { position: fixed; bottom: calc(12px + var(--safe-b)); left: 8px; right: 8px; transform: none; padding: 0 8px; gap: 4px; justify-content: space-between; }\n  .batch-bar .btn { padding: 0 8px; }\n  .batch-bar .wide-text { display: none; }\n  .batch-count { min-width: auto; }\n  .pager-bottom { padding: 14px 4px 4px; }\n  .pager-bottom .hint-2 { display: none; }\n\n  .pop.machine-pop, .pop.sync-pop, .pop.random-pop { width: calc(100vw - 16px); }\n  .drawer { top: auto; left: 0; right: 0; bottom: 0; width: 100%; max-width: 100vw; height: 85vh; height: 85dvh; border-radius: var(--r-panel) var(--r-panel) 0 0; transform: translateY(16px); padding-bottom: var(--safe-b); }\n  .drawer-grip { display: block; width: 36px; height: 4px; border-radius: var(--r-pill); background: var(--surface-3); margin: 8px auto 0; flex: 0 0 auto; }\n  .drawer-head { padding: 8px 12px 6px 18px; }\n  .ref-picker { width: 100vw; max-width: 100vw; height: 85vh; height: 85dvh; margin: auto 0 0; border-radius: var(--r-panel) var(--r-panel) 0 0; }\n  .rp-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 6px; }\n  .rp-head { flex-wrap: wrap; padding: 12px; }\n  .rp-head .rp-search { order: 3; flex: 1 1 100%; }\n  .rp-head .icon-btn { margin-left: auto; }\n  .confirm, .prompt-dialog { width: calc(100vw - 32px); }\n  .toast { bottom: calc(20px + var(--safe-b)); }\n\n  .viewer { display: flex; flex-direction: column; background: #000000; }\n  .viewer-canvas { flex: 1 1 auto; padding: 0; }\n  .viewer.is-video .viewer-canvas { padding: calc(56px + var(--safe-t)) 0 calc(76px + var(--safe-b)); }\n  .viewer-canvas img { border-radius: 0; }\n  .viewer-zoom-bar { display: none !important; }\n  .viewer-zoom-pill { display: inline-flex; align-items: center; bottom: calc(16px + var(--safe-b)); backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px); }\n  .viewer-nav { display: none; }\n\n  .viewer-mob-top {\n    position: absolute;\n    top: 0;\n    left: 0;\n    right: 0;\n    z-index: 4;\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    padding: calc(var(--safe-t) + 10px) 12px 18px;\n    background: linear-gradient(180deg, rgba(0,0,0,.58), transparent);\n    pointer-events: none;\n    transition: opacity 180ms var(--ease), transform 180ms var(--ease);\n  }\n  .viewer-mob-top > * { pointer-events: auto; }\n  .viewer-hud-btn {\n    height: 36px;\n    min-width: 36px;\n    padding: 0;\n    border: 0;\n    border-radius: var(--r-ctrl);\n    background: var(--viewer-btn);\n    box-shadow: inset 0 0 0 1px var(--viewer-btn-ring);\n    backdrop-filter: blur(14px);\n    -webkit-backdrop-filter: blur(14px);\n    color: var(--on-media);\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    gap: 5px;\n    font-size: var(--fs-xs);\n    font-weight: 500;\n  }\n  .viewer-hud-pill { padding: 0 12px; }\n  .viewer-counter {\n    height: 28px;\n    padding: 0 10px;\n    border-radius: var(--r-ctrl);\n    background: var(--viewer-btn);\n    box-shadow: inset 0 0 0 1px var(--viewer-btn-ring);\n    backdrop-filter: blur(14px);\n    -webkit-backdrop-filter: blur(14px);\n    color: var(--on-media);\n    font-size: var(--fs-xs);\n    font-weight: 500;\n    display: inline-flex;\n    align-items: center;\n  }\n  .viewer-counter:empty { display: none; }\n\n  .viewer-mob-bar {\n    position: absolute;\n    left: 12px;\n    right: 12px;\n    bottom: calc(12px + var(--safe-b));\n    z-index: 4;\n    height: 54px;\n    padding: 4px;\n    border-radius: var(--r-panel);\n    background: var(--viewer-btn);\n    box-shadow: inset 0 0 0 1px var(--viewer-btn-ring), 0 10px 30px rgba(0,0,0,.42);\n    backdrop-filter: blur(16px);\n    -webkit-backdrop-filter: blur(16px);\n    display: flex;\n    align-items: stretch;\n    gap: 2px;\n    transition: opacity 180ms var(--ease), transform 180ms var(--ease);\n  }\n  .mob-act {\n    flex: 1 1 0;\n    min-width: 0;\n    padding: 0;\n    border: 0;\n    border-radius: var(--r-ctrl);\n    background: transparent;\n    color: var(--on-media);\n    text-decoration: none;\n    display: flex;\n    flex-direction: column;\n    align-items: center;\n    justify-content: center;\n    gap: 2px;\n    font-size: var(--fs-xs);\n    font-weight: 500;\n    transition: background-color var(--dur) var(--ease);\n  }\n  .mob-act .ico { width: 17px; height: 17px; }\n  .mob-act.is-danger { color: var(--danger); }\n  .mob-act:active { background: var(--media-soft); }\n\n  .viewer.is-chrome-hidden .viewer-mob-top,\n  .viewer.is-zoomed .viewer-mob-top,\n  .viewer.is-sheet-open .viewer-mob-top { opacity: 0; transform: translateY(-10px); pointer-events: none; }\n  .viewer.is-chrome-hidden .viewer-mob-bar,\n  .viewer.is-zoomed .viewer-mob-bar,\n  .viewer.is-sheet-open .viewer-mob-bar { opacity: 0; transform: translateY(14px); pointer-events: none; }\n\n  .viewer-sheet-backdrop {\n    display: block;\n    position: absolute;\n    inset: 0;\n    z-index: 5;\n    background: rgba(0,0,0,.48);\n    opacity: 0;\n    pointer-events: none;\n    transition: opacity 180ms var(--ease);\n  }\n  .viewer.is-sheet-open .viewer-sheet-backdrop { opacity: 1; pointer-events: auto; }\n\n  .viewer-info {\n    position: absolute;\n    left: 0;\n    right: 0;\n    bottom: 0;\n    z-index: 6;\n    margin: 0;\n    max-height: 76dvh;\n    border-radius: var(--r-panel) var(--r-panel) 0 0;\n    padding: 0 16px calc(16px + var(--safe-b));\n    transform: translateY(104%);\n    pointer-events: none;\n    transition: transform 200ms var(--ease);\n  }\n  .viewer.is-sheet-open .viewer-info { transform: none; pointer-events: auto; }\n  .viewer-grip { display: flex; justify-content: center; align-items: center; height: 26px; border: 0; background: transparent; width: 100%; flex: 0 0 auto; padding: 0; }\n  .viewer-grip span { width: 36px; height: 4px; border-radius: var(--r-pill); background: var(--surface-3); }\n  .viewer-info-head .viewer-name { font-size: var(--fs-xs); color: var(--text-2); }\n  .viewer-prompt-box { min-height: 120px; max-height: 36dvh; }\n}\n\n@media (min-width: 768px) and (max-width: 1099px) {\n  .grid { grid-template-columns: repeat(auto-fill, minmax(168px, 1fr)); }\n}\n\n@media (prefers-reduced-motion: reduce) {\n  .pop, .drawer, .toast, .ui-tip, .dialog[open], .dialog.is-closing { transform: none !important; animation: none; }\n  .skeleton::after, .progress.is-indeterminate .progress-fill, .dot-breath, .mini-ring.is-spin { animation: none; }\n  .viewer-info { transition: none; }\n}\n  </style>\n</head>\n<body data-page="create">\n<svg xmlns="http://www.w3.org/2000/svg" class="sprite" aria-hidden="true">\n  <symbol id="i-refresh" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></symbol>\n  <symbol id="i-power" viewBox="0 0 24 24"><path d="M12 2v10"/><path d="M18.4 6.6a9 9 0 1 1-12.77.04"/></symbol>\n  <symbol id="i-bolt" viewBox="0 0 24 24"><path d="M13 2 3 14h8l-1 8 10-12h-8l1-8z"/></symbol>\n  <symbol id="i-external" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></symbol>\n  <symbol id="i-image" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="9" cy="9" r="2"/><path d="m21 15-3.086-3.086a2 2 0 0 0-2.828 0L6 21"/></symbol>\n  <symbol id="i-video" viewBox="0 0 24 24"><rect x="2" y="5" width="14" height="14" rx="2"/><path d="m22 7-6 5 6 5V7z"/></symbol>\n  <symbol id="i-sparkles" viewBox="0 0 24 24"><path d="m12 3-1.9 5.8a2 2 0 0 1-1.3 1.3L3 12l5.8 1.9a2 2 0 0 1 1.3 1.3L12 21l1.9-5.8a2 2 0 0 1 1.3-1.3L21 12l-5.8-1.9a2 2 0 0 1-1.3-1.3Z"/></symbol>\n  <symbol id="i-upload" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></symbol>\n  <symbol id="i-download" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></symbol>\n  <symbol id="i-trash" viewBox="0 0 24 24"><path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></symbol>\n  <symbol id="i-grid" viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/></symbol>\n  <symbol id="i-plus" viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></symbol>\n  <symbol id="i-close" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></symbol>\n  <symbol id="i-search" viewBox="0 0 24 24"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></symbol>\n  <symbol id="i-logout" viewBox="0 0 24 24"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></symbol>\n  <symbol id="i-dice" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="3"/><circle cx="8.5" cy="8.5" r="1.25" fill="currentColor" stroke="none"/><circle cx="15.5" cy="8.5" r="1.25" fill="currentColor" stroke="none"/><circle cx="12" cy="12" r="1.25" fill="currentColor" stroke="none"/><circle cx="8.5" cy="15.5" r="1.25" fill="currentColor" stroke="none"/><circle cx="15.5" cy="15.5" r="1.25" fill="currentColor" stroke="none"/></symbol>\n  <symbol id="i-arrow-up" viewBox="0 0 24 24"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></symbol>\n  <symbol id="i-chevron" viewBox="0 0 24 24"><polyline points="6 9 12 15 18 9"/></symbol>\n  <symbol id="i-check" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></symbol>\n  <symbol id="i-check-square" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="3"/><path d="m9 12 2 2 4-4"/></symbol>\n  <symbol id="i-list" viewBox="0 0 24 24"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></symbol>\n  <symbol id="i-cloud" viewBox="0 0 24 24"><path d="M17.5 19H9a7 7 0 1 1 6.71-9h1.79a4.5 4.5 0 1 1 0 9Z"/></symbol>\n  <symbol id="i-send" viewBox="0 0 24 24"><path d="m22 2-7 20-4-9-9-4Z"/><path d="M22 2 11 13"/></symbol>\n  <symbol id="i-more" viewBox="0 0 24 24"><circle cx="5" cy="12" r="1.25" fill="currentColor" stroke="none"/><circle cx="12" cy="12" r="1.25" fill="currentColor" stroke="none"/><circle cx="19" cy="12" r="1.25" fill="currentColor" stroke="none"/></symbol>\n  <symbol id="i-chevron-left" viewBox="0 0 24 24"><polyline points="15 18 9 12 15 6"/></symbol>\n  <symbol id="i-chevron-right" viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6"/></symbol>\n  <symbol id="i-minus" viewBox="0 0 24 24"><line x1="5" y1="12" x2="19" y2="12"/></symbol>\n</svg>\n\n<div class="app" id="app">\n  <header class="topbar">\n    <button type="button" class="brand-btn" data-action="brand-home" aria-label="回到创作" data-tip="G4 工作台"><svg class="brand-mark-svg" viewBox="68 48 896 904" aria-hidden="true"><path d="M 642 238 A 308 308 0 1 0 464 786" fill="none" stroke="#377DF6" stroke-width="138" stroke-linecap="round"/><path d="M 304 338 A 182 182 0 0 0 284 568" fill="none" stroke="#64BAFF" stroke-width="44" stroke-linecap="round" opacity="0.92"/><path d="M 456 336 C 469 402, 490 423, 556 436 C 490 449, 469 470, 456 536 C 443 470, 422 449, 356 436 C 422 423, 443 402, 456 336 Z" fill="#F6BA2A"/><path d="M 786 352 L 558 688 L 904 688" fill="none" stroke="#4A90FF" stroke-width="116" stroke-linecap="round" stroke-linejoin="round"/><path d="M 786 352 L 786 888" fill="none" stroke="#2260DF" stroke-width="116" stroke-linecap="round"/></svg></button>\n    <nav class="seg seg-on-bg top-nav" id="topNav" aria-label="主导航">\n      <button type="button" class="seg-item is-active" data-action="nav-page" data-page="create" aria-current="page">\n        <svg class="ico" aria-hidden="true"><use href="#i-sparkles"/></svg><span>创作</span>\n      </button>\n      <button type="button" class="seg-item" data-action="nav-page" data-page="works">\n        <svg class="ico" aria-hidden="true"><use href="#i-grid"/></svg><span>作品</span>\n      </button>\n    </nav>\n    <span class="row-spacer"></span>\n    <div class="topbar-right">\n      <button type="button" class="surface top-chip task-pill" id="taskPill" data-action="toggle-task-panel" aria-haspopup="dialog" aria-expanded="false" aria-controls="taskDrawer" data-tip="任务队列">\n        <svg class="ico task-ico-idle" id="taskIcoIdle" aria-hidden="true"><use href="#i-list"/></svg>\n        <svg class="mini-ring task-ico-run" id="taskRing" viewBox="0 0 18 18" aria-hidden="true" hidden>\n          <circle class="ring-track" cx="9" cy="9" r="6.5"/>\n          <circle class="ring-fill" id="taskRingFill" cx="9" cy="9" r="6.5"/>\n        </svg>\n        <span class="chip-text num" id="taskPillText">空闲</span>\n        <span class="chip-badge num" id="taskPillQueue" hidden></span>\n        <span class="chip-paused num" id="taskPillPaused" hidden><span class="dot dot-warn" aria-hidden="true"></span><span id="taskPillPausedText"></span></span>\n        <span class="chip-progress" id="taskPillProgress" hidden><span class="chip-progress-fill" id="taskPillProgressFill"></span></span>\n      </button>\n      <button type="button" class="surface top-chip machine-pill" id="machinePill" data-action="toggle-machine" aria-haspopup="dialog" aria-expanded="false" aria-controls="machinePop" data-tip="机器状态与账号">\n        <span class="dot" id="machineDot" aria-hidden="true"></span>\n        <span class="chip-state" id="machineText">状态未知</span>\n        <span class="chip-right num">\n          <span class="chip-bal" id="machineBal">— CU</span>\n          <span class="chip-rate" id="machineRate" hidden></span>\n        </span>\n      </button>\n      <button type="button" class="icon-btn surface desk-logout" data-action="logout-panel" aria-label="退出登录" data-tip="退出登录">\n        <svg class="ico" aria-hidden="true"><use href="#i-logout"/></svg>\n      </button>\n    </div>\n  </header>\n\n  <main class="pages">\n    <!-- ============ 创作 ============ -->\n    <section class="page page-create is-active" id="pageCreate" data-page="create" data-mode="image" data-editor="write">\n      <div class="panel editor">\n        <div class="editor-head">\n          <div class="seg mode-seg" id="modeSeg" role="group" aria-label="创作类型">\n            <button type="button" class="seg-item is-active" data-action="set-mode" data-mode="image" aria-pressed="true"><svg class="ico" aria-hidden="true"><use href="#i-image"/></svg><span>图片</span></button>\n            <button type="button" class="seg-item" data-action="set-mode" data-mode="video" aria-pressed="false"><svg class="ico" aria-hidden="true"><use href="#i-video"/></svg><span>视频</span></button>\n          </div>\n          <button type="button" class="btn btn-secondary btn-sm back-write-btn" id="backWriteBtn" data-action="set-editor-tab" data-tab="write">\n            <svg class="ico" aria-hidden="true"><use href="#i-chevron-left"/></svg><span>返回编写</span>\n          </button>\n        </div>\n\n        <div class="editor-scroll" id="editorWrite">\n          <section class="block block-prompt" aria-labelledby="promptTitle">\n            <div class="block-head">\n              <label class="block-title" id="promptTitle" for="promptInputImage">提示词</label>\n              <span class="block-aside num" id="promptCount">0 字</span>\n            </div>\n            <textarea id="promptInputImage" class="well field textarea prompt-input" data-mode-only="image" aria-label="图片提示词" placeholder="描述画面，或点下方「AI 帮写」让提示词大师生成" spellcheck="false"></textarea>\n            <textarea id="promptInputVideo" class="well field textarea prompt-input" data-mode-only="video" aria-label="视频提示词" placeholder="描述动作、运镜、台词与环境音" spellcheck="false"></textarea>\n            <div class="prompt-tools">\n              <button type="button" class="btn btn-secondary" id="aiHelpBtn" data-action="set-editor-tab" data-tab="chat" data-tip="打开多轮提示词大师对话">\n                <svg class="ico" aria-hidden="true"><use href="#i-sparkles"/></svg><span>AI 帮写</span>\n              </button>\n              <button type="button" class="btn btn-secondary fix-rand random-trigger" id="randomBtn" data-mode-only="image" data-action="toggle-random-picker" aria-haspopup="dialog" aria-expanded="false" aria-controls="randomPop">\n                <svg class="ico" aria-hidden="true"><use href="#i-dice"/></svg><span>随机</span><svg class="ico chev" aria-hidden="true"><use href="#i-chevron"/></svg>\n              </button>\n              <span class="row-spacer"></span>\n              <button type="button" class="btn btn-secondary" data-action="clear-prompt">清空</button>\n            </div>\n          </section>\n\n          <section class="block" id="refBlock" aria-labelledby="refTitle">\n            <div class="block-head">\n              <span class="block-title" id="refTitle">人脸参考</span>\n              <span class="hint" id="refHint">可选，锁定人脸</span>\n            </div>\n            <div class="ref-zone ref-drop" id="refEmpty">\n              <div class="ref-drop-btns">\n                <button type="button" class="btn btn-secondary btn-sm" data-action="open-ref-picker">从作品选择</button>\n                <button type="button" class="btn btn-secondary btn-sm" data-action="trigger-upload"><svg class="ico" aria-hidden="true"><use href="#i-upload"/></svg><span>上传图片</span></button>\n              </div>\n              <span class="hint-3 ref-drop-note">或拖入图片文件</span>\n            </div>\n            <div class="ref-zone ref-mounted" id="refMounted" hidden>\n              <img class="ref-thumb" id="refThumb" alt="">\n              <div class="ref-info">\n                <span class="ref-name" id="refName"></span>\n                <span class="hint-2" id="refSub">已挂载为人脸参考</span>\n              </div>\n              <button type="button" class="btn btn-ghost btn-sm" data-action="open-ref-picker">更换</button>\n              <button type="button" class="icon-btn icon-btn-sm" data-action="clear-ref" aria-label="移除" data-tip="移除"><svg class="ico" aria-hidden="true"><use href="#i-close"/></svg></button>\n            </div>\n          </section>\n\n          <section class="block" aria-label="参数">\n            <div class="block-head"><span class="block-title">参数</span></div>\n            <div class="params" data-mode-only="image">\n              <div class="param-row">\n                <span class="param-label">尺寸</span>\n                <div class="seg seg-fill seg-2line" id="segImgRes" role="group" aria-label="尺寸">\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="imgRes" data-val="portrait"><span class="seg-val">竖图</span><span class="seg-hint num" id="lblResPortrait">2048×3072</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="imgRes" data-val="landscape"><span class="seg-val">横图</span><span class="seg-hint num" id="lblResLandscape">3072×2048</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="imgRes" data-val="square"><span class="seg-val">方图</span><span class="seg-hint num" id="lblResSquare">2560×2560</span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">步数</span>\n                <div class="seg seg-fill seg-2line" id="segImgSteps" role="group" aria-label="步数">\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="imgSteps" data-val="tier1"><span class="seg-val num" id="lblStepsTier1">13 步</span><span class="seg-hint num" id="hintStepsTier1">8+5</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="imgSteps" data-val="tier2"><span class="seg-val num" id="lblStepsTier2">18 步</span><span class="seg-hint num" id="hintStepsTier2">12+6 默认</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="imgSteps" data-val="tier3"><span class="seg-val num" id="lblStepsTier3">24 步</span><span class="seg-hint num" id="hintStepsTier3">16+8</span></button>\n                </div>\n              </div>\n            </div>\n            <div class="params" data-mode-only="video">\n              <div class="param-row">\n                <span class="param-label">清晰度</span>\n                <div class="seg seg-fill seg-2line" id="segVidQuality" role="group" aria-label="清晰度">\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidQuality" data-val="std"><span class="seg-val">标准 832p</span><span class="seg-hint">直出</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidQuality" data-val="hd"><span class="seg-val">超清 1088p</span><span class="seg-hint">3D 超分</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidQuality" data-val="uhd2k"><span class="seg-val">2K 精修</span><span class="seg-hint">超分 + 重绘</span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">画幅</span>\n                <div class="seg seg-fill seg-2line" id="segVidRes" role="group" aria-label="画幅">\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidRes" data-val="832x1248"><span class="seg-val num" id="lblVidResP">832×1248</span><span class="seg-hint">竖屏 2:3</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidRes" data-val="1248x832"><span class="seg-val num" id="lblVidResL">1248×832</span><span class="seg-hint">横屏 3:2</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidRes" data-val="1024x1024"><span class="seg-val num" id="lblVidResS">1024×1024</span><span class="seg-hint">方屏 1:1</span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">时长</span>\n                <div class="seg seg-fill seg-2line" id="segVidLen" role="group" aria-label="时长">\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidLen" data-val="192"><span class="seg-val num">8.0 秒</span><span class="seg-hint num">192 帧</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidLen" data-val="243"><span class="seg-val num">10.1 秒</span><span class="seg-hint num">243 帧</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidLen" data-val="124"><span class="seg-val num">5.2 秒</span><span class="seg-hint num">124 帧</span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">步数</span>\n                <div class="seg seg-fill seg-2line" id="segVidSteps" role="group" aria-label="视频步数">\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidSteps" data-val="20"><span class="seg-val num">20 步</span><span class="seg-hint">标准</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidSteps" data-val="25"><span class="seg-val num">25 步</span><span class="seg-hint">细节更多</span></button>\n                  <button type="button" class="seg-item" data-action="set-seg" data-key="vidSteps" data-val="16"><span class="seg-val num">16 步</span><span class="seg-hint">快速</span></button>\n                </div>\n              </div>\n            </div>\n          </section>\n        </div>\n\n        <div class="editor-chat" id="editorChat">\n          <div class="chat-bar">\n            <span class="chip num" id="chatInfoText">Qwen 27B · 0 轮</span>\n            <span class="row-spacer"></span>\n            <button type="button" class="btn-link" data-action="clear-chat">新对话</button>\n          </div>\n          <div class="chat-list fade-y" id="chatList" aria-live="polite"></div>\n          <div class="chat-composer">\n            <div class="well field composer-box">\n              <textarea id="chatInput" class="composer-input" rows="1" aria-label="对话输入" placeholder="描述想要的场景、风格，或让它改某一句" spellcheck="false"></textarea>\n              <button type="button" class="send-btn" id="chatSendBtn" data-action="submit-task" aria-label="发送" data-tip="发送"><svg class="ico" aria-hidden="true"><use href="#i-send"/></svg></button>\n            </div>\n          </div>\n        </div>\n\n        <div class="editor-foot" id="editorFoot">\n          <span class="foot-summary num" id="footSummary">竖图 · 18 步</span>\n          <button type="button" class="btn btn-primary run-btn" id="runBtn" data-action="submit-task">\n            <span id="runBtnText">生成</span><span class="kbd" id="runKbd">Ctrl↵</span>\n          </button>\n        </div>\n      </div>\n\n      <div class="panel stage" id="stage" tabindex="0" aria-label="结果">\n        <div class="stage-view" id="stageView">\n          <div class="stage-media" id="stageMedia" hidden></div>\n          <button type="button" class="stage-nav prev" id="stagePrev" data-action="stage-prev" aria-label="上一张" data-tip="上一张 (←)" hidden><svg class="ico ico-20" aria-hidden="true"><use href="#i-chevron-left"/></svg></button>\n          <button type="button" class="stage-nav next" id="stageNext" data-action="stage-next" aria-label="下一张" data-tip="下一张 (→)" hidden><svg class="ico ico-20" aria-hidden="true"><use href="#i-chevron-right"/></svg></button>\n          <div class="stage-box stage-run-box" id="stageRun" hidden>\n            <div class="skeleton" aria-hidden="true"></div>\n            <button type="button" class="btn btn-secondary btn-sm stage-abort" data-action="cancel-job"><svg class="ico" aria-hidden="true"><use href="#i-close"/></svg><span>中止</span></button>\n            <div class="stage-run-card">\n              <span class="stage-run-pct num" id="stageRunPct"></span>\n              <span class="stage-phase num" id="stageRunPhase">准备中</span>\n              <span class="hint-2 num" id="stageRunTime">0:00</span>\n            </div>\n            <div class="progress stage-progress"><span class="progress-fill" id="stageRunFill"></span></div>\n          </div>\n          <div class="stage-box" id="stageFail" hidden>\n            <div class="stage-box-center">\n              <svg class="ico ico-32 fail-ico" aria-hidden="true"><use href="#i-close"/></svg>\n              <span class="fail-title">生成失败</span>\n              <span class="hint-2" id="stageFailMsg"></span>\n              <button type="button" class="btn btn-secondary" data-action="open-task-log">查看日志</button>\n            </div>\n          </div>\n          <div class="empty stage-empty" id="stageEmpty" hidden>\n            <svg class="ico ico-32" aria-hidden="true"><use href="#i-image"/></svg>\n            <span>生成的作品会显示在这里</span>\n          </div>\n        </div>\n        <div class="stage-batch num" id="stageDots" hidden></div>\n        <div class="stage-actions" id="stageActions" hidden>\n          <span class="hint-3 num stage-meta" id="stageMeta"></span>\n          <div class="stage-act-group">\n            <button type="button" class="btn btn-secondary" data-action="stage-view">查看大图</button>\n            <button type="button" class="btn btn-secondary wide-only" data-action="stage-as-ref" data-img-only="1">作为参考</button>\n            <button type="button" class="btn btn-secondary" data-action="stage-to-video" data-img-only="1">做成视频</button>\n            <button type="button" class="btn btn-secondary" data-action="stage-reuse" id="stageReuseBtn">复用提示词</button>\n            <a class="icon-btn icon-btn-fill wide-only" id="stageDownload" href="#" download aria-label="下载" data-tip="下载原图"><svg class="ico" aria-hidden="true"><use href="#i-download"/></svg></a>\n            <button type="button" class="icon-btn icon-btn-fill" data-action="stage-more" aria-label="更多" data-tip="更多操作" aria-haspopup="menu"><svg class="ico" aria-hidden="true"><use href="#i-more"/></svg></button>\n          </div>\n        </div>\n        <div class="recent">\n          <div class="recent-head">\n            <span class="block-title">最近</span>\n            <button type="button" class="btn-link" data-action="goto-works-mode">全部作品</button>\n          </div>\n          <div class="recent-list fade-x" id="recentList" role="listbox" aria-label="最近作品"></div>\n        </div>\n      </div>\n    </section>\n\n    <!-- ============ 作品 ============ -->\n    <section class="page page-works" id="pageWorks" data-page="works">\n      <div class="works-toolbar">\n        <div class="seg seg-on-bg works-seg" id="worksSeg" role="group" aria-label="作品分类">\n          <button type="button" class="seg-item is-active" data-action="switch-tab" data-tab="image">图片 <span class="seg-count num" id="cnt_image">0</span></button>\n          <button type="button" class="seg-item" data-action="switch-tab" data-tab="video">视频 <span class="seg-count num" id="cnt_video">0</span></button>\n          <button type="button" class="seg-item" data-action="switch-tab" data-tab="upload_image">上传 <span class="seg-count num" id="cnt_upload_image">0</span></button>\n          <button type="button" class="seg-item" data-action="switch-tab" data-tab="llm">提示词 <span class="seg-count num" id="cnt_llm">0</span></button>\n        </div>\n        <div class="works-tools">\n          <label class="well field field-on-bg search">\n            <svg class="ico" aria-hidden="true"><use href="#i-search"/></svg>\n            <input type="search" id="gallerySearchInput" class="search-input" placeholder="搜索提示词或文件名" aria-label="搜索作品">\n          </label>\n          <span class="row-spacer"></span>\n          <button type="button" class="btn surface tool-btn fix-sync" id="syncBtn" data-action="toggle-drive-sync-picker" hidden aria-haspopup="dialog" aria-expanded="false" aria-controls="syncPop" data-tip="同步状态">\n            <svg class="ico" aria-hidden="true"><use href="#i-cloud"/></svg><span class="tool-label" id="syncBtnText">同步</span>\n          </button>\n          <button type="button" class="btn surface tool-btn fix-batch" id="batchToggleBtn" data-action="toggle-batch-mode" aria-pressed="false" data-tip="批量选择">\n            <svg class="ico" aria-hidden="true"><use href="#i-check-square"/></svg><span class="tool-label" id="batchToggleText">选择</span>\n          </button>\n          <button type="button" class="icon-btn surface" data-action="works-more" aria-label="更多" data-tip="更多" aria-haspopup="menu"><svg class="ico" aria-hidden="true"><use href="#i-more"/></svg></button>\n        </div>\n      </div>\n      <div class="panel works-sheet">\n        <div class="pager-top">\n          <span class="hint-2 num" id="pagerTopInfo">共 0 项</span>\n          <div class="pager-top-ctl" id="pagerTopCtl" hidden>\n            <button type="button" class="icon-btn icon-btn-sm" data-action="goto-page" data-rel="-1" id="pagerTopPrev" aria-label="上一页" data-tip="上一页"><svg class="ico" aria-hidden="true"><use href="#i-chevron-left"/></svg></button>\n            <span class="hint-2 num pager-top-label" id="pagerTopLabel">1 / 1</span>\n            <button type="button" class="icon-btn icon-btn-sm" data-action="goto-page" data-rel="1" id="pagerTopNext" aria-label="下一页" data-tip="下一页"><svg class="ico" aria-hidden="true"><use href="#i-chevron-right"/></svg></button>\n          </div>\n        </div>\n        <div class="works-scroll" id="worksScroll">\n          <div class="grid" id="galleryGrid"></div>\n          <div class="pager-bottom" id="pagerBottom" hidden>\n            <div class="pager-pages" id="pagerPages"></div>\n            <span class="hint-2">跳至</span>\n            <select id="paginationJump" class="select select-sm" data-dropup="1" aria-label="跳至页码"></select>\n            <button type="button" class="btn btn-ghost btn-sm" data-action="goto-gallery-top"><svg class="ico" aria-hidden="true"><use href="#i-arrow-up"/></svg><span>回顶部</span></button>\n          </div>\n        </div>\n      </div>\n      <div class="panel batch-bar" id="batchBar" hidden>\n        <span class="batch-count num" id="batchCount">已选 0</span>\n        <button type="button" class="btn btn-ghost" data-action="batch-select-page"><span><span class="wide-text">全选</span>本页</span></button>\n        <button type="button" class="btn btn-ghost" data-action="batch-select-filtered"><span><span class="wide-text">全选</span>全部</span></button>\n        <button type="button" class="btn btn-ghost" data-action="batch-clear" id="batchClearBtn">清除</button>\n        <button type="button" class="btn btn-secondary" data-action="batch-download" id="batchDownloadBtn"><svg class="ico" aria-hidden="true"><use href="#i-download"/></svg><span>下载</span></button>\n        <button type="button" class="btn btn-secondary is-danger" data-action="batch-delete" id="batchDeleteBtn"><svg class="ico" aria-hidden="true"><use href="#i-trash"/></svg><span>删除</span></button>\n      </div>\n    </section>\n  </main>\n</div>\n\n<!-- ============ 浮层 ============ -->\n<section class="panel pop pop-panel machine-pop" id="machinePop" aria-label="机器与账号" hidden>\n  <div class="well acct-box">\n    <div class="acct-row">\n      <span class="avatar" id="acctAvatar" aria-hidden="true">G</span>\n      <div class="acct-main">\n        <span class="acct-email" id="acctEmail">—</span>\n        <span class="hint-2 num" id="acctSub">余额 — CU</span>\n      </div>\n      <button type="button" class="btn surface btn-sm fix-switch" data-action="toggle-account-picker" id="acctSwitchBtn" aria-expanded="false">切换</button>\n    </div>\n    <div class="acct-panel" id="acctPanel" hidden>\n      <div class="acct-list" id="acctList"></div>\n      <div class="acct-panel-foot">\n        <button type="button" class="btn btn-ghost btn-sm" data-action="refresh-accounts" id="refreshAccountsBtn"><svg class="ico" aria-hidden="true"><use href="#i-refresh"/></svg><span>刷新余额</span></button>\n        <button type="button" class="btn btn-ghost btn-sm" data-action="toggle-acct-oauth" id="toggleAcctOauthBtn"><svg class="ico" aria-hidden="true"><use href="#i-plus"/></svg><span>登录新号</span></button>\n      </div>\n      <div class="oauth" id="acctOauthBox" hidden>\n        <span class="hint-2">1. 打开 Google 授权页登录新号并复制授权码；2. 粘贴到下方完成绑定（云盘同步保持主号不变）。</span>\n        <div class="oauth-row">\n          <a id="acctOauthLink" class="btn surface btn-sm" href="#" target="_blank" rel="noopener noreferrer"><svg class="ico" aria-hidden="true"><use href="#i-external"/></svg><span>打开授权页</span></a>\n          <button type="button" class="btn btn-ghost btn-sm" data-action="copy-acct-oauth-url">复制链接</button>\n        </div>\n        <div class="oauth-row">\n          <input type="text" id="acctOauthCodeInp" class="well field input" placeholder="粘贴授权码" autocomplete="off" spellcheck="false" aria-label="授权码">\n          <button type="button" class="btn btn-primary btn-sm" data-action="submit-acct-oauth" id="acctOauthSubmitBtn">绑定</button>\n        </div>\n      </div>\n      <span class="hint-2" id="acctPickerHint" aria-live="polite"></span>\n    </div>\n  </div>\n\n  <div class="power-row">\n    <button type="button" class="btn btn-secondary fix-power" data-action="warmup-all" id="warmupBtn" aria-expanded="false" aria-controls="warmupBox"><svg class="ico" aria-hidden="true"><use href="#i-bolt"/></svg><span id="warmupBtnText">预热…</span></button>\n    <span class="row-spacer"></span>\n    <button type="button" class="btn btn-secondary fix-power" data-action="toggle-power" id="powerBtn" disabled><svg class="ico" aria-hidden="true"><use href="#i-power"/></svg><span id="powerBtnText">检查中</span></button>\n  </div>\n\n  <div class="ref-zone warmup" id="warmupBox" hidden>\n    <label class="check"><input type="checkbox" class="check-input" data-warmup-target="v5_int8"><span class="check-box" aria-hidden="true"><svg class="ico" aria-hidden="true"><use href="#i-check"/></svg></span><span id="warmupLblImage">Flux / DiT INT8</span></label>\n    <label class="check"><input type="checkbox" class="check-input" data-warmup-target="llm"><span class="check-box" aria-hidden="true"><svg class="ico" aria-hidden="true"><use href="#i-check"/></svg></span><span id="warmupLblLlm">Qwen 27B GGUF (llama-server)</span></label>\n    <label class="check"><input type="checkbox" class="check-input" data-warmup-target="video"><span class="check-box" aria-hidden="true"><svg class="ico" aria-hidden="true"><use href="#i-check"/></svg></span><span id="warmupLblVideo">24fps 声画视频</span></label>\n    <span class="hint-3">按需勾选要预热常驻的模型；生图/视频与 LLM 会双路并行加载。</span>\n    <div class="warmup-foot">\n      <span class="hint-2" id="warmupHint" aria-live="polite"></span>\n      <span class="row-spacer"></span>\n      <button type="button" class="btn btn-primary btn-sm" data-action="start-warmup" id="startWarmupBtn" disabled>开始预热</button>\n    </div>\n  </div>\n\n  <div class="hw-strip" id="hwGrid">\n    <div class="hw-cell"><div class="hw-top"><span class="hint-3">CPU</span><span class="hw-val num" id="hwValCpu">--</span></div><span class="progress progress-thin"><span class="progress-fill" id="hwBarCpu"></span></span></div>\n    <div class="hw-cell"><div class="hw-top"><span class="hint-3">内存</span><span class="hw-val num" id="hwValRam">--</span></div><span class="progress progress-thin"><span class="progress-fill" id="hwBarRam"></span></span></div>\n    <div class="hw-cell"><div class="hw-top"><span class="hint-3">显存</span><span class="hw-val num" id="hwValVram">--</span></div><span class="progress progress-thin"><span class="progress-fill" id="hwBarVram"></span></span></div>\n    <div class="hw-cell"><div class="hw-top"><span class="hint-3">硬盘</span><span class="hw-val num" id="hwValDisk">--</span></div><span class="progress progress-thin"><span class="progress-fill" id="hwBarDisk"></span></span></div>\n  </div>\n\n  <div class="mob-logout-row">\n    <button type="button" class="btn btn-ghost btn-sm is-danger" data-action="logout-panel"><svg class="ico" aria-hidden="true"><use href="#i-logout"/></svg><span>退出登录</span></button>\n  </div>\n</section>\n\n<section class="panel pop pop-panel random-pop" id="randomPop" aria-label="随机生成" hidden>\n  <span class="hint-2" id="randomPopHint">点选数量立即开始随机生成</span>\n  <div class="rand-grid" id="randomGrid"></div>\n</section>\n\n<section class="panel pop pop-panel sync-pop" id="syncPop" aria-label="云盘与频道同步" hidden>\n  <div class="sync-head">\n    <span class="pop-title">同步</span>\n    <a class="btn-link" id="syncFolderLink" target="_blank" rel="noopener noreferrer" hidden><span>打开云盘</span><svg class="ico" aria-hidden="true"><use href="#i-external"/></svg></a>\n  </div>\n  <div class="well sync-box">\n    <div class="sync-stats">\n      <div class="sync-stat"><span class="hint-2">谷歌加密云</span><span class="sync-num num" id="syncDriveCount">-- 张</span></div>\n      <div class="sync-stat"><span class="hint-2" id="syncChannelLabel">TG 频道</span><span class="sync-num num" id="syncChannelCount">-- 张</span></div>\n    </div>\n    <span class="hint-2 sync-note" id="syncNote">正在读取同步状态…</span>\n  </div>\n  <div class="sync-actions">\n    <button type="button" class="menu-item menu-item-2line" data-action="sync-drive" id="syncDriveBtn"><svg class="ico" aria-hidden="true"><use href="#i-cloud"/></svg><span class="mi-text"><span id="syncDriveTitle">仅同步谷歌云盘</span><span class="hint-3">加密上传新图并同步删除项，不发频道</span></span></button>\n    <button type="button" class="menu-item menu-item-2line" data-action="sync-channel" id="syncChannelBtn"><svg class="ico" aria-hidden="true"><use href="#i-send"/></svg><span class="mi-text"><span id="syncChannelTitle">同步到 TG 频道</span><span class="hint-3">先同步谷歌云，再按 6 张相册更新频道</span></span></button>\n  </div>\n</section>\n\n<div class="panel pop menu-pop" id="menuPop" role="menu" hidden></div>\n<div class="panel pop menu-pop zselect-menu" id="zSelectMenu" role="listbox" tabindex="-1" hidden></div>\n\n<!-- 任务抽屉 -->\n<div class="backdrop" id="drawerBackdrop" data-action="close-task-drawer" hidden></div>\n<aside class="panel drawer" id="taskDrawer" role="dialog" aria-modal="true" aria-label="任务" tabindex="-1" hidden>\n  <div class="drawer-grip" aria-hidden="true"></div>\n  <div class="drawer-head">\n    <span class="drawer-title">任务</span>\n    <button type="button" class="icon-btn" data-action="close-task-drawer" aria-label="关闭" data-tip="关闭 (Esc)"><svg class="ico" aria-hidden="true"><use href="#i-close"/></svg></button>\n  </div>\n  <div class="drawer-seg-wrap">\n    <div class="seg seg-fill" id="drawerSeg" role="tablist" aria-label="任务分组">\n      <button type="button" class="seg-item is-active" role="tab" data-action="set-drawer-tab" data-tab="active">进行中 <span class="seg-count num" id="drawerCntActive">0</span></button>\n      <button type="button" class="seg-item" role="tab" data-action="set-drawer-tab" data-tab="paused">已暂停 <span class="seg-count num" id="drawerCntPaused">0</span></button>\n      <button type="button" class="seg-item" role="tab" data-action="set-drawer-tab" data-tab="history">历史</button>\n    </div>\n  </div>\n  <div class="drawer-body" id="drawerBody"></div>\n  <div class="log-wrap">\n    <div class="log" id="logFold">\n      <button type="button" class="log-head" data-action="toggle-log" aria-expanded="false" aria-controls="logBox">\n        <span class="log-title">运行日志</span>\n        <span class="log-preview" id="logPreview">等待提交任务…</span>\n        <svg class="ico chev" aria-hidden="true"><use href="#i-chevron"/></svg>\n      </button>\n      <pre class="log-box fade-y" id="logBox" hidden></pre>\n    </div>\n  </div>\n</aside>\n\n<!-- 参考选择器 -->\n<dialog class="panel dialog ref-picker" id="refPicker" aria-label="选择参考图" tabindex="-1">\n  <div class="rp-head">\n    <div class="seg" id="refPickerSeg" role="group" aria-label="来源">\n      <button type="button" class="seg-item is-active" data-action="ref-picker-tab" data-tab="image">生成图片</button>\n      <button type="button" class="seg-item" data-action="ref-picker-tab" data-tab="upload_image">上传素材</button>\n    </div>\n    <label class="well field search rp-search">\n      <svg class="ico" aria-hidden="true"><use href="#i-search"/></svg>\n      <input type="search" id="refPickerSearch" class="search-input" placeholder="搜索提示词或文件名" aria-label="搜索">\n    </label>\n    <button type="button" class="icon-btn" data-action="close-ref-picker" aria-label="关闭" data-tip="关闭 (Esc)"><svg class="ico" aria-hidden="true"><use href="#i-close"/></svg></button>\n  </div>\n  <div class="rp-scroll" id="refPickerScroll">\n    <div class="rp-grid" id="refPickerGrid"></div>\n  </div>\n</dialog>\n\n<!-- 详情查看器 -->\n<div class="viewer force-dark" id="viewer" role="dialog" aria-modal="true" aria-label="作品详情" tabindex="-1" hidden>\n  <div class="viewer-mob-top" id="viewerMobTop">\n    <button type="button" class="viewer-hud-btn" data-action="close-viewer" aria-label="关闭"><svg class="ico ico-20" aria-hidden="true"><use href="#i-close"/></svg></button>\n    <span class="viewer-counter num" id="viewerCounter"></span>\n    <button type="button" class="viewer-hud-btn viewer-hud-pill" data-action="toggle-viewer-sheet" aria-label="查看详情与提示词"><svg class="ico" aria-hidden="true"><use href="#i-list"/></svg><span>详情</span></button>\n  </div>\n  <div class="viewer-canvas" id="viewerCanvas">\n    <img id="viewerImg" alt="" draggable="false" hidden>\n    <video id="viewerVideo" controls playsinline preload="metadata" hidden></video>\n    <div class="viewer-zoom-bar" id="viewerZoomBar" hidden>\n      <button type="button" class="zoom-step-btn" data-action="viewer-zoom-out" aria-label="缩小" data-tip="缩小 (-)"><svg class="ico" aria-hidden="true"><use href="#i-minus"/></svg></button>\n      <input type="range" class="zoom-slider" id="viewerZoomSlider" min="100" max="600" step="1" value="100" aria-label="缩放比例">\n      <button type="button" class="zoom-step-btn" data-action="viewer-zoom-in" aria-label="放大" data-tip="放大 (+)"><svg class="ico" aria-hidden="true"><use href="#i-plus"/></svg></button>\n      <button type="button" class="zoom-pct num" id="viewerZoomPct" data-action="viewer-zoom-reset" data-tip="重置为 100% (0)">100%</button>\n    </div>\n    <button type="button" class="viewer-zoom-pill num" id="viewerZoomPill" data-action="viewer-zoom-reset" hidden>100%</button>\n  </div>\n  <div class="viewer-mob-bar" id="viewerMobBar">\n    <button type="button" class="mob-act" data-action="viewer-reuse" id="viewerMobReuse"><svg class="ico" aria-hidden="true"><use href="#i-refresh"/></svg><span>复用</span></button>\n    <button type="button" class="mob-act" data-action="viewer-as-ref" id="viewerMobRef"><svg class="ico" aria-hidden="true"><use href="#i-image"/></svg><span>参考</span></button>\n    <button type="button" class="mob-act" data-action="viewer-to-video" id="viewerMobVideo"><svg class="ico" aria-hidden="true"><use href="#i-video"/></svg><span>视频</span></button>\n    <a class="mob-act" id="viewerMobDownload" href="#" download><svg class="ico" aria-hidden="true"><use href="#i-download"/></svg><span>下载</span></a>\n    <button type="button" class="mob-act is-danger" data-action="viewer-delete"><svg class="ico" aria-hidden="true"><use href="#i-trash"/></svg><span>删除</span></button>\n  </div>\n  <div class="viewer-sheet-backdrop" id="viewerSheetBackdrop" data-action="close-viewer-sheet"></div>\n  <button type="button" class="viewer-nav prev" data-action="lightbox-prev" id="viewerPrev" aria-label="上一张" data-tip="上一张 (←)"><svg class="ico ico-20" aria-hidden="true"><use href="#i-chevron-left"/></svg></button>\n  <button type="button" class="viewer-nav next" data-action="lightbox-next" id="viewerNext" aria-label="下一张" data-tip="下一张 (→)"><svg class="ico ico-20" aria-hidden="true"><use href="#i-chevron-right"/></svg></button>\n  <aside class="panel viewer-info" id="viewerInfo">\n    <button type="button" class="viewer-grip" data-action="close-viewer-sheet" aria-label="收起详情"><span aria-hidden="true"></span></button>\n    <div class="viewer-info-head">\n      <span class="viewer-name" id="viewerName"></span>\n      <button type="button" class="icon-btn" data-action="viewer-head-close" aria-label="关闭" data-tip="关闭 (Esc)"><svg class="ico" aria-hidden="true"><use href="#i-close"/></svg></button>\n    </div>\n    <span class="hint-2 num" id="viewerMeta"></span>\n    <div class="well viewer-prompt-box">\n      <div class="viewer-prompt-head" id="viewerPromptHead">\n        <span class="block-title">提示词</span>\n        <button type="button" class="btn-link" data-action="viewer-copy">复制</button>\n      </div>\n      <div class="viewer-prompt" id="viewerPrompt"></div>\n    </div>\n    <div class="viewer-actions">\n      <button type="button" class="btn btn-primary" data-action="viewer-reuse" id="viewerReuseBtn">复用提示词</button>\n      <button type="button" class="btn btn-secondary" data-action="viewer-as-ref" id="viewerRefBtn">作为参考</button>\n      <button type="button" class="btn btn-secondary" data-action="viewer-to-video" id="viewerVideoBtn">做成视频</button>\n      <a class="btn btn-secondary" id="viewerDownload" href="#" download>下载原图</a>\n      <button type="button" class="btn btn-secondary is-danger viewer-del-btn" data-action="viewer-delete"><svg class="ico" aria-hidden="true"><use href="#i-trash"/></svg><span>删除</span></button>\n    </div>\n  </aside>\n</div>\n\n<!-- 确认弹窗 -->\n<dialog class="panel dialog confirm" id="confirmDialog" tabindex="-1" aria-labelledby="confirmTitle">\n  <h2 class="dialog-title" id="confirmTitle">确认</h2>\n  <p class="dialog-text" id="confirmText"></p>\n  <div class="dialog-actions">\n    <button type="button" class="btn btn-secondary" id="cancelConfirm" data-action="confirm-cancel">取消</button>\n    <button type="button" class="btn btn-primary" id="acceptConfirm" data-action="confirm-accept">确认</button>\n  </div>\n</dialog>\n\n<!-- 提示词详情 -->\n<dialog class="panel dialog prompt-dialog" id="promptDialog" tabindex="-1" aria-labelledby="promptDialogTitle">\n  <div class="pd-head">\n    <div class="pd-titles">\n      <h2 class="dialog-title" id="promptDialogTitle">提示词</h2>\n      <span class="hint-2 num" id="promptDialogSub"></span>\n    </div>\n    <button type="button" class="icon-btn" data-action="close-prompt-dialog" aria-label="关闭" data-tip="关闭 (Esc)"><svg class="ico" aria-hidden="true"><use href="#i-close"/></svg></button>\n  </div>\n  <div class="well pd-body" id="promptDialogBody"></div>\n  <div class="dialog-actions">\n    <button type="button" class="btn btn-secondary" id="promptDialogCopyBtn" data-action="copy-prompt-dialog">复制</button>\n    <button type="button" class="btn btn-primary" data-action="reuse-prompt-dialog">复用</button>\n  </div>\n</dialog>\n\n<div class="ui-tip" id="uiTip" role="tooltip" hidden></div>\n<div class="toast" id="toast" role="status" aria-live="polite"></div>\n<input type="file" id="fileInputImage" class="visually-hidden" accept="image/png,image/jpeg,image/webp" tabindex="-1" aria-hidden="true">\n<form id="batchDownloadForm" method="POST" action="/api/download_batch" class="visually-hidden">\n  <input type="hidden" name="payload" id="batchDownloadPayload" value="">\n</form>\n<script>\n\'use strict\';\n\n/* ==================== 基础工具 ==================== */\nconst $ = (id) => document.getElementById(id);\nconst SVG_NS = \'http://www.w3.org/2000/svg\';\nconst mqMobile = window.matchMedia(\'(max-width: 767px)\');\nconst mqSingle = window.matchMedia(\'(max-width: 1099px)\');\nconst mqFineHover = window.matchMedia(\'(hover: hover) and (pointer: fine)\');\nconst isMobile = () => mqMobile.matches;\nconst IS_MAC = /Mac|iPhone|iPad/i.test(navigator.platform || navigator.userAgent || \'\');\n\nfunction asArr(v) { return Array.isArray(v) ? v : []; }\nfunction asStr(v) { return typeof v === \'string\' ? v : (v == null ? \'\' : String(v)); }\n\nfunction el(tag, cls, text) {\n  const n = document.createElement(tag);\n  if (cls) n.className = cls;\n  if (text !== undefined && text !== null) n.textContent = text;\n  return n;\n}\n\nfunction icon(id, cls = \'ico\') {\n  const s = document.createElementNS(SVG_NS, \'svg\');\n  s.setAttribute(\'class\', cls);\n  s.setAttribute(\'aria-hidden\', \'true\');\n  const u = document.createElementNS(SVG_NS, \'use\');\n  u.setAttribute(\'href\', \'#\' + id);\n  s.appendChild(u);\n  return s;\n}\n\nfunction setTip(node, text) {\n  if (!node) return;\n  const s = asStr(text).trim();\n  if (s) node.dataset.tip = s;\n  else delete node.dataset.tip;\n}\n\nfunction btn(cls, text, action, data = {}, iconId = \'\') {\n  const b = el(\'button\', cls);\n  b.type = \'button\';\n  if (action) b.dataset.action = action;\n  Object.entries(data).forEach(([k, v]) => { b.dataset[k] = String(v); });\n  if (iconId) b.append(icon(iconId));\n  if (text) b.append(el(\'span\', \'\', text));\n  return b;\n}\n\nfunction iconBtn(cls, iconId, label, action, data = {}) {\n  const b = btn(\'icon-btn\' + (cls ? \' \' + cls : \'\'), \'\', action, data, iconId);\n  b.setAttribute(\'aria-label\', label);\n  setTip(b, label);\n  return b;\n}\n\nfunction setText(node, text) {\n  if (node && node.textContent !== text) node.textContent = text;\n}\n\nfunction fileName(path) {\n  return asStr(path).split(\'/\').pop();\n}\n\nfunction fmtClock(sec) {\n  const s = Math.max(0, Math.round(Number(sec) || 0));\n  const m = Math.floor(s / 60);\n  if (m >= 60) return `${Math.floor(m / 60)}:${String(m % 60).padStart(2, \'0\')}:${String(s % 60).padStart(2, \'0\')}`;\n  return `${m}:${String(s % 60).padStart(2, \'0\')}`;\n}\n\nfunction fmtRes(res) {\n  return asStr(res).replace(\'x\', \'×\');\n}\n\nfunction stripEmoji(s) {\n  return asStr(s)\n    .replace(/[\\u{1F000}-\\u{1FAFF}\\u{2600}-\\u{27BF}\\u{2B00}-\\u{2BFF}\\u{FE0F}\\u{200D}]/gu, \'\')\n    .replace(/\\s{2,}/g, \' \')\n    .trim();\n}\n\nfunction clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, n)); }\n\nfunction isVideoItem(it, tab) {\n  return tab === \'video\' || /\\.(mp4|webm|mov)$/i.test((it && it.name) || \'\');\n}\n\n/* 渐进式原图加载：先显缩略图，原图就绪且仍为当前目标时无缝切换 */\nfunction progressiveImg(img, thumbUrl, fullUrl, isCurrent, onFull) {\n  img.src = thumbUrl || fullUrl || \'\';\n  if (!thumbUrl || !fullUrl || thumbUrl === fullUrl) {\n    if (fullUrl && onFull) img.addEventListener(\'load\', () => { if (isCurrent()) onFull(); }, { once: true });\n    return;\n  }\n  const full = new Image();\n  full.onload = () => {\n    if (!isCurrent()) return;\n    img.src = fullUrl;\n    if (onFull) onFull();\n  };\n  full.src = fullUrl;\n}\n\n/* 动态滚动边缘渐隐（仅内容真正溢出该方向时启用，短内容不加遮罩） */\nfunction updateScrollFade(node, axis = \'y\') {\n  if (!node) return;\n  if (axis === \'x\') {\n    const overflow = node.scrollWidth > node.clientWidth + 2;\n    node.dataset.fadeStart = overflow && node.scrollLeft > 2 ? \'1\' : \'0\';\n    node.dataset.fadeEnd = overflow && node.scrollLeft + node.clientWidth < node.scrollWidth - 2 ? \'1\' : \'0\';\n  } else {\n    const overflow = node.scrollHeight > node.clientHeight + 2;\n    node.dataset.fadeStart = overflow && node.scrollTop > 2 ? \'1\' : \'0\';\n    node.dataset.fadeEnd = overflow && node.scrollTop + node.clientHeight < node.scrollHeight - 2 ? \'1\' : \'0\';\n  }\n}\n\nfunction bindScrollFade(node, axis = \'y\') {\n  if (!node) return;\n  const sync = () => updateScrollFade(node, axis);\n  node.addEventListener(\'scroll\', sync, { passive: true });\n  if (typeof ResizeObserver !== \'undefined\') new ResizeObserver(sync).observe(node);\n  requestAnimationFrame(sync);\n}\n\n/* ==================== 存储 ==================== */\nconst store = {\n  get(key, fallback = null, area = localStorage) {\n    try {\n      const v = area.getItem(key);\n      return v === null ? fallback : JSON.parse(v);\n    } catch (_) { return fallback; }\n  },\n  set(key, val, area = localStorage) {\n    try { area.setItem(key, JSON.stringify(val)); } catch (_) {}\n  },\n  raw(key, area = localStorage) {\n    try { return area.getItem(key); } catch (_) { return null; }\n  }\n};\n\n/* ==================== 统一 API 封装 ==================== */\nfunction goLogin() { window.location.replace(\'/\'); }\n\nasync function request(url, opts = {}) {\n  const { timeout = 0, ...rest } = opts;\n  let ctrl = null, timer = null;\n  if (timeout) {\n    ctrl = new AbortController();\n    timer = setTimeout(() => ctrl.abort(), timeout);\n    rest.signal = ctrl.signal;\n  }\n  try {\n    const res = await fetch(url, { cache: \'no-store\', ...rest });\n    if (res.status === 401) { goLogin(); throw Object.assign(new Error(\'登录已失效\'), { status: 401 }); }\n    const data = await res.json().catch(() => ({}));\n    return { res, data };\n  } finally {\n    if (timer) clearTimeout(timer);\n  }\n}\n\nconst api = {\n  async get(url, opts = {}) {\n    const { res, data } = await request(url, opts);\n    if (!res.ok) throw Object.assign(new Error((data && data.message) || `请求失败 (${res.status})`), { status: res.status, data });\n    return data;\n  },\n  async post(url, payload = {}, opts = {}) {\n    const { res, data } = await request(url, {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify(payload),\n      ...opts\n    });\n    if (!res.ok || (data && data.ok === false)) {\n      throw Object.assign(new Error((data && data.message) || `操作失败 (${res.status})`), { status: res.status, data });\n    }\n    return data;\n  },\n  async raw(url, opts = {}) {\n    return request(url, opts);\n  }\n};\n\n/* 面板真实交互上报活跃（实例未运行时后端自动忽略） */\nlet _lastTouchTs = 0;\nfunction reportActivity(e) {\n  if (e && !e.isTrusted) return;\n  const now = Date.now();\n  if (now - _lastTouchTs < 20000) return;\n  _lastTouchTs = now;\n  fetch(\'/api/touch\', { method: \'POST\', headers: { \'Content-Type\': \'application/json\' }, body: \'{"source":"panel"}\' }).catch(() => {});\n}\ndocument.addEventListener(\'pointerdown\', reportActivity, { passive: true });\ndocument.addEventListener(\'keydown\', reportActivity, { passive: true });\n\n/* ==================== Toast / 统一复制 1.5s 回显 ==================== */\nlet _toastTimer = null;\nfunction toast(text) {\n  const t = $(\'toast\');\n  if (!t || !text) return;\n  t.textContent = stripEmoji(text);\n  t.classList.add(\'is-visible\');\n  clearTimeout(_toastTimer);\n  _toastTimer = setTimeout(() => t.classList.remove(\'is-visible\'), 2400);\n}\n\nasync function copyText(str) {\n  if (!str) return false;\n  try {\n    if (navigator.clipboard && navigator.clipboard.writeText) {\n      await navigator.clipboard.writeText(str);\n      return true;\n    }\n  } catch (_) {}\n  try {\n    const ta = el(\'textarea\', \'visually-hidden\');\n    ta.value = str;\n    ta.setAttribute(\'readonly\', \'\');\n    document.body.appendChild(ta);\n    ta.select();\n    ta.setSelectionRange(0, str.length);\n    const ok = document.execCommand(\'copy\');\n    ta.remove();\n    return ok;\n  } catch (_) {\n    return false;\n  }\n}\n\nconst _copyTimers = new WeakMap();\nfunction flashCopied(triggerEl) {\n  if (!triggerEl || !triggerEl.isConnected) return;\n  const prevTimer = _copyTimers.get(triggerEl);\n  if (prevTimer) clearTimeout(prevTimer);\n  const labelNode = triggerEl.querySelector(\'span:last-child\') || triggerEl;\n  if (!triggerEl.dataset.origCopyText) triggerEl.dataset.origCopyText = labelNode.textContent;\n  triggerEl.classList.add(\'is-copied\');\n  if (labelNode.textContent) labelNode.textContent = \'已复制 ✓\';\n  _copyTimers.set(triggerEl, setTimeout(() => {\n    triggerEl.classList.remove(\'is-copied\');\n    if (triggerEl.dataset.origCopyText) {\n      labelNode.textContent = triggerEl.dataset.origCopyText;\n      delete triggerEl.dataset.origCopyText;\n    }\n    _copyTimers.delete(triggerEl);\n  }, 1500));\n}\n\nasync function copyWithToast(str, msg = \'已复制\', triggerEl = null) {\n  const ok = await copyText(asStr(str).trim());\n  if (ok) {\n    if (triggerEl) flashCopied(triggerEl);\n    toast(msg);\n  } else {\n    toast(\'复制失败，请手动选择文本\');\n  }\n}\n/* ==================== 桌面磨砂轻提示（替代全站原生 title=） ==================== */\nconst Tip = {\n  target: null,\n  showTimer: null,\n  hideTimer: null,\n  lastClosedAt: 0,\n\n  init() {\n    document.addEventListener(\'pointerover\', (e) => {\n      if (!mqFineHover.matches || e.pointerType === \'touch\') return;\n      const t = e.target.closest(\'[data-tip]\');\n      if (!t || !t.dataset.tip) { this.scheduleHide(); return; }\n      if (t === this.target) return;\n      this.scheduleShow(t);\n    }, { passive: true });\n    document.addEventListener(\'pointerout\', (e) => {\n      if (!this.target) return;\n      const rel = e.relatedTarget;\n      if (rel && this.target.contains(rel)) return;\n      this.scheduleHide();\n    }, { passive: true });\n    document.addEventListener(\'pointerdown\', () => this.hide(true), { passive: true });\n    document.addEventListener(\'scroll\', () => this.hide(true), { passive: true, capture: true });\n    window.addEventListener(\'blur\', () => this.hide(true));\n  },\n\n  scheduleShow(node) {\n    clearTimeout(this.showTimer);\n    clearTimeout(this.hideTimer);\n    this.target = node;\n    const warm = Date.now() - this.lastClosedAt < 240;\n    this.showTimer = setTimeout(() => this.show(node), warm ? 60 : 320);\n  },\n\n  show(node) {\n    if (!node || !node.isConnected || !node.dataset.tip) return;\n    const tip = $(\'uiTip\');\n    if (!tip) return;\n    setText(tip, node.dataset.tip);\n    tip.hidden = false;\n    const r = node.getBoundingClientRect();\n    const tw = tip.offsetWidth, th = tip.offsetHeight;\n    const vw = window.innerWidth, vh = window.innerHeight;\n    const left = clamp(r.left + r.width / 2 - tw / 2, 8, vw - tw - 8);\n    let top = r.bottom + 7;\n    if (top + th > vh - 8) top = Math.max(8, r.top - th - 7);\n    tip.style.left = Math.round(left) + \'px\';\n    tip.style.top = Math.round(top) + \'px\';\n    requestAnimationFrame(() => tip.classList.add(\'is-open\'));\n  },\n\n  scheduleHide() {\n    clearTimeout(this.showTimer);\n    this.target = null;\n    const tip = $(\'uiTip\');\n    if (!tip || tip.hidden) return;\n    tip.classList.remove(\'is-open\');\n    this.lastClosedAt = Date.now();\n    clearTimeout(this.hideTimer);\n    this.hideTimer = setTimeout(() => { if (!this.target) tip.hidden = true; }, 130);\n  },\n\n  hide(immediate = false) {\n    clearTimeout(this.showTimer);\n    clearTimeout(this.hideTimer);\n    this.target = null;\n    const tip = $(\'uiTip\');\n    if (!tip) return;\n    tip.classList.remove(\'is-open\');\n    if (immediate) tip.hidden = true;\n    else this.hideTimer = setTimeout(() => { if (!this.target) tip.hidden = true; }, 130);\n  }\n};\n\n/* ==================== 浮层管理（同一时间只开一个，对称进出动画） ==================== */\nconst Pop = {\n  cur: null, // { box, anchor, onClose, align, dropup }\n  _hideTimers: new WeakMap(),\n\n  isOpen(box) { return !!(this.cur && this.cur.box === box); },\n\n  position() {\n    const c = this.cur;\n    if (!c) return;\n    const { box, anchor } = c;\n    const r = anchor.getBoundingClientRect();\n    const vw = window.innerWidth, vh = window.innerHeight;\n    const margin = 8;\n    box.style.maxWidth = (vw - margin * 2) + \'px\';\n    if (c.matchWidth) box.style.minWidth = Math.round(r.width) + \'px\';\n    const bw = box.offsetWidth, bh = box.offsetHeight;\n    let left = c.align === \'start\' ? r.left : r.right - bw;\n    left = clamp(left, margin, vw - bw - margin);\n    let top = r.bottom + 6;\n    const fitsBelow = top + bh <= vh - margin;\n    if (c.dropup || !fitsBelow) {\n      const upTop = r.top - 6 - bh;\n      if (c.dropup || upTop >= margin) top = Math.max(margin, upTop);\n    }\n    box.style.left = Math.round(left) + \'px\';\n    box.style.top = Math.round(top) + \'px\';\n  },\n\n  open(box, anchor, opts = {}) {\n    if (!box || !anchor) return;\n    Tip.hide(true);\n    if (this.cur && this.cur.box === box) { this.cur.anchor = anchor; this.position(); return; }\n    this.close(false);\n    const t = this._hideTimers.get(box);\n    if (t) { clearTimeout(t); this._hideTimers.delete(box); }\n    this.cur = { box, anchor, onClose: opts.onClose || null, align: opts.align || \'end\', dropup: !!opts.dropup, matchWidth: !!opts.matchWidth };\n    box.hidden = false;\n    anchor.setAttribute(\'aria-expanded\', \'true\');\n    this.position();\n    requestAnimationFrame(() => box.classList.add(\'is-open\'));\n  },\n\n  close(restoreFocus = false) {\n    const c = this.cur;\n    if (!c) return;\n    this.cur = null;\n    const { box, anchor } = c;\n    box.classList.remove(\'is-open\');\n    anchor.setAttribute(\'aria-expanded\', \'false\');\n    this._hideTimers.set(box, setTimeout(() => { if (!this.isOpen(box)) box.hidden = true; }, 130));\n    if (c.onClose) c.onClose();\n    if (restoreFocus && anchor.isConnected) anchor.focus({ preventScroll: true });\n  },\n\n  toggle(box, anchor, opts = {}) {\n    if (this.isOpen(box)) { this.close(true); return false; }\n    this.open(box, anchor, opts);\n    return true;\n  }\n};\n\ndocument.addEventListener(\'pointerdown\', (e) => {\n  const c = Pop.cur;\n  if (!c) return;\n  if (c.box.contains(e.target) || c.anchor.contains(e.target)) return;\n  Pop.close(false);\n}, { passive: true });\nwindow.addEventListener(\'resize\', () => { if (Pop.cur) Pop.position(); });\ndocument.addEventListener(\'scroll\', (e) => {\n  const c = Pop.cur;\n  if (c && !c.box.contains(e.target)) Pop.position();\n}, { passive: true, capture: true });\n\n/* ==================== 菜单（⋯ 更多） ==================== */\nconst Menu = {\n  items: [],\n  show(anchor, items) {\n    const box = $(\'menuPop\');\n    if (Pop.isOpen(box) && Pop.cur.anchor === anchor) { Pop.close(true); return; }\n    this.items = items.filter(Boolean);\n    box.replaceChildren(...this.items.map((it, i) => {\n      const b = el(\'button\', \'menu-item\' + (it.danger ? \' is-danger\' : \'\'));\n      b.type = \'button\';\n      b.setAttribute(\'role\', \'menuitem\');\n      b.dataset.action = \'menu-item\';\n      b.dataset.idx = String(i);\n      b.disabled = !!it.disabled;\n      if (it.icon) b.append(icon(it.icon));\n      b.append(el(\'span\', \'\', it.label));\n      return b;\n    }));\n    Pop.open(box, anchor, { align: \'end\' });\n  },\n  run(idx) {\n    const it = this.items[idx];\n    Pop.close(false);\n    if (it && typeof it.run === \'function\') it.run();\n  }\n};\n\n/* ==================== 自绘下拉（Zeno Select） ==================== */\nconst ZSelect = {\n  map: new Map(), // select -> { wrap, trigger, label }\n  active: null,\n  focusIdx: -1,\n\n  enhance(sel) {\n    if (!sel || this.map.has(sel)) return;\n    const wrap = el(\'div\', \'zselect\' + (sel.classList.contains(\'select-sm\') ? \' zselect-sm\' : \'\'));\n    const trigger = el(\'button\', \'select-trigger\');\n    trigger.type = \'button\';\n    trigger.dataset.action = \'zselect-open\';\n    trigger.setAttribute(\'aria-haspopup\', \'listbox\');\n    trigger.setAttribute(\'aria-expanded\', \'false\');\n    const lbl = sel.getAttribute(\'aria-label\') || (sel.id && document.querySelector(`label[for="${sel.id}"]`)?.textContent) || \'\';\n    if (lbl) trigger.setAttribute(\'aria-label\', lbl);\n    const label = el(\'span\', \'zselect-label\');\n    trigger.append(label, icon(\'i-chevron\'));\n    sel.parentNode.insertBefore(wrap, sel);\n    wrap.append(sel, trigger);\n    sel.tabIndex = -1;\n    sel.setAttribute(\'aria-hidden\', \'true\');\n    trigger._select = sel;\n    this.map.set(sel, { wrap, trigger, label });\n    sel.addEventListener(\'change\', () => this.sync(sel));\n    trigger.addEventListener(\'keydown\', (e) => {\n      if ([\'ArrowDown\', \'ArrowUp\', \'Enter\', \' \'].includes(e.key) && !Pop.isOpen($(\'zSelectMenu\'))) {\n        e.preventDefault();\n        this.open(sel);\n      }\n    });\n    this.sync(sel);\n  },\n\n  sync(sel) {\n    const z = this.map.get(sel);\n    if (!z) return;\n    const opt = sel.options[sel.selectedIndex];\n    setText(z.label, opt ? opt.textContent : \'\');\n    z.trigger.disabled = sel.disabled;\n    z.wrap.hidden = sel.hidden;\n  },\n\n  syncAll() { this.map.forEach((_, sel) => this.sync(sel)); },\n\n  open(sel) {\n    const z = this.map.get(sel);\n    const box = $(\'zSelectMenu\');\n    if (!z || sel.disabled) return;\n    if (Pop.isOpen(box) && this.active === sel) { Pop.close(true); return; }\n    this.active = sel;\n    const opts = Array.from(sel.options);\n    box.replaceChildren(...opts.map((o, i) => {\n      const b = el(\'button\', \'menu-item\' + (i === sel.selectedIndex ? \' is-selected\' : \'\'));\n      b.type = \'button\';\n      b.setAttribute(\'role\', \'option\');\n      b.setAttribute(\'aria-selected\', i === sel.selectedIndex ? \'true\' : \'false\');\n      b.dataset.action = \'zselect-pick\';\n      b.dataset.idx = String(i);\n      b.append(el(\'span\', \'\', o.textContent));\n      if (i === sel.selectedIndex) b.append(icon(\'i-check\'));\n      return b;\n    }));\n    this.focusIdx = sel.selectedIndex;\n    Pop.open(box, z.trigger, { align: \'start\', dropup: sel.dataset.dropup === \'1\', matchWidth: true, onClose: () => { this.active = null; } });\n    const cur = box.children[this.focusIdx];\n    if (cur) { cur.classList.add(\'is-focused\'); cur.scrollIntoView({ block: \'nearest\' }); }\n    box.focus({ preventScroll: true });\n  },\n\n  pick(idx) {\n    const sel = this.active;\n    if (!sel) return;\n    const z = this.map.get(sel);\n    Pop.close(false);\n    if (idx >= 0 && idx < sel.options.length && idx !== sel.selectedIndex) {\n      sel.selectedIndex = idx;\n      sel.dispatchEvent(new Event(\'change\', { bubbles: true }));\n    }\n    this.sync(sel);\n    z?.trigger.focus({ preventScroll: true });\n  },\n\n  key(e) {\n    const box = $(\'zSelectMenu\');\n    if (!this.active || !Pop.isOpen(box)) return false;\n    const items = Array.from(box.children);\n    if (e.key === \'ArrowDown\' || e.key === \'ArrowUp\') {\n      e.preventDefault();\n      items[this.focusIdx]?.classList.remove(\'is-focused\');\n      this.focusIdx = clamp(this.focusIdx + (e.key === \'ArrowDown\' ? 1 : -1), 0, items.length - 1);\n      items[this.focusIdx]?.classList.add(\'is-focused\');\n      items[this.focusIdx]?.scrollIntoView({ block: \'nearest\' });\n      return true;\n    }\n    if (e.key === \'Enter\' || e.key === \' \') {\n      e.preventDefault();\n      this.pick(this.focusIdx);\n      return true;\n    }\n    return false;\n  }\n};\n\n/* ==================== 弹窗对称退出动画 ==================== */\nconst _dlgTimers = new WeakMap();\nfunction openDialog(d) {\n  if (!d) return;\n  Tip.hide(true);\n  const t = _dlgTimers.get(d);\n  if (t) { clearTimeout(t); _dlgTimers.delete(d); }\n  d.classList.remove(\'is-closing\');\n  if (!d.open) d.showModal();\n  d.focus({ preventScroll: true });\n}\n\nfunction closeDialog(d) {\n  if (!d || !d.open || d.classList.contains(\'is-closing\')) return;\n  d.classList.add(\'is-closing\');\n  const t = setTimeout(() => {\n    d.classList.remove(\'is-closing\');\n    if (d.open) d.close();\n    _dlgTimers.delete(d);\n  }, 125);\n  _dlgTimers.set(d, t);\n}\n\n/* ==================== 确认弹窗 ==================== */\nconst Confirm = {\n  cb: null,\n  ask({ title, text = \'\', accept = \'确认\', danger = false, run }) {\n    const d = $(\'confirmDialog\');\n    setText($(\'confirmTitle\'), title);\n    setText($(\'confirmText\'), text);\n    const a = $(\'acceptConfirm\');\n    a.textContent = accept;\n    a.className = \'btn \' + (danger ? \'btn-danger\' : \'btn-primary\');\n    this.cb = run || null;\n    openDialog(d);\n  },\n  accept() {\n    const fn = this.cb;\n    this.cb = null;\n    closeDialog($(\'confirmDialog\'));\n    if (fn) fn();\n  },\n  cancel() {\n    this.cb = null;\n    closeDialog($(\'confirmDialog\'));\n  }\n};\n\n/* 点击外部 backdrop 或按 Esc 均走对称退出动画（防选字拖出误关） */\n[\'confirmDialog\', \'promptDialog\', \'refPicker\'].forEach(id => {\n  const d = $(id);\n  let downOnBackdrop = false;\n  d.addEventListener(\'pointerdown\', (e) => { downOnBackdrop = e.target === d; });\n  d.addEventListener(\'click\', (e) => {\n    if (downOnBackdrop && e.target === d) closeDialog(d);\n    downOnBackdrop = false;\n  });\n  d.addEventListener(\'cancel\', (e) => {\n    e.preventDefault();\n    closeDialog(d);\n  });\n});\n$(\'confirmDialog\').addEventListener(\'close\', () => { Confirm.cb = null; });\n\n/* ==================== 任务抽屉开合 ==================== */\nconst Drawer = {\n  open: false,\n  _t: null,\n  set(open) {\n    const dr = $(\'taskDrawer\');\n    const bd = $(\'drawerBackdrop\');\n    const pill = $(\'taskPill\');\n    if (open === this.open) return;\n    this.open = open;\n    clearTimeout(this._t);\n    pill.setAttribute(\'aria-expanded\', open ? \'true\' : \'false\');\n    if (open) {\n      Tip.hide(true);\n      Pop.close(false);\n      dr.hidden = false;\n      bd.hidden = false;\n      requestAnimationFrame(() => { dr.classList.add(\'is-open\'); bd.classList.add(\'is-open\'); });\n      dr.focus({ preventScroll: true });\n      Tasks.render(true);\n      Tasks.renderLog(true);\n    } else {\n      dr.classList.remove(\'is-open\');\n      bd.classList.remove(\'is-open\');\n      this._t = setTimeout(() => { dr.hidden = true; bd.hidden = true; }, 130);\n      if (pill.isConnected) pill.focus({ preventScroll: true });\n    }\n  }\n};\n/* ==================== 全局状态（唯一来源） ==================== */\nconst MODES = [\'image\', \'video\'];\nconst PAGES = [\'create\', \'works\'];\nconst PAGE_TITLES = { create: \'创作\', works: \'作品\' };\nconst WORK_TABS = [\'image\', \'video\', \'upload_image\', \'llm\'];\nconst WORK_TAB_NAMES = { image: \'图片\', video: \'视频\', upload_image: \'上传素材\', llm: \'提示词历史\' };\nconst SEG_GROUPS = {\n  imgRes: \'segImgRes\', imgSteps: \'segImgSteps\',\n  vidQuality: \'segVidQuality\', vidRes: \'segVidRes\', vidLen: \'segVidLen\', vidSteps: \'segVidSteps\'\n};\nconst VID_RES_BY_QUALITY = {\n  std: [\'832×1248\', \'1248×832\', \'1024×1024\'],\n  hd: [\'1088×1632\', \'1632×1088\', \'1344×1344\'],\n  uhd2k: [\'1408×2112\', \'2112×1408\', \'1728×1728\']\n};\nconst UI_STATE_KEY = \'g4_panel_ui_state_v2\';\nconst UI_STATE_KEY_V1 = \'g4_panel_ui_state_v1\';\n\nconst S = {\n  page: \'create\',\n  mode: \'image\',\n  editorTab: \'write\',\n  seg: { imgRes: \'portrait\', imgSteps: \'tier2\', vidQuality: \'std\', vidRes: \'832x1248\', vidLen: \'192\', vidSteps: \'20\' },\n  imgModel: \'v5_int8\',\n  ref: { image: \'\', video: \'\' },\n  outputs: { image: [], video: [], upload_image: [], llm: [], counts: {} },\n  outputsLoaded: false,\n  job: null,\n  tasks: [],\n  presets: null,\n  uiMeta: { drive_sync_enabled: false, channel_sync_enabled: false }\n};\n\nconst INPUTS = { image: () => $(\'promptInputImage\'), video: () => $(\'promptInputVideo\') };\nconst curInput = () => INPUTS[S.mode]();\n\n/* ==================== 界面状态保存 / 恢复 ==================== */\nlet _restoring = false;\nfunction saveUiState() {\n  if (_restoring) return;\n  store.set(UI_STATE_KEY, {\n    page: S.page,\n    mode: S.mode,\n    editorTab: S.editorTab,\n    seg: S.seg,\n    works: { tab: Works.tab, page: Works.page, query: Works.query },\n    warmup: { customized: Machine.warmupCustomized, targets: Machine.warmupTargets },\n    drafts: { image: $(\'promptInputImage\').value, video: $(\'promptInputVideo\').value, chat: $(\'chatInput\').value },\n    ts: Date.now()\n  }, sessionStorage);\n}\nlet _saveTimer = null;\nfunction scheduleSave() {\n  if (_restoring) return;\n  clearTimeout(_saveTimer);\n  _saveTimer = setTimeout(saveUiState, 180);\n}\nwindow.addEventListener(\'pagehide\', saveUiState);\n\n/* ==================== 页面导航（无切页动画） ==================== */\nfunction navigate(page) {\n  if (!PAGES.includes(page)) page = \'create\';\n  const changed = S.page !== page;\n  S.page = page;\n  document.body.dataset.page = page;\n  document.title = `${PAGE_TITLES[page]} · G4 工作台`;\n  document.querySelectorAll(\'.page\').forEach(p => p.classList.toggle(\'is-active\', p.dataset.page === page));\n  document.querySelectorAll(\'[data-action="nav-page"]\').forEach(b => {\n    const on = b.dataset.page === page;\n    b.classList.toggle(\'is-active\', on);\n    if (on) b.setAttribute(\'aria-current\', \'page\'); else b.removeAttribute(\'aria-current\');\n  });\n  if (changed) Pop.close(false);\n  if (page === \'works\') Works.render(true);\n  else if (page === \'create\') { Create.autoGrow(curInput()); Stage.render(true); }\n  scheduleSave();\n}\n/* ==================== 机器：状态胶囊 / 浮层 / 账号 / 电源 / 预热 / 硬件 ==================== */\nconst Machine = {\n  active: null,          // true 在线 / false 已关机 / null 未知\n  status: null,          // /api/status 结果\n  statusLoading: false,\n  statusSeq: 0,\n  lastStatusAttempt: 0,\n  jobsKnown: false,      // /api/job 是否已确认电源任务状态\n  power: {\n    start: { submitting: false, pending: false, epoch: 0, completedJid: 0, name: \'开机\', busy: \'开机中\' },\n    stop: { submitting: false, pending: false, epoch: 0, completedJid: 0, name: \'关机\', busy: \'关机中\' },\n    warmup: { submitting: false, pending: false, epoch: 0, completedJid: 0, name: \'预热\', busy: \'预热中\' }\n  },\n  warmupCustomized: false,\n  warmupTargets: [\'v5_int8\', \'llm\'],\n  accounts: { active_email: \'\', drive_sync_email: \'\', accounts: [] },\n  acctLoading: false,\n  acctSwitching: false,\n  oauthUrl: \'\',\n  metricsBusy: false,\n  metricsTimer: null,\n\n  powerBusy() {\n    return [\'stop\', \'start\'].map(k => this.power[k]).find(t => t.submitting || t.pending) || null;\n  },\n\n  /* ---------- 胶囊 ---------- */\n  renderPill() {\n    const dot = $(\'machineDot\');\n    const busyTask = this.powerBusy();\n    const warm = this.power.warmup.submitting || this.power.warmup.pending;\n    let stateText, dotCls;\n    if (busyTask) { stateText = busyTask.busy; dotCls = \'dot dot-warn dot-breath\'; }\n    else if (warm && this.active === true) { stateText = \'预热中\'; dotCls = \'dot dot-warn dot-breath\'; }\n    else if (this.active === true) { stateText = \'G4 在线\'; dotCls = \'dot dot-ok\'; }\n    else if (this.active === false) { stateText = \'已关机\'; dotCls = \'dot\'; }\n    else { stateText = \'状态未知\'; dotCls = \'dot\'; }\n    dot.className = dotCls;\n    setText($(\'machineText\'), stateText);\n    const st = this.status;\n    const balNum = st ? parseFloat(st.balance) : NaN;\n    setText($(\'machineBal\'), Number.isFinite(balNum) ? `${Math.round(balNum)} CU` : \'— CU\');\n    const rateNum = st ? parseFloat(st.rate) : NaN;\n    const showRate = this.active === true && Number.isFinite(rateNum);\n    $(\'machineRate\').hidden = !showRate;\n    if (showRate) setText($(\'machineRate\'), `· ${+rateNum.toFixed(2)}/h`);\n    setTip($(\'machinePill\'), st && st.email ? `机器状态与账号（${st.email}）` : \'机器状态与账号\');\n  },\n\n  /* ---------- 浮层 ---------- */\n  togglePop() {\n    const box = $(\'machinePop\');\n    const opened = Pop.toggle(box, $(\'machinePill\'), {\n      align: \'end\',\n      onClose: () => {\n        this.stopMetrics();\n        $(\'acctPanel\').hidden = true;\n        $(\'acctSwitchBtn\').setAttribute(\'aria-expanded\', \'false\');\n        setText($(\'acctSwitchBtn\'), \'切换\');\n        $(\'acctOauthBox\').hidden = true;\n        $(\'warmupBox\').hidden = true;\n        $(\'warmupBtn\').setAttribute(\'aria-expanded\', \'false\');\n      }\n    });\n    if (opened) {\n      this.renderPopAll();\n      this.loadStatus(true);\n      if (!this.accounts.accounts.length) this.loadAccounts(false);\n      this.startMetrics();\n    }\n  },\n\n  renderPopAll() {\n    this.renderAccountHead();\n    this.renderAccountList();\n    this.renderPowerControls();\n  },\n\n  renderAccountHead() {\n    const st = this.status || {};\n    const email = st.email || this.accounts.active_email || \'\';\n    setText($(\'acctAvatar\'), (email || \'G\').charAt(0).toUpperCase());\n    setText($(\'acctEmail\'), email || \'未读取到账号\');\n    const balNum = parseFloat(st.balance);\n    const bal = Number.isFinite(balNum) ? `${balNum.toFixed(2)} CU` : \'— CU\';\n    const rateNum = parseFloat(st.rate);\n    const rate = Number.isFinite(rateNum) ? ` · 费率 ${rateNum.toFixed(2)}/h` : \'\';\n    setText($(\'acctSub\'), `余额 ${bal}${rate}`);\n  },\n\n  toggleAccountPanel() {\n    const panel = $(\'acctPanel\');\n    panel.hidden = !panel.hidden;\n    $(\'acctSwitchBtn\').setAttribute(\'aria-expanded\', panel.hidden ? \'false\' : \'true\');\n    setText($(\'acctSwitchBtn\'), panel.hidden ? \'切换\' : \'收起\');\n    if (!panel.hidden) { this.renderAccountList(); this.loadAccounts(false); }\n    else $(\'acctOauthBox\').hidden = true;\n    Pop.position();\n  },\n\n  renderAccountList() {\n    const list = $(\'acctList\');\n    const rows = Array.isArray(this.accounts.accounts) ? this.accounts.accounts : [];\n    if (!rows.length) {\n      list.replaceChildren(el(\'div\', \'hint-2 acct-item\', this.acctLoading ? \'正在读取账号与余额…\' : \'暂无已登录的 Colab 账号\'));\n      return;\n    }\n    list.replaceChildren(...rows.map(a => {\n      const b = el(\'button\', \'acct-item\');\n      b.type = \'button\';\n      b.dataset.action = \'switch-colab-account\';\n      b.dataset.email = a.email || \'\';\n      b.disabled = this.acctSwitching;\n      const main = el(\'span\', \'acct-item-main\');\n      main.append(el(\'span\', \'acct-item-email\', a.email || \'未知账号\'));\n      const balNum = typeof a.balance_num === \'number\' ? a.balance_num : parseFloat(a.balance);\n      const parts = [Number.isFinite(balNum) ? `${balNum.toFixed(2)} CU` : \'— CU\'];\n      if (a.active === true) parts.push(\'实例运行中\');\n      if (a.is_drive_sync) parts.push(\'云盘同步\');\n      main.append(el(\'span\', \'acct-item-bal num\', parts.join(\' · \')));\n      b.append(main);\n      if (a.is_active) b.append(icon(\'i-check\', \'ico ico-check\'));\n      return b;\n    }));\n  },\n\n  async loadAccounts(force = false) {\n    if (this.acctLoading && !force) return;\n    this.acctLoading = true;\n    if (force) setText($(\'acctPickerHint\'), \'正在刷新余额…\');\n    this.renderAccountList();\n    try {\n      const data = await api.get(force ? \'/api/colab_accounts?force=1\' : \'/api/colab_accounts\');\n      if (data && data.ok) {\n        this.accounts = {\n          active_email: data.active_email || \'\',\n          drive_sync_email: data.drive_sync_email || \'\',\n          accounts: Array.isArray(data.accounts) ? data.accounts : []\n        };\n      }\n    } catch (_) {\n    } finally {\n      this.acctLoading = false;\n      if ($(\'acctPickerHint\').textContent === \'正在刷新余额…\') setText($(\'acctPickerHint\'), \'\');\n      this.renderAccountList();\n      Pop.position();\n    }\n  },\n\n  async switchAccount(email, stopCurrent = false) {\n    if (!email || this.acctSwitching) return;\n    const cur = (this.accounts.accounts || []).find(a => a.is_active);\n    if (cur && cur.email === email) { toast(`当前已在使用 ${email}`); return; }\n    const askStop = (msg) => Confirm.ask({\n      title: \'切换前先关闭当前实例？\',\n      text: msg || `当前账号（${cur?.email || \'原账号\'}）仍有运行中的 Colab 实例。为避免原账号在后台持续扣费，将先自动关机确认释放后再切换至 ${email}。`,\n      accept: \'关机并切换\',\n      danger: true,\n      run: () => this.switchAccount(email, true)\n    });\n    if (!stopCurrent && (this.active === true || cur?.active === true)) { askStop(); return; }\n    this.acctSwitching = true;\n    setText($(\'acctPickerHint\'), stopCurrent ? \'正在关闭当前实例并切换账号…\' : \'正在切换算力账号…\');\n    this.renderAccountList();\n    try {\n      const { res, data } = await api.raw(\'/api/colab_accounts/switch\', {\n        method: \'POST\', headers: { \'Content-Type\': \'application/json\' },\n        body: JSON.stringify({ email, stop_current: stopCurrent })\n      });\n      if (res.status === 409 && data.need_stop_confirm && !stopCurrent) {\n        this.acctSwitching = false;\n        askStop(data.message);\n        return;\n      }\n      if (!res.ok || !data.ok) { toast(data.message || \'切换账号失败\'); return; }\n      if (data.accounts_state && Array.isArray(data.accounts_state.accounts)) this.accounts = data.accounts_state;\n      toast(data.message || `已切换至 ${email}`);\n      await this.loadStatus(true);\n      await Jobs.poll(true);\n    } catch (e) {\n      toast(e.message || \'网络异常，切换失败\');\n    } finally {\n      this.acctSwitching = false;\n      setText($(\'acctPickerHint\'), \'\');\n      this.renderAccountList();\n    }\n  },\n\n  async toggleOauth() {\n    const box = $(\'acctOauthBox\');\n    if (!box.hidden) { box.hidden = true; Pop.position(); return; }\n    setText($(\'acctPickerHint\'), \'正在生成 Google 授权链接…\');\n    try {\n      const data = await api.post(\'/api/colab_accounts/oauth_start\', {});\n      if (!data.auth_url) throw new Error(data.message || \'生成授权链接失败\');\n      this.oauthUrl = data.auth_url;\n      $(\'acctOauthLink\').href = /^https:\\/\\//i.test(this.oauthUrl) ? this.oauthUrl : \'#\';\n      $(\'acctOauthCodeInp\').value = \'\';\n      box.hidden = false;\n      Pop.position();\n      setTimeout(() => $(\'acctOauthCodeInp\').focus(), 60);\n    } catch (e) {\n      toast(e.message || \'生成授权链接失败\');\n    } finally {\n      setText($(\'acctPickerHint\'), \'\');\n    }\n  },\n\n  async submitOauth() {\n    const inp = $(\'acctOauthCodeInp\');\n    const code = inp.value.trim();\n    if (!code) { toast(\'请先粘贴 Google 授权码\'); inp.focus(); return; }\n    const b = $(\'acctOauthSubmitBtn\');\n    b.disabled = true;\n    try {\n      const data = await api.post(\'/api/colab_accounts/oauth_complete\', { code });\n      inp.value = \'\';\n      $(\'acctOauthBox\').hidden = true;\n      if (data.accounts_state && Array.isArray(data.accounts_state.accounts)) this.accounts = data.accounts_state;\n      else await this.loadAccounts(true);\n      this.renderAccountList();\n      await this.loadStatus(true);\n      toast(data.message || \'新账号登录成功\');\n    } catch (e) {\n      toast(e.message || \'授权验证失败\');\n    } finally {\n      b.disabled = false;\n    }\n  },\n\n  /* ---------- 状态 ---------- */\n  async loadStatus(force = false) {\n    const seq = ++this.statusSeq;\n    const showBusy = this.active === null;\n    if (showBusy) { this.statusLoading = true; this.renderPowerControls(); }\n    this.lastStatusAttempt = Date.now();\n    let st = null;\n    try {\n      const data = await api.get(force ? \'/api/status?force=1\' : \'/api/status\');\n      st = data && typeof data === \'object\' ? data : null;\n    } catch (_) {}\n    if (seq !== this.statusSeq) return;\n    this.statusLoading = false;\n    this.active = st ? (st.active === true ? true : (st.active === false ? false : null)) : null;\n    this.status = st;\n    this.renderPill();\n    this.renderAccountHead();\n    this.renderPowerControls();\n    if (this.active === false) this.renderMetrics(null);\n  },\n\n  /* ---------- 电源 / 预热 ---------- */\n  renderPowerControls() {\n    this.renderPill();\n    const b = $(\'powerBtn\');\n    const busyTask = this.powerBusy();\n    const known = this.jobsKnown && (this.active === true || this.active === false);\n    b.disabled = !!busyTask || this.statusLoading || !known;\n    let text, cls;\n    if (busyTask) { text = busyTask.busy; cls = \'btn btn-secondary\'; }\n    else if (this.statusLoading) { text = \'检查中\'; cls = \'btn btn-secondary\'; }\n    else if (!known) { text = \'状态未知\'; cls = \'btn btn-secondary\'; }\n    else if (this.active) { text = \'关机\'; cls = \'btn btn-secondary is-danger\'; }\n    else { text = \'开机\'; cls = \'btn btn-primary\'; }\n    b.className = cls;\n    setText($(\'powerBtnText\'), text);\n    b.setAttribute(\'aria-label\', text);\n    const w = this.power.warmup;\n    const wBusy = w.submitting || w.pending;\n    setText($(\'warmupBtnText\'), wBusy ? \'预热中\' : \'预热…\');\n    document.querySelectorAll(\'[data-warmup-target]\').forEach(c => { c.checked = this.warmupTargets.includes(c.dataset.warmupTarget); });\n    const blocked = wBusy || !!busyTask || this.statusLoading || !this.jobsKnown || this.active !== true;\n    $(\'startWarmupBtn\').disabled = blocked || !this.warmupTargets.length;\n    setText($(\'warmupHint\'), this.active !== true ? \'请先开机\' : (!this.warmupTargets.length ? \'请至少选择一项\' : (wBusy ? \'正在预热…\' : \'\')));\n  },\n\n  toggleWarmupBox() {\n    const box = $(\'warmupBox\');\n    box.hidden = !box.hidden;\n    $(\'warmupBtn\').setAttribute(\'aria-expanded\', box.hidden ? \'false\' : \'true\');\n    this.renderPowerControls();\n    Pop.position();\n  },\n\n  onWarmupCheck(input) {\n    this.warmupCustomized = true;\n    const k = input.dataset.warmupTarget;\n    this.warmupTargets = input.checked ? [...this.warmupTargets.filter(t => t !== k), k] : this.warmupTargets.filter(t => t !== k);\n    this.renderPowerControls();\n    scheduleSave();\n  },\n\n  togglePower() {\n    if (this.powerBusy() || this.statusLoading || !this.jobsKnown) return;\n    if (this.active === true) {\n      Confirm.ask({\n        title: \'确认关闭 Colab G4 实例？\',\n        text: \'关闭远端 GPU 实例；正在运行和排队的任务将自动暂停保全，下次开机后可手动继续。\',\n        accept: \'关机\', danger: true,\n        run: () => this.submitPower(\'stop\')\n      });\n    } else if (this.active === false) {\n      this.submitPower(\'start\');\n    }\n  },\n\n  async submitPower(mod) {\n    const task = this.power[mod];\n    if (task.submitting || task.pending || this.powerBusy() || this.statusLoading || !this.jobsKnown) return;\n    if (mod === \'warmup\' && this.active !== true) { toast(\'请先开机；预热不会自动创建实例\'); return; }\n    if (mod === \'start\' && this.active !== false) return;\n    if (mod === \'stop\' && this.active !== true) return;\n    if (mod === \'warmup\' && !this.warmupTargets.length) { toast(\'请至少选择一项\'); return; }\n    const targets = [...this.warmupTargets];\n    task.submitting = true;\n    task.epoch += 1;\n    if (mod !== \'warmup\') { this.jobsKnown = false; this.active = null; this.statusSeq += 1; this.statusLoading = false; }\n    this.renderPowerControls();\n    try {\n      const data = await api.post(\'/api/run\', mod === \'warmup\' ? { module: mod, targets } : { module: mod });\n      task.epoch += 1;\n      task.pending = true;\n      if (mod === \'warmup\') { $(\'warmupBox\').hidden = true; Pop.position(); }\n      toast(data.message || (task.name + \'请求已受理\'));\n      await Jobs.poll(true);\n    } catch (e) {\n      task.epoch += 1;\n      task.pending = false;\n      toast(task.name + \'失败：\' + (e.message || \'网络异常\'));\n      if (mod !== \'warmup\') { Jobs.poll(true); this.loadStatus(false); }\n    } finally {\n      task.submitting = false;\n      this.renderPowerControls();\n    }\n  },\n\n  /* 由任务轮询驱动：同步电源任务挂起状态 */\n  applyJob(st, epochs) {\n    let refresh = false;\n    if (epochs.start === this.power.start.epoch && epochs.stop === this.power.stop.epoch) this.jobsKnown = true;\n    for (const mod of [\'start\', \'stop\', \'warmup\']) {\n      const t = this.power[mod];\n      if (epochs[mod] === t.epoch) t.pending = !!st[mod + \'_pending\'];\n      if ((st[mod + \'_completed_jid\'] || 0) > t.completedJid) {\n        if (t.completedJid) refresh = true;\n        t.completedJid = st[mod + \'_completed_jid\'];\n      }\n    }\n    if (refresh) {\n      this.active = null;\n      this.loadStatus(true);\n    } else if (!this.statusLoading && Date.now() - this.lastStatusAttempt >= 30000) {\n      this.loadStatus(false);\n    }\n    this.renderPowerControls();\n  },\n\n  /* ---------- 硬件（仅浮层打开时轮询） ---------- */\n  startMetrics() {\n    this.stopMetrics();\n    this.pollMetrics();\n    this.metricsTimer = setInterval(() => this.pollMetrics(), 3000);\n  },\n  stopMetrics() {\n    if (this.metricsTimer) clearInterval(this.metricsTimer);\n    this.metricsTimer = null;\n  },\n  async pollMetrics() {\n    if (this.active === false) { this.renderMetrics(null); return; }\n    if (document.hidden || this.metricsBusy) return;\n    this.metricsBusy = true;\n    try {\n      const m = await api.get(\'/api/metrics\');\n      this.renderMetrics(m && m.online && this.active !== false ? m : null);\n    } catch (_) {\n    } finally {\n      this.metricsBusy = false;\n    }\n  },\n  renderMetrics(m) {\n    const cell = (key, text, pct) => {\n      setText($(\'hwVal\' + key), text);\n      const bar = $(\'hwBar\' + key);\n      const ok = typeof pct === \'number\' && Number.isFinite(pct);\n      bar.style.transform = `scaleX(${ok ? clamp(pct, 0, 100) / 100 : 0})`;\n      bar.classList.toggle(\'is-warn\', ok && pct > 85);\n    };\n    if (!m) { [\'Cpu\', \'Ram\', \'Vram\', \'Disk\'].forEach(k => cell(k, \'--\', null)); return; }\n    const gb = (u, t, p) => (typeof u === \'number\' && typeof t === \'number\') ? [`${u.toFixed(1)} / ${t.toFixed(0)} GB`, p] : [\'--\', null];\n    cell(\'Cpu\', typeof m.cpu_pct === \'number\' ? `${m.cpu_pct.toFixed(1)}%${m.cpu_cores ? ` · ${m.cpu_cores} 核` : \'\'}` : \'--\', m.cpu_pct);\n    cell(\'Ram\', ...gb(m.ram_used_gb, m.ram_total_gb, m.ram_pct));\n    cell(\'Vram\', ...gb(m.vram_used_gb, m.vram_total_gb, m.vram_pct));\n    cell(\'Disk\', ...gb(m.disk_used_gb, m.disk_total_gb, m.disk_pct));\n  }\n};\n/* ==================== 任务：轮询 / 胶囊 / 抽屉 / 日志 ==================== */\nconst POWER_MODULES = [\'start\', \'stop\', \'warmup\'];\n\nfunction taskName(t) {\n  const label = stripEmoji(t.label || \'\');\n  let m;\n  if ((m = label.match(/^随机3K\\s*\\[(\\d+)\\/(\\d+)/))) return `随机生图 ${m[1]}/${m[2]}`;\n  if ((m = label.match(/^随机写词\\s*\\[(\\d+)\\/(\\d+)/))) return `随机写词 ${m[1]}/${m[2]}`;\n  if ((m = label.match(/^随机.*?\\[(\\d+)\\/(\\d+)/)) && t.module === \'video\') return `随机视频 ${m[1]}/${m[2]}`;\n  if (/^预热/.test(label) || t.module === \'warmup\') return \'预热\';\n  if (t.module === \'start\') return \'开机\';\n  if (t.module === \'stop\') return \'关机\';\n  if (t.module === \'image\') return \'3K 生图\';\n  if (t.module === \'video\') return \'视频\';\n  if (t.module === \'llm\') return \'提示词大师\';\n  return label || \'任务\';\n}\n\nfunction batchPos(t) {\n  const m = stripEmoji(t.label || \'\').match(/^随机.*?\\[(\\d+)\\/(\\d+)/);\n  return m ? { cur: +m[1], total: +m[2] } : null;\n}\n\nfunction cleanPhase(s) {\n  return stripEmoji(s).replace(/^\\[[^\\]]*\\]\\s*/, \'\').replace(/^最近任务:.*$/, \'\').trim();\n}\n\nconst Tasks = {\n  tab: \'active\',\n  expanded: new Set(),\n  busy: new Set(),\n  detail: new Map(),\n  showAll: false,\n  logOpen: false,\n  lastLog: \'\',\n  sig: \'\',\n  clearing: false,\n\n  running() { return S.tasks.filter(t => t.state === \'running\'); },\n  queued() { return S.tasks.filter(t => t.state === \'queued\'); },\n  paused() { return S.tasks.filter(t => t.state === \'paused\'); },\n  history() { return S.tasks.filter(t => ![\'running\', \'queued\', \'paused\'].includes(t.state)); },\n  laneTasks(mod) { return S.tasks.filter(t => t.module === mod && (t.state === \'running\' || t.state === \'queued\')); },\n\n  pct() {\n    const p = (S.job && S.job.progress) || {};\n    if (p.indeterminate === true) return null;\n    return typeof p.percent === \'number\' && Number.isFinite(p.percent) ? clamp(Math.round(p.percent), 0, 99) : null;\n  },\n\n  /* ---------- 顶栏任务块（固定窄宽，零截断、零抖动） ---------- */\n  renderPill() {\n    const st = S.job || {};\n    const pill = $(\'taskPill\');\n    const aCnt = st.active_count || 0;\n    const qCnt = st.queued_count || 0;\n    const pCnt = st.paused_count || 0;\n    const run = this.running()[0];\n    const active = aCnt > 0 || !!st.running;\n    const pct = active ? this.pct() : null;\n    const txt = $(\'taskPillText\');\n    const icoIdle = $(\'taskIcoIdle\');\n    const ring = $(\'taskRing\');\n    const ringFill = $(\'taskRingFill\');\n\n    pill.classList.toggle(\'is-running\', active);\n    icoIdle.toggleAttribute(\'hidden\', active);\n    ring.toggleAttribute(\'hidden\', !active);\n    if (active) {\n      if (pct !== null) {\n        ring.classList.remove(\'is-spin\');\n        ringFill.style.strokeDashoffset = (40.84 * (1 - pct / 100)).toFixed(2);\n      } else {\n        ring.classList.add(\'is-spin\');\n        ringFill.style.strokeDashoffset = \'\';\n      }\n    } else {\n      ring.classList.remove(\'is-spin\');\n    }\n\n    const mob = isMobile();\n    let label;\n    if (active) {\n      const verb = run && POWER_MODULES.includes(run.module) ? taskName(run) + \'中\' : \'生成中\';\n      label = mob ? (pct !== null ? `${pct}%` : \'运行\') : verb + (pct !== null ? ` ${pct}%` : \'\');\n    } else {\n      label = pCnt > 0 ? (mob ? \'\' : `${pCnt} 个暂停`) : \'空闲\';\n    }\n    setText(txt, label);\n    txt.hidden = !label;\n    txt.classList.toggle(\'is-compact-hide\', !active && pCnt > 0);\n    const showQueue = active && qCnt > 0 && !mob;\n    $(\'taskPillQueue\').hidden = !showQueue;\n    setText($(\'taskPillQueue\'), `+${qCnt}`);\n    const showPausedBadge = pCnt > 0 && ((!active && mob) || (active && !showQueue && !mob));\n    $(\'taskPillPaused\').hidden = !showPausedBadge;\n    setText($(\'taskPillPausedText\'), `${pCnt}`);\n    $(\'taskPillProgress\').hidden = !(active && pct !== null);\n    $(\'taskPillProgressFill\').style.transform = `scaleX(${(pct || 0) / 100})`;\n    setTip(pill, active ? `任务：运行 ${aCnt} · 排队 ${qCnt}${pCnt ? ` · 暂停 ${pCnt}` : \'\'}` : \'任务队列\');\n  },\n\n  /* ---------- 抽屉 ---------- */\n  setTab(tab) {\n    this.tab = [\'active\', \'paused\', \'history\'].includes(tab) ? tab : \'active\';\n    this.render(true);\n  },\n\n  render(force = false) {\n    const st = S.job || {};\n    setText($(\'drawerCntActive\'), String(this.running().length + this.queued().length));\n    setText($(\'drawerCntPaused\'), String(st.paused_count ?? this.paused().length));\n    document.querySelectorAll(\'#drawerSeg .seg-item\').forEach(b => {\n      const on = b.dataset.tab === this.tab;\n      b.classList.toggle(\'is-active\', on);\n      b.setAttribute(\'aria-selected\', on ? \'true\' : \'false\');\n    });\n    if (!Drawer.open) return;\n    const sig = JSON.stringify([this.tab, S.tasks, [...this.expanded], [...this.busy], this.detail.size, this.showAll, st.total_history_count, Machine.jobsKnown, !!Machine.powerBusy(), Tasks.pct(), (st.progress || {}).stage_text]);\n    if (!force && sig === this.sig) return;\n    this.sig = sig;\n    const body = $(\'drawerBody\');\n    const scroll = body.scrollTop;\n    const frag = document.createDocumentFragment();\n    if (this.tab === \'active\') this.renderActive(frag);\n    else if (this.tab === \'paused\') this.renderPaused(frag);\n    else this.renderHistory(frag);\n    body.replaceChildren(frag);\n    body.scrollTop = scroll;\n  },\n\n  locked(jid) { return this.busy.has(jid) || !Machine.jobsKnown || !!Machine.powerBusy(); },\n\n  renderActive(frag) {\n    const run = this.running();\n    const q = this.queued();\n    if (!run.length && !q.length) { frag.append(el(\'div\', \'drawer-empty\', \'没有进行中的任务\')); return; }\n    const prog = (S.job && S.job.progress) || {};\n    run.forEach(t => {\n      const blk = el(\'div\', \'run-block\');\n      const l1 = el(\'div\', \'run-line\');\n      const nm = el(\'span\', \'task-name\', taskName(t));\n      setTip(nm, stripEmoji(t.label || \'\'));\n      l1.append(nm);\n      if (t.can_stop) {\n        const b = btn(\'btn btn-ghost btn-sm is-danger-hover\', \'中止\', \'task-stop\', { jid: t.jid });\n        b.disabled = this.locked(t.jid);\n        l1.append(b);\n      }\n      const phase = Stage.phaseText(t) || \'执行中\';\n      const sub = el(\'span\', \'run-sub num\', `${phase} · ${fmtClock(t.elapsed_sec)}`);\n      const bar = el(\'span\', \'progress\');\n      const fill = el(\'span\', \'progress-fill\');\n      bar.append(fill);\n      const pct = (run.length === 1 || t.module === (S.job && S.job.module)) ? this.pct() : null;\n      if (pct === null || prog.indeterminate) bar.classList.add(\'is-indeterminate\');\n      else fill.style.transform = `scaleX(${pct / 100})`;\n      blk.append(l1, sub, bar);\n      frag.append(blk);\n    });\n    if (q.length) {\n      const list = el(\'div\', \'tlist\');\n      q.forEach(t => {\n        const row = el(\'div\', \'trow\');\n        const nm = el(\'span\', \'task-name\', taskName(t));\n        setTip(nm, stripEmoji(t.label || \'\'));\n        row.append(el(\'span\', \'trow-id num\', `#${t.jid}`), nm);\n        if (t.can_stop) {\n          const b = btn(\'btn btn-ghost btn-sm is-danger-hover\', \'取消\', \'task-stop\', { jid: t.jid });\n          b.disabled = this.locked(t.jid);\n          row.append(b);\n        }\n        list.append(row);\n      });\n      frag.append(list);\n    }\n  },\n\n  pauseTime(t) {\n    const m = String(t.time_info || \'\').match(/^(\\d{1,2}:\\d{2}(?::\\d{2})?)/);\n    return m ? m[1] : \'\';\n  },\n\n  renderPaused(frag) {\n    const list = this.paused();\n    const note = el(\'div\', \'drawer-note\');\n    note.append(el(\'span\', \'hint-2\', \'关机或重启时自动暂停的任务，可以继续或丢弃\'));\n    if (list.length) {\n      const allBusy = !Machine.jobsKnown || !!Machine.powerBusy() || this.busy.size > 0;\n      const r = btn(\'btn btn-secondary btn-sm\', \'全部继续\', \'tasks-resume-all\');\n      const d = btn(\'btn btn-ghost btn-sm is-danger\', \'全部丢弃\', \'tasks-discard-all\');\n      r.disabled = allBusy; d.disabled = allBusy;\n      note.append(r, d);\n    }\n    frag.append(note);\n    if (!list.length) { frag.append(el(\'div\', \'drawer-empty\', \'没有已暂停的任务\')); return; }\n    const box = el(\'div\', \'tlist\');\n    list.forEach(t => {\n      const row = el(\'div\', \'trow\');\n      const nm = el(\'span\', \'task-name\', taskName(t));\n      setTip(nm, stripEmoji(t.label || \'\') + (t.resume_disabled_reason ? `（${t.resume_disabled_reason}）` : \'\'));\n      row.append(nm);\n      const pt = this.pauseTime(t);\n      if (pt) row.append(el(\'span\', \'trow-time num\', pt));\n      const acts = el(\'span\', \'trow-acts\');\n      if (t.can_resume) {\n        const b = btn(\'btn btn-secondary btn-sm\', \'继续\', \'task-resume\', { jid: t.jid });\n        b.disabled = this.locked(t.jid);\n        acts.append(b);\n      } else if (t.resume_disabled_reason) {\n        acts.append(el(\'span\', \'hint-3\', t.resume_disabled_reason));\n      }\n      if (t.can_delete) {\n        const b = btn(\'btn btn-ghost btn-sm is-danger-hover\', \'丢弃\', \'task-delete\', { jid: t.jid });\n        b.disabled = this.locked(t.jid);\n        acts.append(b);\n      }\n      row.append(acts);\n      box.append(row);\n    });\n    frag.append(box);\n  },\n\n  renderHistory(frag) {\n    const list = this.history();\n    const st = S.job || {};\n    if (!list.length) frag.append(el(\'div\', \'drawer-empty\', \'暂无历史记录\'));\n    const box = el(\'div\', \'tlist\');\n    list.forEach(t => {\n      const wrap = el(\'div\', \'hrow\');\n      const open = this.expanded.has(t.jid);\n      const head = el(\'button\', \'hrow-head\');\n      head.type = \'button\';\n      head.dataset.action = \'toggle-task\';\n      head.dataset.jid = String(t.jid);\n      head.setAttribute(\'aria-expanded\', open ? \'true\' : \'false\');\n      const [ico, cls, label] = t.state === \'done\' ? [\'i-check\', \'is-ok\', \'已完成\'] : (t.state === \'error\' ? [\'i-close\', \'is-err\', \'失败\'] : [\'i-close\', \'is-off\', \'已中止\']);\n      const si = icon(ico, \'ico st-ico \' + cls);\n      si.setAttribute(\'aria-label\', label);\n      const nm = el(\'span\', \'task-name\', taskName(t));\n      setTip(nm, stripEmoji(t.label || \'\'));\n      head.append(si, nm, el(\'span\', \'trow-time num\', String(t.time_info || \'\').replace(/(\\d)s$/, \'$1 秒\')));\n      wrap.append(head);\n      if (open) {\n        const det = el(\'div\', \'hrow-detail\');\n        const cached = this.detail.get(t.jid);\n        const fp = t.full_prompt || cached?.full_prompt || t.preview || (t.has_detail ? \'加载中…\' : \'\');\n        const badge = stripEmoji(t.badge || \'\');\n        if (t.state === \'error\' && badge) det.append(el(\'span\', \'hint-2\', badge));\n        if (fp) det.append(el(\'div\', \'hrow-prompt\', fp));\n        const cmd = t.cmd_s || cached?.cmd_s || \'\';\n        if (cmd) det.append(el(\'div\', \'hrow-cmd\', cmd));\n        const acts = el(\'div\', \'hrow-acts\');\n        const reuseTarget = t.module === \'video\' ? \'video\' : (t.module === \'image\' || t.module === \'llm\' ? \'image\' : \'\');\n        const fullText = t.full_prompt || cached?.full_prompt || \'\';\n        if (reuseTarget && fullText) acts.append(btn(\'btn btn-secondary btn-sm\', \'复用\', \'task-reuse\', { jid: t.jid, target: reuseTarget }));\n        if (t.can_resume) {\n          const b = btn(\'btn btn-ghost btn-sm\', \'重新运行\', \'task-resume\', { jid: t.jid });\n          b.disabled = this.locked(t.jid);\n          acts.append(b);\n        }\n        if (t.can_delete) {\n          const b = btn(\'btn btn-ghost btn-sm is-danger\', \'删除记录\', \'task-delete\', { jid: t.jid });\n          b.disabled = this.locked(t.jid);\n          acts.append(b);\n        }\n        if (acts.childNodes.length) det.append(acts);\n        wrap.append(det);\n      }\n      box.append(wrap);\n    });\n    frag.append(box);\n    const foot = el(\'div\', \'drawer-foot-row\');\n    const shownHistory = S.tasks.filter(t => t.state !== \'running\' && t.state !== \'queued\').length;\n    if (!this.showAll && (st.total_history_count || 0) > shownHistory) {\n      foot.append(btn(\'btn btn-ghost btn-sm\', `加载更多（共 ${st.total_history_count} 条）`, \'load-all-tasks\'));\n    } else foot.append(el(\'span\'));\n    const hasDel = (st.deletable_count || 0) > 0 || S.tasks.some(t => t.can_delete);\n    if (hasDel) {\n      const c = btn(\'btn btn-ghost btn-sm is-danger\', \'清空记录\', \'clear-tasks\');\n      c.disabled = this.clearing || !Machine.jobsKnown || !!Machine.powerBusy();\n      foot.append(c);\n    }\n    frag.append(foot);\n  },\n\n  toggleExpand(jid) {\n    if (this.expanded.has(jid)) this.expanded.delete(jid);\n    else { this.expanded.add(jid); this.loadDetail(jid); }\n    this.render(true);\n  },\n\n  async loadDetail(jid) {\n    const t = S.tasks.find(x => x.jid === jid);\n    if (!t || t.full_prompt || t.cmd_s || this.detail.has(jid)) return;\n    try {\n      const data = await api.get(\'/api/task_detail?jid=\' + encodeURIComponent(jid));\n      if (data && data.ok) {\n        this.detail.set(jid, { full_prompt: data.full_prompt || \'\', cmd_s: data.cmd_s || \'\' });\n        this.render(true);\n      }\n    } catch (_) {}\n  },\n\n  async action(jid, action, quiet = false) {\n    if (this.locked(jid)) return false;\n    const t = S.tasks.find(x => x.jid === jid);\n    if (!t) return false;\n    if (action === \'delete\' && (t.can_delete !== true || t.state === \'running\' || t.state === \'queued\')) return false;\n    if (action === \'stop\' && t.can_stop !== true) return false;\n    if (action === \'resume\' && t.can_resume !== true) return false;\n    this.busy.add(jid);\n    this.render(true);\n    let ok = false;\n    try {\n      const data = await api.post(\'/api/task_action\', { jid, action });\n      if (action === \'delete\') this.detail.delete(jid);\n      if (!quiet) toast(data.message || \'操作已受理\');\n      ok = true;\n    } catch (e) {\n      toast(e.message || \'操作失败\');\n    } finally {\n      this.busy.delete(jid);\n      if (!quiet) await Jobs.poll(true);\n    }\n    return ok;\n  },\n\n  askDelete(jid) {\n    const t = S.tasks.find(x => x.jid === jid);\n    if (!t || t.can_delete !== true || t.state === \'running\' || t.state === \'queued\') return;\n    const paused = t.state === \'paused\';\n    Confirm.ask({\n      title: paused ? `丢弃暂停的任务 #${jid}？` : `删除任务记录 #${jid}？`,\n      text: paused ? `「${taskName(t)}」将不再继续，记录一并移除。` : `删除「${taskName(t)}」记录后无法恢复（已生成的作品文件不受影响）。`,\n      accept: paused ? \'丢弃\' : \'删除\', danger: true,\n      run: () => this.action(jid, \'delete\')\n    });\n  },\n\n  async bulk(action) {\n    const list = this.paused().filter(t => action === \'resume\' ? t.can_resume : t.can_delete);\n    if (!list.length) { toast(\'没有可操作的暂停任务\'); return; }\n    let n = 0;\n    for (const t of list) { if (await this.action(t.jid, action, true)) n += 1; }\n    toast(action === \'resume\' ? `已继续 ${n} 个任务` : `已丢弃 ${n} 个任务`);\n    await Jobs.poll(true);\n  },\n\n  askDiscardAll() {\n    const n = this.paused().filter(t => t.can_delete).length;\n    if (!n) return;\n    Confirm.ask({ title: `丢弃全部 ${n} 个暂停任务？`, text: \'这些任务将不再继续，记录一并移除，无法恢复。\', accept: \'全部丢弃\', danger: true, run: () => this.bulk(\'delete\') });\n  },\n\n  askClearAll() {\n    const st = S.job || {};\n    const cnt = st.deletable_count || S.tasks.filter(t => t.can_delete).length;\n    if (!cnt || !Machine.jobsKnown || Machine.powerBusy()) return;\n    Confirm.ask({\n      title: \'清空任务记录？\',\n      text: `将移除全部 ${cnt} 条已结束或已暂停的任务记录（正在运行和排队中的任务保留，作品与对话不受影响）。`,\n      accept: \'清空\', danger: true,\n      run: async () => {\n        if (this.clearing) return;\n        this.clearing = true;\n        try {\n          const data = await api.post(\'/api/task_action\', { action: \'clear_all\' });\n          this.expanded.clear(); this.detail.clear(); this.showAll = false;\n          toast(data.message || \'任务记录已清空\');\n        } catch (e) { toast(e.message || \'清空失败\'); }\n        finally { this.clearing = false; await Jobs.poll(true); }\n      }\n    });\n  },\n\n  reuse(jid, target) {\n    const t = S.tasks.find(x => x.jid === jid);\n    const text = t?.full_prompt || this.detail.get(jid)?.full_prompt || \'\';\n    if (!text) return;\n    Drawer.set(false);\n    Create.fillPrompt(target, t.module === \'llm\' ? extractCleanPrompt(text) : text, \'已复用到\' + (target === \'video\' ? \'视频\' : \'图片\') + \'提示词\');\n  },\n\n  /* ---------- 日志 ---------- */\n  setLogOpen(open) {\n    this.logOpen = open;\n    const fold = $(\'logFold\');\n    fold.classList.toggle(\'is-open\', open);\n    fold.parentElement?.classList.toggle(\'is-open\', open);\n    $(\'logBox\').hidden = !open;\n    document.querySelector(\'#logFold .log-head\').setAttribute(\'aria-expanded\', open ? \'true\' : \'false\');\n    if (open) { const b = $(\'logBox\'); b.scrollTop = b.scrollHeight; requestAnimationFrame(() => updateScrollFade(b, \'y\')); }\n  },\n  renderLog(force = false) {\n    const text = (S.job && S.job.log_text) || \'\';\n    const lines = text.trimEnd().split(\'\\n\');\n    setText($(\'logPreview\'), stripEmoji(lines[lines.length - 1] || \'\') || \'等待提交任务…\');\n    const box = $(\'logBox\');\n    if (!force && text === this.lastLog) return;\n    const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 36;\n    const keep = box.scrollTop;\n    this.lastLog = text;\n    box.textContent = text || \'等待提交任务…\';\n    box.scrollTop = (atBottom || force) ? box.scrollHeight : keep;\n    if (this.logOpen) requestAnimationFrame(() => updateScrollFade(box, \'y\'));\n  }\n};\n\n/* ==================== 任务轮询 ==================== */\nconst Jobs = {\n  inFlight: false,\n  pendingPoll: false,\n  seq: 0,\n  lastSeq: 0,\n  lastCompleted: null,\n\n  async poll(force = false) {\n    if (document.hidden && !force && !Create.submitting && !Chat.sending) return false;\n    if (this.inFlight) {\n      if (force) this.pendingPoll = true;\n      return false;\n    }\n    this.inFlight = true;\n    const seq = ++this.seq;\n    const P = Machine.power;\n    const epochs = { start: P.start.epoch, stop: P.stop.epoch, warmup: P.warmup.epoch };\n    try {\n      const st = await api.get(Tasks.showAll ? \'/api/job?limit=0\' : \'/api/job?limit=80\', { timeout: 10000 });\n      if (seq < this.lastSeq) return false;\n      this.lastSeq = seq;\n      S.job = st;\n      S.tasks = Array.isArray(st.tasks) ? st.tasks : [];\n      Machine.applyJob(st, epochs);\n      Tasks.renderPill();\n      Tasks.render();\n      Tasks.renderLog();\n      Create.applyJob(st);\n      Chat.applyJob(st);\n      const cur = st.completed_seq || 0;\n      let needReload = false;\n      if (this.lastCompleted === null) this.lastCompleted = cur;\n      else if (cur !== this.lastCompleted) {\n        this.lastCompleted = cur;\n        needReload = true;\n      }\n      Stage.render();\n      Chat.render();\n      if (needReload) Promise.all([Chat.load(), Works.load()]).catch(() => {});\n      return true;\n    } catch (e) {\n      if (seq < this.lastSeq) return false;\n      this.lastSeq = seq;\n      Machine.jobsKnown = false;\n      Machine.renderPowerControls();\n      return false;\n    } finally {\n      this.inFlight = false;\n      if (this.pendingPoll) {\n        this.pendingPoll = false;\n        this.poll(true);\n      }\n    }\n  }\n};\ndocument.addEventListener(\'visibilitychange\', () => { if (!document.hidden) Jobs.poll(true); });\n/* ==================== 创作页：模式 / 编辑栏 / 参数 / 参考图 / 提交 ==================== */\nfunction extractCleanPrompt(raw) {\n  const txt = String(raw || \'\').trim();\n  const zh = [];\n  let mode = \'\';\n  for (const line of txt.split(/\\r?\\n/)) {\n    const t = line.trim();\n    if (/Prompt in English|英文详细提示词/i.test(t)) { mode = \'en\'; continue; }\n    if (/Prompt in Chinese|中文详细提示词/i.test(t)) { mode = \'zh\'; continue; }\n    if (/^---+$/.test(t) || /^###/.test(t)) continue;\n    if (mode === \'zh\') zh.push(line);\n  }\n  if (zh.join(\'\\n\').trim().length > 20) return zh.join(\'\\n\').replace(/\\*\\*/g, \'\').trim();\n  return txt.replace(/^[\\s\\-#*]+/, \'\').trim();\n}\n\nconst Create = {\n  submitting: false,\n  randomSubmitting: false,\n  lastRandom: { image: 1, video: 1 },\n\n  init() {\n    const n = Number(store.raw(\'g4_last_random_count\'));\n    if (Number.isInteger(n) && n >= 1 && n <= 10) this.lastRandom.image = n;\n    setText($(\'runKbd\'), IS_MAC ? \'⌘↵\' : \'Ctrl↵\');\n    [\'promptInputImage\', \'promptInputVideo\'].forEach(id => {\n      const t = $(id);\n      t.addEventListener(\'input\', () => { this.autoGrow(t); this.updateCount(); scheduleSave(); });\n    });\n    const drop = $(\'refEmpty\');\n    [\'dragenter\', \'dragover\'].forEach(ev => drop.addEventListener(ev, (e) => {\n      if (!e.dataTransfer || !Array.from(e.dataTransfer.types || []).includes(\'Files\')) return;\n      e.preventDefault();\n      drop.classList.add(\'is-dragover\');\n    }));\n    [\'dragleave\', \'drop\'].forEach(ev => drop.addEventListener(ev, () => drop.classList.remove(\'is-dragover\')));\n    drop.addEventListener(\'drop\', (e) => {\n      e.preventDefault();\n      const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];\n      if (f) this.upload(f);\n    });\n    $(\'fileInputImage\').addEventListener(\'change\', (e) => {\n      const f = e.target.files && e.target.files[0];\n      if (f) this.upload(f);\n    });\n  },\n\n  setMode(mode) {\n    if (!MODES.includes(mode)) mode = \'image\';\n    const changed = S.mode !== mode;\n    S.mode = mode;\n    const page = $(\'pageCreate\');\n    page.dataset.mode = mode;\n    document.querySelectorAll(\'#modeSeg .seg-item\').forEach(b => {\n      const on = b.dataset.mode === mode;\n      b.classList.toggle(\'is-active\', on);\n      b.setAttribute(\'aria-pressed\', on ? \'true\' : \'false\');\n    });\n    $(\'promptTitle\').setAttribute(\'for\', curInput().id);\n    setText($(\'refTitle\'), mode === \'video\' ? \'首帧\' : \'人脸参考\');\n    if (changed) Pop.close(false);\n    this.autoGrow(curInput());\n    this.updateCount();\n    this.renderRef();\n    this.renderSummary();\n    this.renderRunBtn();\n    Stage.render(true);\n    Stage.renderRecent(true);\n    scheduleSave();\n  },\n\n  setEditorTab(tab) {\n    S.editorTab = tab === \'chat\' ? \'chat\' : \'write\';\n    $(\'pageCreate\').dataset.editor = S.editorTab;\n    Pop.close(false);\n    if (S.editorTab === \'chat\') { Chat.render(true); Chat.autoGrow(); Chat.scrollBottom(); }\n    else this.autoGrow(curInput());\n    scheduleSave();\n  },\n\n  autoGrow(t) {\n    if (!t || t.offsetParent === null) return;\n    if (!mqSingle.matches) {\n      t.style.height = \'\';\n      return;\n    }\n    t.style.height = \'auto\';\n    t.style.height = t.scrollHeight + 2 + \'px\';\n  },\n\n  renderSummary() {\n    const sum = $(\'footSummary\');\n    if (!sum) return;\n    if (S.mode === \'image\') {\n      const resMap = { portrait: \'竖图\', landscape: \'横图\', square: \'方图\' };\n      const [w, h] = this.imageDims();\n      const tier = S.seg.imgSteps || \'tier2\';\n      const stepEl = $(\'lblStepsTier\' + (tier === \'tier1\' ? \'1\' : tier === \'tier3\' ? \'3\' : \'2\'));\n      const stepTxt = stepEl ? stepEl.textContent : \'18 步\';\n      setText(sum, `${resMap[S.seg.imgRes] || \'竖图\'} ${w}×${h} · ${stepTxt}`);\n    } else {\n      const qMap = { std: \'标准 832p\', hd: \'超清 1088p\', uhd2k: \'2K 精修\' };\n      const sec = ((parseInt(S.seg.vidLen, 10) || 192) / 24).toFixed(1);\n      setText(sum, `${qMap[S.seg.vidQuality] || \'标准 832p\'} · ${sec} 秒 · ${S.seg.vidSteps || 20} 步`);\n    }\n  },\n\n  updateCount() {\n    setText($(\'promptCount\'), `${(curInput().value || \'\').trim().length} 字`);\n  },\n\n  fillPrompt(mode, text, msg = \'\') {\n    const m = MODES.includes(mode) ? mode : \'image\';\n    INPUTS[m]().value = text || \'\';\n    navigate(\'create\');\n    this.setMode(m);\n    this.setEditorTab(\'write\');\n    this.autoGrow(INPUTS[m]());\n    this.updateCount();\n    scheduleSave();\n    if (msg) toast(msg);\n    if (mqSingle.matches) INPUTS[m]().scrollIntoView({ block: \'center\', behavior: \'smooth\' });\n  },\n\n  clearPrompt() {\n    const inp = curInput();\n    if (!inp.value.trim()) { inp.value = \'\'; this.updateCount(); return; }\n    Confirm.ask({\n      title: \'清空当前输入？\',\n      text: `只清空${S.mode === \'video\' ? \'视频\' : \'图片\'}模式的提示词，另一模式和已选参考图保持不变。`,\n      accept: \'清空\',\n      run: () => { inp.value = \'\'; this.autoGrow(inp); this.updateCount(); scheduleSave(); inp.focus({ preventScroll: true }); }\n    });\n  },\n\n  /* ---------- 分段参数 ---------- */\n  setSeg(key, val, silent = false) {\n    const grp = $(SEG_GROUPS[key]);\n    if (!grp || !Array.from(grp.querySelectorAll(\'.seg-item\')).some(b => b.dataset.val === String(val))) return;\n    S.seg[key] = String(val);\n    grp.querySelectorAll(\'.seg-item\').forEach(b => {\n      const on = b.dataset.val === String(val);\n      b.classList.toggle(\'is-active\', on);\n      b.setAttribute(\'aria-pressed\', on ? \'true\' : \'false\');\n    });\n    if (key === \'vidQuality\') {\n      const q = VID_RES_BY_QUALITY[val] || VID_RES_BY_QUALITY.std;\n      setText($(\'lblVidResP\'), q[0]); setText($(\'lblVidResL\'), q[1]); setText($(\'lblVidResS\'), q[2]);\n    }\n    this.renderSummary();\n    if (!silent) { Stage.render(true); scheduleSave(); }\n  },\n\n  async loadPresets() {\n    try {\n      const data = await api.get(\'/api/settings\');\n      if (data && data.ok) {\n        if (data.ui_meta && typeof data.ui_meta === \'object\') {\n          S.uiMeta = data.ui_meta;\n          if (data.ui_meta.image_label) setText($(\'warmupLblImage\'), data.ui_meta.image_label);\n          if (data.ui_meta.llm_label) setText($(\'warmupLblLlm\'), data.ui_meta.llm_label);\n          if (data.ui_meta.video_label) setText($(\'warmupLblVideo\'), data.ui_meta.video_label);\n          Sync.applyVisibility();\n        }\n        if (data.settings) {\n          S.presets = data.settings;\n          this.applySettingsLabels(data.settings);\n        }\n      }\n    } catch (_) {}\n  },\n\n  applySettingsLabels(cfg) {\n    const wf = cfg && cfg.image_workflow;\n    if (!wf) return;\n    const res = wf.resolutions || {};\n    const setRes = (id, r) => { if (r && r.s2_w && r.s2_h) setText($(id), `${r.s2_w}×${r.s2_h}`); };\n    setRes(\'lblResPortrait\', res.portrait); setRes(\'lblResLandscape\', res.landscape); setRes(\'lblResSquare\', res.square);\n    const hires = wf.enable_hires !== false;\n    [[\'tier1\', false], [\'tier2\', true], [\'tier3\', false]].forEach(([k, def], i) => {\n      const t = (wf.step_tiers || {})[k];\n      if (!t) return;\n      const s1 = Number(t.s1) || 8, ref = Number(t.ref) || 0;\n      const use = hires && ref > 0;\n      setText($(\'lblStepsTier\' + (i + 1)), `${use ? s1 + ref : s1} 步`);\n      setText($(\'hintStepsTier\' + (i + 1)), (use ? `${s1}+${ref}` : \'直出\') + (def ? \' 默认\' : \'\'));\n    });\n    this.renderSummary();\n  },\n\n  imageDims() {\n    const resMode = [\'portrait\', \'landscape\', \'square\'].includes(S.seg.imgRes) ? S.seg.imgRes : \'portrait\';\n    const wf = S.presets && S.presets.image_workflow;\n    const r = wf?.resolutions?.[resMode] || (resMode === \'landscape\' ? { s2_w: 3072, s2_h: 2048 } : resMode === \'square\' ? { s2_w: 2560, s2_h: 2560 } : { s2_w: 2048, s2_h: 3072 });\n    return [Number(r.s2_w) || 2048, Number(r.s2_h) || 3072];\n  },\n  videoDims() {\n    const [w, h] = (S.seg.vidRes || \'832x1248\').split(\'x\').map(n => parseInt(n, 10));\n    return [w || 832, h || 1248];\n  },\n\n  /* ---------- 参考图 ---------- */\n  setRef(mode, path) {\n    S.ref[mode] = path || \'\';\n    this.renderRef();\n    RefPicker.markCurrent();\n  },\n\n  renderRef() {\n    const path = S.ref[S.mode];\n    const isVid = S.mode === \'video\';\n    $(\'refEmpty\').hidden = !!path;\n    $(\'refMounted\').hidden = !path;\n    setText($(\'refHint\'), isVid ? \'可选，作为视频第 1 帧；不选则纯文生\' : \'可选，锁定人脸\');\n    if (!path) return;\n    const item = findOutputByPath(path);\n    const thumb = item ? (item.thumb_url || item.url) : \'\';\n    const img = $(\'refThumb\');\n    if (thumb && img.getAttribute(\'src\') !== thumb) img.src = thumb;\n    img.hidden = !thumb;\n    setText($(\'refName\'), fileName(path));\n    setTip($(\'refName\'), fileName(path));\n    setText($(\'refSub\'), isVid ? \'已设为首帧\' : \'已挂载为人脸参考\');\n  },\n\n  async upload(file) {\n    if (!file.type.startsWith(\'image/\')) { toast(\'请选择图片文件\'); return; }\n    if (!file.size || file.size > 200 * 1024 * 1024) { toast(\'文件大小无效或超过 200 MB 限制\'); return; }\n    const mode = S.mode;\n    toast(\'正在上传图片…\');\n    try {\n      const { res, data } = await api.raw(\'/api/upload?filename=\' + encodeURIComponent(file.name), {\n        method: \'POST\', headers: { \'Content-Type\': \'application/octet-stream\' }, body: file\n      });\n      if (!res.ok || !data.ok) throw new Error(data.message || \'上传失败\');\n      await Works.load();\n      this.setRef(mode, data.path);\n      toast(mode === \'video\' ? \'已上传并设为首帧\' : \'已上传并挂载为人脸参考\');\n    } catch (e) {\n      toast((e.message || \'上传失败\') + \'；未自动重传\');\n    } finally {\n      $(\'fileInputImage\').value = \'\';\n    }\n  },\n\n  triggerUpload() {\n    const f = $(\'fileInputImage\');\n    f.value = \'\';\n    f.click();\n  },\n\n  /* ---------- 随机（单行清晰数量格，点选即开始，无默认误导选中态） ---------- */\n  toggleRandom(anchor) {\n    if (this.randomSubmitting || S.mode !== \'image\') return;\n    const grid = $(\'randomGrid\');\n    grid.replaceChildren(...Array.from({ length: 10 }, (_, i) => {\n      const n = i + 1;\n      const b = el(\'button\', \'rand-item\', `${n} 张`);\n      b.type = \'button\';\n      b.dataset.action = \'pick-random-count\';\n      b.dataset.count = String(n);\n      b.setAttribute(\'aria-label\', `随机生成 ${n} 张`);\n      return b;\n    }));\n    setText($(\'randomPopHint\'), \'点选数量立即开始随机生成\');\n    Pop.toggle($(\'randomPop\'), anchor, { align: \'start\' });\n  },\n\n  async submitRandom(mode, count) {\n    const n = clamp(Number(count) || 1, 1, 10);\n    this.lastRandom.image = n;\n    try { localStorage.setItem(\'g4_last_random_count\', String(n)); } catch (_) {}\n    Pop.close(false);\n    if (this.randomSubmitting) return;\n    this.randomSubmitting = true;\n    const btnEl = $(\'randomBtn\');\n    btnEl.disabled = true;\n    try {\n      const data = await api.post(\'/api/run\', { module: \'random\', source: \'panel\', count: n, step_tier: S.seg.imgSteps || \'tier2\' });\n      Stage.track(\'image\', { random: true, count: n });\n      toast(data.message || `已启动 ${n} 张随机生图`);\n      Jobs.poll(true);\n    } catch (e) {\n      toast(e.message || \'网络请求失败\');\n    } finally {\n      this.randomSubmitting = false;\n      btnEl.disabled = false;\n    }\n  },\n\n  /* ---------- 轮询联动 ---------- */\n  applyJob() {\n    this.renderRunBtn();\n  },\n\n  renderRunBtn() {\n    const busy = Tasks.laneTasks(S.mode).length > 0;\n    setText($(\'runBtnText\'), busy ? \'加入队列\' : \'生成\');\n  },\n\n  /* ---------- 提交 ---------- */\n  submit() {\n    if (S.page !== \'create\') return;\n    if (S.editorTab === \'chat\') { Chat.send(); return; }\n    this.submitGenerate();\n  },\n\n  async submitGenerate() {\n    if (this.submitting) return;\n    const mode = S.mode;\n    const inp = INPUTS[mode]();\n    const text = inp.value.trim();\n    if (!text) { toast(\'请先输入提示词\'); inp.focus(); return; }\n    const payload = { module: mode, text };\n    if (mode === \'image\') {\n      const resMode = [\'portrait\', \'landscape\', \'square\'].includes(S.seg.imgRes) ? S.seg.imgRes : \'portrait\';\n      const tier = [\'tier1\', \'tier2\', \'tier3\'].includes(S.seg.imgSteps) ? S.seg.imgSteps : \'tier2\';\n      const wf = S.presets && S.presets.image_workflow;\n      const t = wf?.step_tiers?.[tier] || (tier === \'tier2\' ? { s1: 12, ref: 6 } : tier === \'tier3\' ? { s1: 16, ref: 8 } : { s1: 8, ref: 5 });\n      const useHires = (wf ? wf.enable_hires !== false : true) && Number(t.ref) > 0;\n      const [w, h] = this.imageDims();\n      Object.assign(payload, {\n        res_mode: resMode, step_tier: tier, width: w, height: h, model: S.imgModel,\n        steps: useHires ? Number(t.s1) + Number(t.ref) : Number(t.s1), ref_image: S.ref.image || \'\'\n      });\n    } else {\n      const [w, h] = this.videoDims();\n      Object.assign(payload, {\n        width: w, height: h, length: parseInt(S.seg.vidLen, 10) || 192, steps: parseInt(S.seg.vidSteps, 10) || 20,\n        quality: S.seg.vidQuality || \'std\', ref_image: S.ref.video || \'\'\n      });\n    }\n    this.submitting = true;\n    $(\'runBtn\').disabled = true;\n    try {\n      const data = await api.post(\'/api/run\', payload);\n      const m = String(data.message || \'\').match(/#(\\d+)/);\n      Stage.track(mode, { jid: m ? Number(m[1]) : 0 });\n      toast(data.message || \'任务已提交\');\n      await Jobs.poll(true);\n    } catch (e) {\n      toast((e.message || \'提交失败\') + \'；未自动重试\');\n    } finally {\n      this.submitting = false;\n      $(\'runBtn\').disabled = false;\n    }\n  }\n};\n\nfunction findOutputByPath(path) {\n  if (!path) return null;\n  for (const k of [\'image\', \'upload_image\', \'video\']) {\n    const it = (S.outputs[k] || []).find(x => x.path === path);\n    if (it) return it;\n  }\n  return null;\n}\n\n/* ==================== 参考选择器 ==================== */\nconst RefPicker = {\n  tab: \'image\',\n  query: \'\',\n  list: [],\n  shown: 0,\n  BATCH: 60,\n  observer: null,\n\n  open() {\n    const d = $(\'refPicker\');\n    const cur = S.ref[S.mode];\n    if (cur) {\n      const it = findOutputByPath(cur);\n      if (it && it.source_mod === \'upload_image\') this.tab = \'upload_image\';\n    }\n    this.syncTabs();\n    this.build();\n    openDialog(d);\n  },\n  close() { closeDialog($(\'refPicker\')); },\n  setTab(tab) { this.tab = tab === \'upload_image\' ? \'upload_image\' : \'image\'; this.syncTabs(); this.build(); },\n  syncTabs() {\n    document.querySelectorAll(\'#refPickerSeg .seg-item\').forEach(b => b.classList.toggle(\'is-active\', b.dataset.tab === this.tab));\n  },\n  build() {\n    const q = this.query.trim().toLowerCase();\n    this.list = (S.outputs[this.tab] || []).filter(it => !q || ((it.name || \'\') + \'\\n\' + (it.prompt || \'\')).toLowerCase().includes(q));\n    const grid = $(\'refPickerGrid\');\n    grid.replaceChildren();\n    this.shown = 0;\n    if (!this.list.length) {\n      grid.append(el(\'div\', \'empty grid-empty\', q ? \'没有匹配的图片\' : (this.tab === \'upload_image\' ? \'还没有上传素材\' : \'还没有生成的图片\')));\n      return;\n    }\n    const cur = S.ref[S.mode];\n    const selIdx = this.list.findIndex(x => x.path === cur);\n    this.append(Math.max(this.BATCH, selIdx + 12));\n    $(\'refPickerScroll\').scrollTop = 0;\n  },\n  append(n) {\n    const grid = $(\'refPickerGrid\');\n    grid.querySelector(\'.rp-more\')?.remove();\n    const cur = S.ref[S.mode];\n    const end = Math.min(this.list.length, this.shown + n);\n    const frag = document.createDocumentFragment();\n    for (let i = this.shown; i < end; i++) {\n      const it = this.list[i];\n      const b = el(\'button\', \'rp-item\' + (it.path === cur ? \' is-current\' : \'\'));\n      b.type = \'button\';\n      b.dataset.action = \'pick-ref\';\n      b.dataset.path = it.path || \'\';\n      setTip(b, (it.name || \'\') + (it.res ? ` (${fmtRes(it.res)})` : \'\'));\n      b.setAttribute(\'aria-label\', \'选择 \' + (it.name || \'\'));\n      const img = el(\'img\');\n      img.src = it.thumb_url || it.url;\n      img.alt = \'\';\n      img.loading = \'lazy\';\n      img.decoding = \'async\';\n      b.append(img);\n      if (it.path === cur) { const badge = el(\'span\', \'rp-badge\'); badge.append(icon(\'i-check\')); b.append(badge); }\n      frag.append(b);\n    }\n    this.shown = end;\n    grid.append(frag);\n    if (this.shown < this.list.length) {\n      const more = el(\'div\', \'rp-more\');\n      grid.append(more);\n      if (!this.observer) {\n        this.observer = new IntersectionObserver((ents) => {\n          if (ents.some(e => e.isIntersecting)) this.append(this.BATCH);\n        }, { root: $(\'refPickerScroll\'), rootMargin: \'300px\' });\n      }\n      this.observer.disconnect();\n      this.observer.observe(more);\n    }\n  },\n  markCurrent() {\n    if (!$(\'refPicker\').open) return;\n    this.build();\n  },\n  pick(path) {\n    Create.setRef(S.mode, path);\n    this.close();\n    toast(S.mode === \'video\' ? \'已设为首帧\' : \'已挂载为人脸参考\');\n  }\n};\n/* ==================== 舞台：运行中 / 结果 / 失败 / 最近 ==================== */\nconst Stage = {\n  sel: { image: null, video: null },        // 用户在最近条里选中的作品名；null = 最新\n  manual: { image: false, video: false },   // 运行中用户手动看作品\n  tracks: {},                               // 当前模式提交跟踪 { jid, random, t }\n  fail: { image: null, video: null },       // { msg }\n  topSeen: { image: null, video: null },\n  sig: \'\',\n  recentSig: \'\',\n\n  list(mode) { return S.outputs[mode] || []; },\n\n  current(mode = S.mode) {\n    const list = this.list(mode);\n    if (!list.length) return null;\n    const name = this.sel[mode];\n    return (name && list.find(x => x.name === name)) || list[0];\n  },\n\n  phaseText(t) {\n    const p = (S.job && S.job.progress) || {};\n    if (t && t.state === \'queued\') {\n      const m = String(t.time_info || \'\').match(/前方\\s*(\\d+)\\s*个/);\n      return m && +m[1] > 0 ? `排队中，前面 ${m[1]} 个` : \'排队中\';\n    }\n    const phase = cleanPhase(p.phase_text || \'\');\n    if (typeof p.step_value === \'number\' && typeof p.step_max === \'number\' && p.step_max > 0) {\n      return `${/精绘|二阶段|refine/i.test(phase) ? \'二阶段精绘\' : \'采样\'} ${p.step_value}/${p.step_max}`;\n    }\n    let txt = phase || cleanPhase(p.stage_text || \'\');\n    if (!txt && t) {\n      const ti = String(t.time_info || \'\').split(\'·\').slice(1).join(\'·\');\n      txt = cleanPhase(ti);\n    }\n    txt = txt.replace(/（[^）]*）|\\([^)]*\\)/g, \'\').trim();\n    return txt.length > 22 ? txt.slice(0, 22) + \'…\' : (txt || \'准备中\');\n  },\n\n  onOutputs() {\n    MODES.forEach(mode => {\n      const top = (this.list(mode)[0] || {}).name || null;\n      if (this.topSeen[mode] !== null && top && top !== this.topSeen[mode]) {\n        // 新作品生成完成：插到最前并自动显示\n        this.sel[mode] = null;\n        this.manual[mode] = false;\n        this.fail[mode] = null;\n        if (mode === \'video\') this._autoplay = top;\n      }\n      this.topSeen[mode] = top;\n      if (this.sel[mode] && !this.list(mode).some(x => x.name === this.sel[mode])) this.sel[mode] = null;\n    });\n    this.resolveTrack();\n    this.render(true);\n    this.renderRecent();\n  },\n\n  initResize() {\n    if (typeof ResizeObserver !== \'undefined\') {\n      new ResizeObserver(() => {\n        if (S.page === \'create\') this.render();\n      }).observe($(\'stageView\'));\n    }\n    /* 触屏左右滑动切换当前预览作品 */\n    const view = $(\'stageView\');\n    let sw = null;\n    view.addEventListener(\'pointerdown\', (e) => {\n      if (e.pointerType !== \'touch\' || e.target.closest(\'button, video\')) return;\n      sw = { id: e.pointerId, x: e.clientX, y: e.clientY, t: Date.now() };\n    }, { passive: true });\n    view.addEventListener(\'pointerup\', (e) => {\n      if (!sw || sw.id !== e.pointerId) return;\n      const dx = e.clientX - sw.x, dy = e.clientY - sw.y, dt = Date.now() - sw.t;\n      sw = null;\n      if (Math.abs(dx) > 44 && Math.abs(dx) > Math.abs(dy) * 1.4 && dt < 600) {\n        this._swipedAt = Date.now();\n        this.step(dx < 0 ? 1 : -1);\n      }\n    }, { passive: true });\n    view.addEventListener(\'pointercancel\', () => { sw = null; }, { passive: true });\n  },\n\n  /* 提交后跟踪：优先用 /api/job 任务行的 output_name，其次对比提交前的最新作品名，避免时钟偏差卡死 */\n  resolveTrack() {\n    MODES.forEach(mode => {\n      const tr = this.tracks[mode];\n      if (!tr) return;\n      const lane = Tasks.laneTasks(mode);\n      if (tr.random) {\n        const llmBusy = S.tasks.some(t => t.module === \'llm\' && /^随机/.test(stripEmoji(t.label || \'\')) && (t.state === \'running\' || t.state === \'queued\'));\n        const st = S.job || {};\n        const hiddenBusy = st.running && !(st.active_count || 0) && !(st.queued_count || 0);\n        if (lane.length || llmBusy || hiddenBusy || Date.now() - tr.t < 15000) return;\n        delete this.tracks[mode];\n        return;\n      }\n      if (!tr.jid) {\n        if (lane.length || Date.now() - tr.t < 4000) return;\n        delete this.tracks[mode];\n        return;\n      }\n      const t = S.tasks.find(x => x.jid === tr.jid);\n      if (!t) { if (Date.now() - tr.t > 60000) delete this.tracks[mode]; return; }\n      if (t.state === \'running\' || t.state === \'queued\') return;\n      if (t.state === \'error\') {\n        this.fail[mode] = { msg: stripEmoji(t.badge || \'\') || \'任务异常结束\', jid: t.jid };\n        delete this.tracks[mode];\n        return;\n      }\n      if (t.state === \'done\') {\n        const list = this.list(mode);\n        let hit = t.output_name ? list.find(x => x.name === t.output_name) : null;\n        if (!hit && list[0] && list[0].name !== tr.beforeTop) hit = list[0];\n        if (!hit) {\n          if (!tr.doneAt) { tr.doneAt = Date.now(); Works.load(); return; }\n          if (Date.now() - tr.doneAt < 5000) return;\n          delete this.tracks[mode];\n          return;\n        }\n        this.sel[mode] = hit.name;\n        this.manual[mode] = false;\n        delete this.tracks[mode];\n        return;\n      }\n      delete this.tracks[mode];\n    });\n  },\n\n  render(force = false) {\n    this.resolveTrack();\n    const mode = S.mode;\n    const tr = this.tracks[mode];\n    const lane = Tasks.laneTasks(mode);\n    const runT = lane.find(t => t.state === \'running\') || null;\n    const queuedT = lane.find(t => t.state === \'queued\') || null;\n    const fail = this.fail[mode];\n    const showRun = !this.manual[mode] && (!!tr || !!runT || !!queuedT) && !fail;\n    const item = this.current(mode);\n    let view = \'empty\';\n    if (showRun) view = \'run\';\n    else if (fail && !this.manual[mode]) view = \'fail\';\n    else if (item) view = \'item\';\n\n    const page = $(\'pageCreate\');\n    page.classList.toggle(\'is-blank\', view === \'empty\');\n    $(\'stageView\').classList.toggle(\'is-fit\', view !== \'empty\');\n    $(\'stageRun\').hidden = view !== \'run\';\n    $(\'stageFail\').hidden = view !== \'fail\';\n    $(\'stageEmpty\').hidden = view !== \'empty\';\n    $(\'stageMedia\').hidden = view !== \'item\';\n    $(\'stageActions\').hidden = view !== \'item\';\n    const list = this.list(mode);\n    const showNav = view === \'item\' && list.length > 1;\n    const prevBtn = $(\'stagePrev\'), nextBtn = $(\'stageNext\');\n    prevBtn.hidden = !showNav;\n    nextBtn.hidden = !showNav;\n    if (showNav && item) {\n      const idx = list.findIndex(x => x.name === item.name);\n      prevBtn.disabled = idx <= 0;\n      nextBtn.disabled = idx < 0 || idx >= list.length - 1;\n    }\n\n    if (view === \'run\') this.renderRun(runT || queuedT, mode);\n    else $(\'stageDots\').hidden = true;\n    if (view === \'fail\') {\n      const [w, h] = mode === \'video\' ? Create.videoDims() : Create.imageDims();\n      this.sizeBox($(\'stageFail\'), w, h);\n      setText($(\'stageFailMsg\'), fail.msg);\n    }\n    if (view === \'item\') this.renderItem(item, mode, force);\n  },\n\n  sizeBox(box, w, h) {\n    const view = $(\'stageView\');\n    let vw = view.clientWidth, vh = view.clientHeight;\n    if (mqSingle.matches) {\n      vh = window.innerHeight * 0.46;\n    }\n    if (vw <= 0 || vh <= 0) return;\n    const s = Math.min(vw / w, vh / h);\n    box.style.width = Math.floor(w * s) + \'px\';\n    box.style.height = Math.floor(h * s) + \'px\';\n  },\n\n  renderRun(t, mode) {\n    const [w, h] = mode === \'video\' ? Create.videoDims() : Create.imageDims();\n    const box = $(\'stageRun\');\n    this.sizeBox(box, w, h);\n    const pct = t && t.state === \'running\' ? Tasks.pct() : null;\n    setText($(\'stageRunPct\'), pct !== null ? `${pct}%` : \'\');\n    setText($(\'stageRunPhase\'), t ? this.phaseText(t) : \'准备中\');\n    setText($(\'stageRunTime\'), t && t.state === \'running\' ? fmtClock(t.elapsed_sec) : \'0:00\');\n    const fill = $(\'stageRunFill\');\n    fill.parentElement.classList.toggle(\'is-indeterminate\', pct === null);\n    fill.style.transform = pct === null ? \'\' : `scaleX(${pct / 100})`;\n    box.querySelector(\'.stage-abort\').hidden = !t;\n    const pos = t ? batchPos(t) : null;\n    const dots = $(\'stageDots\');\n    if (pos && pos.total > 1) {\n      dots.hidden = false;\n      const unit = mode === \'video\' ? \'条\' : \'张\';\n      const track = el(\'span\', \'stage-batch-track\');\n      const bf = el(\'span\', \'stage-batch-fill\');\n      bf.style.transform = `scaleX(${clamp(pos.cur / pos.total, 0, 1)})`;\n      track.append(bf);\n      dots.replaceChildren(el(\'span\', \'\', `批量进度 · 第 ${pos.cur} / ${pos.total} ${unit}`), track);\n    } else dots.hidden = true;\n  },\n\n  renderItem(item, mode, force) {\n    const media = $(\'stageMedia\');\n    const sig = mode + \'|\' + item.name + \'|\' + item.mtime;\n    if (force || media.dataset.sig !== sig) {\n      media.dataset.sig = sig;\n      if (isVideoItem(item, mode)) {\n        const v = el(\'video\');\n        v.src = item.url;\n        v.controls = true;\n        v.playsInline = true;\n        v.preload = \'metadata\';\n        v.muted = true;\n        if (this._autoplay === item.name) { v.autoplay = true; this._autoplay = null; }\n        media.replaceChildren(v);\n      } else {\n        const img = el(\'img\');\n        img.alt = item.name || \'\';\n        img.decoding = \'async\';\n        img.dataset.action = \'stage-view\';\n        progressiveImg(img, item.thumb_url, item.url, () => media.dataset.sig === sig);\n        media.replaceChildren(img);\n      }\n    }\n    const meta = [];\n    if (item.res) meta.push(fmtRes(item.res));\n    if (item.steps) meta.push(`${item.steps} 步`);\n    if (item.time_str) meta.push(item.time_str);\n    setText($(\'stageMeta\'), meta.join(\' · \'));\n    const isImg = mode === \'image\';\n    $(\'stageActions\').querySelectorAll(\'[data-img-only]\').forEach(b => { b.hidden = !isImg; });\n    $(\'stageReuseBtn\').hidden = !item.prompt;\n    const dl = $(\'stageDownload\');\n    dl.href = item.url;\n    dl.setAttribute(\'download\', item.name || \'\');\n  },\n\n  renderRecent(force = false) {\n    const mode = S.mode;\n    const list = this.list(mode).slice(0, 20);\n    const cur = this.current(mode);\n    const sig = mode + \'|\' + list.map(x => x.name).join(\',\') + \'|\' + (cur ? cur.name : \'\');\n    if (!force && sig === this.recentSig) return;\n    this.recentSig = sig;\n    const box = $(\'recentList\');\n    if (!list.length) {\n      box.replaceChildren(el(\'span\', \'recent-empty\', mode === \'video\' ? \'还没有视频作品\' : \'还没有图片作品\'));\n      return;\n    }\n    box.replaceChildren(...list.map((it, idx) => {\n      const b = el(\'button\', \'recent-item\' + (cur && cur.name === it.name ? \' is-current\' : \'\'));\n      b.type = \'button\';\n      b.dataset.action = \'recent-pick\';\n      b.dataset.name = it.name;\n      b.setAttribute(\'role\', \'option\');\n      b.setAttribute(\'aria-selected\', cur && cur.name === it.name ? \'true\' : \'false\');\n      setTip(b, it.time_str || it.name);\n      if (mode === \'video\') {\n        const v = el(\'video\');\n        v.src = it.url + \'#t=0.1\';\n        v.preload = \'metadata\';\n        v.muted = true;\n        v.playsInline = true;\n        b.append(v);\n      } else {\n        const img = el(\'img\');\n        img.src = it.thumb_url || it.url;\n        img.alt = \'\';\n        img.loading = idx < 8 ? \'eager\' : \'lazy\';\n        img.decoding = \'async\';\n        b.append(img);\n      }\n      return b;\n    }));\n    requestAnimationFrame(() => updateScrollFade(box, \'x\'));\n  },\n\n  pick(name) {\n    this.sel[S.mode] = name;\n    this.manual[S.mode] = true;\n    this.fail[S.mode] = null;\n    this.render(true);\n    this.renderRecent(true);\n    const c = $(\'recentList\').querySelector(\'.is-current\');\n    if (c) c.scrollIntoView({ block: \'nearest\', inline: \'nearest\' });\n  },\n\n  step(dir) {\n    const list = this.list(S.mode);\n    if (!list.length) return;\n    const cur = this.current(S.mode);\n    const i = Math.max(0, list.findIndex(x => cur && x.name === cur.name));\n    const n = clamp(i + dir, 0, list.length - 1);\n    if (n !== i || this.manual[S.mode] === false) this.pick(list[n].name);\n  },\n\n  more(anchor) {\n    const item = this.current(S.mode);\n    if (!item) return;\n    const tab = S.mode;\n    Menu.show(anchor, [\n      mqSingle.matches && tab === \'image\' && { label: \'作为参考\', icon: \'i-image\', run: () => { Create.setRef(\'image\', item.path); toast(\'已挂载为人脸参考\'); } },\n      item.prompt && { label: \'复制提示词\', icon: \'i-list\', run: () => copyWithToast(item.prompt, \'已复制提示词\') },\n      { label: \'下载\', icon: \'i-download\', run: () => downloadUrl(item.url, item.name) },\n      { label: \'删除\', icon: \'i-trash\', danger: true, run: () => Works.askDelete(tab, item) }\n    ]);\n  }\n};\nStage.track = function (mode, info) {\n  const beforeTop = (this.list(mode)[0] || {}).name || null;\n  this.tracks[mode] = { ...info, beforeTop, t: Date.now() };\n  this.manual[mode] = false;\n  this.fail[mode] = null;\n  this.render(true);\n};\n\nfunction downloadUrl(url, name) {\n  const a = el(\'a\');\n  a.href = url;\n  a.download = name || \'\';\n  document.body.append(a);\n  a.click();\n  a.remove();\n}\n/* ==================== AI 帮写（公开版统一提示词对话） ==================== */\nconst Chat = {\n  history: [],\n  sending: false,\n  optimistic: null,\n  sig: \'\',\n\n  init() {\n    const inp = $(\'chatInput\');\n    inp.addEventListener(\'input\', () => { this.autoGrow(); scheduleSave(); });\n    inp.addEventListener(\'keydown\', (e) => {\n      if ((e.ctrlKey || e.metaKey) && e.key === \'Enter\') { e.preventDefault(); this.send(); }\n    });\n  },\n\n  autoGrow() {\n    const t = $(\'chatInput\');\n    if (t.offsetParent === null) return;\n    t.style.height = \'auto\';\n    t.style.height = Math.min(t.scrollHeight, 140) + \'px\';\n  },\n\n  async load() {\n    try {\n      const data = await api.get(\'/api/chat\');\n      if (Array.isArray(data)) {\n        this.history = data;\n        this.render();\n      }\n    } catch (_) {}\n  },\n\n  applyJob() {},\n\n  pending() {\n    const out = S.tasks.filter(t => t && t.module === \'llm\' && (t.state === \'running\' || t.state === \'queued\') && t.user_msg)\n      .sort((a, b) => (a.jid || 0) - (b.jid || 0));\n    const opt = this.optimistic;\n    if (opt) {\n      const inHistory = this.history.length && this.history.slice(-2).some(m => m.role === \'user\' && m.content === opt.text);\n      if (out.some(t => t.user_msg === opt.text) || inHistory) this.optimistic = null;\n      else out.push({ jid: -1, state: \'running\', user_msg: opt.text, elapsed_sec: 0, optimistic: true });\n    }\n    return out;\n  },\n\n  render(force = false) {\n    const pend = this.pending();\n    const rounds = Math.floor(this.history.filter(m => m.role === \'assistant\').length);\n    setText($(\'chatInfoText\'), `Qwen 27B · ${rounds} 轮${pend.length ? \' · 正在构思\' : \'\'}`);\n    const sig = JSON.stringify(this.history) + JSON.stringify(pend.map(p => [p.jid, p.state, p.user_msg, p.stage || \'\']));\n    const box = $(\'chatList\');\n    if (!force && sig === this.sig) {\n      box.querySelectorAll(\'.typing-text\').forEach((n, i) => { const p = pend[i]; if (p) setText(n, this.typingText(p)); });\n      return;\n    }\n    this.sig = sig;\n    const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 48;\n    const frag = document.createDocumentFragment();\n    if (!this.history.length && !pend.length) {\n      frag.append(el(\'div\', \'chat-empty\', \'告诉提示词大师你想要的场景、风格或修改需求\'));\n      box.replaceChildren(frag);\n      return;\n    }\n    this.history.forEach((m, idx) => {\n      if (m.role === \'user\') {\n        frag.append(el(\'div\', \'msg msg-user\', m.content || \'\'));\n        return;\n      }\n      const wrap = el(\'div\', \'msg msg-ai\');\n      wrap.append(el(\'div\', \'msg-ai-text\', m.content || \'\'));\n      const acts = el(\'div\', \'msg-actions\');\n      acts.append(\n        btn(\'btn btn-secondary btn-sm\', \'填入\', \'chat-to-mod\', { idx }),\n        btn(\'btn btn-secondary btn-sm\', \'复制\', \'copy-chat\', { idx })\n      );\n      wrap.append(acts);\n      frag.append(wrap);\n    });\n    pend.forEach(p => {\n      frag.append(el(\'div\', \'msg msg-user\', p.user_msg || \'\'));\n      const wrap = el(\'div\', \'msg msg-ai\');\n      const ty = el(\'div\', \'typing\');\n      const dots = el(\'span\', \'typing-dots\');\n      dots.append(el(\'i\'), el(\'i\'), el(\'i\'));\n      ty.append(dots, el(\'span\', \'typing-text num\', this.typingText(p)));\n      wrap.append(ty);\n      frag.append(wrap);\n    });\n    box.replaceChildren(frag);\n    if (atBottom || force || pend.length) box.scrollTop = box.scrollHeight;\n    requestAnimationFrame(() => updateScrollFade(box, \'y\'));\n  },\n\n  typingText(p) {\n    if (p.state === \'queued\') return \'排队中\';\n    const stage = cleanPhase(p.stage || \'\') || \'正在构思\';\n    const sec = typeof p.elapsed_sec === \'number\' && p.elapsed_sec > 0 ? ` · ${fmtClock(p.elapsed_sec)}` : \'\';\n    return (stage.length > 28 ? stage.slice(0, 28) + \'…\' : stage) + sec;\n  },\n\n  scrollBottom() { const b = $(\'chatList\'); b.scrollTop = b.scrollHeight; },\n\n  async send() {\n    if (this.sending) return;\n    const inp = $(\'chatInput\');\n    const text = inp.value.trim();\n    if (!text) { toast(\'请先输入内容\'); inp.focus(); return; }\n    this.sending = true;\n    $(\'chatSendBtn\').disabled = true;\n    const now = new Date();\n    this.optimistic = { text, time: `${String(now.getHours()).padStart(2, \'0\')}:${String(now.getMinutes()).padStart(2, \'0\')}` };\n    inp.value = \'\';\n    this.autoGrow();\n    this.render(true);\n    try {\n      await api.post(\'/api/run\', { module: \'llm\', text });\n      await Jobs.poll(true);\n      await this.load();\n      this.render(true);\n    } catch (e) {\n      this.optimistic = null;\n      if (!inp.value.trim()) { inp.value = text; this.autoGrow(); }\n      this.render(true);\n      toast((e.message || \'发送失败\') + \'；未自动重试\');\n    } finally {\n      this.sending = false;\n      $(\'chatSendBtn\').disabled = false;\n      scheduleSave();\n    }\n  },\n\n  askClear() {\n    if (!this.history.length) { toast(\'当前对话已为空\'); return; }\n    Confirm.ask({\n      title: \'开始新对话？\',\n      text: \'只清空当前对话记录；作品里已保存的提示词历史不受影响。\',\n      accept: \'新对话\',\n      run: async () => {\n        try {\n          await api.post(\'/api/chat/clear\', {});\n          await this.load();\n          toast(\'已开始新对话\');\n        } catch (e) { toast(e.message || \'清空失败\'); }\n      }\n    });\n  },\n\n  fill(idx) {\n    const m = this.history[idx];\n    if (!m || !m.content) return;\n    Create.fillPrompt(S.mode, extractCleanPrompt(m.content), \'已填入\');\n    setTimeout(() => curInput().focus({ preventScroll: true }), 160);\n  },\n\n  copy(idx, triggerEl) {\n    const m = this.history[idx];\n    if (m && m.content) copyWithToast(m.content, \'已复制\', triggerEl);\n  }\n};\n\n/* ==================== 作品页：分类 / 搜索 / 网格 / 分页 / 批量 / 同步 ==================== */\nconst Works = {\n  tab: \'image\',\n  page: 1,\n  query: \'\',\n  filtered: [],\n  filterCache: null,\n  pageSize: 60,\n  batch: false,\n  sel: new Set(),\n  lastIdx: -1,\n  sig: \'\',\n\n  async load() {\n    try {\n      const data = await api.get(\'/api/outputs\');\n      if (data && typeof data === \'object\') {\n        WORK_TABS.forEach(k => { if (!Array.isArray(data[k])) data[k] = []; });\n        S.outputs = data;\n        S.outputsLoaded = true;\n        this.renderCounts();\n        Stage.onOutputs();\n        Create.renderRef();\n        if (S.page === \'works\') this.render(true);\n        if ($(\'refPicker\').open) RefPicker.build();\n        if (Viewer.open) Viewer.refreshList();\n        Sync.load();\n      }\n    } catch (_) {}\n  },\n\n  renderCounts() {\n    const c = S.outputs.counts || {};\n    const n = (k) => typeof c[k] === \'number\' ? c[k] : (S.outputs[k] || []).length;\n    WORK_TABS.forEach(k => setText($(\'cnt_\' + k), String(n(k))));\n    document.querySelectorAll(\'#worksSeg .seg-item\').forEach(b => {\n      const on = b.dataset.tab === this.tab;\n      b.classList.toggle(\'is-active\', on);\n      b.setAttribute(\'aria-pressed\', on ? \'true\' : \'false\');\n    });\n  },\n\n  scrollTop(smooth = false) {\n    if (isMobile()) {\n      window.scrollTo({ top: 0, behavior: smooth ? \'smooth\' : \'auto\' });\n      return;\n    }\n    const sc = $(\'worksScroll\');\n    if (smooth) sc.scrollTo({ top: 0, behavior: \'smooth\' });\n    else sc.scrollTop = 0;\n  },\n\n  switchTab(tab) {\n    if (!WORK_TABS.includes(tab) || tab === this.tab) return;\n    this.tab = tab;\n    this.page = 1;\n    this.sel.clear();\n    this.lastIdx = -1;\n    this.renderCounts();\n    this.render(true);\n    this.scrollTop(false);\n    scheduleSave();\n  },\n\n  setQuery(q) {\n    this.query = q || \'\';\n    this.page = 1;\n    this.render(true);\n    scheduleSave();\n  },\n\n  computeFiltered() {\n    const raw = S.outputs[this.tab] || [];\n    const q = this.query.trim().toLowerCase();\n    const c = this.filterCache;\n    if (c && c.raw === raw && c.q === q) return c.items;\n    const items = q ? raw.filter(it => ((it.name || \'\') + \'\\n\' + (it.prompt || \'\') + \'\\n\' + (it.preview_text || \'\') + \'\\n\' + (it.model_label || \'\')).toLowerCase().includes(q)) : raw;\n    this.filterCache = { raw, q, items };\n    return items;\n  },\n\n  /* 每页 = 列数 × 行数，取最接近 60 的整数倍，保证非末页每行都满 */\n  computePageSize() {\n    if (this.tab === \'llm\') return 60;\n    let cols;\n    if (isMobile()) {\n      cols = 2;\n    } else {\n      const grid = $(\'galleryGrid\');\n      const tpl = grid.clientWidth ? getComputedStyle(grid).gridTemplateColumns : \'\';\n      const tracks = tpl && tpl !== \'none\' ? tpl.trim().split(/\\s+/).length : 0;\n      if (tracks > 0) {\n        cols = tracks;\n      } else {\n        const minW = mqSingle.matches ? 168 : 176;\n        const w = grid.clientWidth || Math.max(320, $(\'worksScroll\').clientWidth - 36);\n        cols = Math.max(1, Math.floor((w + 12) / (minW + 12)));\n      }\n    }\n    const rows = Math.max(1, Math.round(60 / cols));\n    return cols * rows;\n  },\n\n  onResize() {\n    if (S.page !== \'works\') return;\n    const next = this.computePageSize();\n    if (next !== this.pageSize) {\n      const first = (this.page - 1) * this.pageSize;\n      this.pageSize = next;\n      this.page = Math.floor(first / next) + 1;\n      this.render(true);\n    }\n  },\n\n  totalPages() { return Math.max(1, Math.ceil(this.filtered.length / this.pageSize)); },\n\n  gotoPage(p, scroll = true) {\n    const total = this.totalPages();\n    const n = clamp(Number(p) || 1, 1, total);\n    if (n === this.page) return;\n    this.page = n;\n    this.render(true);\n    if (scroll) this.scrollTop(false);\n    scheduleSave();\n  },\n\n  render(force = false) {\n    if (S.page !== \'works\' && !force) return;\n    this.pageSize = this.computePageSize();\n    this.filtered = this.computeFiltered();\n    const total = this.filtered.length;\n    const pages = this.totalPages();\n    if (S.outputsLoaded && this.page > pages) this.page = pages;\n    if (this.page < 1) this.page = 1;\n    const start = (this.page - 1) * this.pageSize;\n    const items = this.filtered.slice(start, start + this.pageSize);\n    this.renderBatchBar();\n    this.renderPager(total, pages, start, items.length);\n    const sig = [this.tab, this.page, this.pageSize, this.query, this.batch ? [...this.sel].join(\',\') : \'-\', total, items.map(x => x.name + \'_\' + x.mtime).join(\'|\')].join(\'::\');\n    if (!force && sig === this.sig) return;\n    this.sig = sig;\n    const grid = $(\'galleryGrid\');\n    grid.className = \'grid\' + (this.tab === \'llm\' ? \' is-list\' : \'\');\n    if (!items.length) {\n      const e = el(\'div\', \'empty grid-empty\');\n      e.append(icon(this.tab === \'llm\' ? \'i-list\' : (this.tab === \'video\' ? \'i-video\' : \'i-image\'), \'ico ico-32\'));\n      e.append(el(\'span\', \'\', !S.outputsLoaded ? \'正在读取作品…\' : (this.query.trim() ? \'没有匹配的作品\' : \'这个分类还没有作品\')));\n      grid.replaceChildren(e);\n      return;\n    }\n    const frag = document.createDocumentFragment();\n    items.forEach((it, i) => frag.append(this.tab === \'llm\' ? this.promptRow(it, start + i) : this.card(it, start + i, i)));\n    grid.replaceChildren(frag);\n  },\n\n  card(it, idx, local) {\n    const card = el(\'div\', \'card\' + (this.batch && this.sel.has(it.name) ? \' is-selected\' : \'\'));\n    card.dataset.action = this.batch ? \'batch-toggle-item\' : \'open-lightbox\';\n    card.dataset.idx = String(idx);\n    card.setAttribute(\'role\', \'button\');\n    card.tabIndex = 0;\n    card.setAttribute(\'aria-label\', (this.batch ? \'选择 \' : \'查看 \') + (it.name || \'\'));\n    const isVid = isVideoItem(it, this.tab);\n    if (isVid) {\n      const v = el(\'video\');\n      v.src = it.url + \'#t=0.1\';\n      v.preload = \'metadata\';\n      v.muted = true;\n      v.playsInline = true;\n      const badge = el(\'span\', \'card-duration num\');\n      badge.hidden = true;\n      v.addEventListener(\'loadedmetadata\', () => {\n        if (Number.isFinite(v.duration) && v.duration > 0) { badge.textContent = fmtClock(v.duration); badge.hidden = false; }\n      }, { once: true });\n      card.append(v, badge);\n    } else {\n      const img = el(\'img\');\n      img.src = it.thumb_url || it.url;\n      img.alt = \'\';\n      img.loading = local < 12 ? \'eager\' : \'lazy\';\n      img.decoding = \'async\';\n      img.addEventListener(\'error\', () => { if (!img.dataset.fb && it.url) { img.dataset.fb = \'1\'; img.src = it.url; } }, { once: true });\n      card.append(img);\n    }\n    const chk = el(\'span\', \'card-check\');\n    chk.append(icon(\'i-check\'));\n    card.append(chk);\n    const ov = el(\'div\', \'card-overlay\');\n    ov.append(el(\'span\', \'card-date num\', it.time_str || \'\'));\n    const bs = el(\'div\', \'card-btns\');\n    if (it.prompt) bs.append(iconBtn(\'icon-btn-sm\', \'i-refresh\', \'复用提示词\', \'reuse-img-spec\', { idx }));\n    const dl = el(\'a\', \'icon-btn icon-btn-sm\');\n    dl.href = it.url;\n    dl.setAttribute(\'download\', it.name || \'\');\n    dl.setAttribute(\'aria-label\', \'下载\');\n    setTip(dl, \'下载\');\n    dl.append(icon(\'i-download\'));\n    bs.append(dl, iconBtn(\'icon-btn-sm\', \'i-more\', \'更多\', \'card-more\', { idx }));\n    ov.append(bs);\n    card.append(ov);\n    return card;\n  },\n\n  promptRow(it, idx) {\n    const row = el(\'div\', \'prow\' + (this.batch && this.sel.has(it.name) ? \' is-selected\' : \'\'));\n    row.dataset.action = this.batch ? \'batch-toggle-item\' : \'open-prompt-dialog\';\n    row.dataset.idx = String(idx);\n    row.setAttribute(\'role\', \'button\');\n    row.tabIndex = 0;\n    const chk = el(\'span\', \'prow-check\');\n    chk.append(icon(\'i-check\'));\n    const main = el(\'div\', \'prow-main\');\n    main.append(el(\'div\', \'prow-text\', (it.preview_text || \'\').trim() || it.name));\n    main.append(el(\'span\', \'hint-3 num\', [it.time_str, it.size_kb ? `${it.size_kb} KB` : \'\'].filter(Boolean).join(\' · \')));\n    const acts = el(\'div\', \'prow-acts\');\n    acts.append(\n      btn(\'btn surface btn-sm\', \'复用\', \'reuse-llm-file\', { idx }),\n      btn(\'btn surface btn-sm\', \'复制\', \'copy-llm-file\', { idx }),\n      iconBtn(\'icon-btn-sm\', \'i-more\', \'更多\', \'prow-more\', { idx })\n    );\n    row.append(chk, main, acts);\n    return row;\n  },\n\n  renderPager(total, pages, start, count) {\n    const multi = pages > 1;\n    setText($(\'pagerTopInfo\'), !S.outputsLoaded ? \'\' : (multi ? `第 ${start + 1}–${start + count} 项 · 共 ${total}` : `共 ${total} 项`));\n    $(\'pagerTopCtl\').hidden = !multi;\n    $(\'pagerBottom\').hidden = !multi;\n    if (!multi) return;\n    setText($(\'pagerTopLabel\'), `${this.page} / ${pages}`);\n    $(\'pagerTopPrev\').disabled = this.page <= 1;\n    $(\'pagerTopNext\').disabled = this.page >= pages;\n    const box = $(\'pagerPages\');\n    const frag = document.createDocumentFragment();\n    const prev = iconBtn(\'icon-btn-sm\', \'i-chevron-left\', \'上一页\', \'goto-page\', { rel: -1 });\n    prev.disabled = this.page <= 1;\n    frag.append(prev);\n    let nums;\n    if (pages <= 7) nums = Array.from({ length: pages }, (_, i) => i + 1);\n    else {\n      const set = new Set([1, pages, this.page, this.page - 1, this.page + 1]);\n      if (this.page <= 3) [2, 3, 4].forEach(n => set.add(n));\n      if (this.page >= pages - 2) [pages - 1, pages - 2, pages - 3].forEach(n => set.add(n));\n      nums = [...set].filter(n => n >= 1 && n <= pages).sort((a, b) => a - b);\n    }\n    let last = 0;\n    nums.forEach(n => {\n      if (last && n - last > 1) frag.append(el(\'span\', \'page-ellipsis\', \'…\'));\n      const b = el(\'button\', \'page-btn\' + (n === this.page ? \' is-current\' : \'\'), String(n));\n      b.type = \'button\';\n      b.dataset.action = \'goto-page\';\n      b.dataset.page = String(n);\n      if (n === this.page) b.setAttribute(\'aria-current\', \'page\');\n      frag.append(b);\n      last = n;\n    });\n    const next = iconBtn(\'icon-btn-sm\', \'i-chevron-right\', \'下一页\', \'goto-page\', { rel: 1 });\n    next.disabled = this.page >= pages;\n    frag.append(next);\n    box.replaceChildren(frag);\n    const sel = $(\'paginationJump\');\n    if (Number(sel.dataset.total) !== pages) {\n      sel.replaceChildren(...Array.from({ length: pages }, (_, i) => { const o = el(\'option\', \'\', `第 ${i + 1} 页`); o.value = String(i + 1); return o; }));\n      sel.dataset.total = String(pages);\n    }\n    sel.value = String(this.page);\n    ZSelect.sync(sel);\n  },\n\n  /* ---------- 批量 ---------- */\n  setBatch(on) {\n    this.batch = typeof on === \'boolean\' ? on : !this.batch;\n    if (!this.batch) { this.sel.clear(); this.lastIdx = -1; }\n    this.render(true);\n  },\n\n  renderBatchBar() {\n    $(\'pageWorks\').classList.toggle(\'is-batch\', this.batch);\n    $(\'batchBar\').hidden = !this.batch;\n    const t = $(\'batchToggleBtn\');\n    t.setAttribute(\'aria-pressed\', this.batch ? \'true\' : \'false\');\n    t.classList.toggle(\'is-on\', this.batch);\n    setText($(\'batchToggleText\'), this.batch ? \'完成\' : \'选择\');\n    setTip(t, this.batch ? \'完成选择\' : \'批量选择\');\n    if (!this.batch) return;\n    const valid = new Set((S.outputs[this.tab] || []).map(x => x.name));\n    [...this.sel].forEach(n => { if (!valid.has(n)) this.sel.delete(n); });\n    const n = this.sel.size;\n    setText($(\'batchCount\'), `已选 ${n}`);\n    [\'batchClearBtn\', \'batchDownloadBtn\', \'batchDeleteBtn\'].forEach(id => { $(id).disabled = n === 0; });\n  },\n\n  toggleItem(idx, shift) {\n    const it = this.filtered[idx];\n    if (!it || !it.name) return;\n    const select = !this.sel.has(it.name);\n    if (shift && this.lastIdx >= 0 && this.lastIdx < this.filtered.length) {\n      const lo = Math.min(this.lastIdx, idx), hi = Math.max(this.lastIdx, idx);\n      for (let i = lo; i <= hi; i++) { const nm = this.filtered[i]?.name; if (nm) select ? this.sel.add(nm) : this.sel.delete(nm); }\n    } else select ? this.sel.add(it.name) : this.sel.delete(it.name);\n    this.lastIdx = idx;\n    this.render(true);\n  },\n\n  selectPage() {\n    const start = (this.page - 1) * this.pageSize;\n    const items = this.filtered.slice(start, start + this.pageSize);\n    const all = items.length && items.every(x => this.sel.has(x.name));\n    items.forEach(x => all ? this.sel.delete(x.name) : this.sel.add(x.name));\n    this.render(true);\n  },\n\n  selectAll() {\n    const all = this.filtered.length && this.filtered.every(x => this.sel.has(x.name));\n    this.filtered.forEach(x => all ? this.sel.delete(x.name) : this.sel.add(x.name));\n    this.render(true);\n  },\n\n  clearSel() { this.sel.clear(); this.lastIdx = -1; this.render(true); },\n\n  batchDownload() {\n    const names = [...this.sel];\n    if (!names.length) { toast(\'请先选择要下载的作品\'); return; }\n    $(\'batchDownloadPayload\').value = JSON.stringify({ module: this.tab, names });\n    $(\'batchDownloadForm\').submit();\n    toast(`正在打包下载 ${names.length} 个文件…`);\n  },\n\n  batchDelete() {\n    const names = [...this.sel];\n    if (!names.length) { toast(\'请先选择要删除的作品\'); return; }\n    const tab = this.tab;\n    Confirm.ask({\n      title: `删除已选的 ${names.length} 项？`,\n      text: \'删除后无法恢复（图片会同步移入 Google Drive 回收站）。\',\n      accept: \'删除\', danger: true,\n      run: async () => {\n        const set = new Set(names);\n        const paths = new Set((S.outputs[tab] || []).filter(x => set.has(x.name)).map(x => x.path));\n        if (await this.doDelete({ module: tab, names }, paths)) {\n          this.sel.clear(); this.lastIdx = -1;\n          toast(`已删除 ${names.length} 项`);\n        }\n      }\n    });\n  },\n\n  async doDelete(payload, paths) {\n    try {\n      await api.post(\'/api/delete\', payload);\n      if (paths) MODES.forEach(m => { if (paths.has(S.ref[m])) Create.setRef(m, \'\'); });\n      await this.load();\n      return true;\n    } catch (e) {\n      toast(e.message || \'删除失败\');\n      return false;\n    }\n  },\n\n  askDelete(tab, it, after) {\n    if (!it) return;\n    const noun = tab === \'video\' ? \'这个视频\' : (tab === \'llm\' ? \'这条提示词\' : \'这张图片\');\n    Confirm.ask({\n      title: `删除${noun}？`,\n      text: `${it.name || \'\'}\\n删除后无法恢复（图片会同步移入云盘回收站）。`,\n      accept: \'删除\', danger: true,\n      run: async () => {\n        if (await this.doDelete({ module: tab, name: it.name }, new Set([it.path]))) {\n          toast(\'已删除\');\n          if (after) after();\n        }\n      }\n    });\n  },\n\n  askDeleteAll() {\n    const items = S.outputs[this.tab] || [];\n    if (!items.length) { toast(\'当前分类为空\'); return; }\n    const tab = this.tab;\n    Confirm.ask({\n      title: `清空「${WORK_TAB_NAMES[tab]}」？`,\n      text: `将删除「${WORK_TAB_NAMES[tab]}」分类下全部 ${items.length} 个文件，删除后无法恢复。`,\n      accept: \'清空\', danger: true,\n      run: async () => {\n        if (await this.doDelete({ module: tab, all: true }, new Set(items.map(x => x.path)))) {\n          this.page = 1;\n          toast(\'当前分类已清空\');\n        }\n      }\n    });\n  },\n\n  more(anchor) {\n    Menu.show(anchor, [\n      { label: \'刷新\', icon: \'i-refresh\', run: () => this.load().then(() => toast(\'作品已刷新\')) },\n      { label: \'清空此分类\', icon: \'i-trash\', danger: true, disabled: !(S.outputs[this.tab] || []).length, run: () => this.askDeleteAll() }\n    ]);\n  },\n\n  cardMore(anchor, idx) {\n    const it = this.filtered[idx];\n    if (!it) return;\n    const tab = this.tab;\n    const isImg = tab === \'image\' || tab === \'upload_image\';\n    Menu.show(anchor, [\n      it.prompt && { label: \'复制提示词\', icon: \'i-list\', run: () => copyWithToast(it.prompt, \'已复制提示词\') },\n      isImg && { label: \'作为参考\', icon: \'i-image\', run: () => useAsRef(it) },\n      isImg && { label: \'做成视频\', icon: \'i-video\', run: () => makeVideo(it) },\n      { label: \'删除\', icon: \'i-trash\', danger: true, run: () => this.askDelete(tab, it) }\n    ]);\n  },\n\n  rowMore(anchor, idx) {\n    const it = this.filtered[idx];\n    if (!it) return;\n    Menu.show(anchor, [{ label: \'删除\', icon: \'i-trash\', danger: true, run: () => this.askDelete(\'llm\', it) }]);\n  }\n};\n\nfunction useAsRef(it) {\n  Create.setRef(\'image\', it.path);\n  navigate(\'create\');\n  Create.setMode(\'image\');\n  Create.setEditorTab(\'write\');\n  toast(\'已挂载为人脸参考\');\n}\n\nfunction makeVideo(it) {\n  Create.setRef(\'video\', it.path);\n  navigate(\'create\');\n  Create.setMode(\'video\');\n  Create.setEditorTab(\'write\');\n  toast(\'已设为视频首帧\');\n}\n\n/* ==================== 同步（谷歌加密云 / TG 频道） ==================== */\nconst Sync = {\n  st: null,\n  timer: null,\n  submitting: false,\n\n  async load(silent = true) {\n    try {\n      const data = await api.get(\'/api/drive_sync\');\n      if (data && data.ok) {\n        const was = !!(this.st && this.st.task && this.st.task.running);\n        this.st = data;\n        this.render();\n        const now = !!(data.task && data.task.running);\n        if (now) this.schedule();\n        else { this.stop(); if (was && data.task && data.task.last_message) toast(data.task.last_message); }\n      }\n    } catch (_) { if (!silent) toast(\'读取同步状态失败\'); }\n  },\n  schedule() { if (!this.timer) this.timer = setInterval(() => this.load(true), 1500); },\n  stop() { if (this.timer) { clearInterval(this.timer); this.timer = null; } },\n\n  applyVisibility() {\n    const st = this.st;\n    const dsOn = st ? (st.enabled !== false) : !!S.uiMeta?.drive_sync_enabled;\n    const chOn = st ? !!st.channel_enabled : !!S.uiMeta?.channel_sync_enabled;\n    $(\'syncBtn\').hidden = !dsOn;\n    $(\'syncChannelBtn\').hidden = !chOn;\n  },\n\n  render() {\n    const st = this.st;\n    this.applyVisibility();\n    const task = (st && st.task) || {};\n    const running = !!task.running || this.submitting;\n    const mode = task.mode || \'\';\n    const pendUp = Number(st?.pending_upload || 0), pendDel = Number(st?.pending_delete || 0), errs = Number(st?.errors || 0);\n    const pend = pendUp + pendDel;\n    setText($(\'syncBtnText\'), running ? (mode === \'channel\' ? \'推频道中\' : \'同步中\') : (pend > 0 ? `同步 ${pend}` : \'同步\'));\n    setTip($(\'syncBtn\'), $(\'syncBtnText\').textContent);\n    const link = $(\'syncFolderLink\');\n    if (st?.folder_url && /^https:\\/\\//i.test(String(st.folder_url))) { link.href = st.folder_url; link.hidden = false; } else link.hidden = true;\n    setText($(\'syncDriveCount\'), st ? `${Number(st.drive_uploaded || 0)} 张` : \'-- 张\');\n    setText($(\'syncChannelLabel\'), `TG 频道 ${st?.channel_username || \'\'}`.trim());\n    setText($(\'syncChannelCount\'), st ? `${Number(st.channel_synced || 0)} 张` : \'-- 张\');\n    const note = $(\'syncNote\');\n    let txt, warn = false;\n    if (running) txt = task.progress ? stripEmoji(task.progress) : (mode === \'channel\' ? \'正在把谷歌云图片同步到频道…\' : \'正在加密同步作品到 Google Drive…\');\n    else if (errs > 0) { txt = `有 ${errs} 项同步异常，可点下方按钮重试`; warn = true; }\n    else if (pend > 0) { txt = [pendUp ? `待上传 ${pendUp} 张` : \'\', pendDel ? `待移入回收站 ${pendDel} 张` : \'\'].filter(Boolean).join(\' · \'); warn = true; }\n    else if (st) txt = `${st.account_email ? \'主号 \' + st.account_email.split(\'@\')[0] : \'AES-256-GCM 加密\'} · ${st.updated_str ? \'最近同步 \' + st.updated_str : \'已对齐\'}`;\n    else txt = \'正在读取同步状态…\';\n    setText(note, txt);\n    note.classList.toggle(\'is-warn\', warn);\n    const d = $(\'syncDriveBtn\'), c = $(\'syncChannelBtn\');\n    d.disabled = running; c.disabled = running;\n    setText($(\'syncDriveTitle\'), running && mode === \'drive\' ? \'正在同步谷歌云盘…\' : \'仅同步谷歌云盘\');\n    setText($(\'syncChannelTitle\'), running && mode === \'channel\' ? \'正在同步到 TG 频道…\' : \'同步到 TG 频道\');\n  },\n\n  toggle(anchor) {\n    if (Pop.toggle($(\'syncPop\'), anchor, { align: \'end\' })) this.load(false);\n  },\n\n  async run(mode) {\n    if (this.submitting || this.st?.task?.running) { toast(\'同步任务正在执行，请稍候\'); return; }\n    this.submitting = true;\n    if (!this.st) this.st = { ok: true, task: { running: true, mode } };\n    else this.st.task = { ...(this.st.task || {}), running: true, mode };\n    this.render();\n    try {\n      const { res, data } = await api.raw(\'/api/drive_sync\', { method: \'POST\', headers: { \'Content-Type\': \'application/json\' }, body: JSON.stringify({ mode }) });\n      this.submitting = false;\n      if (data && typeof data === \'object\' && data.drive_uploaded !== undefined) this.st = { ...data, ok: true };\n      if (!res.ok || data.ok === false) {\n        if (this.st?.task && data.drive_uploaded === undefined) this.st.task.running = false;\n        this.render();\n        toast(data.message || \'同步启动失败\');\n        if (data.task?.running) this.schedule();\n        return;\n      }\n      this.render();\n      toast(data.message || (mode === \'channel\' ? \'已开始同步到频道\' : \'已开始同步谷歌云\'));\n      this.schedule();\n    } catch (e) {\n      this.submitting = false;\n      if (this.st?.task) this.st.task.running = false;\n      this.render();\n      toast(e.message || \'网络请求失败\');\n    }\n  }\n};\n\n/* ==================== 提示词详情弹窗 ==================== */\nconst PromptDialog = {\n  item: null,\n  selText: \'\',\n  open(item, kind) {\n    if (!item) return;\n    const text = kind === \'llm\' ? (item.preview_text || \'\') : (item.prompt || \'\');\n    if (!text.trim()) return;\n    this.item = { ...item, _text: text.trim(), _kind: kind };\n    setText($(\'promptDialogTitle\'), kind === \'llm\' ? \'提示词\' : (item.model_label ? stripEmoji(item.model_label) : \'提示词\'));\n    const sub = [];\n    if (item.res) sub.push(fmtRes(item.res));\n    if (item.steps) sub.push(`${item.steps} 步`);\n    if (item.time_str) sub.push(item.time_str);\n    sub.push(`${text.trim().length} 字`);\n    setText($(\'promptDialogSub\'), sub.join(\' · \'));\n    const body = $(\'promptDialogBody\');\n    body.textContent = text.trim();\n    body.scrollTop = 0;\n    this.resetCopy();\n    openDialog($(\'promptDialog\'));\n  },\n  close() { closeDialog($(\'promptDialog\')); },\n  resetCopy() { this.selText = \'\'; setText($(\'promptDialogCopyBtn\'), \'复制\'); },\n  onSelection() {\n    if (!$(\'promptDialog\').open) return;\n    const sel = window.getSelection();\n    const str = sel ? sel.toString().trim() : \'\';\n    const inside = str && sel.rangeCount > 0 && $(\'promptDialogBody\').contains(sel.anchorNode) && $(\'promptDialogBody\').contains(sel.focusNode);\n    if (inside) { this.selText = str; setText($(\'promptDialogCopyBtn\'), \'复制选中\'); }\n    else if (this.selText) this.resetCopy();\n  },\n  copy(triggerEl) {\n    if (this.selText) copyWithToast(this.selText, \'已复制选中片段\', triggerEl);\n    else if (this.item) copyWithToast(this.item._text, \'已复制提示词\', triggerEl);\n  },\n  reuse() {\n    const it = this.item;\n    if (!it) return;\n    this.close();\n    Create.fillPrompt(\'image\', it._kind === \'llm\' ? extractCleanPrompt(it._text) : it._text, \'已复用到图片提示词\');\n  }\n};\n$(\'promptDialog\').addEventListener(\'close\', () => PromptDialog.resetCopy());\n$(\'promptDialogCopyBtn\').addEventListener(\'pointerdown\', (e) => { if (PromptDialog.selText) e.preventDefault(); });\ndocument.addEventListener(\'selectionchange\', () => PromptDialog.onSelection());\n/* ==================== 详情查看器（桌面滚轮/触控板 + 手机双指无级缩放、拖拽平移、双击快切） ==================== */\nconst Viewer = {\n  open: false,\n  tab: \'image\',\n  list: [],\n  idx: -1,\n  lastFocus: null,\n  seq: 0,\n  scale: 1,\n  tx: 0,\n  ty: 0,\n  MIN_SCALE: 1,\n  MAX_SCALE: 6,\n  ptrs: new Map(),     // pointerId -> { x, y }\n  pinch: null,         // { dist, mx, my, s, tx, ty }\n  drag: null,          // { id, x, y, tx, ty, moved }\n  swipe: null,         // { id, x, y, t, target, type }\n  lastTap: null,       // { x, y, t }\n  tapTimer: null,\n  touchClosedAt: 0,\n\n  setSheet(open) {\n    $(\'viewer\').classList.toggle(\'is-sheet-open\', !!open);\n  },\n\n  toggleChrome() {\n    $(\'viewer\').classList.toggle(\'is-chrome-hidden\');\n  },\n\n  show(list, idx, tab) {\n    if (!list || !list.length) return;\n    this.list = list;\n    this.tab = tab;\n    this.idx = clamp(idx, 0, list.length - 1);\n    if (!this.open) {\n      this.lastFocus = document.activeElement;\n      this.open = true;\n      Tip.hide(true);\n      Pop.close(false);\n      const v = $(\'viewer\');\n      v.classList.remove(\'is-chrome-hidden\', \'is-sheet-open\');\n      v.hidden = false;\n      requestAnimationFrame(() => v.classList.add(\'is-open\'));\n      v.focus({ preventScroll: true });\n    }\n    this.setSheet(false);\n    this.renderItem();\n  },\n\n  close(byTouch = false) {\n    if (!this.open) return;\n    this.open = false;\n    clearTimeout(this.tapTimer);\n    this.tapTimer = null;\n    if (byTouch) this.touchClosedAt = Date.now();\n    const v = $(\'viewer\');\n    v.classList.remove(\'is-open\', \'is-sheet-open\', \'is-chrome-hidden\');\n    const vid = $(\'viewerVideo\');\n    vid.pause();\n    setTimeout(() => {\n      if (this.open) return;\n      v.hidden = true;\n      vid.removeAttribute(\'src\');\n      vid.load();\n      $(\'viewerImg\').removeAttribute(\'src\');\n    }, 110);\n    this.resetZoom(false);\n    if (this.lastFocus && this.lastFocus.isConnected) this.lastFocus.focus({ preventScroll: true });\n  },\n\n  cur() { return this.list[this.idx] || null; },\n\n  step(dir) {\n    if (this.list.length < 2) return;\n    this.idx = (this.idx + dir + this.list.length) % this.list.length;\n    this.renderItem();\n  },\n\n  refreshList(preferIdx) {\n    const curName = this.cur() && this.cur().name;\n    const fresh = this.tab === \'stage\' ? Stage.list(S.mode) : (this.tab && Works.tab === this.tab ? Works.computeFiltered() : (S.outputs[this.tab] || []));\n    this.list = fresh;\n    if (!fresh.length) { this.close(); return; }\n    const i = fresh.findIndex(x => x.name === curName);\n    this.idx = i >= 0 ? i : clamp(preferIdx ?? this.idx, 0, fresh.length - 1);\n    this.renderItem();\n  },\n\n  isZoomed() { return this.scale > 1.01; },\n\n  /* 限制平移不超出画布边界 */\n  clampPan(s, tx, ty) {\n    if (s <= 1.01) return [0, 0];\n    const c = $(\'viewerCanvas\'), img = $(\'viewerImg\');\n    const bw = img.offsetWidth || 0, bh = img.offsetHeight || 0;\n    const maxX = Math.max(0, (bw * s - c.clientWidth) / 2);\n    const maxY = Math.max(0, (bh * s - c.clientHeight) / 2);\n    return [clamp(tx, -maxX, maxX), clamp(ty, -maxY, maxY)];\n  },\n\n  applyTransform(animate = false) {\n    const img = $(\'viewerImg\'), v = $(\'viewer\'), pill = $(\'viewerZoomPill\');\n    const slider = $(\'viewerZoomSlider\'), pctBtn = $(\'viewerZoomPct\');\n    img.classList.toggle(\'is-anim\', animate);\n    if (this.scale <= 1.01) {\n      this.scale = 1;\n      this.tx = 0;\n      this.ty = 0;\n      img.style.transform = \'\';\n      v.classList.remove(\'is-zoomed\');\n      pill.hidden = true;\n    } else {\n      const [cx, cy] = this.clampPan(this.scale, this.tx, this.ty);\n      this.tx = cx;\n      this.ty = cy;\n      img.style.transform = `translate3d(${cx.toFixed(2)}px, ${cy.toFixed(2)}px, 0) scale(${this.scale.toFixed(4)})`;\n      v.classList.add(\'is-zoomed\');\n      const pctText = `${Math.round(this.scale * 100)}%`;\n      setText(pill, pctText);\n      pill.hidden = false;\n    }\n    const pctNum = Math.round(this.scale * 100);\n    if (slider && slider.value !== String(pctNum)) slider.value = String(pctNum);\n    if (pctBtn) setText(pctBtn, `${pctNum}%`);\n  },\n\n  resetZoom(animate = true) {\n    this.ptrs.clear();\n    this.pinch = null;\n    this.drag = null;\n    $(\'viewerCanvas\').classList.remove(\'is-dragging\');\n    this.scale = 1;\n    this.tx = 0;\n    this.ty = 0;\n    this.applyTransform(animate);\n  },\n\n  /* 以屏幕坐标 (ax, ay) 为锚点无级缩放 */\n  zoomTo(nextScale, ax, ay, animate = false) {\n    const img = $(\'viewerImg\');\n    if (img.hidden) return;\n    const c = $(\'viewerCanvas\');\n    const cr = c.getBoundingClientRect();\n    const cx = cr.left + cr.width / 2;\n    const cy = cr.top + cr.height / 2;\n    const ns = clamp(nextScale, this.MIN_SCALE, this.MAX_SCALE);\n    const ratio = ns / this.scale;\n    const x = typeof ax === \'number\' ? ax : cx;\n    const y = typeof ay === \'number\' ? ay : cy;\n    this.tx = (x - cx) - ((x - cx) - this.tx) * ratio;\n    this.ty = (y - cy) - ((y - cy) - this.ty) * ratio;\n    this.scale = ns;\n    this.applyTransform(animate);\n  },\n\n  zoomBy(factor, ax, ay, animate = false) {\n    this.zoomTo(this.scale * factor, ax, ay, animate);\n  },\n\n  renderItem() {\n    const it = this.cur();\n    if (!it) return;\n    const seq = ++this.seq;\n    const isVid = isVideoItem(it, this.tab);\n    const img = $(\'viewerImg\'), vid = $(\'viewerVideo\');\n    $(\'viewer\').classList.toggle(\'is-video\', isVid);\n    this.resetZoom(false);\n    if (isVid) {\n      img.hidden = true;\n      img.removeAttribute(\'src\');\n      $(\'viewerZoomBar\').hidden = true;\n      vid.hidden = false;\n      if (vid.getAttribute(\'src\') !== it.url) vid.src = it.url;\n    } else {\n      vid.pause();\n      vid.hidden = true;\n      vid.removeAttribute(\'src\');\n      img.hidden = false;\n      $(\'viewerZoomBar\').hidden = false;\n      img.alt = it.name || \'\';\n      progressiveImg(img, it.thumb_url, it.url, () => seq === this.seq, () => {\n        if (this.isZoomed()) this.applyTransform(false);\n      });\n    }\n    setText($(\'viewerName\'), it.name || \'\');\n    setTip($(\'viewerName\'), it.name || \'\');\n    const meta = [];\n    if (it.res) meta.push(fmtRes(it.res));\n    if (it.steps) meta.push(`${it.steps}\\u00A0步`);\n    if (it.model_label) meta.push(stripEmoji(it.model_label));\n    if (it.time_str) meta.push(it.time_str);\n    if (it.size_kb) meta.push(`${(it.size_kb / 1024).toFixed(1)}\\u00A0MB`);\n    const metaEl = $(\'viewerMeta\');\n    metaEl.replaceChildren(...meta.map((m, i) => {\n      const s = el(\'span\', \'meta-tok\', m);\n      if (i > 0) s.dataset.sep = \'1\';\n      return s;\n    }));\n    const p = $(\'viewerPrompt\');\n    p.textContent = it.prompt || \'没有提示词信息\';\n    p.classList.toggle(\'is-empty\', !it.prompt);\n    p.scrollTop = 0;\n    $(\'viewerReuseBtn\').hidden = !it.prompt;\n    $(\'viewerPromptHead\').querySelector(\'[data-action="viewer-copy"]\').hidden = !it.prompt;\n    $(\'viewerRefBtn\').hidden = isVid;\n    $(\'viewerVideoBtn\').hidden = isVid;\n    const dl = $(\'viewerDownload\');\n    dl.href = it.url;\n    dl.setAttribute(\'download\', it.name || \'\');\n    setText(dl, isVid ? \'下载视频\' : \'下载原图\');\n    /* 同步手机顶部计数与底部精简操作坞 */\n    setText($(\'viewerCounter\'), this.list.length > 1 ? `${this.idx + 1} / ${this.list.length}` : \'\');\n    $(\'viewerMobReuse\').hidden = !it.prompt;\n    $(\'viewerMobRef\').hidden = isVid;\n    $(\'viewerMobVideo\').hidden = isVid;\n    const mdl = $(\'viewerMobDownload\');\n    mdl.href = it.url;\n    mdl.setAttribute(\'download\', it.name || \'\');\n    const multi = this.list.length > 1;\n    $(\'viewerPrev\').hidden = !multi;\n    $(\'viewerNext\').hidden = !multi;\n  },\n\n  srcTab() { return this.tab === \'stage\' ? S.mode : this.tab; },\n\n  reuse() {\n    const it = this.cur();\n    if (!it || !it.prompt) return;\n    const target = isVideoItem(it, this.srcTab()) ? \'video\' : \'image\';\n    this.close();\n    Create.fillPrompt(target, it.prompt, \'已复用提示词\');\n  },\n  asRef() { const it = this.cur(); if (it) { this.close(); useAsRef(it); } },\n  toVideo() { const it = this.cur(); if (it) { this.close(); makeVideo(it); } },\n  copy(triggerEl) { const it = this.cur(); if (it && it.prompt) copyWithToast(it.prompt, \'已复制提示词\', triggerEl); },\n  del() {\n    const it = this.cur();\n    if (!it) return;\n    const keepIdx = this.idx;\n    Works.askDelete(this.srcTab(), it, () => { if (this.open) this.refreshList(keepIdx); });\n  },\n\n  /* ---------- 指针 + 滚轮：桌面无级滚轮/触控板缩放 + 手机双指捏合缩放 + 平移 + 双击快切 ---------- */\n  initPointer() {\n    const c = $(\'viewerCanvas\');\n    const img = $(\'viewerImg\');\n\n    /* 防止 iOS Safari 双指捏合缩放整页 */\n    [\'gesturestart\', \'gesturechange\', \'gestureend\'].forEach(ev => {\n      c.addEventListener(ev, (e) => { if (this.open) e.preventDefault(); }, { passive: false });\n    });\n\n    /* 桌面无级缩放滑杆 */\n    const slider = $(\'viewerZoomSlider\');\n    slider.addEventListener(\'input\', () => {\n      const s = clamp(Number(slider.value) / 100, this.MIN_SCALE, this.MAX_SCALE);\n      this.zoomTo(s, undefined, undefined, false);\n    });\n\n    /* 桌面滚轮 / 触控板双指捏合：连续无级缩放 */\n    c.addEventListener(\'wheel\', (e) => {\n      if (!this.open || img.hidden) return;\n      e.preventDefault();\n      const speed = e.ctrlKey ? 0.012 : 0.0022;\n      const factor = Math.exp(-clamp(e.deltaY, -180, 180) * speed);\n      this.zoomBy(factor, e.clientX, e.clientY, false);\n    }, { passive: false });\n\n    const twoFinger = () => {\n      const pts = [...this.ptrs.values()];\n      const dx = pts[1].x - pts[0].x, dy = pts[1].y - pts[0].y;\n      return { dist: Math.hypot(dx, dy) || 1, mx: (pts[0].x + pts[1].x) / 2, my: (pts[0].y + pts[1].y) / 2 };\n    };\n\n    c.addEventListener(\'pointerdown\', (e) => {\n      if (e.button > 0 || e.target.tagName === \'VIDEO\' || e.target.closest(\'.viewer-zoom-pill, .viewer-zoom-bar\')) return;\n      this.ptrs.set(e.pointerId, { x: e.clientX, y: e.clientY });\n      if (this.ptrs.size === 2 && !img.hidden) {\n        const g = twoFinger();\n        this.pinch = { dist: g.dist, mx: g.mx, my: g.my, s: this.scale, tx: this.tx, ty: this.ty };\n        this.drag = null;\n        this.swipe = null;\n        this.lastTap = null;\n        c.classList.remove(\'is-dragging\');\n        return;\n      }\n      if (this.ptrs.size === 1) {\n        if (this.isZoomed()) {\n          this.drag = { id: e.pointerId, x: e.clientX, y: e.clientY, tx: this.tx, ty: this.ty, moved: false };\n          this.swipe = null;\n        } else {\n          this.swipe = { id: e.pointerId, x: e.clientX, y: e.clientY, t: Date.now(), target: e.target, type: e.pointerType };\n          this.drag = null;\n        }\n      }\n    });\n\n    c.addEventListener(\'pointermove\', (e) => {\n      if (!this.ptrs.has(e.pointerId)) return;\n      this.ptrs.set(e.pointerId, { x: e.clientX, y: e.clientY });\n      if (this.pinch && this.ptrs.size >= 2) {\n        const g = twoFinger();\n        const cr = c.getBoundingClientRect();\n        const cx = cr.left + cr.width / 2, cy = cr.top + cr.height / 2;\n        const ns = clamp(this.pinch.s * (g.dist / this.pinch.dist), this.MIN_SCALE, this.MAX_SCALE);\n        const ratio = ns / this.pinch.s;\n        this.scale = ns;\n        this.tx = (g.mx - cx) - ((this.pinch.mx - cx) - this.pinch.tx) * ratio;\n        this.ty = (g.my - cy) - ((this.pinch.my - cy) - this.pinch.ty) * ratio;\n        this.applyTransform(false);\n        return;\n      }\n      const d = this.drag;\n      if (d && d.id === e.pointerId) {\n        const dx = e.clientX - d.x, dy = e.clientY - d.y;\n        if (!d.moved && Math.abs(dx) + Math.abs(dy) > 4) {\n          d.moved = true;\n          c.classList.add(\'is-dragging\');\n        }\n        if (d.moved) {\n          this.tx = d.tx + dx;\n          this.ty = d.ty + dy;\n          this.applyTransform(false);\n        }\n      }\n    });\n\n    const endPointer = (e) => {\n      if (!this.ptrs.has(e.pointerId)) return;\n      this.ptrs.delete(e.pointerId);\n\n      /* 结束双指捏合：若还剩 1 指且已放大，平滑转为单指平移 */\n      if (this.pinch) {\n        if (this.ptrs.size < 2) {\n          this.pinch = null;\n          if (this.ptrs.size === 1 && this.isZoomed()) {\n            const [[id, pt]] = this.ptrs.entries();\n            this.drag = { id, x: pt.x, y: pt.y, tx: this.tx, ty: this.ty, moved: true };\n          }\n        }\n        return;\n      }\n\n      const d = this.drag;\n      if (d && d.id === e.pointerId) {\n        this.drag = null;\n        c.classList.remove(\'is-dragging\');\n        if (!d.moved && e.type === \'pointerup\') this.checkDoubleTap(e.clientX, e.clientY, true);\n        return;\n      }\n\n      const s = this.swipe;\n      if (s && s.id === e.pointerId) {\n        this.swipe = null;\n        if (e.type !== \'pointerup\') return;\n        const dx = e.clientX - s.x, dy = e.clientY - s.y, dt = Date.now() - s.t;\n        if (Math.abs(dx) > 48 && Math.abs(dx) > Math.abs(dy) * 1.5 && dt < 600) {\n          this.step(dx < 0 ? 1 : -1);\n          return;\n        }\n        /* 手机未放大时：下滑关闭查看器，上滑打开详情提示词 */\n        if (isMobile() && Math.abs(dy) > 64 && Math.abs(dy) > Math.abs(dx) * 1.5 && dt < 600) {\n          if (dy > 0) this.close(true);\n          else this.setSheet(true);\n          return;\n        }\n        if (Math.abs(dx) < 6 && Math.abs(dy) < 6) {\n          if (s.target === img && !img.hidden) this.checkDoubleTap(e.clientX, e.clientY, false);\n          else if (s.target === c) {\n            if (isMobile()) this.toggleChrome();\n            else this.close(s.type === \'touch\');\n          }\n        }\n      }\n    };\n    c.addEventListener(\'pointerup\', endPointer);\n    c.addEventListener(\'pointercancel\', endPointer);\n  },\n\n  /* 双击/双触快速在 1× 与 2.5× 之间切换；手机单击图片静默切显隐 HUD（绝不缩放） */\n  checkDoubleTap(x, y, zoomed) {\n    const now = Date.now();\n    const prev = this.lastTap;\n    clearTimeout(this.tapTimer);\n    this.tapTimer = null;\n    if (prev && now - prev.t < 280 && Math.hypot(x - prev.x, y - prev.y) < 28) {\n      this.lastTap = null;\n      if (zoomed || this.isZoomed()) this.resetZoom(true);\n      else this.zoomTo(2.5, x, y, true);\n    } else {\n      this.lastTap = { x, y, t: now };\n      if (isMobile() && !zoomed && !this.isZoomed()) {\n        this.tapTimer = setTimeout(() => {\n          this.tapTimer = null;\n          if (this.open && !this.isZoomed()) this.toggleChrome();\n        }, 240);\n      }\n    }\n  }\n};\n/* ==================== 事件委托：data-action → 处理函数 ==================== */\nconst num = (t, k = \'idx\') => Number(t.dataset[k]);\nconst ACTIONS = {\n  /* 框架 */\n  \'brand-home\': () => { navigate(\'create\'); },\n  \'nav-page\': (t) => navigate(t.dataset.page),\n  \'logout-panel\': () => {\n    Pop.close(false);\n    Confirm.ask({\n      title: \'退出登录？\', text: \'将清除当前登录状态并吊销会话凭证。\', accept: \'退出登录\',\n      run: async () => { try { await api.post(\'/api/logout\', {}); } catch (_) {} goLogin(); }\n    });\n  },\n  \'set-mode\': (t) => Create.setMode(t.dataset.mode),\n  \'toggle-task-panel\': () => Drawer.set(!Drawer.open),\n  \'toggle-machine\': () => Machine.togglePop(),\n\n  /* 创作 */\n  \'set-editor-tab\': (t) => Create.setEditorTab(t.dataset.tab),\n  \'clear-prompt\': () => Create.clearPrompt(),\n  \'toggle-random-picker\': (t) => Create.toggleRandom(t),\n  \'pick-random-count\': (t) => Create.submitRandom(\'image\', num(t, \'count\')),\n  \'open-ref-picker\': () => RefPicker.open(),\n  \'ref-picker-tab\': (t) => RefPicker.setTab(t.dataset.tab),\n  \'close-ref-picker\': () => RefPicker.close(),\n  \'pick-ref\': (t) => RefPicker.pick(t.dataset.path || \'\'),\n  \'trigger-upload\': () => Create.triggerUpload(),\n  \'clear-ref\': () => Create.setRef(S.mode, \'\'),\n  \'set-seg\': (t) => Create.setSeg(t.dataset.key, t.dataset.val),\n  \'submit-task\': () => Create.submit(),\n  \'cancel-job\': () => Confirm.ask({\n    title: \'中止生成？\',\n    text: \'会中止所有正在运行和排队中的生成任务（包括随机批次剩下的张数）。\',\n    accept: \'中止\',\n    run: async () => { try { await api.post(\'/api/cancel\', {}); } catch (e) { toast(e.message || \'中止失败\'); } await Jobs.poll(true); }\n  }),\n  \'open-task-log\': () => { Tasks.setTab(\'history\'); Drawer.set(true); Tasks.setLogOpen(true); },\n\n  /* AI 帮写 */\n  \'clear-chat\': () => Chat.askClear(),\n  \'chat-to-mod\': (t) => Chat.fill(num(t)),\n  \'copy-chat\': (t) => Chat.copy(num(t), t),\n\n  /* 舞台 */\n  \'stage-prev\': () => Stage.step(-1),\n  \'stage-next\': () => Stage.step(1),\n  \'stage-view\': () => { if (Stage._swipedAt && Date.now() - Stage._swipedAt < 300) return; const list = Stage.list(S.mode); const it = Stage.current(); if (it) Viewer.show(list, list.indexOf(it), \'stage\'); },\n  \'stage-as-ref\': () => { const it = Stage.current(); if (it) { Create.setRef(\'image\', it.path); toast(\'已挂载为人脸参考\'); } },\n  \'stage-to-video\': () => { const it = Stage.current(); if (it) makeVideo(it); },\n  \'stage-reuse\': () => { const it = Stage.current(); if (it && it.prompt) Create.fillPrompt(S.mode, it.prompt, \'已复用提示词\'); },\n  \'stage-more\': (t) => Stage.more(t),\n  \'recent-pick\': (t) => Stage.pick(t.dataset.name),\n  \'goto-works-mode\': () => { Works.switchTab(S.mode); navigate(\'works\'); },\n\n  /* 作品 */\n  \'switch-tab\': (t) => Works.switchTab(t.dataset.tab),\n  \'toggle-drive-sync-picker\': (t) => Sync.toggle(t),\n  \'sync-drive\': () => Sync.run(\'drive\'),\n  \'sync-channel\': () => Sync.run(\'channel\'),\n  \'toggle-batch-mode\': () => Works.setBatch(),\n  \'works-more\': (t) => Works.more(t),\n  \'goto-page\': (t) => {\n    const fromBottom = !!t.closest(\'#pagerBottom\');\n    Works.gotoPage(t.dataset.rel ? Works.page + Number(t.dataset.rel) : Number(t.dataset.page), true);\n    if (fromBottom) Works.scrollTop(false);\n  },\n  \'goto-gallery-top\': () => Works.scrollTop(true),\n  \'batch-toggle-item\': (t, e) => Works.toggleItem(num(t), !!e.shiftKey),\n  \'batch-select-page\': () => Works.selectPage(),\n  \'batch-select-filtered\': () => Works.selectAll(),\n  \'batch-clear\': () => Works.clearSel(),\n  \'batch-download\': () => Works.batchDownload(),\n  \'batch-delete\': () => Works.batchDelete(),\n  \'open-lightbox\': (t) => Viewer.show(Works.filtered, num(t), Works.tab),\n  \'reuse-img-spec\': (t) => {\n    const it = Works.filtered[num(t)];\n    if (!it || !it.prompt) return;\n    const target = isVideoItem(it, Works.tab) ? \'video\' : \'image\';\n    Create.fillPrompt(target, it.prompt, target === \'video\' ? \'已复用到视频提示词\' : \'已复用到图片提示词\');\n  },\n  \'card-more\': (t) => Works.cardMore(t, num(t)),\n  \'open-prompt-dialog\': (t) => PromptDialog.open(Works.filtered[num(t)], \'llm\'),\n  \'reuse-llm-file\': (t) => { const it = Works.filtered[num(t)]; if (it && it.preview_text) Create.fillPrompt(\'image\', extractCleanPrompt(it.preview_text), \'已填入图片提示词\'); },\n  \'copy-llm-file\': (t) => { const it = Works.filtered[num(t)]; if (it) copyWithToast(it.preview_text, \'已复制提示词\', t); },\n  \'prow-more\': (t) => Works.rowMore(t, num(t)),\n  \'close-prompt-dialog\': () => PromptDialog.close(),\n  \'copy-prompt-dialog\': (t) => PromptDialog.copy(t),\n  \'reuse-prompt-dialog\': () => PromptDialog.reuse(),\n\n  /* 查看器 */\n  \'lightbox-prev\': () => Viewer.step(-1),\n  \'lightbox-next\': () => Viewer.step(1),\n  \'close-viewer\': () => Viewer.close(),\n  \'viewer-head-close\': () => { if (isMobile()) Viewer.setSheet(false); else Viewer.close(); },\n  \'toggle-viewer-sheet\': () => Viewer.setSheet(!$(\'viewer\').classList.contains(\'is-sheet-open\')),\n  \'close-viewer-sheet\': () => Viewer.setSheet(false),\n  \'viewer-reuse\': () => Viewer.reuse(),\n  \'viewer-as-ref\': () => Viewer.asRef(),\n  \'viewer-to-video\': () => Viewer.toVideo(),\n  \'viewer-delete\': () => Viewer.del(),\n  \'viewer-copy\': (t) => Viewer.copy(t),\n  \'viewer-zoom-in\': () => Viewer.zoomBy(1.25, undefined, undefined, true),\n  \'viewer-zoom-out\': () => Viewer.zoomBy(0.8, undefined, undefined, true),\n  \'viewer-zoom-reset\': () => Viewer.resetZoom(true),\n\n  /* 机器浮层 */\n  \'toggle-account-picker\': () => Machine.toggleAccountPanel(),\n  \'refresh-accounts\': () => { Machine.loadAccounts(true); Machine.loadStatus(true); },\n  \'switch-colab-account\': (t) => Machine.switchAccount(t.dataset.email || \'\', false),\n  \'toggle-acct-oauth\': () => Machine.toggleOauth(),\n  \'copy-acct-oauth-url\': (t) => { if (Machine.oauthUrl) copyWithToast(Machine.oauthUrl, \'已复制授权链接\', t); },\n  \'submit-acct-oauth\': () => Machine.submitOauth(),\n  \'warmup-all\': () => Machine.toggleWarmupBox(),\n  \'start-warmup\': () => Machine.submitPower(\'warmup\'),\n  \'toggle-power\': () => Machine.togglePower(),\n\n  /* 任务抽屉 */\n  \'close-task-drawer\': () => Drawer.set(false),\n  \'set-drawer-tab\': (t) => Tasks.setTab(t.dataset.tab),\n  \'toggle-log\': () => Tasks.setLogOpen(!Tasks.logOpen),\n  \'toggle-task\': (t) => Tasks.toggleExpand(num(t, \'jid\')),\n  \'task-stop\': (t) => Tasks.action(num(t, \'jid\'), \'stop\'),\n  \'task-resume\': (t) => Tasks.action(num(t, \'jid\'), \'resume\'),\n  \'task-delete\': (t) => Tasks.askDelete(num(t, \'jid\')),\n  \'task-reuse\': (t) => Tasks.reuse(num(t, \'jid\'), t.dataset.target),\n  \'tasks-resume-all\': () => Tasks.bulk(\'resume\'),\n  \'tasks-discard-all\': () => Tasks.askDiscardAll(),\n  \'load-all-tasks\': () => { Tasks.showAll = true; Jobs.poll(true); },\n  \'clear-tasks\': () => Tasks.askClearAll(),\n\n  /* 通用组件 */\n  \'confirm-accept\': () => Confirm.accept(),\n  \'confirm-cancel\': () => Confirm.cancel(),\n  \'menu-item\': (t) => Menu.run(num(t)),\n  \'zselect-open\': (t) => ZSelect.open(t._select),\n  \'zselect-pick\': (t) => ZSelect.pick(num(t))\n};\n\ndocument.addEventListener(\'click\', (e) => {\n  if (Viewer.touchClosedAt && Date.now() - Viewer.touchClosedAt < 200 && !e.target.closest(\'#viewer\')) {\n    e.preventDefault();\n    e.stopPropagation();\n    return;\n  }\n  const t = e.target.closest(\'[data-action]\');\n  if (!t) return;\n  if (e.target.closest(\'a[download]\') && !t.matches(\'a\')) return;\n  if (t.disabled || t.getAttribute(\'aria-disabled\') === \'true\') return;\n  const fn = ACTIONS[t.dataset.action];\n  if (!fn) return;\n  if (t.closest(\'.card-btns\') || t.closest(\'.prow-acts\')) e.stopPropagation();\n  fn(t, e);\n}, true);\n\n/* 非 button 的 role=button 元素：Enter / Space 触发 */\ndocument.addEventListener(\'keydown\', (e) => {\n  if (e.key !== \'Enter\' && e.key !== \' \') return;\n  const c = e.target.closest(\'[role="button"][data-action]\');\n  if (!c || c !== e.target) return;\n  e.preventDefault();\n  c.click();\n});\n\n/* 全局键盘 */\ndocument.addEventListener(\'keydown\', (e) => {\n  if (ZSelect.key(e)) return;\n  if (e.key === \'Escape\') {\n    Tip.hide(true);\n    if (Pop.cur) { e.preventDefault(); Pop.close(true); return; }\n    if (Viewer.open) {\n      e.preventDefault();\n      if (Viewer.isZoomed()) Viewer.resetZoom(true);\n      else Viewer.close();\n      return;\n    }\n    if (document.querySelector(\'dialog[open]\')) return;\n    if (Drawer.open) { e.preventDefault(); Drawer.set(false); return; }\n    return;\n  }\n  if (Viewer.open) {\n    if (e.key === \'ArrowLeft\') { e.preventDefault(); Viewer.step(-1); }\n    else if (e.key === \'ArrowRight\') { e.preventDefault(); Viewer.step(1); }\n    else if (e.key === \'+\' || e.key === \'=\') { e.preventDefault(); Viewer.zoomBy(1.25, undefined, undefined, true); }\n    else if (e.key === \'-\') { e.preventDefault(); Viewer.zoomBy(0.8, undefined, undefined, true); }\n    else if (e.key === \'0\') { e.preventDefault(); Viewer.resetZoom(true); }\n    return;\n  }\n  if ((e.ctrlKey || e.metaKey) && e.key === \'Enter\') {\n    if (e.target === $(\'chatInput\') || document.querySelector(\'dialog[open]\') || Drawer.open) return;\n    if (S.page === \'create\') { e.preventDefault(); Create.submit(); }\n    return;\n  }\n  if (e.target === $(\'stage\') && (e.key === \'ArrowLeft\' || e.key === \'ArrowRight\')) {\n    e.preventDefault();\n    Stage.step(e.key === \'ArrowLeft\' ? -1 : 1);\n  }\n});\n\ndocument.addEventListener(\'change\', (e) => {\n  const w = e.target.closest(\'[data-warmup-target]\');\n  if (w) { Machine.onWarmupCheck(w); return; }\n  if (e.target === $(\'paginationJump\')) Works.gotoPage(Number(e.target.value), true);\n});\n\nlet _searchTimer = null;\n$(\'gallerySearchInput\').addEventListener(\'input\', (e) => {\n  clearTimeout(_searchTimer);\n  _searchTimer = setTimeout(() => Works.setQuery(e.target.value), 150);\n});\nlet _rpTimer = null;\n$(\'refPickerSearch\').addEventListener(\'input\', (e) => {\n  clearTimeout(_rpTimer);\n  _rpTimer = setTimeout(() => { RefPicker.query = e.target.value || \'\'; RefPicker.build(); }, 150);\n});\n\n/* 最近条：滚轮竖向转横向 */\n$(\'recentList\').addEventListener(\'wheel\', (e) => {\n  const box = e.currentTarget;\n  if (Math.abs(e.deltaY) <= Math.abs(e.deltaX) || box.scrollWidth <= box.clientWidth) return;\n  e.preventDefault();\n  box.scrollLeft += e.deltaY;\n}, { passive: false });\n\nlet _resizeRaf = 0;\nwindow.addEventListener(\'resize\', () => {\n  cancelAnimationFrame(_resizeRaf);\n  _resizeRaf = requestAnimationFrame(() => {\n    Works.onResize();\n    Stage.render();\n    Tasks.renderPill();\n    Create.autoGrow(curInput());\n  });\n});\nmqMobile.addEventListener(\'change\', () => { Tasks.renderPill(); Works.render(true); });\n\n/* ==================== 启动 ==================== */\n(function boot() {\n  Tip.init();\n  document.querySelectorAll(\'select.select\').forEach(s => ZSelect.enhance(s));\n  bindScrollFade($(\'recentList\'), \'x\');\n  bindScrollFade($(\'chatList\'), \'y\');\n  bindScrollFade($(\'logBox\'), \'y\');\n  Create.init();\n  Chat.init();\n  Stage.initResize();\n  Viewer.initPointer();\n  Machine.renderPill();\n  Machine.renderMetrics(null);\n  Machine.renderPowerControls();\n\n  _restoring = true;\n  let saved = store.get(UI_STATE_KEY, null, sessionStorage);\n  if (!saved) {\n    const v1 = store.get(UI_STATE_KEY_V1, null, sessionStorage);\n    if (v1 && typeof v1 === \'object\') {\n      saved = {\n        mode: v1.mod === \'video\' ? \'video\' : \'image\',\n        page: v1.navPage === \'works\' ? \'works\' : \'create\',\n        editorTab: v1.mod === \'llm\' ? \'chat\' : \'write\',\n        seg: v1.segs,\n        works: { tab: v1.tab, page: v1.page, query: v1.search },\n        warmup: { customized: v1.warmupCustomized, targets: v1.warmupTargets },\n        drafts: { image: v1.drafts?.image, video: v1.drafts?.video, chat: v1.drafts?.llm }\n      };\n    }\n  }\n  try {\n    if (saved && typeof saved === \'object\') {\n      if (saved.seg) Object.keys(SEG_GROUPS).forEach(k => { if (saved.seg[k]) Create.setSeg(k, saved.seg[k], true); });\n      const d = saved.drafts || {};\n      if (typeof d.image === \'string\') $(\'promptInputImage\').value = d.image;\n      if (typeof d.video === \'string\') $(\'promptInputVideo\').value = d.video;\n      if (typeof d.chat === \'string\') $(\'chatInput\').value = d.chat;\n      const w = saved.works || {};\n      if (WORK_TABS.includes(w.tab)) Works.tab = w.tab;\n      if (Number.isInteger(w.page) && w.page >= 1) Works.page = w.page;\n      if (typeof w.query === \'string\') { Works.query = w.query; $(\'gallerySearchInput\').value = w.query; }\n      const wu = saved.warmup || {};\n      if (wu.customized && Array.isArray(wu.targets)) {\n        const valid = wu.targets.filter(k => [\'v5_int8\', \'video\', \'llm\'].includes(k));\n        if (valid.length) { Machine.warmupCustomized = true; Machine.warmupTargets = valid; }\n      }\n    }\n    Object.keys(SEG_GROUPS).forEach(k => Create.setSeg(k, S.seg[k], true));\n    Create.setMode(saved && MODES.includes(saved.mode) ? saved.mode : \'image\');\n    Create.setEditorTab(saved && saved.editorTab === \'chat\' ? \'chat\' : \'write\');\n    navigate(saved && PAGES.includes(saved.page) ? saved.page : \'create\');\n    Works.renderCounts();\n    Machine.renderPowerControls();\n    Chat.autoGrow();\n  } finally {\n    _restoring = false;\n  }\n  saveUiState();\n\n  Create.loadPresets();\n  Chat.load();\n  Works.load();\n  Jobs.poll(true).then(() => { if (!Machine.lastStatusAttempt) Machine.loadStatus(false); });\n  setInterval(() => Jobs.poll(), 1500);\n  setInterval(() => { if (!document.hidden && !Sync.timer) Sync.load(true); }, 20000);\n})();\n</script>\n</body>\n</html>\n'

class PanelHTTPServer(ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True
    allow_reuse_address = True


class PanelHandler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        try:
            self.request.settimeout(60.0)
        except OSError:
            pass

    def version_string(self):
        return "colabbb-panel"

    def log_message(self, format, *args):
        pass

    def _safe_write(self, data: bytes) -> bool:
        if self.command == "HEAD":
            return True
        try:
            self.wfile.write(data)
            return True
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            return False

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self._safe_write(body)

    def _read_json_body(self, max_bytes=2 * 1024 * 1024, required=False):
        raw_len = (self.headers.get("Content-Length") or "0").strip()
        try:
            length = int(raw_len)
        except ValueError:
            self._send_json({"ok": False, "message": "无效的 Content-Length"}, 400)
            return None
        if length < 0 or (required and length == 0):
            self._send_json({"ok": False, "message": "请求内容为空或长度无效"}, 400)
            return None
        if length > max_bytes:
            self._send_json({"ok": False, "message": "请求体超出大小限制"}, 413)
            return None
        if length == 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        try:
            data = json.loads(raw or "{}")
        except Exception:
            self._send_json({"ok": False, "message": "JSON 格式解析失败"}, 400)
            return None
        if not isinstance(data, dict):
            self._send_json({"ok": False, "message": "请求体必须为 JSON 对象"}, 400)
            return None
        return data

    def _client_real_ip(self) -> str:
        peer = self.client_address[0] if self.client_address else "unknown"
        if _is_trusted_proxy_ip(peer):
            # Prefer rightmost X-Forwarded-For appended by the trusted local reverse proxy
            # rather than client-spoofable X-Real-IP.
            xff = self.headers.get("X-Forwarded-For") or ""
            for cand in reversed([p.strip() for p in xff.split(",") if p.strip()]):
                try:
                    return str(ipaddress.ip_address(cand))
                except ValueError:
                    continue
            real_ip = (self.headers.get("X-Real-IP") or "").strip()
            if real_ip:
                try:
                    return str(ipaddress.ip_address(real_ip))
                except ValueError:
                    pass
        return peer

    def _is_internal_request(self) -> bool:
        peer_ip = self.client_address[0] if self.client_address else ""
        if peer_ip in ("127.0.0.1", "::1", "::ffff:127.0.0.1") and not self.headers.get("X-Forwarded-For"):
            ipc_hdr = (self.headers.get("X-G4-Internal") or "").strip()
            cfg_tok = (_get_auth_config().get("internal_ipc_token") or "").strip()
            if ipc_hdr and cfg_tok and hmac.compare_digest(ipc_hdr, cfg_tok):
                return True
        return False

    def _check_csrf_origin(self) -> bool:
        if self._is_internal_request():
            return True
        fetch_site = (self.headers.get("Sec-Fetch-Site") or "").strip().lower()
        if fetch_site == "cross-site":
            return False
        origin = (self.headers.get("Origin") or "").strip()
        referer = (self.headers.get("Referer") or "").strip()
        if not origin and not referer:
            return True
        host_hdr = (self.headers.get("Host") or "").strip().lower()
        allowed_hosts = {host_hdr} if host_hdr else set()
        if host_hdr and ":" not in host_hdr:
            allowed_hosts.add(f"{host_hdr}:443")
            allowed_hosts.add(f"{host_hdr}:80")
        if origin:
            if origin.lower() == "null":
                return False
            parsed = urllib.parse.urlparse(origin)
            if parsed.scheme not in ("http", "https") or (parsed.netloc or "").lower() not in allowed_hosts:
                return False
            return True
        parsed_ref = urllib.parse.urlparse(referer)
        return parsed_ref.scheme in ("http", "https") and (parsed_ref.netloc or "").lower() in allowed_hosts

    def _is_authenticated(self) -> bool:
        if self._is_internal_request():
            return True
        raw_cookie = self.headers.get("Cookie") or ""
        for part in raw_cookie.split(";"):
            k, sep, v = part.strip().partition("=")
            if sep and k == SESSION_COOKIE_NAME:
                if _verify_session_cookie(v.strip()):
                    return True
        return False

    def _send_login_page(self):
        body = LOGIN_VIEW_TEMPLATE.replace("<!--INIT_NOTICE-->", "请先运行 --init-admin 初始化管理员账号" if not _get_auth_config() else "").encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "frame-ancestors 'none'")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self._safe_write(body)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html"):
            if not self._is_authenticated():
                self._send_login_page()
                return
            touch_activity(PANEL_IDLE_MIN)
            body = get_live_html_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "frame-ancestors 'none'")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self._safe_write(body)
            return

        if path == "/manifest.webmanifest":
            m_bytes = json.dumps({
                "name": "G4 工作台",
                "short_name": "G4 工作台",
                "start_url": "/",
                "display": "standalone",
                "background_color": "#0B111C",
                "theme_color": "#0B111C",
                "icons": [
                    {"src": "/icons/g4-192.png", "sizes": "192x192", "type": "image/png"},
                    {"src": "/icons/g4-512.png", "sizes": "512x512", "type": "image/png"},
                    {"src": "/apple-touch-icon.png?v=g5", "sizes": "180x180", "type": "image/png"},
                ],
            }, ensure_ascii=False).encode("utf-8")
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/manifest+json; charset=utf-8")
                self.send_header("Content-Length", str(len(m_bytes)))
                self.send_header("Cache-Control", "public, max-age=3600")
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                return
            self._safe_write(m_bytes)
            return

        if path in ("/g4-icon.svg", "/favicon.ico", "/favicon.png", "/apple-touch-icon.png", "/apple-touch-icon-precomposed.png", "/icons/g4-192.png", "/icons/g4-512.png"):
            icon_map = {
                "/g4-icon.svg": ("g4-icon.svg", "image/svg+xml"),
                "/favicon.ico": ("favicon.ico", "image/x-icon"),
                "/favicon.png": ("g4-icon-64.png", "image/png"),
                "/apple-touch-icon.png": ("g4-icon-180.png", "image/png"),
                "/apple-touch-icon-precomposed.png": ("g4-icon-180.png", "image/png"),
                "/icons/g4-192.png": ("g4-icon-192.png", "image/png"),
                "/icons/g4-512.png": ("g4-icon-512.png", "image/png"),
            }
            fn, ctype = icon_map[path]
            fp = os.path.join(BASE_DIR, "ui", "icons", fn)
            if os.path.isfile(fp):
                with open(fp, "rb") as fh:
                    data = fh.read()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "public, max-age=86400")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self._safe_write(data)
                return

        if not self._is_authenticated():
            if path.startswith("/api/"):
                self._send_json({"ok": False, "auth_required": True, "message": "未登录或会话已过期"}, 401)
            else:
                self.send_error(401, "Unauthorized")
            return

        if path == "/api/job":
            qs = urllib.parse.parse_qs(parsed.query)
            full = qs.get("full", ["0"])[0] == "1"
            try:
                limit_done = int(qs.get("limit", ["0" if full else "80"])[0])
            except ValueError:
                limit_done = 80
            self._send_json(JOB.get_state(limit_done=limit_done, slim=not full))
            return

        if path == "/api/task_detail":
            qs = urllib.parse.parse_qs(parsed.query)
            try:
                jid = int(qs.get("jid", ["0"])[0])
            except ValueError:
                self._send_json({"ok": False, "message": "无效任务编号"}, 400)
                return
            res = JOB.get_task_detail(jid)
            self._send_json(res, 200 if res.get("ok") else 404)
            return

        if path == "/api/metrics":
            self._send_json(read_g4_hw_metrics())
            return

        if path == "/api/status":
            qs = urllib.parse.parse_qs(parsed.query)
            force = qs.get("force", ["0"])[0] == "1"
            self._send_json(get_colab_status(force=force))
            return

        if path == "/api/colab_accounts":
            qs = urllib.parse.parse_qs(parsed.query)
            force = qs.get("force", ["0"])[0] == "1"
            self._send_json(colab_accounts.list_accounts_with_status(force=force, drive_sync_enabled=is_drive_sync_enabled()))
            return

        if path == "/api/outputs":
            self._send_json(list_outputs())
            return

        if path == "/api/drive_sync":
            self._send_json(get_drive_sync_overview())
            return

        if path == "/api/chat":
            self._send_json(load_chat_history())
            return

        if path == "/api/settings":
            self._send_json({
                "ok": True,
                "settings": load_studio_settings(),
                "defaults": DEFAULT_STUDIO_SETTINGS,
                "ui_meta": {
                    "image_label": MODEL_LABELS["v5_int8"],
                    "video_label": MODEL_LABELS["video"],
                    "llm_label": MODEL_LABELS["llm"],
                    "comfy_url": os.environ.get("COLABBB_COMFY_URL", ""),
                    "drive_sync_enabled": is_drive_sync_enabled(),
                    "channel_sync_enabled": is_channel_sync_enabled(),
                },
            })
            return

        if path.startswith("/thumbs/"):
            rel = urllib.parse.unquote(path[len("/thumbs/"):]).lstrip("/")
            parts = rel.split("/", 1)
            if len(parts) != 2 or parts[0] not in ("image", "upload_image"):
                self.send_error(404, "Thumb not found")
                return
            mod_name, fname = parts[0], os.path.basename(parts[1])
            src_path = os.path.realpath(os.path.join(OUT_ROOT, mod_name, fname))
            if os.path.commonpath([os.path.realpath(OUT_ROOT), src_path]) != os.path.realpath(OUT_ROOT) or not os.path.isfile(src_path):
                self.send_error(404, "Source not found")
                return
            thumb_path = ensure_webp_thumb(mod_name, fname, sync=True)
            serve_path = thumb_path if (thumb_path and os.path.isfile(thumb_path)) else src_path
            ctype = "image/webp" if serve_path == thumb_path else (mimetypes.guess_type(serve_path)[0] or "application/octet-stream")
            st = os.stat(serve_path)
            etag = f'W/"{int(st.st_mtime)}-{st.st_size}"'
            if self.headers.get("If-None-Match") == etag:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.send_header("Cache-Control", "public, max-age=31536000, immutable")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("ETag", etag)
            self.send_header("Cache-Control", "public, max-age=31536000, immutable")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(st.st_size))
            self.end_headers()
            if self.command != "HEAD":
                with open(serve_path, "rb") as fh:
                    while True:
                        chunk = fh.read(65536)
                        if not chunk:
                            break
                        if not self._safe_write(chunk):
                            break
            return

        if path.startswith("/files/"):
            rel = urllib.parse.unquote(path[len("/files/"):]).lstrip("/")
            full_path = os.path.realpath(os.path.join(OUT_ROOT, rel))
            if os.path.commonpath([os.path.realpath(OUT_ROOT), full_path]) != os.path.realpath(OUT_ROOT) or not os.path.isfile(full_path):
                self.send_error(404, "File not found")
                return
            ctype, _ = mimetypes.guess_type(full_path)
            ctype = ctype or "application/octet-stream"
            st = os.stat(full_path)
            fsize = st.st_size
            etag = f'W/"{int(st.st_mtime)}-{fsize}"'
            range_hdr = (self.headers.get("Range") or "").strip()
            if not range_hdr and self.headers.get("If-None-Match") == etag:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.send_header("Cache-Control", "private, max-age=86400")
                self.end_headers()
                return
            start_pos, end_pos, is_partial = 0, fsize - 1, False
            if range_hdr.startswith("bytes="):
                spec = range_hdr[len("bytes="):].split(",", 1)[0].strip()
                if "-" in spec:
                    s_part, e_part = spec.split("-", 1)
                    try:
                        if s_part == "" and e_part:
                            suffix = int(e_part)
                            if suffix <= 0 or fsize == 0:
                                raise ValueError("invalid suffix")
                            start_pos = max(0, fsize - suffix)
                            end_pos = fsize - 1
                        else:
                            start_pos = int(s_part)
                            end_pos = int(e_part) if e_part else (fsize - 1)
                        if start_pos < 0 or start_pos >= fsize or end_pos < start_pos:
                            self.send_response(416)
                            self.send_header("Content-Range", f"bytes */{fsize}")
                            self.send_header("Content-Length", "0")
                            self.end_headers()
                            return
                        end_pos = min(end_pos, fsize - 1)
                        is_partial = True
                    except ValueError:
                        is_partial = False
            send_len = (end_pos - start_pos + 1) if fsize > 0 else 0
            self.send_response(206 if is_partial else 200)
            self.send_header("Content-Type", ctype)
            self.send_header("ETag", etag)
            self.send_header("Cache-Control", "private, max-age=86400")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'none'; sandbox; frame-ancestors 'none'")
            if ctype not in _SAFE_INLINE_MEDIA_MIMES:
                safe_dl = os.path.basename(full_path).replace('"', "_")
                self.send_header("Content-Disposition", f'attachment; filename="{safe_dl}"')
            self.send_header("Accept-Ranges", "bytes")
            if is_partial:
                self.send_header("Content-Range", f"bytes {start_pos}-{end_pos}/{fsize}")
            self.send_header("Content-Length", str(send_len))
            self.end_headers()
            if self.command != "HEAD" and send_len > 0:
                with open(full_path, "rb") as fh:
                    fh.seek(start_pos)
                    rem = send_len
                    while rem > 0:
                        chunk = fh.read(min(65536, rem))
                        if not chunk:
                            break
                        if not self._safe_write(chunk):
                            break
                        rem -= len(chunk)
            return

        self.send_error(404, "Not found")

    def do_POST(self):
        try:
            self._do_POST()
        except (ValueError, TypeError, AttributeError):
            self._send_json({"ok": False, "message": "请求格式或参数无效"}, 400)

    def _do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if not self._check_csrf_origin():
            self._send_json({"ok": False, "message": "跨源请求已被拒绝"}, 403)
            return

        if parsed.path == "/api/login":
            ip_key = self._client_real_ip()
            if not _check_login_rate_limit(ip_key):
                self._send_json({"ok": False, "message": "尝试次数过多，请 5 分钟后再试"}, 429)
                return
            data = self._read_json_body(max_bytes=16384, required=True)
            if data is None:
                return
            uname = str(data.get("username") or "")
            upass = str(data.get("password") or "")
            if not _verify_login_credentials(uname, upass):
                _record_login_failure(ip_key, False)
                self._send_json({"ok": False, "message": "账号或密码不正确"}, 401)
                return
            _record_login_failure(ip_key, True)
            tok = _make_session_cookie(uname.strip())
            is_https = (self.headers.get("X-Forwarded-Proto") or "").lower() == "https"
            cookie_val = f"{SESSION_COOKIE_NAME}={tok}; Path=/; Max-Age={SESSION_TTL_SEC}; HttpOnly; SameSite=Lax"
            if is_https:
                cookie_val += "; Secure"
            touch_if_running(PANEL_IDLE_MIN)
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Set-Cookie", cookie_val)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self._safe_write(body)
            return

        if parsed.path == "/api/logout":
            if self._is_authenticated():
                _bump_session_version()
            is_https = (self.headers.get("X-Forwarded-Proto") or "").lower() == "https"
            cookie_val = f"{SESSION_COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"
            if is_https:
                cookie_val += "; Secure"
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Set-Cookie", cookie_val)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self._safe_write(body)
            return

        if not self._is_authenticated():
            self._send_json({"ok": False, "auth_required": True, "message": "未登录或会话已过期"}, 401)
            return

        if parsed.path == "/api/colab_accounts/switch":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 16384:
                self._send_json({"ok": False, "message": "请求参数无效"}, 400)
                return
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            data = json.loads(raw or "{}")
            target_em = str(data.get("email") or "").strip()
            stop_cur = bool(data.get("stop_current", False))
            st_job = JOB.get_state(limit_done=1, slim=True)
            if st_job.get("running"):
                self._send_json({"ok": False, "message": "当前有正在运行或排队的任务，请等待完成或先停止后再切换账号"}, 400)
                return
            ok, msg, extra = colab_accounts.switch_active_account(
                target_em,
                stop_current=stop_cur,
                colab_ai_bin=COLAB_AI_BIN,
            )
            with _STATUS_LOCK:
                _STATUS_CACHE["ts"] = 0.0
                _STATUS_CACHE["data"] = None
            if not ok:
                code_http = 409 if extra.get("need_stop_confirm") else 400
                self._send_json({"ok": False, "message": msg, **extra}, code_http)
                return
            st_now = get_colab_status(force=True)
            if st_now.get("active") is True:
                touch_activity(PANEL_IDLE_MIN)
            accts_now = colab_accounts.list_accounts_with_status(force=True, drive_sync_enabled=is_drive_sync_enabled())
            self._send_json({
                "ok": True,
                "message": msg,
                "status": st_now,
                "accounts_state": accts_now,
            })
            return

        if parsed.path == "/api/colab_accounts/oauth_start":
            res_oauth = colab_accounts.begin_oauth_login()
            self._send_json(res_oauth)
            return

        if parsed.path == "/api/colab_accounts/oauth_complete":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 16384:
                self._send_json({"ok": False, "message": "请求无效"}, 400)
                return
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            data = json.loads(raw or "{}")
            auth_code = str(data.get("code") or "").strip()
            ok, msg, extra = colab_accounts.complete_oauth_login(auth_code)
            if not ok:
                self._send_json({"ok": False, "message": msg}, 400)
                return
            with _STATUS_LOCK:
                _STATUS_CACHE["ts"] = 0.0
                _STATUS_CACHE["data"] = None
            accts_now = colab_accounts.list_accounts_with_status(force=True, drive_sync_enabled=is_drive_sync_enabled())
            self._send_json({
                "ok": True,
                "message": msg,
                "accounts_state": accts_now,
                **extra,
            })
            return

        if parsed.path == "/api/drive_sync":
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0 or length > 16384:
                self._send_json({"ok": False, "message": "请求无效"}, 400)
                return
            raw = self.rfile.read(length).decode("utf-8", errors="ignore") if length > 0 else "{}"
            data = json.loads(raw or "{}")
            mode = str(data.get("mode") or "drive").strip().lower()
            ok_sync, msg_sync, _ = trigger_panel_drive_sync(mode)
            overview = get_drive_sync_overview()
            overview["ok"] = ok_sync
            overview["message"] = msg_sync
            self._send_json(overview, 200 if ok_sync else 409)
            return

        if parsed.path == "/api/touch":
            data = self._read_json_body(max_bytes=16384, required=False)
            if data is None:
                return
            touch_if_running(_resolve_source_idle_minutes(data.get("source")))
            self._send_json({"ok": True})
            return

        if parsed.path == "/api/cancel":
            data = self._read_json_body(max_bytes=16384, required=False)
            if data is None:
                return
            touch_if_running(_resolve_source_idle_minutes(data.get("source")))
            ok, msg = JOB.cancel_job()
            self._send_json({"ok": ok, "message": msg})
            return

        if parsed.path == "/api/task_action":
            data = self._read_json_body(max_bytes=65536, required=True)
            if data is None:
                return
            act = (data.get("action") or "").strip()
            src_idle = _resolve_source_idle_minutes(data.get("source"))
            if act == "clear_all":
                touch_if_running(src_idle)
                ok, msg = JOB.clear_tasks()
                self._send_json({"ok": ok, "message": msg}, 200 if ok else 400)
                return
            if act == "delete":
                jid = data.get("jid")
                if type(jid) is not int:
                    self._send_json({"ok": False, "message": "任务编号必须为整数"}, 400)
                    return
                touch_if_running(src_idle)
                ok, msg = JOB.delete_task(jid)
                self._send_json({"ok": ok, "message": msg})
                return
            jid = int(data.get("jid") or 0)
            if act == "stop":
                touch_if_running(src_idle)
                ok, msg = JOB.stop_task(jid)
                self._send_json({"ok": ok, "message": msg})
                return
            if act == "resume":
                ok, msg = JOB.resume_task(jid, idle_minutes=src_idle)
                self._send_json({"ok": ok, "message": msg})
                return
            self._send_json({"ok": False, "message": "未知任务操作"}, 400)
            return

        if parsed.path == "/api/chat/clear":
            data = self._read_json_body(max_bytes=16384, required=False)
            if data is None:
                return
            touch_if_running(_resolve_source_idle_minutes(data.get("source")))
            save_chat_history([])
            self._send_json({"ok": True})
            return

        if parsed.path == "/api/upload":
            qs = urllib.parse.parse_qs(parsed.query)
            raw_name = (qs.get("filename", ["upload.png"])[0] or "upload.png").strip()
            safe_base = re.sub(r"[^A-Za-z0-9._\-\u4e00-\u9fa5]+", "_", os.path.basename(raw_name)).lstrip(".")
            if not safe_base:
                safe_base = "upload.png"
            ext = os.path.splitext(safe_base)[1].lower()
            if ext not in _ALLOWED_UPLOAD_EXTS:
                self._send_json({"ok": False, "message": f"不支持的文件类型: {ext or '未知'}（仅支持图片与视频）"}, 400)
                return
            mod_dir_name = "upload_image"
            target_dir = os.path.abspath(os.path.join(OUT_ROOT, mod_dir_name))
            os.makedirs(target_dir, exist_ok=True)
            fname = f"up_{time.time_ns()}_{os.urandom(4).hex()}_{safe_base}"
            full_path = os.path.join(target_dir, fname)
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 200 * 1024 * 1024:
                self._send_json({"ok": False, "message": "文件大小无效或超过 200MB 限制"}, 400)
                return
            try:
                self.request.settimeout(300.0)
            except OSError:
                pass
            remaining = length
            with open(full_path, "wb") as fh:
                while remaining > 0:
                    chunk = self.rfile.read(min(65536, remaining))
                    if not chunk:
                        break
                    fh.write(chunk)
                    remaining -= len(chunk)
            if remaining or not _validate_uploaded_media(full_path, ext):
                try:
                    os.remove(full_path)
                except OSError:
                    pass
                self._send_json({"ok": False, "message": "上传内容不完整或非有效媒体文件，未保留"}, 400)
                return
            touch_activity(PANEL_IDLE_MIN)
            self._send_json({
                "ok": True,
                "module": mod_dir_name,
                "name": fname,
                "path": full_path,
                "url": f"/files/{mod_dir_name}/{urllib.parse.quote(fname)}",
            })
            return

        if parsed.path == "/api/delete":
            data = self._read_json_body(max_bytes=2 * 1024 * 1024, required=True)
            if data is None:
                return
            mod = data.get("module", "").strip()
            if mod not in ("image", "video", "upload_image", "llm"):
                self._send_json({"ok": False, "message": "非法模块分类"}, 400)
                return
            mod_dir = os.path.abspath(os.path.join(OUT_ROOT, mod))
            if data.get("all"):
                removed = 0
                dirs_to_scan = [(mod, mod_dir)]
                if mod == "video":
                    dirs_to_scan.append(("upload_image", os.path.abspath(os.path.join(OUT_ROOT, "upload_image"))))
                for cur_mod, cur_dir in dirs_to_scan:
                    if os.path.isdir(cur_dir):
                        for fn in os.listdir(cur_dir):
                            if mod == "video" and cur_mod == "upload_image" and not fn.lower().endswith((".mp4", ".webm", ".mov")):
                                continue
                            if mod == "upload_image" and fn.lower().endswith((".mp4", ".webm", ".mov")):
                                continue
                            fp = os.path.abspath(os.path.join(cur_dir, fn))
                            if fp.startswith(cur_dir + os.sep) and os.path.isfile(fp) and not fn.startswith("."):
                                try:
                                    if cur_mod == "image":
                                        remove_synced_image(fp)
                                    else:
                                        os.remove(fp)
                                    th = os.path.join(THUMB_ROOT, cur_mod, fn + ".webp")
                                    if os.path.exists(th):
                                        os.unlink(th)
                                    removed += 1
                                except Exception:
                                    pass
                self._send_json({"ok": True, "removed": removed})
                return

            if isinstance(data.get("names"), list):
                touch_if_running(PANEL_IDLE_MIN)
                removed = 0
                seen_names = set()
                for raw_n in data["names"]:
                    if not isinstance(raw_n, str):
                        continue
                    bn = os.path.basename(raw_n.strip())
                    if not bn or bn.startswith(".") or bn in seen_names:
                        continue
                    seen_names.add(bn)
                    t_path = os.path.abspath(os.path.join(mod_dir, bn))
                    act_mod = mod
                    if (not os.path.isfile(t_path)) and mod == "video":
                        alt_dir = os.path.abspath(os.path.join(OUT_ROOT, "upload_image"))
                        alt_t = os.path.abspath(os.path.join(alt_dir, bn))
                        if alt_t.startswith(alt_dir + os.sep) and os.path.isfile(alt_t):
                            t_path = alt_t
                            act_mod = "upload_image"
                    if not t_path.startswith(os.path.abspath(OUT_ROOT) + os.sep) or not os.path.isfile(t_path):
                        continue
                    try:
                        if act_mod == "image":
                            remove_synced_image(t_path)
                        else:
                            os.remove(t_path)
                        th = os.path.join(THUMB_ROOT, act_mod, bn + ".webp")
                        if os.path.exists(th):
                            os.unlink(th)
                        removed += 1
                    except Exception:
                        pass
                self._send_json({"ok": True, "removed": removed})
                return

            name = os.path.basename(data.get("name", "").strip())
            if not name or name.startswith("."):
                self._send_json({"ok": False, "message": "无效文件名"}, 400)
                return
            target = os.path.abspath(os.path.join(mod_dir, name))
            actual_mod = mod
            if (not os.path.isfile(target)) and mod == "video":
                alt_dir = os.path.abspath(os.path.join(OUT_ROOT, "upload_image"))
                alt_target = os.path.abspath(os.path.join(alt_dir, name))
                if alt_target.startswith(alt_dir + os.sep) and os.path.isfile(alt_target):
                    target = alt_target
                    actual_mod = "upload_image"
            if not os.path.isfile(target):
                self._send_json({"ok": False, "message": "文件不存在"}, 404)
                return
            try:
                if actual_mod == "image":
                    remove_synced_image(target)
                else:
                    os.remove(target)
                th = os.path.join(THUMB_ROOT, actual_mod, name + ".webp")
                if os.path.exists(th):
                    os.unlink(th)
                self._send_json({"ok": True})
            except Exception as e:
                self._send_json({"ok": False, "message": str(e)}, 500)
            return

        if parsed.path == "/api/download_batch":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 2 * 1024 * 1024:
                self._send_json({"ok": False, "message": "无效的批量下载请求"}, 400)
                return
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            ctype = (self.headers.get("Content-Type") or "").lower()
            if "application/x-www-form-urlencoded" in ctype:
                form = urllib.parse.parse_qs(raw)
                data = json.loads((form.get("payload") or ["{}"])[0])
            else:
                data = json.loads(raw or "{}")
            mod = (data.get("module") or "").strip()
            names = data.get("names")
            if mod not in ("image", "video", "upload_image", "llm") or not isinstance(names, list) or not names:
                self._send_json({"ok": False, "message": "请选择要下载的作品"}, 400)
                return
            touch_if_running(PANEL_IDLE_MIN)
            mod_dir = os.path.abspath(os.path.join(OUT_ROOT, mod))
            valid_files = []
            seen_n = set()
            for raw_n in names[:500]:
                if not isinstance(raw_n, str):
                    continue
                bn = os.path.basename(raw_n.strip())
                if not bn or bn.startswith(".") or bn in seen_n:
                    continue
                seen_n.add(bn)
                fp = os.path.abspath(os.path.join(mod_dir, bn))
                if (not os.path.isfile(fp)) and mod == "video":
                    alt_dir = os.path.abspath(os.path.join(OUT_ROOT, "upload_image"))
                    alt_fp = os.path.abspath(os.path.join(alt_dir, bn))
                    if alt_fp.startswith(alt_dir + os.sep) and os.path.isfile(alt_fp):
                        fp = alt_fp
                if fp.startswith(os.path.abspath(OUT_ROOT) + os.sep) and os.path.isfile(fp):
                    valid_files.append((bn, fp))
            if not valid_files:
                self._send_json({"ok": False, "message": "所选文件不存在"}, 404)
                return
            zip_name = f"g4_{mod}_{len(valid_files)}items_{time.strftime('%Y%m%d_%H%M%S')}.zip"
            with tempfile.SpooledTemporaryFile(max_size=32 * 1024 * 1024, dir="/tmp") as ztmp:
                with zipfile.ZipFile(ztmp, mode="w", compression=zipfile.ZIP_STORED) as zf:
                    for bn, fp in valid_files:
                        zf.write(fp, arcname=bn)
                size = ztmp.tell()
                ztmp.seek(0)
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", f'attachment; filename="{zip_name}"')
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(size))
                self.end_headers()
                while True:
                    chunk = ztmp.read(65536)
                    if not chunk:
                        break
                    if not self._safe_write(chunk):
                        break
            return

        if parsed.path == "/api/run":
            data = self._read_json_body(max_bytes=2 * 1024 * 1024, required=True)
            if data is None:
                return
            mod = data.get("module", "image").strip()
            text = data.get("text", "").strip()
            req_idle_min = _resolve_source_idle_minutes(data.get("source"))

            if mod == "stop":
                PANEL_RANDOM.cancel_all()
                ok, msg = JOB.start_job([COLAB_AI_BIN, "stop"], "stop")
                self._send_json({"ok": ok, "message": msg})
                return

            if mod == "random":
                with JOB.cond:
                    stop_pending = any(
                        JOB._is_stop_job(it["module"], it["cmd_list"])
                        for it in list(JOB.active_procs.values()) + [it for q in JOB.queues.values() for it in q]
                    )
                if stop_pending:
                    self._send_json({"ok": False, "message": "关机正在进行，请等待完成后再提交"}, 400)
                    return
                touch_if_running(req_idle_min)
                try:
                    req_cnt = int(data.get("count", 1))
                except (TypeError, ValueError):
                    req_cnt = 1
                if isinstance(data.get("count"), bool) or not 1 <= req_cnt <= 10:
                    self._send_json({"ok": False, "message": "随机生成张数需在 1～10 张之间"}, 400)
                    return
                step_tier = str(data.get("step_tier") or "tier2").strip().lower()
                if step_tier not in ("tier1", "tier2", "tier3"):
                    step_tier = "tier2"
                ahead, accepted_cnt = PANEL_RANDOM.enqueue(count=req_cnt, step_tier=step_tier, idle_minutes=req_idle_min)
                msg = (
                    f"已加入 {accepted_cnt} 张随机生图至队列（前方还有 {ahead} 张）"
                    if ahead > 0
                    else f"已启动 {accepted_cnt} 张随机生图（两步构思 + 3K 渲染，仅面板内生成）"
                )
                self._send_json({"ok": True, "message": msg, "ahead": ahead, "count": accepted_cnt})
                return

            if mod.lower() in ("edit", "krea2edit", "legacyedit", "qwen", "qwen2.1"):
                self._send_json({"ok": False, "message": "改图暂未启用"}, 400)
                return
            image_model = data.get("image_model", "v5_int8")
            if mod in ("start", "warmup"):
                if mod == "start":
                    cmd_list, label = [COLAB_AI_BIN, "start"], ""
                else:
                    try:
                        if "targets" in data:
                            targets = _validate_warmup_targets(data["targets"])
                        elif text.startswith("colab-ai warmup"):
                            targets = _parse_warmup_args(shlex.split(text)[2:])
                        else:
                            if image_model not in IMAGE_KEYS: raise ValueError("未知预热图像模型")
                            targets = [image_model, "llm"]
                    except ValueError as exc:
                        self._send_json({"ok": False, "message": str(exc)}, 400)
                        return
                    cmd_list = [COLAB_AI_BIN, "warmup", "--targets"] + targets
                    label = "预热：" + " / ".join(MODEL_LABELS.get(t, "提示词 LLM") for t in targets)
                ok, msg = JOB.start_job(cmd_list, mod, meta_label=label, idle_minutes=req_idle_min)
                self._send_json({"ok": ok, "message": msg})
                return

            if text.startswith("colab-ai "):
                cmd_list = [COLAB_AI_BIN] + shlex.split(text)[1:]
                raw_sub = cmd_list[1] if len(cmd_list) > 1 else ""
                if not self._is_internal_request() and raw_sub not in ("start", "stop", "warmup", "status"):
                    self._send_json({"ok": False, "message": f"不允许通过此接口直接执行子命令: {raw_sub}"}, 403)
                    return
                targets = []
                if raw_sub in ("start", "stop", "warmup", "status"):
                    mod = raw_sub
                    if mod == "warmup":
                        try:
                            targets = _validate_warmup_targets(data["targets"]) if "targets" in data else _parse_warmup_args(cmd_list[2:])
                        except ValueError as exc:
                            self._send_json({"ok": False, "message": str(exc)}, 400)
                            return
                        cmd_list = [COLAB_AI_BIN, "warmup", "--targets"] + targets
                    else:
                        cmd_list = [COLAB_AI_BIN, mod]
                label = "预热：" + " / ".join(MODEL_LABELS.get(t, "提示词 LLM") for t in targets) if mod == "warmup" else ""
                ok, msg = JOB.start_job(cmd_list, mod, meta_label=label, idle_minutes=req_idle_min)
                self._send_json({"ok": ok, "message": msg})
                return

            if mod == "image":
                img_mod = data.get("model", "v5_int8")
                if img_mod not in IMAGE_KEYS:
                    self._send_json({"ok": False, "message": "未知生图模型，当前仅保留 V5 INT8"}, 400)
                    return
                clean_text = re.sub(r"^[\s\-#*]+", "", text).strip() or text.strip()
                os.makedirs(STATE_DIR, exist_ok=True)
                pfile = os.path.join(STATE_DIR, f"img_req_prompt_{time.time_ns()}_{os.urandom(4).hex()}.txt")
                with open(pfile, "w", encoding="utf-8") as fh:
                    fh.write(clean_text)

                cur_cfg = load_studio_settings()
                wf_cfg = cur_cfg["image_workflow"]
                req_w = int(data.get("width", 2048))
                req_h = int(data.get("height", 3072))
                req_steps = int(data.get("steps", 0))
                res_mode = str(data.get("res_mode") or "").strip().lower()
                if res_mode not in ("portrait", "landscape", "square"):
                    res_mode = "portrait" if req_h > req_w else ("landscape" if req_w > req_h else "square")
                step_tier = str(data.get("step_tier") or "").strip().lower()
                if step_tier not in ("tier1", "tier2", "tier3"):
                    if req_steps <= 0:
                        step_tier = "tier2"
                    elif req_steps <= 13:
                        step_tier = "tier1"
                    elif req_steps <= 18:
                        step_tier = "tier2"
                    else:
                        step_tier = "tier3"
                r_info = wf_cfg["resolutions"][res_mode]
                t_info = wf_cfg["step_tiers"][step_tier]
                actual_w = int(r_info["s2_w"])
                actual_h = int(r_info["s2_h"])
                use_hires = bool(wf_cfg.get("enable_hires", True)) and int(t_info["ref"]) > 0
                st_val = (int(t_info["s1"]) + int(t_info["ref"])) if use_hires else int(t_info["s1"])

                cmd_list = [
                    COLAB_AI_BIN, "image",
                    "--prompt-file", pfile,
                    "--model", img_mod,
                    "--width", str(actual_w),
                    "--height", str(actual_h),
                    "--steps", str(st_val),
                    "--res-mode", res_mode,
                    "--step-tier", step_tier,
                ]
                ref_img = _resolve_safe_out_file(data.get("ref_image") or "")
                if ref_img:
                    cmd_list.extend(["--ref-image", ref_img])
                res_lbl = f"{actual_w}×{actual_h}"
                stage_tag = "3K生图" if use_hires else "直出生图"
                ok, msg = JOB.start_job(cmd_list, "image", meta_label=f"{stage_tag} [{MODEL_LABELS[img_mod].replace(' ', '-')}] ({res_lbl}·{st_val}步)", steps=max(1, st_val), prompt_preview=clean_text, idle_minutes=req_idle_min)
                self._send_json({"ok": ok, "message": msg})
                return

            if mod == "video":
                clean_text = re.sub(r"^[\s\-#*]+", "", text).strip() or text.strip()
                os.makedirs(STATE_DIR, exist_ok=True)
                pfile = os.path.join(STATE_DIR, f"vid_req_prompt_{time.time_ns()}_{os.urandom(4).hex()}.txt")
                with open(pfile, "w", encoding="utf-8") as fh:
                    fh.write(clean_text)
                vw = int(data.get("width", 832))
                vh = int(data.get("height", 1248))
                len_val = int(data.get("length", 192))
                st_val = int(data.get("steps", 8))
                q_val = str(data.get("quality") or "std").strip().lower()
                if q_val not in ("std", "hd", "uhd2k"):
                    q_val = "std"
                cmd_list = [
                    COLAB_AI_BIN, "video",
                    "--prompt-file", pfile,
                    "--width", str(vw),
                    "--height", str(vh),
                    "--length", str(len_val),
                    "--steps", str(st_val),
                    "--quality", q_val,
                ]
                ref_img = _resolve_safe_out_file(data.get("ref_image") or "")
                if ref_img:
                    cmd_list.extend(["--ref-image", ref_img])
                import remote_video as _rv_mod
                _, _, out_w, out_h, q_val = _rv_mod.resolve_h3_stage_dims(vw, vh, q_val, ref_img)
                res_lbl = f"{out_w}×{out_h}"
                sec_lbl = round(len_val / 24.0, 1)
                tot_st = st_val + (3 if q_val == "uhd2k" else 0)
                q_tag = {"std": "标准", "hd": "超清1088p", "uhd2k": "2K精修"}[q_val]
                ok, msg = JOB.start_job(cmd_list, "video", meta_label=f"24fps 声画视频 {q_tag} ({sec_lbl}s·{res_lbl})", steps=tot_st, prompt_preview=clean_text, idle_minutes=req_idle_min)
                self._send_json({"ok": ok, "message": msg})
                return

            if mod == "llm":
                m_key = "qwen27b"
                m_names = {
                    "qwen27b": "Qwen 27B GGUF  Q4_K_M",
                    "qwen30b": "Qwen 27B GGUF",
                }
                ephemeral = bool(data.get("ephemeral", False))
                if ephemeral:
                    req_msgs = [{"role": "user", "content": text}]
                else:
                    hist = load_chat_history()
                    req_msgs = [{"role": m["role"], "content": m["content"]} for m in hist if m.get("role") in ("user", "assistant")]
                    req_msgs.append({"role": "user", "content": text})
                os.makedirs(STATE_DIR, exist_ok=True)
                real_state = os.path.realpath(STATE_DIR)
                req_file = os.path.join(STATE_DIR, f"llm_req_{time.time_ns()}_{os.urandom(4).hex()}.json")
                with open(req_file, "w", encoding="utf-8") as fh:
                    json.dump(req_msgs, fh, ensure_ascii=False)
                cmd_list = [COLAB_AI_BIN, "llm", "--model", m_key, "--messages-file", req_file]
                sys_file_in = data.get("system_file")
                custom_sys = data.get("system")
                is_ipc = self._is_internal_request()
                _llm_tmp_prefixes = ("llm_", "auto_llm_", "tg_gen_llm_")
                _llm_tmp_exts = (".txt", ".json", ".md")
                if is_ipc and isinstance(sys_file_in, str) and sys_file_in.strip():
                    real_sf = os.path.realpath(sys_file_in.strip())
                    sf_name = os.path.basename(real_sf)
                    if (
                        os.path.dirname(real_sf) == real_state
                        and sf_name.startswith(_llm_tmp_prefixes)
                        and sf_name.endswith(_llm_tmp_exts)
                        and os.path.isfile(real_sf)
                    ):
                        cmd_list.extend(["--system-file", real_sf])
                elif isinstance(custom_sys, str) and custom_sys.strip():
                    sys_file = os.path.join(STATE_DIR, f"llm_sys_{time.time_ns()}_{os.urandom(4).hex()}.txt")
                    with open(sys_file, "w", encoding="utf-8") as fh:
                        fh.write(custom_sys.strip())
                    cmd_list.extend(["--system-file", sys_file])
                out_file_in = data.get("out_file")
                if is_ipc and isinstance(out_file_in, str) and out_file_in.strip():
                    real_of = os.path.realpath(out_file_in.strip())
                    of_name = os.path.basename(real_of)
                    if (
                        os.path.dirname(real_of) == real_state
                        and of_name.startswith(_llm_tmp_prefixes)
                        and of_name.endswith(_llm_tmp_exts)
                    ):
                        cmd_list.extend(["--out-file", real_of])
                if bool(data.get("ephemeral_draft", False)):
                    cmd_list.append("--ephemeral-draft")
                if "temperature" in data and not isinstance(data["temperature"], bool) and isinstance(data["temperature"], (int, float)):
                    cmd_list.extend(["--temperature", str(max(0.0, min(2.0, float(data["temperature"]))))])
                if "max_tokens" in data and not isinstance(data["max_tokens"], bool) and isinstance(data["max_tokens"], int):
                    cmd_list.extend(["--max-tokens", str(max(128, min(8192, int(data["max_tokens"]))))])
                lbl_prefix = "自动提示词" if ephemeral else "提示词"
                custom_label = data.get("meta_label")
                final_label = custom_label.strip() if isinstance(custom_label, str) and custom_label.strip() else f"{lbl_prefix} ({m_names[m_key]})"
                ok, msg = JOB.start_job(cmd_list, "llm", user_msg=(None if ephemeral else text), meta_label=final_label, prompt_preview=text, idle_minutes=req_idle_min)
                self._send_json({"ok": ok, "message": msg})
                return

            self._send_json({"ok": False, "message": f"未知模块: {mod}"}, 400)
            return

        self.send_error(404, "Not found")

def init_admin(username=None):
    import getpass
    username = (username or os.environ.get('COLABBB_ADMIN_USER') or input('管理员用户名: ')).strip()
    if not username or '|' in username or len(username) > 128:
        raise ValueError('用户名不能为空、过长或包含 |')
    password = os.environ.get('COLABBB_ADMIN_PASS')
    if password is None:
        password = getpass.getpass('管理员密码（至少12字符）: ')
        if password != getpass.getpass('再次输入密码: '):
            raise ValueError('两次密码不一致')
    if len(password) < 12:
        raise ValueError('密码至少12字符')
    salt = os.urandom(32)
    doc = {'username': username, 'salt_hex': salt.hex(), 'iterations': 600000,
           'pwd_hash_hex': hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 600000).hex(),
           'session_secret_hex': os.urandom(32).hex(), 'internal_ipc_token': os.urandom(32).hex()}
    os.makedirs(STATE_DIR, mode=0o700, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.auth-', dir=STATE_DIR)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            os.fchmod(fh.fileno(), 0o600)
            json.dump(doc, fh, ensure_ascii=False)
            fh.flush(); os.fsync(fh.fileno())
        os.replace(tmp, PANEL_AUTH_FILE)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    print('管理员已初始化：' + PANEL_AUTH_FILE)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--init-admin', action='store_true')
    parser.add_argument('--username')
    args = parser.parse_args()
    if args.init_admin:
        init_admin(args.username)
        return
    os.makedirs(STATE_DIR, mode=0o700, exist_ok=True)
    if not _get_auth_config():
        print('请先运行 --init-admin 初始化管理员账号；当前拒绝所有登录。', flush=True)
    srv = PanelHTTPServer((os.environ.get('COLABBB_PANEL_HOST', '0.0.0.0'), PORT), PanelHandler)
    print(f'[Colabbb] Listening on port {PORT}', flush=True)
    srv.serve_forever()

if __name__ == "__main__":
    main()
