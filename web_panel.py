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

class SettingsValidationError(ValueError):
    pass

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

def _llm_sys_path(state_dir=None) -> str:
    if state_dir is not None:
        return os.path.join(state_dir, "llm_default_system.txt")
    if STATE_DIR != "/opt/colabbb/state":
        return os.path.join(STATE_DIR, "llm_default_system.txt")
    return LLM_SYS_FILE

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

def _req_float(val, lo: float, hi: float, label: str) -> float:
    if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(float(val)):
        raise SettingsValidationError(f"{label}必须为有效数字")
    fv = float(val)
    if fv < lo or fv > hi:
        raise SettingsValidationError(f"{label}必须在 {lo} ~ {hi} 之间")
    return round(fv, 4)

def _req_int(val, lo: int, hi: int, label: str) -> int:
    if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(float(val)):
        raise SettingsValidationError(f"{label}必须为整数")
    iv = int(round(float(val)))
    if iv < lo or iv > hi:
        raise SettingsValidationError(f"{label}必须在 {lo} ~ {hi} 之间")
    return iv

def _req_dim16(val, label: str) -> int:
    if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(float(val)):
        raise SettingsValidationError(f"{label}必须为有效像素数值")
    fv = float(val)
    if fv < 256 or fv > 4096:
        raise SettingsValidationError(f"{label}必须在 256 ~ 4096 像素范围内")
    return _align16(fv)

def validate_and_normalize_settings(raw_data, base_cfg=None) -> dict:
    if not isinstance(raw_data, dict):
        raise SettingsValidationError("参数格式错误：顶层必须为 JSON 对象")
    for sec in ("llm", "image_prompt", "image_workflow"):
        if sec in raw_data and not isinstance(raw_data[sec], dict):
            raise SettingsValidationError(f"参数分区 {sec} 格式错误：必须为对象")
    merged = _deep_merge_settings(base_cfg if isinstance(base_cfg, dict) else DEFAULT_STUDIO_SETTINGS, raw_data)

    llm = merged["llm"]
    if not isinstance(llm.get("system_prompt"), str) or not llm["system_prompt"].strip():
        raise SettingsValidationError("提示词大师的系统提示词不能为空")
    llm["system_prompt"] = llm["system_prompt"].strip()
    if not isinstance(llm.get("multi_turn_hint"), str):
        raise SettingsValidationError("多轮修改附加提示必须为字符串")
    if not isinstance(llm.get("check_prefix"), str):
        raise SettingsValidationError("自检引导前缀必须为字符串")
    llm["temperature"] = _req_float(llm.get("temperature"), 0.0, 2.0, "LLM 温度 (Temperature)")
    llm["max_tokens"] = _req_int(llm.get("max_tokens"), 128, 8192, "LLM 最大生成长度 (Max Tokens)")
    llm["top_p"] = _req_float(llm.get("top_p"), 0.05, 1.0, "LLM Top-P")
    llm["top_k"] = _req_int(llm.get("top_k"), 1, 200, "LLM Top-K")
    llm["repeat_penalty"] = _req_float(llm.get("repeat_penalty"), 0.8, 2.0, "LLM 重复惩罚系数 (Repeat Penalty)")
    if not isinstance(llm.get("strip_terms"), list) or any(not isinstance(x, str) for x in llm["strip_terms"]):
        raise SettingsValidationError("后置自动剔除词条必须为字符串列表")
    llm["strip_terms"] = [
        x.replace("\r", "").replace("\n", "")
        for x in llm["strip_terms"]
        if x.replace("\r", "").replace("\n", "") != ""
    ]

    ip = merged["image_prompt"]
    for bk, blbl in (("enable_clean", "词条清洗开关"), ("enable_prefix", "前置词开关"), ("enable_suffix", "追加后缀开关")):
        if not isinstance(ip.get(bk), bool):
            raise SettingsValidationError(f"{blbl}必须为布尔值")
    for sk, slbl in (
        ("zh_prefix", "中文前置词"), ("en_prefix", "英文前置词"),
        ("zh_suffix", "中文追加后缀"), ("en_suffix", "英文追加后缀"),
        ("ref_face_prompt", "参考图面部引导词"),
    ):
        if not isinstance(ip.get(sk), str):
            raise SettingsValidationError(f"{slbl}必须为字符串")
    if not isinstance(ip.get("strip_regexes"), list) or any(not isinstance(x, str) for x in ip["strip_regexes"]):
        raise SettingsValidationError("旧尾巴剥离正则列表必须为字符串数组")
    norm_regexes = []
    for pat in ip["strip_regexes"]:
        s = pat.strip()
        if not s:
            continue
        try:
            re.compile(s)
        except re.error as exc:
            raise SettingsValidationError(f"旧尾巴剥离正则语法无效 ({s}): {exc}") from None
        norm_regexes.append(s)
    ip["strip_regexes"] = norm_regexes
    if not isinstance(ip.get("replace_pairs"), list):
        raise SettingsValidationError("词条替换映射列表必须为数组")
    norm_pairs = []
    for it in ip["replace_pairs"]:
        if not isinstance(it, dict) or not isinstance(it.get("from"), str) or not isinstance(it.get("to", ""), str):
            raise SettingsValidationError("词条替换规则格式无效，每项需包含字符串 from 与 to")
        if it["from"] != "":
            norm_pairs.append({"from": it["from"], "to": it.get("to", "")})
    ip["replace_pairs"] = norm_pairs
    ip["ref_scale_w"] = _req_dim16(ip.get("ref_scale_w"), "参考图缩放宽度")
    ip["ref_scale_h"] = _req_dim16(ip.get("ref_scale_h"), "参考图缩放高度")
    ip["ref_crop_w"] = _req_dim16(ip.get("ref_crop_w"), "参考图裁剪宽度")
    ip["ref_crop_h"] = _req_dim16(ip.get("ref_crop_h"), "参考图裁剪高度")
    ip["ref_crop_x"] = _req_int(ip.get("ref_crop_x"), 0, 4096, "参考图裁剪偏移 X")
    ip["ref_crop_y"] = _req_int(ip.get("ref_crop_y"), 0, 4096, "参考图裁剪偏移 Y")

    wf = merged["image_workflow"]
    if not isinstance(wf.get("enable_hires"), bool):
        raise SettingsValidationError("二阶段超清精绘开关必须为布尔值")
    wf["hires_denoise"] = _req_float(wf.get("hires_denoise"), 0.01, 1.0, "二阶段精绘幅度 (Denoise)")
    wf["cfg"] = _req_float(wf.get("cfg"), 0.1, 20.0, "CFG 引导系数")
    if wf.get("sampler_name") not in VALID_SAMPLERS:
        raise SettingsValidationError(f"不支持的采样器: {wf.get('sampler_name')}")
    if wf.get("scheduler") not in VALID_SCHEDULERS:
        raise SettingsValidationError(f"不支持的调度器: {wf.get('scheduler')}")
    if wf.get("upscale_method") not in VALID_UPSCALE_METHODS:
        raise SettingsValidationError(f"不支持的放大插值算法: {wf.get('upscale_method')}")

    if not isinstance(wf.get("resolutions"), dict):
        raise SettingsValidationError("画幅尺寸配置必须为对象")
    res_names = {"portrait": "竖图", "landscape": "横图", "square": "方图"}
    for rk, rlbl in res_names.items():
        rcur = wf["resolutions"].get(rk)
        if not isinstance(rcur, dict):
            raise SettingsValidationError(f"缺少{rlbl}尺寸配置")
        rcur["s1_w"] = _req_dim16(rcur.get("s1_w"), f"{rlbl}一阶段宽度")
        rcur["s1_h"] = _req_dim16(rcur.get("s1_h"), f"{rlbl}一阶段高度")
        rcur["s2_w"] = _req_dim16(rcur.get("s2_w"), f"{rlbl}二阶段目标宽度")
        rcur["s2_h"] = _req_dim16(rcur.get("s2_h"), f"{rlbl}二阶段目标高度")

    if not isinstance(wf.get("step_tiers"), dict):
        raise SettingsValidationError("步数档位配置必须为对象")
    tier_names = {"tier1": "第 1 档", "tier2": "第 2 档", "tier3": "第 3 档"}
    for tk, tlbl in tier_names.items():
        tcur = wf["step_tiers"].get(tk)
        if not isinstance(tcur, dict):
            raise SettingsValidationError(f"缺少{tlbl}步数配置")
        tcur["s1"] = _req_int(tcur.get("s1"), 1, 60, f"{tlbl}一阶段构图步数")
        tcur["ref"] = _req_int(tcur.get("ref"), 0, 40, f"{tlbl}二阶段精绘步数")

    return merged

def save_studio_settings(raw_data, state_dir=None) -> dict:
    with SETTINGS_LOCK:
        cur = load_studio_settings(state_dir=state_dir)
        normalized = validate_and_normalize_settings(raw_data, base_cfg=cur)
        spath = _settings_path(state_dir)
        lpath = _llm_sys_path(state_dir)
        _atomic_write_text(spath, json.dumps(normalized, ensure_ascii=False, indent=2) + "\n")
        _atomic_write_text(lpath, normalized["llm"]["system_prompt"].strip() + "\n")
        return normalized

def reset_studio_settings(section: str = "all", state_dir=None) -> dict:
    sec = (section or "all").strip()
    if sec not in ("all", "llm", "image_prompt", "image_workflow"):
        raise SettingsValidationError("无效的重置分区参数")
    with SETTINGS_LOCK:
        if sec == "all":
            target = copy.deepcopy(DEFAULT_STUDIO_SETTINGS)
        else:
            target = load_studio_settings(state_dir=state_dir)
            target[sec] = copy.deepcopy(DEFAULT_STUDIO_SETTINGS[sec])
        spath = _settings_path(state_dir)
        lpath = _llm_sys_path(state_dir)
        _atomic_write_text(spath, json.dumps(target, ensure_ascii=False, indent=2) + "\n")
        _atomic_write_text(lpath, target["llm"]["system_prompt"].strip() + "\n")
        return target

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

LOGIN_VIEW_TEMPLATE = '<!doctype html>\n<html lang="zh-CN">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n  <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'sha256-KLNzgjEDQkQ7X/QMaSvAIjv1o+9VUoretBVEl7irTQk=\'; style-src \'unsafe-inline\'; img-src \'self\' data:; connect-src \'self\'; form-action \'self\'; base-uri \'none\'">\n  <title>登录 · G4 工作台</title>\n  <link rel="icon" type="image/svg+xml" href="/g4-icon.svg?v=g5">\n  <link rel="icon" type="image/png" sizes="64x64" href="/favicon.png?v=g5">\n  <link rel="shortcut icon" href="/favicon.ico?v=g5">\n  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v=g5">\n  <style>\n    :root {\n      color-scheme: light dark;\n      --bg: #EEF1F6;\n      --surface: #FFFFFF;\n      --card-2: rgba(20, 28, 40, 0.048);\n      --hover: rgba(20, 28, 40, 0.078);\n      --press: rgba(20, 28, 40, 0.12);\n      --fg: #161A22;\n      --fg-secondary: #3B4454;\n      --muted: #647084;\n      --line: rgba(15, 23, 42, 0.075);\n      --line-strong: rgba(15, 23, 42, 0.14);\n      --blue: #0071E3;\n      --blue-hover: #0062C7;\n      --blue-soft: rgba(0, 113, 227, 0.10);\n      --blue-border: rgba(0, 113, 227, 0.36);\n      --red: #DC2626;\n      --red-soft: rgba(220, 38, 38, 0.095);\n      --shadow-card: 0 1px 3px rgba(15, 23, 42, 0.05), 0 12px 32px -10px rgba(15, 23, 42, 0.08);\n      --autofill-bg: #F4F6FA;\n      --ease: cubic-bezier(0.2, 0.8, 0.2, 1);\n    }\n    html[data-theme="dark"] {\n      color-scheme: dark;\n      --bg: #0B111C;\n      --surface: #131B29;\n      --card-2: rgba(255, 255, 255, 0.055);\n      --hover: rgba(255, 255, 255, 0.09);\n      --press: rgba(255, 255, 255, 0.135);\n      --fg: #F3F6FA;\n      --fg-secondary: #C8D2E1;\n      --muted: #8B98AD;\n      --line: rgba(255, 255, 255, 0.075);\n      --line-strong: rgba(255, 255, 255, 0.15);\n      --blue: #3B82F6;\n      --blue-hover: #2563EB;\n      --blue-soft: rgba(59, 130, 246, 0.16);\n      --blue-border: rgba(59, 130, 246, 0.44);\n      --red: #F85149;\n      --red-soft: rgba(248, 81, 73, 0.14);\n      --shadow-card: 0 1px 2px rgba(0, 0, 0, 0.26), 0 18px 44px -10px rgba(0, 0, 0, 0.52);\n      --autofill-bg: #182233;\n    }\n    *, *::before, *::after, *:focus, *:focus-visible {\n      box-sizing: border-box;\n      outline: none !important;\n      -webkit-tap-highlight-color: transparent;\n    }\n    html, body { height: 100%; margin: 0; }\n    body {\n      min-height: 100dvh;\n      padding: 20px 16px calc(24px + env(safe-area-inset-bottom, 0px));\n      background: var(--bg);\n      color: var(--fg);\n      font-family: "Segoe UI Variable Text", "Segoe UI", ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei UI", sans-serif;\n      font-size: 13px;\n      line-height: 1.48;\n      display: flex;\n      align-items: center;\n      justify-content: center;\n      text-rendering: optimizeLegibility;\n    }\n    @media (-webkit-min-device-pixel-ratio: 2), (min-resolution: 2dppx) {\n      body { -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; }\n    }\n    .login-card {\n      width: 100%;\n      max-width: 384px;\n      background: var(--surface);\n      border: 1px solid var(--line);\n      border-radius: 14px;\n      padding: 24px 24px 20px;\n      box-shadow: var(--shadow-card);\n      display: flex;\n      flex-direction: column;\n      gap: 18px;\n    }\n    @keyframes shakeCard {\n      0%, 100% { transform: translateX(0); }\n      22% { transform: translateX(-5px); }\n      44% { transform: translateX(5px); }\n      66% { transform: translateX(-3px); }\n      88% { transform: translateX(3px); }\n    }\n    .login-card.shake { animation: shakeCard 280ms var(--ease); }\n    .login-head {\n      display: flex;\n      align-items: center;\n      justify-content: space-between;\n      gap: 12px;\n      padding-bottom: 16px;\n      border-bottom: 1px solid var(--line);\n    }\n    .brand-row { display: flex; align-items: center; gap: 11px; min-width: 0; }\n    .brand-mark {\n      width: 38px;\n      height: 38px;\n      flex: 0 0 38px;\n      background: transparent;\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      user-select: none;\n    }\n    .brand-mark-svg {\n      width: 100%;\n      height: 100%;\n      display: block;\n      overflow: visible;\n    }\n    .brand-info { display: flex; flex-direction: column; gap: 2px; min-width: 0; }\n    .brand-title { font-size: 15px; font-weight: 600; letter-spacing: -0.015em; color: var(--fg); line-height: 1.25; white-space: nowrap; }\n    .brand-sub { font-size: 11.5px; color: var(--muted); line-height: 1.25; white-space: nowrap; }\n    .guard-pill {\n      height: 24px;\n      padding: 0 8px;\n      border-radius: 6px;\n      background: var(--card-2);\n      color: var(--muted);\n      font-size: 11px;\n      font-weight: 500;\n      display: inline-flex;\n      align-items: center;\n      gap: 4px;\n      flex-shrink: 0;\n      user-select: none;\n    }\n    .ico { width: 15px; height: 15px; flex-shrink: 0; stroke: currentColor; stroke-width: 1.75; stroke-linecap: round; stroke-linejoin: round; fill: none; }\n    .ico-13 { width: 13px; height: 13px; }\n    .login-form { display: flex; flex-direction: column; gap: 13px; margin: 0; }\n    .field-group { display: flex; flex-direction: column; gap: 6px; }\n    .field-label { font-size: 12px; font-weight: 500; color: var(--fg-secondary); user-select: none; }\n    .input-shell {\n      height: 38px;\n      padding: 0 6px 0 11px;\n      border-radius: 8px;\n      background: var(--card-2);\n      box-shadow: inset 0 0 0 1px var(--line);\n      display: flex;\n      align-items: center;\n      gap: 8px;\n      transition: background-color 130ms var(--ease), box-shadow 130ms var(--ease);\n    }\n    .input-shell .field-ico { color: var(--muted); transition: color 130ms var(--ease); }\n    .input-shell:focus-within {\n      background: var(--surface);\n      box-shadow: inset 0 0 0 1.5px var(--blue-border), 0 0 0 3px var(--blue-soft);\n    }\n    .input-shell:focus-within .field-ico { color: var(--blue); }\n    .field-input {\n      flex: 1;\n      min-width: 0;\n      width: 100%;\n      height: 100%;\n      border: 0;\n      background: transparent;\n      color: var(--fg);\n      font-family: inherit;\n      font-size: 13px;\n      padding: 0;\n    }\n    .field-input::placeholder { color: var(--muted); opacity: 0.72; }\n    .field-input:-webkit-autofill,\n    .field-input:-webkit-autofill:hover,\n    .field-input:-webkit-autofill:focus {\n      -webkit-text-fill-color: var(--fg);\n      -webkit-box-shadow: 0 0 0 1000px var(--autofill-bg) inset;\n      border-radius: 6px;\n      transition: background-color 9999s ease-out 0s;\n    }\n    .eye-btn {\n      width: 28px;\n      height: 28px;\n      flex: 0 0 28px;\n      padding: 0;\n      border: 0;\n      border-radius: 6px;\n      background: transparent;\n      color: var(--muted);\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      cursor: pointer;\n      transition: background-color 130ms var(--ease), color 130ms var(--ease);\n    }\n    .eye-btn:active { background: var(--press); }\n    .error-box {\n      display: none;\n      align-items: center;\n      gap: 7px;\n      padding: 8px 11px;\n      border-radius: 8px;\n      background: var(--red-soft);\n      color: var(--red);\n      font-size: 12px;\n      font-weight: 500;\n      line-height: 1.4;\n    }\n    .error-box.visible { display: flex; }\n    .submit-btn {\n      margin-top: 3px;\n      width: 100%;\n      height: 38px;\n      padding: 0 16px;\n      border: 0;\n      border-radius: 8px;\n      background: var(--blue);\n      color: #FFFFFF;\n      font-family: inherit;\n      font-size: 13px;\n      font-weight: 500;\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      gap: 7px;\n      cursor: pointer;\n      user-select: none;\n      transition: background-color 130ms var(--ease), opacity 130ms var(--ease);\n    }\n    .submit-btn:active:not(:disabled) { background: var(--blue-hover); }\n    .submit-btn:disabled { opacity: 0.56; cursor: not-allowed; }\n    .login-foot { font-size: 11px; color: var(--muted); text-align: center; padding-top: 2px; user-select: none; }\n    @keyframes spin { to { transform: rotate(360deg); } }\n    .spin { animation: spin 560ms linear infinite; }\n    @media (hover: hover) and (pointer: fine) {\n      .input-shell:hover:not(:focus-within) { background: var(--hover); box-shadow: inset 0 0 0 1px var(--line-strong); }\n      .eye-btn:hover { background: var(--hover); color: var(--fg); }\n      .submit-btn:hover:not(:disabled) { background: var(--blue-hover); }\n    }\n  </style>\n</head>\n<body>\n  <svg xmlns="http://www.w3.org/2000/svg" style="display:none" aria-hidden="true">\n    <symbol id="i-lock" viewBox="0 0 24 24"><rect x="4" y="11" width="16" height="10" rx="2.5"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></symbol>\n    <symbol id="i-user" viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"/><path d="M4.5 20a7.5 7.5 0 0 1 15 0"/></symbol>\n    <symbol id="i-eye" viewBox="0 0 24 24"><path d="M2 12s3.6-6.5 10-6.5S22 12 22 12s-3.6 6.5-10 6.5S2 12 2 12z"/><circle cx="12" cy="12" r="2.8"/></symbol>\n    <symbol id="i-eye-off" viewBox="0 0 24 24"><path d="M17.94 17.94A10.4 10.4 0 0 1 12 18.5C5.6 18.5 2 12 2 12a18.8 18.8 0 0 1 4.4-5.26M9.9 5.73A9.8 9.8 0 0 1 12 5.5c6.4 0 10 6.5 10 6.5a18.8 18.8 0 0 1-2.55 3.68"/><line x1="3" y1="3" x2="21" y2="21"/></symbol>\n    <symbol id="i-alert" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><line x1="12" y1="8" x2="12" y2="12.5"/><circle cx="12" cy="16" r="0.6" fill="currentColor"/></symbol>\n    <symbol id="i-spin" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/></symbol>\n  </svg>\n\n  <main class="login-card" id="loginCard">\n    <div class="login-head">\n      <div class="brand-row">\n        <span class="brand-mark" aria-hidden="true"><svg class="brand-mark-svg" viewBox="68 48 896 904" aria-hidden="true"><defs><linearGradient id="bm_bright" gradientUnits="userSpaceOnUse" x1="150" y1="100" x2="880" y2="900"><stop offset="0%" stop-color="#9BD4FF"/><stop offset="46%" stop-color="#4A90FF"/><stop offset="100%" stop-color="#1D54E0"/></linearGradient><linearGradient id="bm_main" gradientUnits="userSpaceOnUse" x1="180" y1="120" x2="880" y2="920"><stop offset="0%" stop-color="#75BEFF"/><stop offset="50%" stop-color="#377DF6"/><stop offset="100%" stop-color="#1745BE"/></linearGradient><linearGradient id="bm_deep" gradientUnits="userSpaceOnUse" x1="180" y1="140" x2="860" y2="940"><stop offset="0%" stop-color="#4B92FF"/><stop offset="55%" stop-color="#2260DF"/><stop offset="100%" stop-color="#123597"/></linearGradient><linearGradient id="bm_gold" gradientUnits="userSpaceOnUse" x1="350" y1="120" x2="850" y2="850"><stop offset="0%" stop-color="#FFF0A8"/><stop offset="48%" stop-color="#FACC3E"/><stop offset="100%" stop-color="#E59213"/></linearGradient><linearGradient id="bm_cyan" gradientUnits="userSpaceOnUse" x1="200" y1="120" x2="820" y2="850"><stop offset="0%" stop-color="#B8ECFF"/><stop offset="50%" stop-color="#58B4FF"/><stop offset="100%" stop-color="#256AF0"/></linearGradient><mask id="bm_knock" maskUnits="userSpaceOnUse" x="-100" y="-100" width="1224" height="1224"><rect x="-100" y="-100" width="1224" height="1224" fill="#fff"/><path d="M 786 352 L 558 688 L 904 688 M 786 352 L 786 888" fill="none" stroke="#000" stroke-width="174" stroke-linecap="round" stroke-linejoin="round"/></mask></defs><g mask="url(#bm_knock)"><path d="M 642 238 A 308 308 0 1 0 464 786" fill="none" stroke="url(#bm_main)" stroke-width="138" stroke-linecap="round"/><path d="M 304 338 A 182 182 0 0 0 284 568" fill="none" stroke="url(#bm_cyan)" stroke-width="44" stroke-linecap="round" opacity="0.9"/></g><path d="M 456 336 C 469 402, 490 423, 556 436 C 490 449, 469 470, 456 536 C 443 470, 422 449, 356 436 C 422 423, 443 402, 456 336 Z" fill="url(#bm_gold)"/><path d="M 786 352 L 558 688 L 904 688" fill="none" stroke="url(#bm_bright)" stroke-width="116" stroke-linecap="round" stroke-linejoin="round"/><path d="M 786 352 L 786 888" fill="none" stroke="url(#bm_deep)" stroke-width="116" stroke-linecap="round"/></svg></span>\n        <div class="brand-info">\n          <span class="brand-title">G4 工作台</span>\n          <span class="brand-sub">请输入账号密码验证身份</span>\n        </div>\n      </div>\n      <span class="guard-pill">\n        <svg class="ico ico-13" aria-hidden="true"><use href="#i-lock"/></svg>\n        <span>访问保护</span>\n      </span>\n    </div>\n\n    <form class="login-form" id="loginForm" autocomplete="on" novalidate>\n      <div class="field-group">\n        <label class="field-label" for="loginUser">账号</label>\n        <div class="input-shell">\n          <svg class="ico field-ico" aria-hidden="true"><use href="#i-user"/></svg>\n          <input class="field-input" id="loginUser" name="username" type="text" autocomplete="username" autocapitalize="none" spellcheck="false" placeholder="输入管理账号" required>\n        </div>\n      </div>\n\n      <div class="field-group">\n        <label class="field-label" for="loginPass">密码</label>\n        <div class="input-shell">\n          <svg class="ico field-ico" aria-hidden="true"><use href="#i-lock"/></svg>\n          <input class="field-input" id="loginPass" name="password" type="password" autocomplete="current-password" placeholder="输入访问密码" required>\n          <button type="button" class="eye-btn" id="togglePwdBtn" aria-label="显示密码" title="显示/隐藏密码">\n            <svg class="ico" aria-hidden="true"><use id="eyeUse" href="#i-eye"/></svg>\n          </button>\n        </div>\n      </div>\n\n      <div class="error-box" id="loginErr" role="alert">\n        <svg class="ico" aria-hidden="true"><use href="#i-alert"/></svg>\n        <span id="loginErrText">账号或密码错误</span>\n      </div>\n\n      <button type="submit" class="submit-btn" id="loginSubmitBtn">\n        <span id="submitBtnLabel">进入工作台</span>\n      </button>\n    </form><p class="hint"><!--INIT_NOTICE--></p>\n\n    <div class="login-foot">登录凭证加密校验 · 当前设备保持 30 天免重复登录</div>\n  </main>\n\n  <script>\'use strict\';\nconst sysDark = window.matchMedia(\'(prefers-color-scheme: dark)\');\nfunction syncTheme() {\n  const dark = sysDark.matches;\n  document.documentElement.setAttribute(\'data-theme\', dark ? \'dark\' : \'light\');\n}\nsysDark.addEventListener(\'change\', syncTheme);\nsyncTheme();\n\nconst userInp = document.getElementById(\'loginUser\');\nconst passInp = document.getElementById(\'loginPass\');\nconst eyeBtn = document.getElementById(\'togglePwdBtn\');\nconst eyeUse = document.getElementById(\'eyeUse\');\nconst errBox = document.getElementById(\'loginErr\');\nconst errTxt = document.getElementById(\'loginErrText\');\nconst card = document.getElementById(\'loginCard\');\nconst subBtn = document.getElementById(\'loginSubmitBtn\');\n\nif (window.matchMedia(\'(hover: hover) and (pointer: fine)\').matches) {\n  userInp?.focus();\n}\n\neyeBtn?.addEventListener(\'click\', () => {\n  const show = passInp.type === \'password\';\n  passInp.type = show ? \'text\' : \'password\';\n  eyeUse?.setAttribute(\'href\', show ? \'#i-eye-off\' : \'#i-eye\');\n  eyeBtn.setAttribute(\'aria-label\', show ? \'隐藏密码\' : \'显示密码\');\n});\n\nfunction showErr(msg) {\n  errTxt.textContent = msg || \'账号或密码错误\';\n  errBox.classList.add(\'visible\');\n  card.classList.remove(\'shake\');\n  void card.offsetWidth;\n  card.classList.add(\'shake\');\n}\n\n[userInp, passInp].forEach(el => el?.addEventListener(\'input\', () => {\n  errBox.classList.remove(\'visible\');\n}));\n\nlet busy = false;\ndocument.getElementById(\'loginForm\')?.addEventListener(\'submit\', async (e) => {\n  e.preventDefault();\n  if (busy) return;\n  const username = (userInp.value || \'\').trim();\n  const password = passInp.value || \'\';\n  if (!username || !password) {\n    showErr(\'请输入账号和密码\');\n    (!username ? userInp : passInp).focus();\n    return;\n  }\n  busy = true;\n  subBtn.disabled = true;\n  errBox.classList.remove(\'visible\');\n  const ns = \'http://www.w3.org/2000/svg\';\n  const sp = document.createElementNS(ns, \'svg\');\n  sp.setAttribute(\'class\', \'ico spin\');\n  const u = document.createElementNS(ns, \'use\');\n  u.setAttribute(\'href\', \'#i-spin\');\n  sp.appendChild(u);\n  const st = document.createElement(\'span\');\n  st.textContent = \'正在验证...\';\n  subBtn.replaceChildren(sp, st);\n\n  try {\n    const res = await fetch(\'/api/login\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ username, password })\n    });\n    const data = await res.json().catch(() => ({}));\n    if (!res.ok || !data.ok) {\n      showErr(data.message || \'账号或密码不正确\');\n      busy = false;\n      subBtn.disabled = false;\n      const lbl = document.createElement(\'span\');\n      lbl.textContent = \'进入工作台\';\n      subBtn.replaceChildren(lbl);\n      passInp.select();\n      return;\n    }\n    window.location.replace(\'/\');\n  } catch (_) {\n    showErr(\'网络连接异常，请重试\');\n    busy = false;\n    subBtn.disabled = false;\n    const lbl = document.createElement(\'span\');\n    lbl.textContent = \'进入工作台\';\n    subBtn.replaceChildren(lbl);\n  }\n});\n</script>\n</body>\n</html>'

# 权威 UI 只存储一份；运行时保持与磁盘模板字节一致。
HTML_PAGE = '<!doctype html>\n<html lang="zh-CN">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n  <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'sha256-Evdend8CnX+TOX2islbcCdEaefo11xYtxX0Oy72zavw=\'; style-src \'unsafe-inline\'; img-src \'self\' data: blob:; connect-src \'self\'; form-action \'self\'; object-src \'none\'; base-uri \'none\'; frame-src \'none\'; media-src \'self\'; font-src \'none\'; worker-src \'none\'">\n  <meta name="color-scheme" content="light dark">\n  <meta name="theme-color" id="metaThemeColor" content="#EEF1F6">\n  <meta name="apple-mobile-web-app-capable" content="yes">\n  <meta name="mobile-web-app-capable" content="yes">\n  <meta name="apple-mobile-web-app-status-bar-style" content="default">\n  <title>G4 工作台</title>\n  <link rel="icon" type="image/svg+xml" href="/g4-icon.svg?v=g5">\n  <link rel="icon" type="image/png" sizes="64x64" href="/favicon.png?v=g5">\n  <link rel="shortcut icon" href="/favicon.ico?v=g5">\n  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v=g5">\n  <link rel="manifest" href="/manifest.webmanifest">\n  <style>\n  /* ==========================================================================\n     Colabbb Studio — Unified Design System\n     1. Borderless Surface Elevation (--card-2 / --hover / --press)\n     2. Single Unified Segmented Control (.seg-group & .tabs-bar -> HIG Elevated Thumb)\n     3. Strict 3-Tier Button Geometry (36px CTA / 32px Standard / 28px Inline)\n     4. Precision Input Surface (.inp / .search-wrap / .z-select-trigger) & 36x20 Switch\n     5. Monochrome Action Icons; Semantic Color Strictly Reserved for State & Telemetry\n     ========================================================================== */\n  :root {\n    color-scheme: light dark;\n    --bg: #EEF1F6;\n    --surface: #FFFFFF;\n    --surface-elevated: #FFFFFF;\n    --card-2: rgba(20, 28, 40, 0.048);\n    --card-3: rgba(20, 28, 40, 0.075);\n    --hover: rgba(20, 28, 40, 0.08);\n    --press: rgba(20, 28, 40, 0.12);\n    --seg-thumb: #FFFFFF;\n    --seg-thumb-shadow: 0 1px 3px rgba(15, 23, 42, 0.10), 0 0 0 0.5px rgba(15, 23, 42, 0.06);\n\n    --fg: #161A22;\n    --fg-secondary: #3B4454;\n    --muted: #647084;\n\n    --line: rgba(15, 23, 42, 0.075);\n    --line-strong: rgba(15, 23, 42, 0.14);\n\n    --blue: #0071E3;\n    --blue-hover: #0062C7;\n    --blue-soft: rgba(0, 113, 227, 0.10);\n    --blue-border: rgba(0, 113, 227, 0.34);\n\n    --green: #16A34A;\n    --green-bar: #22C55E;\n    --green-soft: rgba(22, 163, 74, 0.11);\n    --green-border: rgba(22, 163, 74, 0.30);\n\n    --amber: #D97706;\n    --amber-bar: #F59E0B;\n    --amber-soft: rgba(217, 119, 6, 0.11);\n    --amber-border: rgba(217, 119, 6, 0.30);\n\n    --gold: #CA8A04;\n    --purple: #8B5CF6;\n    --sky: #0EA5E9;\n\n    --red: #DC2626;\n    --red-bar: #EF4444;\n    --red-soft: rgba(220, 38, 38, 0.095);\n    --red-hover: rgba(220, 38, 38, 0.16);\n    --red-border: rgba(220, 38, 38, 0.28);\n\n    --r-card: 14px;\n    --r-tray: 10px;\n    --r-ctl: 8px;\n    --r-chip: 6px;\n    --r-pill: 999px;\n\n    --shadow-card: 0 1px 3px rgba(15, 23, 42, 0.045), 0 6px 18px -8px rgba(15, 23, 42, 0.05);\n    --shadow-float: 0 12px 32px -4px rgba(15, 23, 42, 0.15), 0 2px 6px rgba(15, 23, 42, 0.06);\n    --shadow-modal: 0 24px 64px rgba(15, 23, 42, 0.20);\n\n    --nav-bg: rgba(255, 255, 255, 0.90);\n    --console-bg: #0D131F;\n    --console-fg: #DCE4F0;\n    --track-bg: rgba(15, 23, 42, 0.09);\n    --backdrop-bg: rgba(15, 23, 42, 0.28);\n\n    --ease: cubic-bezier(0.2, 0.8, 0.2, 1);\n    --t-fast: 130ms;\n    --t: 180ms;\n  }\n\n  html[data-theme="dark"] {\n    color-scheme: dark;\n    --bg: #0B111C;\n    --surface: #131B29;\n    --surface-elevated: #182234;\n    --card-2: rgba(255, 255, 255, 0.055);\n    --card-3: rgba(255, 255, 255, 0.09);\n    --hover: rgba(255, 255, 255, 0.09);\n    --press: rgba(255, 255, 255, 0.135);\n    --seg-thumb: rgba(255, 255, 255, 0.13);\n    --seg-thumb-shadow: 0 1px 2px rgba(0, 0, 0, 0.36), inset 0 0.5px 0 rgba(255, 255, 255, 0.09);\n\n    --fg: #F3F6FA;\n    --fg-secondary: #C8D2E1;\n    --muted: #8B98AD;\n\n    --line: rgba(255, 255, 255, 0.075);\n    --line-strong: rgba(255, 255, 255, 0.15);\n\n    --blue: #3B82F6;\n    --blue-hover: #2563EB;\n    --blue-soft: rgba(59, 130, 246, 0.16);\n    --blue-border: rgba(59, 130, 246, 0.42);\n\n    --green: #3FB950;\n    --green-bar: #22C55E;\n    --green-soft: rgba(63, 185, 80, 0.15);\n    --green-border: rgba(63, 185, 80, 0.34);\n\n    --amber: #E09F24;\n    --amber-bar: #F59E0B;\n    --amber-soft: rgba(224, 159, 36, 0.15);\n    --amber-border: rgba(224, 159, 36, 0.34);\n\n    --gold: #EEAD0E;\n    --purple: #A371F7;\n    --sky: #38BDF8;\n\n    --red: #F85149;\n    --red-bar: #EF4444;\n    --red-soft: rgba(248, 81, 73, 0.14);\n    --red-hover: rgba(248, 81, 73, 0.22);\n    --red-border: rgba(248, 81, 73, 0.34);\n\n    --shadow-card: 0 1px 2px rgba(0, 0, 0, 0.22), 0 8px 24px -8px rgba(0, 0, 0, 0.36);\n    --shadow-float: 0 14px 36px rgba(0, 0, 0, 0.52), 0 1px 3px rgba(0, 0, 0, 0.32);\n    --shadow-modal: 0 24px 64px rgba(0, 0, 0, 0.62);\n\n    --nav-bg: rgba(24, 34, 52, 0.90);\n    --console-bg: #080D16;\n    --console-fg: #CBD5E1;\n    --track-bg: rgba(255, 255, 255, 0.11);\n    --backdrop-bg: rgba(2, 6, 16, 0.58);\n  }\n  html[data-theme="light"] { color-scheme: light; }\n\n  /* ==================== Base Reset & Scrollbars ==================== */\n  *, *::before, *::after {\n    box-sizing: border-box;\n    -webkit-tap-highlight-color: transparent;\n    scrollbar-width: thin;\n    scrollbar-color: var(--line-strong) transparent;\n  }\n  ::-webkit-scrollbar { width: 6px; height: 6px; }\n  ::-webkit-scrollbar-track { background: transparent; }\n  ::-webkit-scrollbar-thumb { background: var(--line-strong); border-radius: 999px; }\n\n  html {\n    overflow-x: clip;\n    overscroll-behavior-x: none;\n  }\n  body {\n    margin: 0;\n    padding: 0 0 28px;\n    overflow-x: clip;\n    overscroll-behavior-x: none;\n    background: var(--bg);\n    color: var(--fg);\n    font-family: "Segoe UI Variable Text", "Segoe UI", ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei UI", sans-serif;\n    font-size: 13px;\n    font-weight: 400;\n    line-height: 1.48;\n    font-variant-numeric: tabular-nums;\n    text-rendering: optimizeLegibility;\n  }\n  @media (-webkit-min-device-pixel-ratio: 2), (min-resolution: 2dppx) {\n    body {\n      -webkit-font-smoothing: antialiased;\n      -moz-osx-font-smoothing: grayscale;\n    }\n  }\n\n  input[type="number"]::-webkit-outer-spin-button,\n  input[type="number"]::-webkit-inner-spin-button { -webkit-appearance: none; margin: 0; }\n  input[type="number"] { -moz-appearance: textfield; }\n  input[type="search"]::-webkit-search-decoration,\n  input[type="search"]::-webkit-search-cancel-button,\n  input[type="search"]::-webkit-search-results-button,\n  input[type="search"]::-webkit-search-results-decoration { -webkit-appearance: none; appearance: none; display: none; }\n\n  *, *::before, *::after,\n  *:focus, *:focus-visible {\n    outline: none !important;\n  }\n  button::-moz-focus-inner { border: 0; }\n\n  /* ==================== Icon System (Monochrome Default, Semantic Only for Telemetry/State) ==================== */\n  .ico {\n    width: 15px;\n    height: 15px;\n    flex-shrink: 0;\n    stroke: currentColor;\n    stroke-width: 1.75;\n    stroke-linecap: round;\n    stroke-linejoin: round;\n    fill: none;\n  }\n  .ico-14 { width: 14px; height: 14px; }\n  /* Neutralize decorative icon colors on ordinary buttons; keep semantic colors only where meaningful */\n  .ico-blue { color: var(--blue); }\n  .ico-green { color: var(--green); }\n  .ico-gold { color: var(--gold); }\n  .ico-purple { color: var(--purple); }\n  .ico-sky { color: var(--sky); }\n  .ico-orange { color: var(--amber); }\n  .ico-red { color: var(--red); }\n  .btn:not(.btn-primary):not(.btn-danger):not(.btn-selected) .ico:not(.ico-green):not(.ico-red) {\n    color: var(--fg-secondary);\n  }\n  .seg-btn .ico {\n    color: currentColor;\n    opacity: 0.85;\n  }\n  .seg-btn.active .ico {\n    color: var(--blue);\n    opacity: 1;\n  }\n\n  /* ==================== 1. Unified Studio Buttons (3 Strict Sizes, Borderless Surface Elevation) ==================== */\n  .btn {\n    position: relative;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    gap: 6px;\n    height: 32px;\n    padding: 0 12px;\n    border: 0;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    color: var(--fg);\n    font-family: inherit;\n    font-size: 12.5px;\n    font-weight: 500;\n    line-height: 1;\n    cursor: pointer;\n    text-decoration: none;\n    white-space: nowrap;\n    user-select: none;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease), opacity var(--t-fast) var(--ease);\n  }\n  .btn[hidden] { display: none !important; }\n  .btn:disabled { opacity: 0.42; cursor: not-allowed; }\n  .btn:active:not(:disabled) { background: var(--press); }\n\n  /* Tier M: 32px Standard */\n  .btn-sm { height: 32px; padding: 0 12px; font-size: 12.5px; border-radius: var(--r-ctl); }\n  /* Tier S: 28px Compact Inline */\n  .btn-xs { height: 28px; padding: 0 10px; font-size: 12px; border-radius: 7px; }\n\n  /* Semantic Button Variants */\n  .btn-primary {\n    background: var(--blue);\n    color: #FFFFFF;\n  }\n  .btn-primary .ico { color: #FFFFFF !important; }\n  .btn-primary:active:not(:disabled) { background: var(--blue-hover); }\n\n  .btn-danger {\n    background: var(--red-soft);\n    color: var(--red);\n  }\n  .btn-danger .ico { color: var(--red) !important; }\n  .btn-danger:active:not(:disabled) { background: var(--red-hover); }\n\n  .btn-selected {\n    background: var(--blue-soft);\n    color: var(--blue);\n  }\n  .btn-selected .ico { color: var(--blue) !important; }\n\n  /* ==================== 2. Unified Segmented Control (.seg-group & .tabs-bar — Identical Everywhere!) ==================== */\n  .seg-group,\n  .tabs-bar {\n    display: inline-flex;\n    align-items: center;\n    background: var(--card-2);\n    border: 0;\n    border-radius: var(--r-tray);\n    padding: 3px;\n    gap: 2px;\n    min-width: 0;\n  }\n  .seg-group.full {\n    display: grid;\n    grid-template-columns: repeat(3, minmax(0, 1fr));\n    width: 100%;\n    height: 36px;\n    padding: 3px;\n  }\n  .seg-btn,\n  .tab-pill {\n    border: 0;\n    background: transparent;\n    color: var(--muted);\n    height: 28px;\n    padding: 0 11px;\n    border-radius: 7px;\n    font-family: inherit;\n    font-size: 12.5px;\n    font-weight: 500;\n    line-height: 1;\n    cursor: pointer;\n    white-space: nowrap;\n    user-select: none;\n    min-width: 0;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    gap: 6px;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease), box-shadow var(--t-fast) var(--ease);\n  }\n  .seg-group.full .seg-btn {\n    height: 30px;\n    font-size: 13px;\n  }\n  .seg-btn.active,\n  .tab-pill.active {\n    background: var(--seg-thumb);\n    color: var(--fg);\n    box-shadow: var(--seg-thumb-shadow);\n  }\n  .option-value, .option-hint { display: inline; }\n  .option-hint { font-size: 11px; font-weight: 400; color: var(--muted); margin-left: 2px; }\n  .seg-btn.active .option-hint { color: var(--fg-secondary); }\n\n  .tab-count {\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    min-width: 20px;\n    height: 18px;\n    padding: 0 6px;\n    font-size: 11px;\n    font-weight: 500;\n    border-radius: var(--r-pill);\n    background: var(--card-3);\n    color: var(--muted);\n    flex-shrink: 0;\n  }\n  .seg-btn.active .tab-count,\n  .tab-pill.active .tab-count {\n    background: var(--blue-soft);\n    color: var(--blue);\n  }\n\n  /* ==================== 3. Unified Precision Inputs, Search, Switches & Custom Dropdowns ==================== */\n  .mod-input,\n  .settings-input,\n  .settings-textarea,\n  .z-select-trigger,\n  .search-wrap {\n    border: 0;\n    border-radius: var(--r-ctl);\n    background-color: var(--card-2);\n    box-shadow: inset 0 0 0 1px var(--line);\n    color: var(--fg);\n    font-family: inherit;\n    transition: background-color var(--t-fast) var(--ease), box-shadow var(--t-fast) var(--ease);\n  }\n  .mod-input:focus,\n  .settings-input:focus,\n  .settings-textarea:focus,\n  .z-select.open .z-select-trigger,\n  .search-wrap:focus-within {\n    background-color: var(--surface);\n    box-shadow: inset 0 0 0 1.5px var(--blue-border), 0 0 0 3px var(--blue-soft);\n    outline: none;\n  }\n\n  /* Studio 36x20 Sliding Switch for Settings Checkboxes */\n  .settings-check {\n    display: inline-flex;\n    align-items: center;\n    gap: 9px;\n    font-size: 13px;\n    font-weight: 500;\n    color: var(--fg);\n    cursor: pointer;\n    user-select: none;\n  }\n  .settings-check input[type="checkbox"] {\n    -webkit-appearance: none;\n    appearance: none;\n    width: 36px;\n    height: 20px;\n    flex: 0 0 36px;\n    margin: 0;\n    border: 0;\n    border-radius: 999px;\n    background: var(--track-bg);\n    position: relative;\n    cursor: pointer;\n    transition: background-color var(--t-fast) var(--ease);\n  }\n  .settings-check input[type="checkbox"]::after {\n    content: "";\n    position: absolute;\n    top: 2px;\n    left: 2px;\n    width: 16px;\n    height: 16px;\n    border-radius: 50%;\n    background: #FFFFFF;\n    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.24);\n    transition: transform var(--t-fast) var(--ease);\n  }\n  .settings-check input[type="checkbox"]:checked {\n    background: var(--blue);\n  }\n  .settings-check input[type="checkbox"]:checked::after {\n    transform: translateX(16px);\n  }\n\n  /* Multi-select checkboxes inside Warmup Picker */\n  .warmup-options input[type="checkbox"] {\n    -webkit-appearance: none;\n    appearance: none;\n    width: 17px;\n    height: 17px;\n    flex: 0 0 17px;\n    margin: 0;\n    border: 1.5px solid var(--line-strong);\n    border-radius: 5px;\n    background: transparent;\n    cursor: pointer;\n    display: inline-grid;\n    place-content: center;\n    transition: background-color var(--t-fast) var(--ease), border-color var(--t-fast) var(--ease);\n  }\n  .warmup-options input[type="checkbox"]::after {\n    content: "";\n    width: 4.5px;\n    height: 8.5px;\n    border-right: 2px solid #FFFFFF;\n    border-bottom: 2px solid #FFFFFF;\n    transform: rotate(45deg) translate(-0.5px, -1px) scale(0);\n    transition: transform var(--t-fast) var(--ease);\n  }\n  .warmup-options input[type="checkbox"]:checked {\n    background-color: var(--blue);\n    border-color: var(--blue);\n  }\n  .warmup-options input[type="checkbox"]:checked::after {\n    transform: rotate(45deg) translate(-0.5px, -1px) scale(1);\n  }\n\n  /* Custom Studio Select Popover */\n  .z-native-select-hidden { display: none !important; }\n  .z-select { position: relative; display: inline-flex; min-width: 0; width: 100%; }\n  .z-select[hidden] { display: none !important; }\n  .z-select-trigger {\n    width: 100%;\n    height: 34px;\n    padding: 0 10px 0 11px;\n    font-size: 12.5px;\n    font-weight: 500;\n    display: inline-flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n    cursor: pointer;\n    user-select: none;\n    text-align: left;\n    min-width: 0;\n  }\n  .z-select-label {\n    overflow: hidden;\n    text-overflow: ellipsis;\n    white-space: nowrap;\n    min-width: 0;\n    flex: 1 1 auto;\n  }\n  .z-select-arrow {\n    width: 13px;\n    height: 13px;\n    flex: 0 0 13px;\n    color: var(--muted);\n    transition: transform var(--t) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .z-select.open .z-select-arrow {\n    transform: rotate(180deg);\n    color: var(--blue);\n  }\n  .z-select-menu {\n    position: absolute;\n    top: calc(100% + 5px);\n    left: 0;\n    min-width: 100%;\n    margin: 0;\n    z-index: 80;\n    padding: 4px;\n    background: var(--surface-elevated);\n    color: var(--fg);\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-tray);\n    box-shadow: var(--shadow-float);\n    display: none;\n    flex-direction: column;\n    gap: 2px;\n    max-height: 268px;\n    overflow-y: auto;\n    -webkit-overflow-scrolling: touch;\n    overscroll-behavior: contain;\n  }\n  .z-select-menu.flip-up {\n    top: auto;\n    bottom: calc(100% + 5px);\n  }\n  .z-select-menu.in-dialog {\n    position: fixed;\n    top: auto;\n    bottom: auto;\n    left: auto;\n    min-width: 0;\n    z-index: 230;\n  }\n  .z-select-menu.open {\n    display: flex;\n    animation: studioPopIn 120ms cubic-bezier(0.2, 0, 0, 1);\n  }\n  @keyframes studioPopIn {\n    0% { opacity: 0; transform: translateY(-4px) scale(0.985); }\n    100% { opacity: 1; transform: translateY(0) scale(1); }\n  }\n  .z-select-opt {\n    width: 100%;\n    min-height: 32px;\n    padding: 0 10px;\n    border: 0;\n    border-radius: var(--r-chip);\n    background: transparent;\n    color: var(--fg-secondary);\n    font-family: inherit;\n    font-size: 12.5px;\n    font-weight: 400;\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 10px;\n    cursor: pointer;\n    user-select: none;\n    text-align: left;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .z-select-opt.selected {\n    background: var(--blue-soft);\n    color: var(--blue);\n    font-weight: 500;\n  }\n  .z-select-opt-check {\n    width: 14px;\n    height: 14px;\n    flex: 0 0 14px;\n    color: var(--blue);\n    opacity: 0;\n  }\n  .z-select-opt.selected .z-select-opt-check { opacity: 1; }\n\n  /* Search Input with Inline Icon (Studio .search) */\n  .search-wrap {\n    display: inline-flex;\n    align-items: center;\n    gap: 7px;\n    height: 32px;\n    padding: 0 10px;\n    width: 196px;\n    min-width: 120px;\n  }\n  .search-wrap .ico { color: var(--muted); }\n  .filter-search {\n    flex: 1;\n    min-width: 0;\n    width: 100%;\n    height: 100%;\n    border: 0;\n    background: transparent;\n    color: var(--fg);\n    font-family: inherit;\n    font-size: 12px;\n    padding: 0;\n    outline: none;\n  }\n  .filter-search::placeholder { color: var(--muted); }\n\n  /* ==================== 4. Unified Card Shell (Floating Sidebar Card + Top Instance Status Card + 5-Tab Mobile Bottom Nav) ==================== */\n  :root {\n    --sidebar-w: 218px;\n    --shell-gap: 14px;\n    --task-col-w: 368px;\n    --mob-nav-h: 60px;\n  }\n\n  body {\n    padding: 0;\n    min-height: 100dvh;\n  }\n\n  .app-shell {\n    display: flex;\n    min-height: 100dvh;\n    width: 100%;\n    min-width: 0;\n  }\n\n  /* ---- Desktop Left Sidebar Card (Same Card Language: --surface, 1px --line, --r-card, --shadow-card) ---- */\n  .app-sidebar {\n    position: fixed;\n    top: var(--shell-gap);\n    left: var(--shell-gap);\n    bottom: var(--shell-gap);\n    width: var(--sidebar-w);\n    z-index: 40;\n    background: var(--surface);\n    border: 1px solid var(--line);\n    border-radius: var(--r-card);\n    box-shadow: var(--shadow-card);\n    display: flex;\n    flex-direction: column;\n    min-height: 0;\n    user-select: none;\n    overflow: hidden;\n  }\n  .side-head {\n    flex: 0 0 auto;\n    padding: 13px 12px 12px 14px;\n    border-bottom: 1px solid var(--line);\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n  }\n  .brand {\n    display: inline-flex;\n    align-items: center;\n    gap: 9px;\n    min-width: 0;\n    cursor: pointer;\n    user-select: none;\n  }\n  .brand-mark {\n    width: 28px;\n    height: 28px;\n    flex: 0 0 28px;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n  }\n  .brand-mark-svg {\n    width: 100%;\n    height: 100%;\n    display: block;\n    overflow: visible;\n  }\n  .brand-text {\n    display: flex;\n    flex-direction: column;\n    min-width: 0;\n  }\n  .brand-title {\n    font-size: 14.5px;\n    font-weight: 600;\n    letter-spacing: -0.015em;\n    color: var(--fg);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n    line-height: 1.25;\n  }\n  .side-icon-btn {\n    width: 28px;\n    height: 28px;\n    flex: 0 0 28px;\n    padding: 0;\n    border: 0;\n    border-radius: 7px;\n    background: transparent;\n    color: var(--muted);\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    cursor: pointer;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .side-icon-btn:active { background: var(--press); }\n  .side-icon-btn.is-spinning .ico { animation: spinIcon 700ms linear infinite; }\n  @keyframes spinIcon { to { transform: rotate(360deg); } }\n\n  .side-nav-scroll {\n    flex: 1 1 auto;\n    min-height: 0;\n    overflow-y: auto;\n    padding: 12px 10px;\n    display: flex;\n    flex-direction: column;\n    gap: 14px;\n  }\n  .side-nav-group {\n    display: flex;\n    flex-direction: column;\n    gap: 3px;\n  }\n  .side-nav-label {\n    font-size: 11px;\n    font-weight: 500;\n    color: var(--muted);\n    padding: 0 10px 4px;\n    letter-spacing: 0.02em;\n  }\n  .side-nav-btn {\n    position: relative;\n    width: 100%;\n    height: 36px;\n    padding: 0 10px 0 11px;\n    border: 0;\n    border-radius: var(--r-ctl);\n    background: transparent;\n    color: var(--fg-secondary);\n    font-family: inherit;\n    font-size: 13px;\n    font-weight: 500;\n    display: flex;\n    align-items: center;\n    gap: 9px;\n    cursor: pointer;\n    text-align: left;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .side-nav-btn .ico {\n    width: 16px;\n    height: 16px;\n    color: var(--muted);\n    transition: color var(--t-fast) var(--ease);\n  }\n  .side-nav-btn .side-nav-text {\n    flex: 1 1 auto;\n    min-width: 0;\n    overflow: hidden;\n    text-overflow: ellipsis;\n    white-space: nowrap;\n  }\n  .side-nav-btn.active {\n    background: var(--blue-soft);\n    color: var(--blue);\n  }\n  .side-nav-btn.active::before {\n    content: "";\n    position: absolute;\n    left: 0;\n    top: 8px;\n    bottom: 8px;\n    width: 3px;\n    border-radius: 0 3px 3px 0;\n    background: var(--blue);\n  }\n  .side-nav-btn.active .ico { color: var(--blue); }\n  .side-nav-btn:active { background: var(--press); }\n  .side-nav-btn .tab-count {\n    min-width: 20px;\n    height: 18px;\n    padding: 0 6px;\n    font-size: 11px;\n    background: var(--card-3);\n    color: var(--muted);\n  }\n  .side-nav-btn.active .tab-count {\n    background: rgba(0, 113, 227, 0.16);\n    color: var(--blue);\n  }\n  html[data-theme="dark"] .side-nav-btn.active .tab-count {\n    background: rgba(59, 130, 246, 0.24);\n  }\n\n  .side-footer {\n    flex: 0 0 auto;\n    padding: 10px 12px;\n    border-top: 1px solid var(--line);\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n  }\n  .side-util-note {\n    font-size: 11.5px;\n    color: var(--muted);\n    white-space: nowrap;\n  }\n  .side-logout-btn {\n    height: 28px;\n    padding: 0 9px;\n    border: 0;\n    border-radius: 7px;\n    background: transparent;\n    color: var(--muted);\n    font-family: inherit;\n    font-size: 12px;\n    font-weight: 500;\n    display: inline-flex;\n    align-items: center;\n    gap: 5px;\n    cursor: pointer;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .side-logout-btn:active { background: var(--press); }\n\n  /* ---- Right Content Column (Unified Card Spacing) ---- */\n  .app-content {\n    flex: 1 1 auto;\n    min-width: 0;\n    margin-left: calc(var(--sidebar-w) + var(--shell-gap));\n    padding: var(--shell-gap);\n    display: flex;\n    flex-direction: column;\n    gap: var(--shell-gap);\n    min-height: 100dvh;\n    max-width: 1580px;\n  }\n\n  /* ---- Unified Top Instance Status Card (Present Across ALL Pages, Matching Card Surface) ---- */\n  .top-status-card {\n    background: var(--surface);\n    border: 1px solid var(--line);\n    border-radius: var(--r-card);\n    padding: 12px 16px;\n    box-shadow: var(--shadow-card);\n    display: flex;\n    flex-direction: column;\n    gap: 11px;\n    min-width: 0;\n  }\n  .top-card-row1 {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    flex-wrap: wrap;\n    gap: 10px 14px;\n    min-width: 0;\n  }\n  .top-card-left {\n    display: flex;\n    align-items: center;\n    gap: 10px;\n    min-width: 0;\n  }\n  .mob-header-brand {\n    display: none;\n    align-items: center;\n    gap: 7px;\n    flex-shrink: 0;\n  }\n  .mob-header-brand .brand-mark { width: 25px; height: 25px; flex: 0 0 25px; }\n  .page-header-title {\n    margin: 0;\n    font-size: 15px;\n    font-weight: 600;\n    letter-spacing: -0.015em;\n    color: var(--fg);\n    white-space: nowrap;\n    flex-shrink: 0;\n  }\n  .page-header-sub {\n    font-size: 12px;\n    color: var(--muted);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n    min-width: 0;\n  }\n  .top-card-right {\n    display: flex;\n    align-items: center;\n    justify-content: flex-end;\n    flex-wrap: wrap;\n    gap: 7px;\n    min-width: 0;\n    margin-left: auto;\n  }\n  .top-compute-group {\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n    flex-wrap: wrap;\n    min-width: 0;\n  }\n  .status-pill {\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n    height: 32px;\n    padding: 0 11px;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    color: var(--fg);\n    font-size: 12px;\n    font-weight: 500;\n    white-space: nowrap;\n    line-height: 1;\n    user-select: none;\n    transition: background-color var(--t-fast) var(--ease);\n  }\n  .status-pill[role="button"] { cursor: pointer; }\n  .status-pill .dot {\n    width: 7px;\n    height: 7px;\n    flex: 0 0 7px;\n    border-radius: 50%;\n    background: var(--muted);\n  }\n  .status-pill.on { background: var(--green-soft); }\n  .status-pill.on .dot { background: var(--green); }\n  .status-pill.on .status-state { color: var(--green); }\n  .status-pill.off { background: var(--red-soft); }\n  .status-pill.off .dot { background: var(--red); }\n  .status-pill.off .status-state { color: var(--red); }\n  .status-sep { color: var(--muted); font-weight: 400; margin: 0 2px; opacity: 0.7; }\n  .status-meta { color: var(--fg); font-weight: 600; font-variant-numeric: tabular-nums; }\n\n  .hdr-cu-btn {\n    height: 32px;\n    padding: 0 11px;\n    border: 0;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    color: var(--fg);\n    font-family: inherit;\n    font-size: 12px;\n    font-weight: 500;\n    display: inline-flex;\n    align-items: center;\n    gap: 5px;\n    cursor: pointer;\n    white-space: nowrap;\n    min-width: 0;\n    transition: background-color var(--t-fast) var(--ease);\n  }\n  .hdr-cu-btn:active { background: var(--press); }\n  .hdr-cu-btn[aria-expanded="true"] { background: var(--blue-soft); color: var(--blue); }\n  .hdr-cu-btn[aria-expanded="true"] .cu-chevron { transform: rotate(180deg); color: var(--blue); }\n  .hdr-cu-btn .cu-acct-desk { color: var(--fg-secondary); font-weight: 500; }\n  .hdr-cu-btn .cu-acct-mob { display: none; color: var(--fg-secondary); font-weight: 500; }\n  .hdr-cu-btn #cuText { font-weight: 600; color: var(--fg); font-variant-numeric: tabular-nums; }\n  .hdr-cu-btn .cu-chevron { width: 12px; height: 12px; color: var(--muted); transition: transform var(--t) var(--ease), color var(--t-fast) var(--ease); flex-shrink: 0; }\n\n  .hdr-power-btns {\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n    flex-shrink: 0;\n  }\n  .hdr-sep {\n    width: 1px;\n    height: 18px;\n    background: var(--line-strong);\n    margin: 0 2px;\n    flex-shrink: 0;\n  }\n  .global-task-group {\n    display: inline-flex;\n    align-items: center;\n    gap: 5px;\n    flex-shrink: 0;\n  }\n  .global-task-pill {\n    height: 32px;\n    padding: 0 11px;\n    border: 0;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    color: var(--fg-secondary);\n    font-family: inherit;\n    font-size: 12px;\n    font-weight: 500;\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n    cursor: pointer;\n    white-space: nowrap;\n    max-width: 240px;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .global-task-pill.is-running {\n    background: var(--blue-soft);\n    color: var(--blue);\n  }\n  .global-task-pill.is-panel-open {\n    background: var(--card-3);\n    color: var(--fg);\n  }\n  .global-task-pill.is-running.is-panel-open {\n    background: var(--blue-soft);\n    color: var(--blue);\n  }\n  .global-task-pill-text {\n    overflow: hidden;\n    text-overflow: ellipsis;\n    white-space: nowrap;\n    font-variant-numeric: tabular-nums;\n  }\n  .nav-run-dot {\n    width: 7px;\n    height: 7px;\n    border-radius: 50%;\n    background: var(--blue);\n    flex-shrink: 0;\n    animation: navDotPulse 1.6s infinite ease-in-out;\n  }\n  .nav-run-dot[hidden] { display: none !important; }\n  @keyframes navDotPulse {\n    0%, 100% { opacity: 1; transform: scale(1); }\n    50% { opacity: 0.36; transform: scale(0.82); }\n  }\n  #globalStopMiniBtn {\n    height: 32px;\n    padding: 0 10px;\n    border-radius: var(--r-ctl);\n  }\n  .mob-fold-top-btn {\n    display: none;\n    width: 30px;\n    height: 30px;\n    flex: 0 0 30px;\n    padding: 0;\n    border: 0;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    color: var(--fg-secondary);\n    align-items: center;\n    justify-content: center;\n    cursor: pointer;\n    transition: background-color var(--t-fast) var(--ease);\n  }\n  .mob-fold-top-btn .ico {\n    width: 14px;\n    height: 14px;\n    transform: rotate(180deg);\n    transition: transform 180ms var(--ease);\n  }\n  .top-status-card.mob-collapsed .mob-fold-top-btn .ico {\n    transform: rotate(0deg);\n  }\n\n  /* 4-Column Zeno Hardware Telemetry Bar inside Top Status Card */\n  .topbar-hw {\n    width: 100%;\n    padding-top: 10px;\n    border-top: 1px solid var(--line);\n    display: grid;\n    grid-template-columns: repeat(4, minmax(0, 1fr));\n    align-items: center;\n    min-width: 0;\n  }\n  .topbar-hw[data-offline="true"] .hw-val { color: var(--muted); }\n  .hw-cell {\n    display: flex;\n    flex-direction: column;\n    gap: 5px;\n    padding: 0 16px;\n    border-right: 1px solid var(--line);\n    min-width: 0;\n  }\n  .hw-cell:first-child { padding-left: 2px; }\n  .hw-cell:last-child { padding-right: 2px; border-right: 0; }\n  .hw-head {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n    min-width: 0;\n    line-height: 1.2;\n  }\n  .hw-label {\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n    font-size: 12px;\n    font-weight: 500;\n    color: var(--fg-secondary);\n    white-space: nowrap;\n  }\n  .hw-val {\n    font-size: 12px;\n    font-weight: 500;\n    font-variant-numeric: tabular-nums;\n    color: var(--fg);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n  }\n  .hw-val-sub {\n    color: var(--muted);\n    font-size: 11px;\n    font-weight: 400;\n    margin-left: 4px;\n  }\n  .hw-bar-track {\n    width: 100%;\n    height: 6px;\n    border-radius: var(--r-pill);\n    background: var(--track-bg);\n    overflow: hidden;\n  }\n  .hw-bar-fill {\n    height: 100%;\n    width: 0%;\n    border-radius: var(--r-pill);\n    background: var(--green-bar);\n    transition: width 280ms var(--ease), background-color 200ms var(--ease);\n  }\n  .hw-bar-fill.warn { background: var(--amber-bar); }\n  .hw-bar-fill.danger { background: var(--red-bar); }\n\n  /* ---- Main Stage & Unified Fixed Viewport App-Shell (All 5 Views Identical Outer Frame + Docked Dual Column) ---- */\n  .app-stage {\n    flex: 1 1 auto;\n    width: 100%;\n    display: grid;\n    grid-template-columns: minmax(0, 1fr);\n    gap: var(--shell-gap);\n    align-items: stretch;\n    min-width: 0;\n    min-height: 0;\n  }\n  .app-stage[data-task-mode="docked"] {\n    grid-template-columns: minmax(0, 1fr) var(--task-col-w);\n    align-items: stretch;\n  }\n  @media (min-width: 1025px) {\n    html, body {\n      height: 100%;\n      overflow: hidden;\n    }\n    .app-shell {\n      height: 100dvh;\n      min-height: 0;\n      overflow: hidden;\n    }\n    .app-content {\n      height: 100dvh;\n      min-height: 0;\n      overflow: hidden;\n    }\n    .top-status-card {\n      flex: 0 0 auto;\n    }\n    .top-card-row1 {\n      flex-wrap: nowrap;\n    }\n    .top-card-left {\n      flex: 1 1 auto;\n      min-width: 0;\n      overflow: hidden;\n    }\n    .top-card-right {\n      flex: 0 0 auto;\n      flex-wrap: nowrap;\n    }\n    .top-compute-group {\n      flex-wrap: nowrap;\n    }\n    #g4Badge {\n      width: 118px;\n      justify-content: center;\n      overflow: hidden;\n    }\n    #cuBadge {\n      width: 176px;\n      justify-content: center;\n      overflow: hidden;\n    }\n    #startG4Btn,\n    #warmupG4Btn {\n      width: 76px;\n      padding: 0 8px;\n      justify-content: center;\n    }\n    #globalStopMiniBtn {\n      width: 48px;\n      padding: 0 6px;\n      justify-content: center;\n    }\n    #globalTaskPill {\n      width: 152px;\n      max-width: 152px;\n      padding: 0 10px;\n      justify-content: center;\n    }\n    .app-stage {\n      flex: 1 1 0;\n      min-height: 0;\n      overflow: hidden;\n    }\n    .app-pages {\n      display: flex;\n      flex-direction: column;\n      height: 100%;\n      min-height: 0;\n      overflow: hidden;\n    }\n    .page-view.active {\n      flex: 1 1 auto;\n      height: 100%;\n      min-height: 0;\n      overflow: hidden;\n    }\n    #pageCreation.active .card-main,\n    #pageCreation.active .creation-body {\n      flex: 1 1 auto;\n      height: 100%;\n      display: flex;\n      flex-direction: column;\n      min-height: 0;\n      overflow: hidden;\n    }\n    #pageCreation .creation-body:not([data-mod="llm"]) .param-panel {\n      flex: 0 1 auto;\n      min-height: 0;\n      overflow-y: auto;\n      padding-right: 2px;\n    }\n    #pageCreation .creation-body:not([data-mod="llm"]) .prompt-label-row {\n      flex: 0 0 auto;\n    }\n    #pageCreation .creation-body:not([data-mod="llm"]) .mod-input.active {\n      flex: 1 1 124px;\n      height: auto;\n      min-height: 96px;\n      resize: none;\n    }\n    #pageCreation .creation-body .action-bar {\n      flex: 0 0 auto;\n    }\n    #pageCreation .creation-body[data-mod="llm"] .param-panel {\n      flex: 1 1 0;\n      display: flex;\n      flex-direction: column;\n      min-height: 0;\n      overflow: hidden;\n    }\n    #pageCreation .creation-body[data-mod="llm"] #modSecLlm.active {\n      flex: 1 1 0;\n      min-height: 0;\n      display: flex;\n      flex-direction: column;\n      overflow: hidden;\n    }\n    #pageCreation .creation-body[data-mod="llm"] .chat-toolbar,\n    #pageCreation .creation-body[data-mod="llm"] .llm-video-ctx {\n      flex: 0 0 auto;\n    }\n    #pageCreation .creation-body[data-mod="llm"] .chat-box {\n      flex: 1 1 0;\n      min-height: 160px;\n      max-height: none;\n    }\n    #pageCreation .creation-body[data-mod="llm"] #promptInputLlm.active {\n      flex: 0 0 92px;\n      height: 92px;\n      min-height: 92px;\n      resize: none;\n    }\n    #pageWorks.active .card-works {\n      flex: 1 1 auto;\n      height: 100%;\n      min-height: 0;\n      display: flex;\n      flex-direction: column;\n      overflow: hidden;\n      container: works-card / inline-size;\n    }\n    #pageWorks.active .works-top-card {\n      flex: 0 0 auto;\n      position: relative;\n      z-index: 12;\n      overflow: visible;\n    }\n    #pageWorks.active .works-gallery-card {\n      flex: 1 1 0;\n      min-height: 0;\n      overflow-y: auto;\n      padding-right: 2px;\n    }\n    #pageSettings.active .settings-page-card {\n      flex: 1 1 auto;\n      height: 100%;\n      min-height: 0;\n      display: flex;\n      flex-direction: column;\n      overflow: hidden;\n      container: settings-card / inline-size;\n    }\n    #pageSettings.active .settings-page-card .settings-body {\n      flex: 1 1 0;\n      min-height: 0;\n      overflow-y: auto;\n    }\n    #pageSettings.active .settings-page-card .settings-foot {\n      position: static;\n      box-shadow: none;\n      flex: 0 0 auto;\n    }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap {\n      position: static;\n      top: auto;\n      height: 100%;\n      min-height: 0;\n      max-height: none;\n    }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap .card-progress {\n      height: 100%;\n      min-height: 0;\n      max-height: none;\n      overflow: hidden;\n    }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap .task-ledger {\n      height: auto;\n      min-height: 156px;\n      max-height: none;\n      flex: 1 1 52%;\n    }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap .log-console {\n      height: auto;\n      min-height: 156px;\n      flex: 1 1 48%;\n    }\n    .float-dock {\n      right: calc(max(0px, 100vw - var(--sidebar-w) - var(--shell-gap) - 1580px) + var(--shell-gap) + 18px);\n      bottom: calc(var(--shell-gap) + 18px);\n    }\n    body.task-docked-open .float-dock {\n      right: calc(max(0px, 100vw - var(--sidebar-w) - var(--shell-gap) - 1580px) + var(--task-col-w) + var(--shell-gap) * 2 + 18px);\n    }\n  }\n  .app-stage[data-task-mode="docked"].task-collapsed {\n    grid-template-columns: minmax(0, 1fr);\n  }\n  .app-stage[data-task-mode="docked"].task-collapsed .task-panel-wrap {\n    display: none;\n  }\n\n  .app-pages {\n    min-width: 0;\n    width: 100%;\n  }\n  .page-view {\n    display: none;\n    flex-direction: column;\n    gap: var(--shell-gap);\n    min-width: 0;\n    width: 100%;\n  }\n  .page-view.active {\n    display: flex;\n  }\n\n  /* Live Task Panel */\n  .task-panel-wrap {\n    min-width: 0;\n    display: flex;\n    flex-direction: column;\n  }\n  .app-stage[data-task-mode="docked"] .task-panel-wrap {\n    position: static;\n    height: 100%;\n    min-height: 0;\n    max-height: none;\n  }\n  .app-stage[data-task-mode="docked"] .task-panel-wrap .card-progress {\n    height: 100%;\n    min-height: 0;\n    max-height: none;\n    overflow: hidden;\n  }\n  .app-stage[data-task-mode="docked"] .task-panel-wrap .task-ledger {\n    height: auto;\n    min-height: 156px;\n    max-height: none;\n    flex: 1 1 52%;\n  }\n  .app-stage[data-task-mode="docked"] .task-panel-wrap .log-console {\n    height: auto;\n    min-height: 156px;\n    flex: 1 1 48%;\n  }\n\n  .app-stage[data-task-mode="drawer"] .task-panel-wrap {\n    position: fixed;\n    top: 14px;\n    right: 14px;\n    bottom: 14px;\n    width: min(410px, calc(100vw - 28px));\n    z-index: 90;\n    opacity: 0;\n    visibility: hidden;\n    pointer-events: none;\n    transform: translate3d(24px, 0, 0);\n    transition: transform 200ms var(--ease), opacity 180ms var(--ease), visibility 0s linear 200ms;\n  }\n  .app-stage[data-task-mode="drawer"].drawer-open .task-panel-wrap {\n    opacity: 1;\n    visibility: visible;\n    pointer-events: auto;\n    transform: translate3d(0, 0, 0);\n    transition: transform 200ms var(--ease), opacity 180ms var(--ease), visibility 0s linear 0s;\n  }\n  .app-stage[data-task-mode="drawer"] .task-panel-wrap .card-progress {\n    height: 100%;\n    box-shadow: var(--shadow-modal);\n    border-color: var(--line-strong);\n  }\n  .app-stage[data-task-mode="drawer"] .task-panel-wrap .task-ledger {\n    flex: 1 1 50%;\n    height: auto;\n    min-height: 160px;\n  }\n  .app-stage[data-task-mode="drawer"] .task-panel-wrap .log-console {\n    flex: 0 0 210px;\n    height: 210px;\n  }\n  .task-drawer-close {\n    display: none;\n    width: 28px;\n    height: 28px;\n    padding: 0;\n    flex: 0 0 28px;\n  }\n  .app-stage[data-task-mode="drawer"] .task-drawer-close {\n    display: inline-flex;\n  }\n  .task-sheet-handle {\n    display: none;\n    width: 36px;\n    height: 4px;\n    border-radius: 999px;\n    background: var(--line-strong);\n    margin: -4px auto 2px;\n  }\n  .task-drawer-backdrop {\n    position: fixed;\n    inset: 0;\n    z-index: 85;\n    background: var(--backdrop-bg);\n    opacity: 0;\n    visibility: hidden;\n    pointer-events: none;\n    transition: opacity 180ms var(--ease), visibility 0s linear 180ms;\n  }\n  .task-drawer-backdrop.open {\n    opacity: 1;\n    visibility: visible;\n    pointer-events: auto;\n    transition: opacity 180ms var(--ease), visibility 0s linear 0s;\n  }\n\n  /* ---- Enhanced Creation Editor Proportions ---- */\n  #pageCreation .creation-body:not([data-mod="llm"]) .mod-input.active {\n    height: 124px;\n    min-height: 98px;\n    resize: vertical;\n  }\n  #pageCreation .creation-body[data-mod="llm"] .chat-box {\n    min-height: 260px;\n    max-height: calc(100dvh - 400px);\n  }\n  #pageCreation .creation-body[data-mod="llm"] #promptInputLlm.active {\n    height: 94px;\n    resize: vertical;\n  }\n\n  /* ---- Full-Page Inline Settings Card ---- */\n  .settings-page-card {\n    max-width: none;\n    width: 100%;\n    padding: 0;\n    gap: 0;\n    overflow: visible;\n  }\n  .settings-page-card .settings-head {\n    border-radius: var(--r-card) var(--r-card) 0 0;\n    background: var(--surface);\n    padding: 14px 20px;\n    gap: 10px;\n  }\n  .settings-page-card .settings-body {\n    overflow: visible;\n    padding: 18px 20px;\n  }\n  .settings-page-card .settings-foot {\n    position: sticky;\n    bottom: 0;\n    z-index: 12;\n    background: var(--surface);\n    border-top: 1px solid var(--line);\n    border-radius: 0 0 var(--r-card) var(--r-card);\n    padding: 12px 20px;\n    box-shadow: 0 -6px 18px -10px rgba(15, 23, 42, 0.14);\n  }\n  html[data-theme="dark"] .settings-page-card .settings-foot {\n    box-shadow: 0 -8px 22px -10px rgba(0, 0, 0, 0.45);\n  }\n  .mob-only-btn { display: none; }\n\n  /* ---- Fixed Floating Popovers (Account Switcher & Warmup Picker) ---- */\n  .account-picker-fixed {\n    position: fixed !important;\n    top: auto;\n    right: auto;\n    left: auto;\n    z-index: 126;\n    width: min(396px, calc(100vw - 20px));\n    transform-origin: top right;\n  }\n  .account-picker-fixed .acct-head { align-items: flex-start; }\n  .account-picker-fixed .acct-head-left {\n    display: flex;\n    flex-direction: column;\n    align-items: flex-start;\n    gap: 2px;\n    min-width: 0;\n  }\n  .warmup-picker-fixed {\n    position: fixed !important;\n    top: auto;\n    right: auto;\n    z-index: 125;\n    width: min(384px, calc(100vw - 20px));\n    transform-origin: top right;\n  }\n\n  /* ---- Floating Back-to-Top Button ---- */\n  .float-dock {\n    position: fixed;\n    right: 22px;\n    bottom: 22px;\n    z-index: 55;\n    pointer-events: none;\n  }\n  .back-top-btn {\n    pointer-events: auto;\n    width: 38px;\n    height: 38px;\n    padding: 0;\n    border-radius: 50%;\n    background: var(--surface-elevated);\n    border: 1px solid var(--line-strong);\n    color: var(--fg-secondary);\n    box-shadow: var(--shadow-float);\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    cursor: pointer;\n    user-select: none;\n    transition: transform var(--t-fast) var(--ease), border-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .back-top-btn[hidden] { display: none; }\n  .back-top-btn:active { transform: scale(0.94); }\n\n  /* ---- Mobile Zero-Mask Card Dock Inside Locked App-Shell (position:absolute + transparent wrapper -> ZERO Safari bottom mask, ZERO gray slab, ZERO auto-lift) ---- */\n  .mob-bottom-nav {\n    display: none;\n    position: absolute;\n    left: 0;\n    right: 0;\n    bottom: 0;\n    z-index: 40;\n    background: transparent;\n    padding: 0 10px 4px;\n    pointer-events: none;\n    user-select: none;\n    transform: translate3d(0, 0, 0);\n    opacity: 1;\n    transition: transform 200ms cubic-bezier(0.22, 0.8, 0.28, 1), opacity 160ms ease;\n  }\n  html.pwa-standalone .mob-bottom-nav {\n    padding-bottom: max(24px, env(safe-area-inset-bottom, 24px));\n  }\n  html.pwa-standalone .app-content {\n    padding-bottom: calc(74px + max(24px, env(safe-area-inset-bottom, 24px)));\n  }\n  html.pwa-standalone .float-dock,\n  html.pwa-standalone .toast {\n    bottom: calc(64px + max(24px, env(safe-area-inset-bottom, 24px)));\n  }\n  html.pwa-standalone .app-stage[data-task-mode="drawer"] .task-panel-wrap {\n    bottom: calc(62px + max(24px, env(safe-area-inset-bottom, 24px)));\n  }\n  @media (display-mode: standalone) {\n    .mob-bottom-nav {\n      padding-bottom: max(24px, env(safe-area-inset-bottom, 24px));\n    }\n    .app-content {\n      padding-bottom: calc(74px + max(24px, env(safe-area-inset-bottom, 24px)));\n    }\n    .float-dock,\n    .toast {\n      bottom: calc(64px + max(24px, env(safe-area-inset-bottom, 24px)));\n    }\n    .app-stage[data-task-mode="drawer"] .task-panel-wrap {\n      bottom: calc(62px + max(24px, env(safe-area-inset-bottom, 24px)));\n    }\n  }\n  .mob-bottom-nav.nav-hidden,\n  .mob-bottom-nav.kb-hidden {\n    transform: translate3d(0, calc(100% + 24px), 0);\n    opacity: 0;\n  }\n  .mob-bottom-nav.nav-hidden .mob-nav-card,\n  .mob-bottom-nav.kb-hidden .mob-nav-card {\n    pointer-events: none;\n  }\n  .mob-nav-card {\n    pointer-events: auto;\n    width: 100%;\n    max-width: 480px;\n    margin: 0 auto;\n    padding: 3px;\n    background: var(--surface);\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    box-shadow: 0 8px 24px -6px rgba(15, 23, 42, 0.16), 0 2px 6px rgba(15, 23, 42, 0.06);\n  }\n  html[data-theme="dark"] .mob-nav-card {\n    box-shadow: 0 10px 28px -4px rgba(0, 0, 0, 0.54), 0 2px 6px rgba(0, 0, 0, 0.32);\n  }\n  .mob-nav-track {\n    position: relative;\n    width: 100%;\n    display: grid;\n    grid-template-columns: repeat(5, minmax(0, 1fr));\n    gap: 0;\n  }\n  .mob-nav-glider {\n    position: absolute;\n    top: 0;\n    bottom: 0;\n    left: 0;\n    width: 20%;\n    border-radius: 11px;\n    background: var(--blue-soft);\n    box-shadow: inset 0 0 0 1px rgba(0, 113, 227, 0.14);\n    pointer-events: none;\n    z-index: 0;\n    transform: translate3d(0%, 0, 0);\n    transition: transform 230ms cubic-bezier(0.28, 0.82, 0.24, 1);\n    will-change: transform;\n  }\n  html[data-theme="dark"] .mob-nav-glider {\n    box-shadow: inset 0 0 0 1px rgba(59, 130, 246, 0.24);\n  }\n  .mob-nav-item {\n    position: relative;\n    z-index: 1;\n    border: 0;\n    background: transparent;\n    color: var(--muted);\n    font-family: inherit;\n    height: 44px;\n    padding: 3px 2px;\n    border-radius: 11px;\n    display: flex;\n    flex-direction: column;\n    align-items: center;\n    justify-content: center;\n    gap: 2.5px;\n    min-width: 0;\n    cursor: pointer;\n    transition: color 150ms var(--ease), transform 100ms var(--ease);\n  }\n  .mob-nav-item:active {\n    transform: scale(0.94);\n  }\n  .mob-nav-icon-wrap {\n    position: relative;\n    width: 20px;\n    height: 20px;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n  }\n  .mob-nav-icon-wrap .ico {\n    width: 18px;\n    height: 18px;\n    stroke-width: 1.75;\n    transition: stroke-width 150ms var(--ease);\n  }\n  .mob-nav-item.active {\n    color: var(--blue);\n  }\n  .mob-nav-item.active .ico {\n    stroke-width: 2;\n  }\n  .mob-nav-lbl {\n    font-size: 10.5px;\n    font-weight: 500;\n    line-height: 1.1;\n    letter-spacing: 0.01em;\n    white-space: nowrap;\n  }\n  .mob-nav-item.active .mob-nav-lbl {\n    font-weight: 600;\n  }\n\n  .side-comfy-link {\n    margin: 0 10px 8px;\n    height: 32px;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    color: var(--fg-secondary);\n    font-size: 12px;\n    font-weight: 500;\n    text-decoration: none;\n    display: flex;\n    align-items: center;\n    justify-content: center;\n    gap: 6px;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .side-comfy-link[hidden] { display: none !important; }\n  .card {\n    background: var(--surface);\n    border: 1px solid var(--line);\n    border-radius: var(--r-card);\n    padding: 16px 18px;\n    box-shadow: var(--shadow-card);\n    display: flex;\n    flex-direction: column;\n    gap: 12px;\n    min-height: 0;\n    min-width: 0;\n  }\n  .card-head {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    flex-wrap: wrap;\n    gap: 8px 12px;\n    min-height: 32px;\n    min-width: 0;\n  }\n  .card-title {\n    font-size: 14px;\n    font-weight: 600;\n    letter-spacing: -0.01em;\n    color: var(--fg);\n    display: inline-flex;\n    align-items: center;\n    gap: 7px;\n    min-width: 0;\n  }\n  .card-title .ico { color: var(--fg-secondary); }\n  .card-head-actions {\n    display: flex;\n    align-items: center;\n    gap: 6px;\n    flex-wrap: wrap;\n    flex-shrink: 0;\n  }\n\n  .creation-body {\n    display: flex;\n    flex-direction: column;\n    gap: 12px;\n    min-width: 0;\n  }\n  .card-works {\n    display: flex;\n    width: 100%;\n    gap: 12px;\n  }\n  .works-top-card,\n  .works-gallery-card {\n    display: flex;\n    flex-direction: column;\n    gap: 12px;\n    min-width: 0;\n    width: 100%;\n  }\n\n  /* Parameters Panel */\n  .param-panel {\n    display: flex;\n    flex-direction: column;\n    min-height: 0;\n    min-width: 0;\n    padding: 2px 0 12px;\n    border-bottom: 1px solid var(--line);\n  }\n  .mod-section { display: none; flex-direction: column; gap: 10px; min-width: 0; }\n  .mod-section.active { display: flex; }\n  .param-grid { display: flex; flex-direction: column; gap: 8px; min-width: 0; }\n  .param-row {\n    display: grid;\n    grid-template-columns: 64px minmax(0, 1fr);\n    align-items: center;\n    gap: 10px;\n    min-height: 34px;\n    min-width: 0;\n  }\n  .param-label { font-size: 12px; font-weight: 500; color: var(--muted); white-space: nowrap; }\n  .param-row .seg-group {\n    display: grid;\n    grid-auto-flow: column;\n    grid-auto-columns: minmax(0, 1fr);\n    width: 100%;\n    height: 34px;\n  }\n  .param-row .seg-btn { width: 100%; height: 28px; padding: 0 8px; }\n\n  /* Reference Image / First-Frame Strip */\n  .ref-strip-box {\n    display: flex;\n    flex-direction: column;\n    gap: 8px;\n    min-width: 0;\n    padding-top: 10px;\n    border-top: 1px solid var(--line);\n  }\n  .ref-strip-head {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 10px;\n    min-height: 28px;\n    min-width: 0;\n  }\n  .ref-head-info { display: flex; align-items: center; gap: 6px; min-width: 0; flex: 1; }\n  .ref-head-title {\n    font-size: 12px;\n    font-weight: 500;\n    color: var(--fg-secondary);\n    white-space: nowrap;\n    flex-shrink: 0;\n    display: inline-flex;\n    align-items: center;\n    gap: 5px;\n  }\n  .ref-head-title .ico { color: var(--muted); }\n  .ref-head-actions { display: flex; gap: 6px; flex-shrink: 0; }\n  .ref-status-text {\n    font-size: 12px;\n    color: var(--muted);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n    min-width: 0;\n  }\n  .ref-status-text.has-ref { color: var(--blue); font-weight: 500; }\n  .ref-strip-list {\n    display: flex;\n    align-items: center;\n    gap: 8px;\n    min-height: 82px;\n    min-width: 0;\n    overflow-x: auto;\n    overflow-y: hidden;\n    padding: 2px;\n    -webkit-overflow-scrolling: touch;\n  }\n  .ref-thumb-card {\n    flex: 0 0 62px;\n    width: 62px;\n    height: 80px;\n    border-radius: var(--r-ctl);\n    border: 0;\n    overflow: hidden;\n    position: relative;\n    cursor: pointer;\n    background: transparent;\n    transition: transform var(--t-fast) var(--ease), box-shadow var(--t-fast) var(--ease);\n  }\n  .ref-thumb-card.selected {\n    box-shadow: 0 0 0 2.5px var(--blue);\n  }\n  .ref-thumb-card img { width: 100%; height: 100%; object-fit: cover; display: block; border-radius: inherit; -webkit-user-drag: none; user-select: none; }\n\n  /* Prompt Editor + Live Character Counter + Split Execution Bar */\n  .prompt-label-row {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n    margin-bottom: -4px;\n  }\n  .prompt-label { font-size: 12px; font-weight: 500; color: var(--fg-secondary); }\n  .prompt-meta-right {\n    display: inline-flex;\n    align-items: center;\n    gap: 8px;\n    font-size: 11px;\n    color: var(--muted);\n  }\n  .prompt-char-badge {\n    height: 19px;\n    padding: 0 7px;\n    border-radius: var(--r-pill);\n    background: var(--card-2);\n    color: var(--fg-secondary);\n    font-size: 11px;\n    font-weight: 500;\n    font-variant-numeric: tabular-nums;\n    display: inline-flex;\n    align-items: center;\n  }\n  .mod-input {\n    display: none;\n    width: 100%;\n    height: 112px;\n    padding: 10px 12px;\n    font-size: 13px;\n    line-height: 1.56;\n    resize: vertical;\n  }\n  .mod-input::placeholder { color: var(--muted); opacity: 0.75; }\n  .mod-input.active { display: block; }\n\n  /* Tier L: Unified 36px Execution Action Bar (Left: Stop/Clear | Right: Random/Submit) */\n  .action-bar {\n    position: relative;\n    display: flex;\n    align-items: center;\n    gap: 8px;\n    min-height: 36px;\n    flex-wrap: wrap;\n  }\n  .action-bar > .btn {\n    height: 36px;\n    padding: 0 14px;\n    font-size: 13px;\n    border-radius: var(--r-ctl);\n  }\n  .action-spacer { flex: 1 1 auto; }\n  #runBtn { min-width: 118px; justify-content: center; }\n  #randomRunBtn { width: 116px; justify-content: center; }\n  #randomRunBtn .rand-chevron {\n    width: 12px;\n    height: 12px;\n    flex: 0 0 12px;\n    opacity: 0.8;\n    transition: transform var(--t) var(--ease);\n  }\n  #randomRunBtn[aria-expanded="true"] {\n    box-shadow: inset 0 0 0 1.5px var(--blue-border), 0 0 0 3px var(--blue-soft);\n  }\n  #randomRunBtn[aria-expanded="true"] .rand-chevron { transform: rotate(180deg); }\n\n  /* Prompt Master Chat */\n  .chat-toolbar {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n    flex-wrap: wrap;\n    min-height: 28px;\n    min-width: 0;\n  }\n  .chat-count-label { font-size: 12px; font-weight: 500; color: var(--muted); min-width: 0; flex: 1 1 180px; }\n  .chat-box {\n    max-height: 260px;\n    min-height: 140px;\n    overflow-y: auto;\n    display: flex;\n    flex-direction: column;\n    gap: 8px;\n    padding: 2px 4px 2px 0;\n  }\n  .chat-msg {\n    padding: 10px 12px;\n    border-radius: var(--r-tray);\n    font-size: 12.5px;\n    line-height: 1.58;\n    white-space: pre-wrap;\n    word-break: break-word;\n    background: var(--card-2);\n    color: var(--fg);\n  }\n  .chat-msg.user { background: var(--blue-soft); color: var(--fg); }\n  .chat-msg.assistant.thinking { border: 1px dashed var(--line-strong); background: transparent; color: var(--muted); }\n  .thinking-inline { display: inline-flex; align-items: center; gap: 8px; font-size: 12px; font-weight: 500; color: var(--blue); }\n  .thinking-dots { display: inline-flex; align-items: center; gap: 4px; }\n  .thinking-dots i {\n    width: 5px;\n    height: 5px;\n    border-radius: 50%;\n    background: var(--blue);\n    display: inline-block;\n    animation: chatDotPulse 1.2s infinite ease-in-out;\n  }\n  .thinking-dots i:nth-child(2) { animation-delay: 0.2s; }\n  .thinking-dots i:nth-child(3) { animation-delay: 0.4s; }\n  @keyframes chatDotPulse {\n    0%, 80%, 100% { opacity: 0.25; transform: scale(0.85); }\n    40% { opacity: 1; transform: scale(1.1); }\n  }\n  .chat-meta {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n    font-size: 11px;\n    font-weight: 500;\n    color: var(--muted);\n    margin-bottom: 6px;\n  }\n  .chat-meta-actions {\n    display: grid;\n    grid-template-columns: repeat(2, minmax(0, 1fr));\n    gap: 6px;\n    margin-bottom: 8px;\n    padding-bottom: 8px;\n    border-bottom: 1px solid var(--line);\n  }\n  .chat-meta-actions .btn { width: 100%; padding: 0 6px; }\n\n  /* ==================== 7. Right Card: Live Progress + Task Ledger + Terminal ==================== */\n  .card-progress { min-height: 300px; container-type: inline-size; }\n  #jobTimer {\n    height: 24px;\n    padding: 0 8px;\n    border-radius: var(--r-pill);\n    background: var(--card-2);\n    font-size: 11px;\n    font-weight: 500;\n    color: var(--muted);\n    display: inline-flex;\n    align-items: center;\n  }\n  .progress-box {\n    display: flex;\n    flex-direction: column;\n    gap: 6px;\n    padding: 0 0 10px;\n    border-bottom: 1px solid var(--line);\n    min-width: 0;\n  }\n  .topbar-inner,\n  .card-progress,\n  .task-ledger,\n  .log-console {\n    overflow-anchor: none;\n  }\n  .card-progress .card-head {\n    flex-wrap: nowrap;\n    min-width: 0;\n  }\n  .card-progress .card-title {\n    flex-shrink: 0;\n  }\n  .card-progress .card-head-actions {\n    flex-wrap: nowrap;\n    flex: 1 1 auto;\n    min-width: 0;\n    justify-content: flex-end;\n  }\n  #jobTimer {\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n    min-width: 0;\n    flex: 0 1 auto;\n    display: block;\n    line-height: 24px;\n  }\n  #clearTasksBtn {\n    flex-shrink: 0;\n  }\n  .progress-top {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    flex-wrap: nowrap;\n    gap: 8px;\n    height: 18px;\n    min-width: 0;\n  }\n  .progress-stage {\n    font-size: 12px;\n    font-weight: 500;\n    color: var(--fg);\n    min-width: 0;\n    flex: 1 1 auto;\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n  }\n  .progress-eta {\n    font-size: 11.5px;\n    font-weight: 500;\n    color: var(--muted);\n    white-space: nowrap;\n    flex-shrink: 0;\n  }\n  .progress-track {\n    width: 100%;\n    height: 8px;\n    background: var(--track-bg);\n    border-radius: var(--r-pill);\n    overflow: hidden;\n    position: relative;\n  }\n  .progress-fill {\n    height: 100%;\n    width: 0%;\n    background: var(--green-bar);\n    border-radius: var(--r-pill);\n    transition: width 260ms var(--ease), background-color 200ms var(--ease);\n  }\n  .progress-fill.tone-warn { background: var(--amber-bar); }\n  .progress-fill.tone-blue { background: var(--blue); }\n  .progress-fill.indeterminate {\n    width: 36%;\n    background: var(--blue);\n    opacity: 0.65;\n    animation: progSlide 1.35s infinite ease-in-out;\n  }\n  @keyframes progSlide {\n    0% { transform: translateX(-100%); }\n    100% { transform: translateX(310%); }\n  }\n\n  .task-ledger {\n    height: 204px;\n    overflow-y: auto;\n    border-radius: var(--r-tray);\n    background: var(--card-2);\n    display: flex;\n    flex-direction: column;\n    min-width: 0;\n  }\n  .task-empty {\n    color: var(--muted);\n    font-size: 12px;\n    text-align: center;\n    line-height: 1.5;\n    padding: 22px 14px;\n  }\n  .task-item-wrap {\n    position: relative;\n    display: flex;\n    flex-direction: column;\n    border-bottom: 1px solid var(--line);\n    background: transparent;\n    flex-shrink: 0;\n    min-width: 0;\n  }\n  .task-item-wrap:last-child { border-bottom: 0; }\n  .task-item-wrap {\n    transition: background-color var(--t-fast) var(--ease);\n  }\n  .task-item-wrap.running { background: var(--blue-soft); }\n  .task-item-wrap.queued { background: var(--amber-soft); }\n\n  .task-row {\n    display: grid;\n    grid-template-columns: auto minmax(0, 1.05fr) minmax(0, 1fr) auto auto;\n    align-items: center;\n    gap: 8px;\n    min-height: 36px;\n    padding: 5px 10px 5px 12px;\n    font-size: 11.5px;\n    cursor: pointer;\n    user-select: none;\n    min-width: 0;\n  }\n  .task-row > * { min-width: 0; }\n  .task-badge {\n    display: inline-flex;\n    align-items: center;\n    gap: 5px;\n    height: 20px;\n    padding: 0 7px;\n    border-radius: var(--r-chip);\n    background: var(--card-3);\n    font-size: 10.5px;\n    font-weight: 500;\n    white-space: nowrap;\n    color: var(--fg-secondary);\n  }\n  .task-badge .s-dot { width: 5.5px; height: 5.5px; border-radius: 50%; background: var(--muted); }\n  .task-item-wrap.running .task-badge { background: var(--blue-soft); color: var(--blue); }\n  .task-item-wrap.running .task-badge .s-dot { background: var(--blue); }\n  .task-item-wrap.queued .task-badge { background: var(--amber-soft); color: var(--amber); }\n  .task-item-wrap.queued .task-badge .s-dot { background: var(--amber); }\n  .task-item-wrap.done .task-badge { background: var(--green-soft); color: var(--green); }\n  .task-item-wrap.done .task-badge .s-dot { background: var(--green); }\n  .task-item-wrap.error .task-badge { background: var(--red-soft); color: var(--red); }\n  .task-item-wrap.error .task-badge .s-dot { background: var(--red); }\n\n  .task-name { font-weight: 500; color: var(--fg); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n  .task-preview { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--muted); }\n  .task-time { text-align: right; color: var(--muted); white-space: nowrap; font-size: 11px; }\n  .task-actions { display: flex; justify-content: flex-end; align-items: center; gap: 4px; }\n  .task-detail-box {\n    display: none;\n    padding: 8px 12px 10px;\n    border-top: 1px solid var(--line);\n    background: var(--surface);\n    font-size: 12px;\n    line-height: 1.55;\n    color: var(--fg-secondary);\n    white-space: pre-wrap;\n    word-break: break-word;\n  }\n  .task-detail-box.open { display: block; }\n  .task-mini-btn {\n    height: 24px;\n    padding: 0 8px;\n    font-family: inherit;\n    font-size: 11px;\n    font-weight: 500;\n    border-radius: var(--r-chip);\n    border: 0;\n    background: var(--card-3);\n    color: var(--fg-secondary);\n    cursor: pointer;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    white-space: nowrap;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .task-mini-btn:disabled { opacity: 0.40; cursor: not-allowed; }\n  .task-mini-btn.stop, .task-mini-btn.delete { background: var(--red-soft); color: var(--red); }\n  .task-mini-btn.resume { background: var(--blue-soft); color: var(--blue); }\n  .task-more-row { padding: 8px 12px; text-align: center; border-top: 1px solid var(--line); }\n\n  .log-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: -6px; }\n  .log-label { font-size: 12px; font-weight: 500; color: var(--muted); }\n  .log-console {\n    background: var(--console-bg);\n    color: var(--console-fg);\n    border: 1px solid var(--line);\n    font-family: "Cascadia Mono", "JetBrains Mono", "SF Mono", ui-monospace, Menlo, Consolas, monospace;\n    font-size: 11.5px;\n    line-height: 1.5;\n    padding: 9px 12px;\n    border-radius: var(--r-tray);\n    height: 168px;\n    overflow-y: auto;\n    white-space: pre-wrap;\n    word-break: break-all;\n  }\n\n  @container (max-width: 620px) {\n    .task-row {\n      grid-template-columns: auto minmax(0, 1fr) auto;\n      gap: 4px 8px;\n      padding: 7px 10px 7px 12px;\n    }\n    .task-badge { grid-column: 1; grid-row: 1; width: max-content; }\n    .task-time { grid-column: 2 / -1; grid-row: 1; overflow: hidden; text-overflow: ellipsis; }\n    .task-name { grid-column: 1 / -1; grid-row: 2; }\n    .task-preview { grid-column: 1 / 3; grid-row: 3; }\n    .task-actions { grid-column: 3; grid-row: 3; flex-wrap: nowrap; }\n  }\n\n  /* ==================== 8. Multimedia Works Gallery (Streamlined Single Studio Header) ==================== */\n  .works-toolbar {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    flex-wrap: wrap;\n    gap: 10px 12px;\n    padding-bottom: 12px;\n    border-bottom: 1px solid var(--line);\n    min-width: 0;\n  }\n  .works-head-left {\n    display: flex;\n    align-items: center;\n    gap: 10px;\n    flex-wrap: wrap;\n    min-width: 0;\n  }\n  .works-view-nav {\n    display: inline-flex;\n    align-items: center;\n    gap: 10px;\n    flex-shrink: 0;\n  }\n  .works-view-nav .top-view-seg {\n    height: 34px;\n    border-radius: var(--r-tray);\n  }\n  .works-view-nav .top-view-seg .seg-btn {\n    height: 28px;\n    border-radius: 7px;\n  }\n  .works-view-nav .top-seg-glider {\n    border-radius: 7px;\n  }\n  .works-mob-nav-bar {\n    display: none;\n    align-items: center;\n    justify-content: space-between;\n    width: 100%;\n  }\n  .works-body.mob-works-collapsed .mob-fold-top-btn .ico {\n    transform: rotate(0deg);\n  }\n  .creation-right .works-view-nav {\n    display: none !important;\n  }\n  .tabs-bar {\n    height: 34px;\n    padding: 3px;\n    gap: 2px;\n  }\n  .tabs-bar .tab-pill {\n    height: 28px;\n    padding: 0 12px;\n    font-size: 12.5px;\n  }\n  .works-filter-group {\n    display: grid;\n    grid-template-columns: minmax(160px, 1fr) max-content;\n    align-items: center;\n    gap: 6px;\n    min-width: 0;\n  }\n  .works-filter-group > .search-wrap {\n    width: 100%;\n    min-width: 0;\n    max-width: none;\n  }\n  .works-actions-inline {\n    display: inline-flex;\n    align-items: center;\n    flex: 0 0 auto;\n    gap: 6px;\n    min-width: 0;\n  }\n  #toggleBatchBtn { min-width: 88px; justify-content: center; }\n\n  /* Batch Selection Bar (Studio Recessed Tray + Elevated Controls) */\n  @keyframes studioBatchReveal {\n    0% { opacity: 0; transform: translateY(-4px); }\n    100% { opacity: 1; transform: translateY(0); }\n  }\n  .batch-toolbar {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    flex-wrap: wrap;\n    gap: 8px 12px;\n    padding: 5px 6px;\n    border-radius: var(--r-tray);\n    background: var(--card-2);\n    box-shadow: inset 0 0 0 1px var(--line);\n    min-width: 0;\n    animation: studioBatchReveal 140ms var(--ease);\n  }\n  .batch-toolbar[hidden] { display: none !important; }\n  .batch-left, .batch-right {\n    display: flex;\n    align-items: center;\n    flex-wrap: wrap;\n    gap: 5px;\n    min-width: 0;\n  }\n  .batch-count-pill {\n    height: 28px;\n    padding: 0 11px;\n    border-radius: 7px;\n    background: var(--seg-thumb);\n    box-shadow: var(--seg-thumb-shadow);\n    color: var(--fg-secondary);\n    font-size: 12px;\n    font-weight: 500;\n    font-variant-numeric: tabular-nums;\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n    white-space: nowrap;\n    user-select: none;\n  }\n  .batch-count-pill .b-dot {\n    width: 6px;\n    height: 6px;\n    border-radius: 50%;\n    background: var(--muted);\n    opacity: 0.6;\n    transition: background-color var(--t-fast) var(--ease), opacity var(--t-fast) var(--ease);\n  }\n  .batch-count-pill.has-sel {\n    color: var(--fg);\n  }\n  .batch-count-pill.has-sel .b-dot {\n    background: var(--blue);\n    opacity: 1;\n  }\n  .batch-count-pill strong {\n    font-weight: 600;\n    color: var(--fg);\n  }\n  .batch-count-pill.has-sel strong {\n    color: var(--blue);\n  }\n  .batch-sep {\n    width: 1px;\n    height: 16px;\n    background: var(--line-strong);\n    margin: 0 2px;\n  }\n  .batch-quick-group {\n    display: inline-flex;\n    align-items: center;\n    gap: 4px;\n    min-width: 0;\n  }\n  .batch-elev-btn {\n    height: 28px;\n    padding: 0 11px;\n    border: 0;\n    border-radius: 7px;\n    background: var(--seg-thumb);\n    box-shadow: var(--seg-thumb-shadow);\n    color: var(--fg-secondary);\n    font-family: inherit;\n    font-size: 12px;\n    font-weight: 500;\n    line-height: 1;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    gap: 5px;\n    cursor: pointer;\n    white-space: nowrap;\n    user-select: none;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease), opacity var(--t-fast) var(--ease);\n  }\n  .batch-elev-btn:disabled {\n    background: transparent;\n    box-shadow: none;\n    color: var(--muted);\n    opacity: 0.45;\n    cursor: not-allowed;\n  }\n  .batch-elev-btn:active:not(:disabled) {\n    background: var(--press);\n  }\n\n  /* Gallery Grid & Asset Cards */\n  .gallery-grid {\n    display: grid;\n    grid-template-columns: repeat(4, minmax(0, 1fr));\n    grid-auto-rows: max-content;\n    gap: 14px;\n    min-height: 200px;\n    min-width: 0;\n  }\n  @media (max-width: 1220px) { .gallery-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }\n  @media (max-width: 880px)  { .gallery-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; } }\n\n  @media (max-width: 359px) { .gallery-grid { grid-template-columns: minmax(0, 1fr); } }\n  .gallery-grid.single-col { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n  @media (max-width: 760px) { .gallery-grid.single-col { grid-template-columns: minmax(0, 1fr); } }\n\n  .media-item {\n    position: relative;\n    border: 1px solid var(--line);\n    border-radius: var(--r-tray);\n    overflow: hidden;\n    background: var(--surface);\n    display: flex;\n    flex-direction: column;\n    height: max-content;\n    min-width: 0;\n    transition: border-color 150ms var(--ease);\n  }\n  .media-item.batch-selectable { cursor: pointer; user-select: none; }\n  .media-item.batch-selectable .media-thumb-wrap { cursor: pointer; }\n  .media-item.batch-selected {\n    border-color: var(--blue);\n    box-shadow: 0 0 0 2px var(--blue-soft);\n  }\n  .batch-check-badge {\n    position: absolute;\n    top: 8px;\n    left: 8px;\n    z-index: 4;\n    width: 24px;\n    height: 24px;\n    border-radius: 50%;\n    background: rgba(13, 19, 31, 0.56);\n    backdrop-filter: blur(6px);\n    -webkit-backdrop-filter: blur(6px);\n    border: 1.5px solid rgba(255, 255, 255, 0.78);\n    color: transparent;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    pointer-events: none;\n    transition: background-color var(--t-fast) var(--ease), border-color var(--t-fast) var(--ease), transform var(--t-fast) var(--ease);\n  }\n  .media-item.llm-card .batch-check-badge { top: 10px; right: 10px; left: auto; }\n  .media-item.batch-selected .batch-check-badge {\n    background: var(--blue);\n    border-color: #FFFFFF;\n    color: #FFFFFF;\n    transform: scale(1.04);\n  }\n\n  .media-thumb-wrap {\n    width: 100%;\n    aspect-ratio: 2 / 3;\n    background: var(--card-2);\n    overflow: hidden;\n    position: relative;\n    cursor: zoom-in;\n  }\n  .media-thumb-wrap.video-wrap { aspect-ratio: 2 / 3; cursor: default; }\n  .media-thumb-wrap img, .media-thumb-wrap video { width: 100%; height: 100%; object-fit: cover; display: block; -webkit-user-drag: none; user-select: none; }\n\n  .media-footer {\n    padding: 9px 11px;\n    display: flex;\n    flex-direction: column;\n    gap: 6px;\n    border-top: 1px solid var(--line);\n    min-width: 0;\n  }\n  .media-top-row {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 6px;\n    width: 100%;\n    min-width: 0;\n  }\n  .media-info {\n    display: flex;\n    flex-direction: column;\n    gap: 2px;\n    min-width: 0;\n    flex: 1 1 auto;\n    overflow: hidden;\n  }\n  .media-info strong {\n    font-size: 12px;\n    font-weight: 600;\n    color: var(--fg);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n  }\n  .media-info span {\n    font-size: 11px;\n    color: var(--muted);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n  }\n  .media-icon-actions { display: inline-flex; align-items: center; gap: 5px; flex-shrink: 0; }\n\n  .img-prompt-box {\n    padding: 8px 11px 10px;\n    border-top: 1px solid var(--line);\n    background: var(--card-2);\n    font-size: 11.5px;\n    min-width: 0;\n  }\n  .img-prompt-head {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 6px;\n    min-height: 24px;\n    margin-bottom: 4px;\n  }\n  .img-prompt-meta {\n    font-size: 11px;\n    color: var(--muted);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n    user-select: none;\n  }\n  .img-prompt-meta[data-action] {\n    cursor: pointer;\n  }\n  .img-prompt-actions { display: inline-flex; align-items: center; gap: 4px; flex-shrink: 0; margin-left: auto; }\n  .img-prompt-actions .btn-xs { height: 24px; padding: 0 7px; font-size: 11px; border-radius: 6px; }\n  .img-prompt-text {\n    color: var(--fg-secondary);\n    display: -webkit-box;\n    -webkit-line-clamp: 2;\n    -webkit-box-orient: vertical;\n    overflow: hidden;\n    line-height: 1.54;\n    overflow-wrap: anywhere;\n    word-break: break-word;\n    border-radius: 5px;\n    transition: color var(--t-fast) var(--ease);\n  }\n  .img-prompt-text[data-action] {\n    cursor: pointer;\n  }\n\n  .text-card {\n    margin: 0;\n    padding: 12px 14px;\n    font-size: 12.5px;\n    line-height: 1.6;\n    white-space: pre-wrap;\n    overflow-wrap: anywhere;\n    word-break: break-word;\n    height: 164px;\n    overflow-y: auto;\n    font-family: inherit;\n    color: var(--fg);\n    background: transparent;\n  }\n\n  /* Synchronized Top & Bottom Pagination Bars (Unified Studio .btn-xs Geometry) */\n  .pagination-bar {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    flex-wrap: wrap;\n    gap: 10px;\n    padding-top: 10px;\n    border-top: 1px solid var(--line);\n    min-width: 0;\n  }\n  .pagination-bar.pagination-top {\n    padding-top: 0;\n    padding-bottom: 10px;\n    border-top: 0;\n    border-bottom: 1px solid var(--line);\n  }\n  .pagination-bar[hidden] { display: none; }\n  .pagination-meta-left {\n    display: inline-flex;\n    align-items: center;\n    gap: 8px;\n    min-width: 0;\n    flex-wrap: wrap;\n  }\n  .gallery-refresh-btn {\n    height: 26px;\n    padding: 0 9px;\n    border-radius: 7px;\n  }\n  .drive-sync-anchor {\n    position: relative;\n    display: inline-flex;\n    align-items: center;\n    flex-shrink: 0;\n  }\n  .drive-sync-trigger .ds-chevron {\n    width: 12px;\n    height: 12px;\n    flex: 0 0 12px;\n    opacity: 0.72;\n    transition: transform var(--t) var(--ease);\n  }\n  .drive-sync-trigger[aria-expanded="true"] {\n    background: var(--blue-soft);\n    color: var(--blue);\n  }\n  .drive-sync-trigger[aria-expanded="true"] .ds-chevron {\n    transform: rotate(180deg);\n  }\n  .drive-sync-picker {\n    position: absolute;\n    top: calc(100% + 7px);\n    right: 0;\n    left: auto;\n    z-index: 92;\n    width: min(308px, calc(100vw - 24px));\n    padding: 12px 13px;\n    background: var(--surface-elevated);\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    box-shadow: var(--shadow-float);\n    display: flex;\n    flex-direction: column;\n    gap: 10px;\n    opacity: 0;\n    visibility: hidden;\n    pointer-events: none;\n    transform: translateY(-4px) scale(0.985);\n    transform-origin: top right;\n    transition: opacity 140ms var(--ease), transform 140ms var(--ease), visibility 0s linear 140ms;\n  }\n  .drive-sync-picker.open {\n    opacity: 1;\n    visibility: visible;\n    pointer-events: auto;\n    transform: none;\n    transition: opacity 140ms var(--ease), transform 140ms var(--ease), visibility 0s linear 0s;\n  }\n  .ds-pop-head {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n    padding-bottom: 8px;\n    border-bottom: 1px solid var(--line);\n  }\n  .ds-pop-title {\n    font-size: 13px;\n    font-weight: 600;\n    color: var(--fg);\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n  }\n  .ds-folder-link {\n    display: inline-flex;\n    align-items: center;\n    gap: 4px;\n    height: 24px;\n    padding: 0 8px;\n    border-radius: 6px;\n    background: var(--card-2);\n    color: var(--fg-secondary);\n    font-size: 11.5px;\n    font-weight: 500;\n    text-decoration: none;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .ds-folder-link:hover {\n    background: var(--hover);\n    color: var(--blue);\n  }\n  .ds-stats-grid {\n    display: grid;\n    grid-template-columns: repeat(2, minmax(0, 1fr));\n    gap: 6px;\n  }\n  .ds-stat-box {\n    padding: 8px 10px;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    display: flex;\n    flex-direction: column;\n    gap: 2px;\n    min-width: 0;\n  }\n  .ds-stat-label {\n    font-size: 11px;\n    color: var(--muted);\n    white-space: nowrap;\n  }\n  .ds-stat-val {\n    font-size: 13px;\n    font-weight: 600;\n    color: var(--fg);\n    font-variant-numeric: tabular-nums;\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n  }\n  .ds-status-note {\n    font-size: 11.5px;\n    color: var(--muted);\n    line-height: 1.42;\n    padding: 0 2px;\n  }\n  .ds-status-note.is-syncing { color: var(--blue); font-weight: 500; }\n  .ds-status-note.is-pending { color: var(--amber); font-weight: 500; }\n  .ds-status-note.is-error { color: var(--red); font-weight: 500; }\n  .ds-action-col {\n    display: flex;\n    flex-direction: column;\n    gap: 6px;\n  }\n  .ds-action-btn {\n    width: 100%;\n    height: auto;\n    min-height: 44px;\n    padding: 8px 11px;\n    border: 0;\n    border-radius: var(--r-tray);\n    background: var(--card-2);\n    color: var(--fg);\n    font-family: inherit;\n    text-align: left;\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 10px;\n    cursor: pointer;\n    user-select: none;\n    transition: background-color var(--t-fast) var(--ease), box-shadow var(--t-fast) var(--ease), opacity var(--t-fast) var(--ease);\n  }\n  .ds-action-btn:disabled { opacity: 0.48; cursor: not-allowed; }\n  .ds-action-btn:active:not(:disabled) { background: var(--press); }\n  @media (hover: hover) and (pointer: fine) {\n    .ds-action-btn:not(:disabled):hover {\n      background: var(--hover);\n      box-shadow: inset 0 0 0 1px var(--line-strong);\n    }\n  }\n  .ds-action-btn.is-busy {\n    background: var(--blue-soft);\n    box-shadow: inset 0 0 0 1px var(--blue-border);\n  }\n  .ds-action-left {\n    display: flex;\n    align-items: center;\n    gap: 9px;\n    min-width: 0;\n  }\n  .ds-action-text {\n    display: flex;\n    flex-direction: column;\n    gap: 1.5px;\n    min-width: 0;\n  }\n  .ds-action-title {\n    font-size: 12.5px;\n    font-weight: 600;\n    color: var(--fg);\n  }\n  .ds-action-sub {\n    font-size: 11px;\n    color: var(--muted);\n    white-space: nowrap;\n    overflow: hidden;\n    text-overflow: ellipsis;\n  }\n  .pagination-bar.pagination-top .works-only-sync {\n    display: none !important;\n  }\n  @media (max-width: 1024px) {\n    .creation-right .pagination-bar.pagination-top .works-only-sync,\n    #worksStandalonePane .works-body.mob-works-collapsed .pagination-bar.pagination-top .works-only-sync {\n      display: inline-flex !important;\n    }\n  }\n  .pagination-info { font-size: 12px; color: var(--muted); }\n  .pagination-pages { display: inline-flex; align-items: center; gap: 4px; flex-wrap: wrap; }\n  .page-btn {\n    min-width: 30px;\n    height: 28px;\n    padding: 0 9px;\n    border-radius: 7px;\n    border: 0;\n    background: var(--card-2);\n    color: var(--fg-secondary);\n    font-family: inherit;\n    font-size: 12px;\n    font-weight: 500;\n    cursor: pointer;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .page-btn:disabled { opacity: 0.38; cursor: not-allowed; }\n  .page-btn.active { background: var(--blue); color: #FFFFFF; }\n  .page-ellipsis { padding: 0 4px; color: var(--muted); font-size: 12px; }\n\n  /* ==================== 9. Popovers, Lightbox, Toast & Studio Modals ==================== */\n  /* Random 1~10 Count Popover & Warmup Popover */\n  .random-count-picker {\n    position: absolute;\n    bottom: calc(100% + 8px);\n    right: 0;\n    margin: 0;\n    z-index: 85;\n    width: min(326px, 100%);\n    padding: 12px 14px 14px;\n    background: var(--surface-elevated);\n    color: var(--fg);\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    box-shadow: var(--shadow-float);\n    display: none;\n    flex-direction: column;\n    gap: 10px;\n  }\n  .random-count-picker.open {\n    display: flex;\n    animation: studioPopIn 120ms cubic-bezier(0.2, 0, 0, 1);\n  }\n  .rand-pop-head { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }\n  .rand-pop-title { font-size: 13px; font-weight: 600; color: var(--fg); }\n  .rand-pop-sub { font-size: 11px; color: var(--muted); }\n  .rand-count-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 6px; }\n  .rand-count-btn {\n    width: 100%;\n    height: 34px;\n    padding: 0;\n    border: 0;\n    border-radius: 7px;\n    background: var(--card-2);\n    color: var(--fg);\n    font-family: inherit;\n    line-height: 1;\n    display: flex;\n    align-items: center;\n    justify-content: center;\n    gap: 2.5px;\n    cursor: pointer;\n    user-select: none;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease), transform 110ms var(--ease);\n  }\n  .rand-count-btn:active { transform: scale(0.95); }\n  .rand-count-btn .rc-num { font-size: 13px; font-weight: 600; line-height: 1; font-variant-numeric: tabular-nums; display: inline-flex; align-items: center; }\n  .rand-count-btn .rc-unit { font-size: 11.5px; font-weight: 400; line-height: 1; color: var(--muted); display: inline-flex; align-items: center; }\n  .rand-count-btn.is-last { background: var(--blue-soft); color: var(--blue); }\n  .rand-count-btn.is-last .rc-unit { color: var(--blue); }\n\n  .warmup-picker {\n    position: absolute;\n    top: calc(100% + 8px);\n    right: 0;\n    z-index: 60;\n    width: min(400px, 100%);\n    margin: 0;\n    padding: 14px 16px;\n    background: var(--surface-elevated);\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    box-shadow: var(--shadow-float);\n    opacity: 0;\n    visibility: hidden;\n    pointer-events: none;\n    transform: translateY(-4px) scale(0.985);\n    transform-origin: top right;\n    transition: opacity 140ms var(--ease), transform 140ms var(--ease), visibility 0s linear 140ms;\n  }\n  .warmup-picker.open {\n    opacity: 1;\n    visibility: visible;\n    pointer-events: auto;\n    transform: none;\n    transition: opacity 140ms var(--ease), transform 140ms var(--ease), visibility 0s linear 0s;\n  }\n  .warmup-title { font-size: 13.5px; font-weight: 600; color: var(--fg); }\n  .warmup-options { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; margin: 10px 0 8px; }\n  .warmup-options label {\n    display: flex;\n    align-items: center;\n    gap: 8px;\n    padding: 8px 10px;\n    border-radius: var(--r-ctl);\n    background: var(--card-2);\n    font-size: 12.5px;\n    font-weight: 500;\n    cursor: pointer;\n    user-select: none;\n    transition: background-color var(--t-fast) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .warmup-options label:has(input:checked) { background: var(--blue-soft); color: var(--blue); }\n  .warmup-hint { font-size: 11.5px; color: var(--muted); margin: 6px 0 10px; line-height: 1.45; }\n  .warmup-picker .dialog-actions { flex-wrap: wrap; align-items: center; }\n  #warmupHint { margin: 0 auto 0 0; }\n  #warmupHint:empty { display: none; }\n\n  .cu-badge-anchor {\n    position: relative;\n    display: inline-flex;\n    min-width: 0;\n  }\n  .cu-badge-anchor > #cuBadge {\n    width: 100%;\n  }\n  .account-picker {\n    position: absolute;\n    top: calc(100% + 6px);\n    right: 0;\n    left: auto;\n    z-index: 95;\n    width: min(392px, calc(100vw - 24px));\n    margin: 0;\n    padding: 14px 16px;\n    background: var(--surface-elevated);\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    box-shadow: var(--shadow-float);\n    opacity: 0;\n    visibility: hidden;\n    pointer-events: none;\n    transform: translateY(-4px) scale(0.985);\n    transform-origin: top right;\n    transition: opacity 140ms var(--ease), transform 140ms var(--ease), visibility 0s linear 140ms;\n  }\n  .account-picker.open {\n    opacity: 1;\n    visibility: visible;\n    pointer-events: auto;\n    transform: none;\n    transition: opacity 140ms var(--ease), transform 140ms var(--ease), visibility 0s linear 0s;\n  }\n  .acct-head {\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 8px;\n  }\n  .acct-head-left {\n    display: inline-flex;\n    align-items: baseline;\n    gap: 8px;\n    min-width: 0;\n  }\n  .acct-total-cu {\n    font-size: 11.5px;\n    font-weight: 500;\n    color: var(--muted);\n    font-variant-numeric: tabular-nums;\n  }\n  .acct-list {\n    display: flex;\n    flex-direction: column;\n    gap: 7px;\n    margin: 10px 0;\n  }\n  .acct-row {\n    width: 100%;\n    padding: 9px 11px;\n    border: 0;\n    border-radius: var(--r-tray);\n    background: var(--card-2);\n    color: var(--fg);\n    font-family: inherit;\n    text-align: left;\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 10px;\n    cursor: pointer;\n    user-select: none;\n    transition: background-color var(--t-fast) var(--ease), box-shadow var(--t-fast) var(--ease), opacity var(--t-fast) var(--ease);\n  }\n  .acct-row:disabled { opacity: 0.55; cursor: wait; }\n  .acct-row.is-active {\n    background: var(--blue-soft);\n    box-shadow: inset 0 0 0 1px var(--blue-border);\n  }\n  @media (hover: hover) and (pointer: fine) {\n    .acct-row:not(.is-active):not(:disabled):hover { background: var(--hover); }\n  }\n  .acct-row:active:not(:disabled) { background: var(--press); }\n  .acct-main {\n    display: flex;\n    flex-direction: column;\n    gap: 3px;\n    min-width: 0;\n    flex: 1 1 auto;\n  }\n  .acct-email-line {\n    display: flex;\n    align-items: center;\n    flex-wrap: wrap;\n    gap: 5px;\n    min-width: 0;\n  }\n  .acct-email {\n    font-size: 12.5px;\n    font-weight: 600;\n    color: var(--fg);\n    overflow: hidden;\n    text-overflow: ellipsis;\n    white-space: nowrap;\n    max-width: 100%;\n  }\n  .acct-row.is-active .acct-email { color: var(--blue); }\n  .acct-tag {\n    display: inline-flex;\n    align-items: center;\n    height: 18px;\n    padding: 0 6px;\n    border-radius: 5px;\n    font-size: 10.5px;\n    font-weight: 500;\n    line-height: 1;\n    background: var(--card-3);\n    color: var(--fg-secondary);\n    white-space: nowrap;\n  }\n  .acct-tag.primary {\n    background: var(--blue);\n    color: #FFFFFF;\n  }\n  .acct-sub {\n    font-size: 11px;\n    color: var(--muted);\n    font-variant-numeric: tabular-nums;\n  }\n  .acct-sub.online { color: var(--green); font-weight: 500; }\n  .acct-right {\n    display: flex;\n    align-items: center;\n    gap: 8px;\n    flex-shrink: 0;\n  }\n  .acct-bal {\n    font-size: 13px;\n    font-weight: 600;\n    color: var(--fg);\n    font-variant-numeric: tabular-nums;\n    white-space: nowrap;\n  }\n  .acct-check {\n    width: 15px;\n    height: 15px;\n    color: var(--blue);\n    opacity: 0;\n    flex-shrink: 0;\n  }\n  .acct-row.is-active .acct-check { opacity: 1; }\n  .acct-oauth-box {\n    margin: 8px 0 10px;\n    padding: 10px 11px;\n    border-radius: var(--r-tray);\n    background: var(--card-2);\n    display: flex;\n    flex-direction: column;\n    gap: 8px;\n  }\n  .acct-oauth-box[hidden] { display: none !important; }\n  .acct-oauth-row {\n    display: flex;\n    align-items: center;\n    gap: 6px;\n  }\n  .acct-oauth-inp {\n    flex: 1 1 auto;\n    min-width: 0;\n    height: 32px;\n    padding: 0 10px;\n    font-size: 12px;\n  }\n  .account-picker .dialog-actions { flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 8px; }\n  #acctPickerHint { margin: 0; flex: 1 1 auto; }\n  #acctPickerHint:empty { display: none; }\n\n  /* Fullscreen Lossless 3K Lightbox */\n  body.lightbox-lock { overflow: hidden; }\n  body.lightbox-lock .float-dock { visibility: hidden; pointer-events: none; }\n  .lightbox {\n    position: fixed;\n    inset: 0;\n    z-index: 100;\n    background: rgba(8, 13, 22, 0.94);\n    display: none;\n    align-items: center;\n    justify-content: center;\n    overflow: hidden;\n    touch-action: none;\n  }\n  .lightbox.open { display: flex; }\n  .lightbox-stage {\n    width: 100vw;\n    height: 100dvh;\n    display: flex;\n    align-items: center;\n    justify-content: center;\n    overflow: hidden;\n    touch-action: none;\n    cursor: grab;\n  }\n  .lightbox-stage.dragging { cursor: grabbing; }\n  .lightbox-stage img {\n    max-width: 96vw;\n    max-height: 96dvh;\n    border-radius: var(--r-chip);\n    object-fit: contain;\n    user-select: none;\n    touch-action: none;\n    transform-origin: center center;\n    box-shadow: 0 24px 64px rgba(0, 0, 0, 0.58);\n  }\n  .lightbox-hud {\n    position: fixed;\n    top: 16px;\n    left: 50%;\n    transform: translateX(-50%);\n    z-index: 103;\n    padding: 5px 12px;\n    border-radius: var(--r-pill);\n    background: rgba(15, 23, 42, 0.72);\n    backdrop-filter: blur(10px);\n    -webkit-backdrop-filter: blur(10px);\n    border: 1px solid rgba(255, 255, 255, 0.14);\n    color: #F8FAFC;\n    font-size: 11.5px;\n    font-weight: 500;\n    pointer-events: none;\n    white-space: nowrap;\n    max-width: calc(100vw - 32px);\n    overflow: hidden;\n    text-overflow: ellipsis;\n  }\n  .lightbox-nav {\n    position: fixed;\n    top: 50%;\n    transform: translateY(-50%);\n    z-index: 102;\n    width: 42px;\n    height: 42px;\n    border-radius: 50%;\n    border: 1px solid rgba(255, 255, 255, 0.20);\n    background: rgba(15, 23, 42, 0.62);\n    color: #FFFFFF;\n    font-size: 24px;\n    line-height: 1;\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    cursor: pointer;\n    user-select: none;\n    padding: 0 0 2px;\n    transition: background-color var(--t-fast) var(--ease), border-color var(--t-fast) var(--ease);\n  }\n  .lightbox-nav.prev { left: 20px; }\n  .lightbox-nav.next { right: 20px; }\n  .lightbox-nav[hidden] { display: none; }\n  @media (max-width: 640px), (hover: none) and (pointer: coarse) {\n    .lightbox-nav { display: none !important; }\n  }\n\n  /* Studio Modals & Toast */\n  .confirm-dialog {\n    width: 400px;\n    max-width: calc(100vw - 28px);\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    padding: 18px 20px;\n    background: var(--surface-elevated);\n    color: var(--fg);\n    box-shadow: var(--shadow-modal);\n  }\n  .confirm-dialog::backdrop, .settings-dialog::backdrop, .prompt-dialog::backdrop {\n    background: var(--backdrop-bg);\n  }\n  .confirm-dialog h2 { margin: 0 0 8px; font-size: 15.5px; font-weight: 600; }\n  .confirm-dialog p { margin: 0 0 18px; font-size: 12.5px; color: var(--fg-secondary); line-height: 1.6; white-space: pre-wrap; }\n  .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; }\n\n  .prompt-dialog {\n    width: min(500px, calc(100vw - 28px));\n    max-width: calc(100vw - 28px);\n    max-height: min(78dvh, 580px);\n    padding: 0;\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    background: var(--surface-elevated);\n    color: var(--fg);\n    box-shadow: var(--shadow-modal);\n    overflow: hidden;\n  }\n  .prompt-dialog:not([open]) { display: none; }\n  .prompt-dialog[open] { display: flex; flex-direction: column; }\n  .prompt-dialog-head {\n    flex: 0 0 auto;\n    padding: 13px 16px 11px;\n    border-bottom: 1px solid var(--line);\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 10px;\n  }\n  .prompt-dialog-titles { display: flex; flex-direction: column; gap: 2px; min-width: 0; }\n  .prompt-dialog-title { margin: 0; font-size: 14px; font-weight: 600; color: var(--fg); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n  .prompt-dialog-sub { font-size: 11.5px; color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }\n  .prompt-dialog-body {\n    flex: 1 1 auto;\n    min-height: 0;\n    overflow-y: auto;\n    padding: 14px 16px;\n    font-size: 13.5px;\n    line-height: 1.68;\n    color: var(--fg);\n    white-space: pre-wrap;\n    overflow-wrap: anywhere;\n    word-break: break-word;\n    user-select: text;\n    -webkit-user-select: text;\n    -webkit-overflow-scrolling: touch;\n  }\n  .prompt-dialog-foot {\n    flex: 0 0 auto;\n    padding: 11px 16px;\n    border-top: 1px solid var(--line);\n    display: flex;\n    align-items: center;\n    justify-content: flex-end;\n    gap: 8px;\n  }\n\n  .toast {\n    position: fixed;\n    bottom: 24px;\n    left: 50%;\n    transform: translateX(-50%) translateY(8px);\n    background: var(--surface-elevated);\n    color: var(--fg);\n    border: 1px solid var(--line-strong);\n    box-shadow: var(--shadow-float);\n    padding: 8px 16px;\n    max-width: calc(100vw - 32px);\n    border-radius: var(--r-tray);\n    font-size: 12.5px;\n    font-weight: 500;\n    pointer-events: none;\n    opacity: 0;\n    transition: opacity 150ms var(--ease), transform 150ms var(--ease);\n    z-index: 120;\n    text-align: center;\n  }\n  .toast.visible { opacity: 1; transform: translateX(-50%) translateY(0); }\n\n  .settings-dialog {\n    width: min(860px, calc(100vw - 28px));\n    max-width: calc(100vw - 28px);\n    max-height: min(88dvh, 800px);\n    padding: 0;\n    border: 1px solid var(--line-strong);\n    border-radius: var(--r-card);\n    background: var(--surface-elevated);\n    color: var(--fg);\n    box-shadow: var(--shadow-modal);\n    overflow: visible;\n  }\n  .settings-dialog:not([open]) { display: none; }\n  .settings-dialog[open] {\n    display: flex;\n    flex-direction: column;\n  }\n  .settings-shell { display: flex; flex-direction: column; width: 100%; max-height: min(88dvh, 800px); min-height: 0; border-radius: calc(var(--r-card) - 1px); overflow: hidden; }\n  .settings-head {\n    flex: 0 0 auto;\n    padding: 14px 20px 12px;\n    border-bottom: 1px solid var(--line);\n    background: var(--surface-elevated);\n    display: flex;\n    flex-direction: column;\n    gap: 12px;\n  }\n  .settings-head-top { display: flex; align-items: center; justify-content: space-between; gap: 12px; }\n  .settings-title-wrap { display: flex; flex-direction: column; gap: 2px; min-width: 0; }\n  .settings-title { margin: 0; font-size: 15.5px; font-weight: 600; color: var(--fg); }\n  .settings-sub { margin: 0; font-size: 12px; color: var(--muted); }\n  .settings-close-btn { width: 28px; height: 28px; padding: 0; flex: 0 0 28px; border-radius: 7px; }\n  .settings-body {\n    flex: 1 1 auto;\n    overflow-y: auto;\n    padding: 16px 20px;\n    display: flex;\n    flex-direction: column;\n    gap: 14px;\n    min-height: 0;\n    -webkit-overflow-scrolling: touch;\n  }\n  .settings-pane { display: none; flex-direction: column; gap: 14px; min-width: 0; }\n  .settings-pane.active { display: flex; }\n  .settings-block {\n    border: 1px solid var(--line);\n    border-radius: var(--r-tray);\n    padding: 14px 16px;\n    display: flex;\n    flex-direction: column;\n    gap: 12px;\n    min-width: 0;\n  }\n  .settings-block-head { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; }\n  .settings-block-title { font-size: 13px; font-weight: 600; color: var(--fg); }\n  .settings-field { display: flex; flex-direction: column; gap: 5px; min-width: 0; }\n  .settings-label {\n    font-size: 12px;\n    font-weight: 500;\n    color: var(--fg-secondary);\n    display: flex;\n    align-items: baseline;\n    justify-content: space-between;\n    gap: 6px;\n    flex-wrap: wrap;\n  }\n  .settings-hint { font-size: 11.5px; font-weight: 400; color: var(--muted); line-height: 1.45; }\n  .settings-input { width: 100%; height: 34px; padding: 0 10px; font-size: 12.5px; min-width: 0; }\n  .settings-textarea { width: 100%; padding: 8px 11px; font-size: 12.5px; line-height: 1.5; resize: vertical; min-width: 0; }\n  .settings-grid-5 { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; }\n  .settings-grid-6 { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 8px; }\n  .settings-grid-2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }\n  .replace-pairs-list { display: flex; flex-direction: column; gap: 6px; max-height: 240px; overflow-y: auto; padding-right: 2px; }\n  .replace-pair-row { display: grid; grid-template-columns: minmax(0, 1fr) 22px minmax(0, 1fr) auto; align-items: center; gap: 6px; }\n  .replace-arrow { text-align: center; color: var(--muted); font-size: 13px; user-select: none; }\n\n  .settings-fold { border-top: 1px solid var(--line); padding-top: 10px; }\n  .settings-fold summary {\n    list-style: none;\n    cursor: pointer;\n    font-size: 12px;\n    font-weight: 500;\n    color: var(--fg-secondary);\n    user-select: none;\n    display: inline-flex;\n    align-items: center;\n    gap: 6px;\n    padding: 3px 0;\n    transition: color var(--t-fast) var(--ease);\n  }\n  .settings-fold summary::-webkit-details-marker { display: none; }\n  .settings-fold summary::marker { content: ""; }\n  .settings-fold .fold-chevron {\n    width: 14px;\n    height: 14px;\n    flex: 0 0 14px;\n    color: var(--muted);\n    transform: rotate(-90deg);\n    transition: transform var(--t) var(--ease), color var(--t-fast) var(--ease);\n  }\n  .settings-fold[open] .fold-chevron { transform: rotate(0deg); color: var(--blue); }\n  .settings-fold[open] summary { margin-bottom: 8px; color: var(--fg); }\n\n  .res-matrix, .tier-matrix { display: flex; flex-direction: column; gap: 8px; }\n  .res-row-item {\n    display: grid;\n    grid-template-columns: 74px minmax(0, 1fr) 18px minmax(0, 1fr) 26px minmax(0, 1fr) 18px minmax(0, 1fr);\n    align-items: center;\n    gap: 6px;\n    background: var(--card-2);\n    border-radius: var(--r-ctl);\n    padding: 8px 10px;\n  }\n  .tier-row-item {\n    display: grid;\n    grid-template-columns: 106px minmax(0, 1fr) 20px minmax(0, 1fr) minmax(96px, auto);\n    align-items: center;\n    gap: 8px;\n    background: var(--card-2);\n    border-radius: var(--r-ctl);\n    padding: 8px 10px;\n  }\n  .res-row-item .settings-input, .tier-row-item .settings-input { text-align: center; background: var(--surface); }\n  .row-tag { font-size: 12px; font-weight: 500; color: var(--fg); white-space: nowrap; }\n  .row-sym { text-align: center; font-size: 12px; color: var(--muted); }\n  .tier-total-pill {\n    font-size: 12px;\n    font-weight: 500;\n    color: var(--blue);\n    background: var(--blue-soft);\n    border-radius: 7px;\n    padding: 5px 10px;\n    text-align: center;\n    white-space: nowrap;\n  }\n  .settings-foot {\n    flex: 0 0 auto;\n    padding: 12px 20px;\n    border-top: 1px solid var(--line);\n    background: var(--surface-elevated);\n    display: flex;\n    align-items: center;\n    justify-content: space-between;\n    gap: 10px;\n    flex-wrap: wrap;\n  }\n  .settings-foot-left, .settings-foot-right { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }\n\n  @keyframes spinOnce { to { transform: rotate(360deg); } }\n  .is-spinning .ico { animation: spinOnce 520ms linear infinite; }\n\n  /* ==================== 10. Pointer Hover Precision ==================== */\n  @media (hover: hover) and (pointer: fine) {\n    .btn:not(.btn-primary):not(.btn-danger):not(.btn-selected):hover:not(:disabled) {\n      background: var(--hover);\n      color: var(--fg);\n    }\n    .btn-primary:hover:not(:disabled) { background: var(--blue-hover); }\n    .btn-danger:hover:not(:disabled),\n    .task-mini-btn.delete:hover:not(:disabled),\n    .task-mini-btn.stop:hover:not(:disabled) { background: var(--red-hover); color: var(--red); }\n    .btn-selected:hover:not(:disabled) { background: color-mix(in srgb, var(--blue) 16%, transparent); }\n\n    .seg-btn:not(.active):hover,\n    .tab-pill:not(.active):hover { color: var(--fg-secondary); }\n    .page-btn:not(.active):hover:not(:disabled) { background: var(--hover); color: var(--fg); }\n    .batch-elev-btn:hover:not(:disabled) { background: var(--surface); color: var(--fg); }\n    .img-prompt-text[data-action]:hover,\n    .img-prompt-meta[data-action]:hover { color: var(--fg); }\n\n    .mod-input:hover:not(:focus),\n    .settings-input:hover:not(:focus),\n    .settings-textarea:hover:not(:focus),\n    .z-select:not(.open) .z-select-trigger:hover,\n    .search-wrap:hover:not(:focus-within) {\n      background-color: var(--hover);\n      box-shadow: inset 0 0 0 1px var(--line-strong);\n    }\n    .z-select-opt:not(.selected):hover,\n    .z-select-opt.focused:not(.selected) { background: var(--hover); color: var(--fg); }\n    .rand-count-btn:hover { background: var(--blue-soft); color: var(--blue); }\n    .rand-count-btn:hover .rc-unit { color: var(--blue); }\n    .warmup-options label:hover { background: var(--hover); }\n    .settings-fold summary:hover { color: var(--blue); }\n    .status-pill[role="button"]:hover { background: var(--hover); }\n\n    .task-item-wrap:not(.running):not(.queued):hover { background: var(--hover); }\n    .task-mini-btn:not(.stop):not(.delete):hover:not(:disabled) { background: var(--hover); color: var(--fg); }\n    .ref-thumb-card:not(.selected):hover { box-shadow: 0 0 0 1.5px var(--line-strong); }\n    .media-item:hover { border-color: var(--line-strong); }\n    .lightbox-nav:hover { background: var(--blue); border-color: var(--blue); }\n    .back-top-btn:hover {\n      border-color: var(--blue-border);\n      color: var(--blue);\n    }\n  }\n\n  /* ==================== 11. Hover & Responsive Breakpoints (Desktop / Tablet / Mobile 5-Tab Bottom Nav) ==================== */\n  @media (hover: hover) and (pointer: fine) {\n    .side-nav-btn:not(.active):hover { background: var(--hover); color: var(--fg); }\n    .side-nav-btn:not(.active):hover .ico { color: var(--fg-secondary); }\n    .side-icon-btn:hover { background: var(--hover); color: var(--fg); }\n    .hdr-cu-btn:hover { background: var(--hover); }\n    .hdr-cu-btn[aria-expanded="true"]:hover { background: var(--blue-soft); }\n    .status-pill[role="button"]:hover { background: var(--hover); }\n    .status-pill.on[role="button"]:hover { background: var(--green-soft); filter: brightness(0.97); }\n    .status-pill.off[role="button"]:hover { background: var(--red-soft); filter: brightness(0.97); }\n    .side-logout-btn:hover { background: var(--red-soft); color: var(--red); }\n    .global-task-pill:hover { background: var(--hover); color: var(--fg); }\n    .global-task-pill.is-running:hover { background: var(--blue-soft); filter: brightness(0.97); }\n  }\n\n  @media (max-width: 1500px) {\n    .page-header-sub { display: none; }\n  }\n\n  @media (min-width: 1025px) and (max-width: 1320px) {\n    :root {\n      --sidebar-w: 198px;\n      --task-col-w: 332px;\n      --shell-gap: 12px;\n    }\n    .top-status-card { padding: 11px 14px; }\n    #g4Badge { width: 104px; padding: 0 8px; }\n    #cuBadge { width: 148px; padding: 0 8px; }\n    #startG4Btn, #warmupG4Btn { width: 70px; padding: 0 6px; }\n    #globalStopMiniBtn { width: 44px; padding: 0 5px; }\n    #globalTaskPill { width: 136px; max-width: 136px; padding: 0 8px; }\n  }\n\n  /* Tablet & Mobile (<= 1024px): Locked 100% App-Shell + Internal Scroll (Zero Safari Fixed Bottom Mask / Auto-Lift) */\n  @media (max-width: 1024px) {\n    html, body {\n      height: 100%;\n      overflow: hidden;\n      overscroll-behavior: none;\n    }\n    .app-shell {\n      position: relative;\n      flex-direction: column;\n      height: 100%;\n      min-height: 0;\n      overflow: hidden;\n    }\n    .app-sidebar { display: none !important; }\n    .app-content {\n      flex: 1 1 auto;\n      min-height: 0;\n      height: 100%;\n      margin-left: 0;\n      overflow-y: auto;\n      overflow-x: hidden;\n      -webkit-overflow-scrolling: touch;\n      overscroll-behavior-y: contain;\n      padding: 10px 10px 66px;\n      gap: 10px;\n    }\n    .mob-header-brand { display: inline-flex; }\n    .mob-fold-top-btn { display: inline-flex; }\n    .mob-only-btn { display: inline-flex; }\n    .page-header-sub { display: none; }\n    .hdr-sep { display: none; }\n    .mob-bottom-nav { display: block; }\n\n    .top-status-card { padding: 11px 12px; gap: 10px; }\n    .top-card-row1 { gap: 8px; }\n    .top-card-left { flex: 1 1 auto; }\n    .top-card-right {\n      margin-left: 0;\n      display: contents;\n    }\n    .global-task-group { order: 2; }\n    .global-task-pill { width: 136px; max-width: 136px; justify-content: center; }\n    .mob-fold-top-btn { order: 3; }\n    .top-compute-group {\n      order: 4;\n      flex: 0 0 100%;\n      width: 100%;\n      padding-top: 9px;\n      border-top: 1px solid var(--line);\n      display: grid;\n      grid-template-columns: repeat(2, minmax(0, 1fr));\n      gap: 6px;\n    }\n    .top-compute-group .status-pill,\n    .top-compute-group .hdr-cu-btn {\n      width: 100%;\n      justify-content: center;\n      height: 31px;\n      padding: 0 8px;\n      font-size: 11.5px;\n    }\n    .hdr-power-btns {\n      grid-column: 1 / -1;\n      display: grid;\n      grid-template-columns: repeat(3, minmax(0, 1fr));\n      gap: 6px;\n      width: 100%;\n    }\n    .hdr-power-btns .btn {\n      width: 100%;\n      height: 31px;\n      padding: 0 6px;\n      font-size: 12px;\n    }\n    .hdr-cu-btn .cu-acct-desk { display: none !important; }\n    .hdr-cu-btn .cu-acct-mob { display: inline; }\n\n    .top-status-card.mob-collapsed { gap: 0; }\n    .top-status-card.mob-collapsed .top-compute-group,\n    .top-status-card.mob-collapsed .topbar-hw {\n      display: none !important;\n    }\n\n    .app-stage,\n    .app-stage[data-task-mode="docked"] {\n      grid-template-columns: minmax(0, 1fr);\n      gap: 10px;\n    }\n    .app-stage[data-task-mode="docked"].task-collapsed .task-panel-wrap { display: flex; }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap { position: static; max-height: none; }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap .card-progress { max-height: none; }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap .task-ledger { max-height: 220px; }\n    .app-stage[data-task-mode="docked"] .task-panel-wrap .log-console { height: 164px; }\n\n    .app-stage[data-task-mode="drawer"] .task-panel-wrap {\n      top: auto;\n      left: 8px;\n      right: 8px;\n      bottom: 64px;\n      width: auto;\n      max-height: min(76dvh, 620px);\n      transform: translate3d(0, 28px, 0);\n    }\n    .app-stage[data-task-mode="drawer"].drawer-open .task-panel-wrap { transform: translate3d(0, 0, 0); }\n    .app-stage[data-task-mode="drawer"] .task-sheet-handle { display: block; }\n    .app-stage[data-task-mode="drawer"] .task-panel-wrap .card-progress { max-height: min(76dvh, 620px); height: auto; }\n    .app-stage[data-task-mode="drawer"] .task-panel-wrap .task-ledger { max-height: 200px; }\n    .app-stage[data-task-mode="drawer"] .task-panel-wrap .log-console { flex: 0 0 148px; height: 148px; }\n\n    .float-dock {\n      right: 14px;\n      bottom: 66px;\n    }\n    .toast {\n      bottom: 66px;\n    }\n    .settings-page-card .settings-foot { position: static; box-shadow: none; }\n  }\n\n  @media (max-width: 960px) and (min-width: 681px) {\n    .works-toolbar { grid-template-columns: minmax(0, 1fr); gap: 9px; }\n    .works-head-left { width: 100%; }\n  }\n\n  @media (max-width: 820px) {\n    .topbar-hw {\n      grid-template-columns: repeat(2, minmax(0, 1fr));\n      row-gap: 10px;\n    }\n    .hw-cell { padding: 0 10px; }\n    .hw-cell:nth-child(odd) { padding-left: 2px; }\n    .hw-cell:nth-child(even) { padding-right: 2px; border-right: 0; }\n    .hw-cell:nth-child(n+3) { padding-top: 9px; border-top: 1px solid var(--line); }\n  }\n\n  @media (max-width: 680px) {\n    .page-header-title { font-size: 14.5px; }\n    .global-task-pill { height: 30px; width: 122px; max-width: 122px; padding: 0 8px; font-size: 11.5px; justify-content: center; }\n    #globalStopMiniBtn { height: 30px; padding: 0 8px; font-size: 11.5px; }\n    .card { padding: 13px 12px; gap: 10px; }\n    .desk-only { display: none; }\n\n    .param-row { grid-template-columns: 54px minmax(0, 1fr); height: auto; min-height: 38px; gap: 7px; }\n    .param-row .seg-group { height: auto; padding: 3px; }\n    .param-row .seg-btn { height: auto; min-height: 36px; padding: 4px 2px; font-size: 11.5px; flex-direction: column; gap: 2px; }\n    .option-value, .option-hint { display: block; margin-left: 0; }\n    .option-hint { font-size: 10px; }\n    .ref-strip-head { flex-direction: column; align-items: stretch; gap: 6px; }\n    .ref-head-actions { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; }\n\n    .action-spacer { display: none; }\n    .action-bar { display: grid; grid-template-columns: 1fr 1fr 1.35fr; gap: 8px; }\n    .creation-body[data-mod="image"] .action-bar,\n    .creation-body[data-mod="video"] .action-bar { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n    .creation-body[data-mod="video"] .action-bar #runBtn { grid-column: 1 / -1; }\n    .action-bar > .btn, #randomRunBtn, #randomVideoBtn, #videoPromptBtn, #runBtn { width: 100%; min-width: 0; height: 38px; }\n\n    #pageCreation .creation-body:not([data-mod="llm"]) .mod-input.active { height: 92px; padding: 8px 10px; }\n    #pageCreation .creation-body[data-mod="llm"] .chat-box { min-height: 200px; max-height: 44dvh; }\n    #pageCreation .creation-body[data-mod="llm"] #promptInputLlm.active { height: 76px; padding: 8px 10px; }\n    .ref-strip-list { min-height: 70px; }\n    .ref-thumb-card { flex: 0 0 54px; width: 54px; height: 68px; }\n\n    .card-works {\n      background: transparent;\n      border: 0;\n      border-radius: 0;\n      padding: 0 !important;\n      box-shadow: none;\n      gap: 10px;\n    }\n    .works-top-card,\n    .works-gallery-card {\n      background: var(--surface);\n      border: 1px solid var(--line);\n      border-radius: var(--r-card);\n      padding: 11px 12px;\n      box-shadow: var(--shadow-card);\n      gap: 10px;\n    }\n    .works-toolbar { display: flex; flex-direction: column; align-items: stretch; gap: 9px; padding-bottom: 0; border-bottom: 0; }\n    .works-head-left { flex-direction: column; align-items: stretch; gap: 8px; width: 100%; }\n    .tabs-bar { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); height: auto; gap: 4px; width: 100%; }\n    .tabs-bar .tab-pill { height: 32px; }\n    .works-filter-group { display: flex; flex-direction: column; align-items: stretch; gap: 7px; width: 100%; }\n    .search-wrap { width: 100%; height: 34px; }\n    .works-filter-group > .search-wrap { width: 100%; min-width: 0; padding: 0 10px; gap: 7px; }\n    .works-actions-inline { display: grid !important; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 6px; width: 100%; }\n    .works-actions-inline > .drive-sync-anchor { width: 100%; min-width: 0; }\n    .works-actions-inline > .drive-sync-anchor > .btn { width: 100%; }\n    .pagination-top > .pagination-info { flex: 0 0 100%; }\n    .pagination-top .pagination-controls-row { flex-basis: 100%; gap: 6px; }\n    .works-actions-inline .btn { width: 100%; min-width: 0; padding: 0 6px; gap: 5px; justify-content: center; }\n    .works-actions-inline #toggleBatchBtn { min-width: 0; }\n    .works-actions-inline .drive-sync-trigger .control-label { min-width: 0; overflow: hidden; text-overflow: ellipsis; }\n    .batch-toolbar { flex-direction: column; align-items: stretch; gap: 6px; padding: 7px; }\n    .batch-sep { display: none; }\n    .batch-left { flex-direction: column; align-items: stretch; gap: 6px; width: 100%; }\n    .batch-quick-group { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 5px; width: 100%; }\n    .batch-quick-group .batch-elev-btn { width: 100%; padding: 0 4px; font-size: 11.5px; }\n    .batch-right { display: grid; grid-template-columns: 1fr 1fr auto; gap: 5px; width: 100%; }\n    .batch-right .btn { width: 100%; }\n\n    .settings-page-card .settings-head,\n    .settings-page-card .settings-body,\n    .settings-page-card .settings-foot { padding-left: 12px; padding-right: 12px; }\n    .settings-grid-5 { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n    .settings-grid-6 { grid-template-columns: repeat(3, minmax(0, 1fr)); }\n    .settings-grid-2 { grid-template-columns: minmax(0, 1fr); }\n    .res-row-item { grid-template-columns: minmax(0, 1fr) 16px minmax(0, 1fr); }\n    .res-row-item .row-tag, .res-row-item .res-arrow-sep { grid-column: 1 / -1; text-align: left; }\n    .tier-row-item { grid-template-columns: minmax(0, 1fr) 18px minmax(0, 1fr); }\n    .tier-row-item .row-tag, .tier-row-item .tier-total-pill { grid-column: 1 / -1; }\n    .settings-foot { flex-direction: column; align-items: stretch; gap: 8px; }\n    .settings-foot-left, .settings-foot-right { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); width: 100%; gap: 6px; }\n    .settings-foot .btn { width: 100%; min-width: 0; }\n  }\n\n  @media (max-width: 560px) {\n    .gallery-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }\n    .media-footer { padding: 8px 10px; gap: 6px; }\n    .media-top-row { flex-direction: column; align-items: stretch; gap: 6px; }\n    .media-info { width: 100%; }\n    .media-icon-actions { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 5px; width: 100%; }\n    .media-icon-actions .btn { width: 100%; padding: 0 4px; font-size: 11px; }\n    .img-prompt-box { padding: 7px 9px 9px; }\n    .img-prompt-head { gap: 4px; }\n    .img-prompt-meta { font-size: 10.5px; }\n    .img-prompt-actions { gap: 3px; }\n    .img-prompt-actions .btn-xs { padding: 0 5.5px; height: 23px; font-size: 10.5px; }\n  }\n\n  @container works-card (max-width: 1020px) {\n    .gallery-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }\n  }\n  @container works-card (max-width: 1080px) {\n    .works-toolbar { grid-template-columns: minmax(0, 1fr); gap: 9px; }\n    .works-head-left { width: 100%; }\n  }\n  @container works-card (max-width: 660px) {\n    .gallery-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }\n    .gallery-grid.single-col { grid-template-columns: minmax(0, 1fr); }\n  }\n  @container works-card (max-width: 560px) {\n    .works-filter-group { display: flex; flex-direction: column; align-items: stretch; gap: 7px; }\n    .works-filter-group > .search-wrap { width: 100%; }\n    .works-actions-inline { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 6px; width: 100%; }\n    .works-actions-inline > .drive-sync-anchor,\n    .works-actions-inline > .drive-sync-anchor > .btn,\n    .works-actions-inline .btn { width: 100%; min-width: 0; justify-content: center; }\n    .works-actions-inline #toggleBatchBtn { min-width: 0; }\n  }\n  @container works-card (max-width: 359px) {\n    .gallery-grid { grid-template-columns: minmax(0, 1fr); }\n  }\n  @container settings-card (max-width: 740px) {\n    .settings-grid-5 { grid-template-columns: repeat(3, minmax(0, 1fr)); }\n    .settings-grid-6 { grid-template-columns: repeat(3, minmax(0, 1fr)); }\n    .res-row-item { grid-template-columns: minmax(0, 1fr) 16px minmax(0, 1fr); }\n    .res-row-item .row-tag, .res-row-item .res-arrow-sep { grid-column: 1 / -1; text-align: left; }\n    .tier-row-item { grid-template-columns: minmax(0, 1fr) 18px minmax(0, 1fr); }\n    .tier-row-item .row-tag, .tier-row-item .tier-total-pill { grid-column: 1 / -1; }\n  }\n  @container settings-card (max-width: 540px) {\n    .settings-grid-5 { grid-template-columns: repeat(2, minmax(0, 1fr)); }\n    .settings-grid-2 { grid-template-columns: minmax(0, 1fr); }\n    .settings-foot { flex-direction: column; align-items: stretch; gap: 8px; }\n  }\n\n  @media (prefers-reduced-motion: reduce) {\n    *, *::before, *::after {\n      animation-duration: 0.01ms !important;\n      animation-iteration-count: 1 !important;\n      transition-duration: 0.01ms !important;\n    }\n  }\n  </style>\n</head>\n<body>\n<!-- SVG 图标符号库（统一视觉规范：16/18px 线性图标，颜色走语义色，无 Emoji 装饰） -->\n<svg xmlns="http://www.w3.org/2000/svg" style="width:0;height:0;position:absolute;overflow:hidden;pointer-events:none" aria-hidden="true">\n  <symbol id="i-refresh" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></symbol>\n  <symbol id="i-power" viewBox="0 0 24 24"><path d="M12 2v10"/><path d="M18.4 6.6a9 9 0 1 1-12.77.04"/></symbol>\n  <symbol id="i-bolt" viewBox="0 0 24 24"><path d="M13 2 3 14h8l-1 8 10-12h-8l1-8z"/></symbol>\n  <symbol id="i-sliders" viewBox="0 0 24 24"><line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/><line x1="2" y1="14" x2="6" y2="14"/><line x1="10" y1="8" x2="14" y2="8"/><line x1="18" y1="16" x2="22" y2="16"/></symbol>\n  <symbol id="i-external" viewBox="0 0 24 24"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></symbol>\n  <symbol id="i-sun" viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m6.34 17.66-1.41 1.41"/><path d="m19.07 4.93-1.41 1.41"/></symbol>\n  <symbol id="i-moon" viewBox="0 0 24 24"><path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/></symbol>\n  <symbol id="i-monitor" viewBox="0 0 24 24"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></symbol>\n  <symbol id="i-image" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="9" cy="9" r="2"/><path d="m21 15-3.086-3.086a2 2 0 0 0-2.828 0L6 21"/></symbol>\n  <symbol id="i-video" viewBox="0 0 24 24"><rect x="2" y="5" width="14" height="14" rx="2"/><path d="m22 7-6 5 6 5V7z"/></symbol>\n  <symbol id="i-sparkles" viewBox="0 0 24 24"><path d="m12 3-1.9 5.8a2 2 0 0 1-1.3 1.3L3 12l5.8 1.9a2 2 0 0 1 1.3 1.3L12 21l1.9-5.8a2 2 0 0 1 1.3-1.3L21 12l-5.8-1.9a2 2 0 0 1-1.3-1.3Z"/></symbol>\n  <symbol id="i-upload" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></symbol>\n  <symbol id="i-download" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></symbol>\n  <symbol id="i-trash" viewBox="0 0 24 24"><path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></symbol>\n  <symbol id="i-grid" viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/></symbol>\n  <symbol id="i-coins" viewBox="0 0 24 24"><circle cx="8" cy="8" r="6"/><path d="M18.09 10.37A6 6 0 1 1 10.34 18"/></symbol>\n  <symbol id="i-plus" viewBox="0 0 24 24"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></symbol>\n  <symbol id="i-close" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></symbol>\n  <symbol id="i-cpu" viewBox="0 0 24 24"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6" rx="1"/><path d="M9 1v3"/><path d="M15 1v3"/><path d="M9 20v3"/><path d="M15 20v3"/><path d="M20 9h3"/><path d="M20 14h3"/><path d="M1 9h3"/><path d="M1 14h3"/></symbol>\n  <symbol id="i-ram" viewBox="0 0 24 24"><path d="M6 19v-3"/><path d="M10 19v-3"/><path d="M14 19v-3"/><path d="M18 19v-3"/><path d="M8 11V9"/><path d="M16 11V9"/><path d="M12 11V9"/><path d="M2 15h20"/><path d="M2 7a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2Z"/></symbol>\n  <symbol id="i-gpu" viewBox="0 0 24 24"><rect x="3" y="6" width="18" height="12" rx="2"/><circle cx="9" cy="12" r="2.5"/><circle cx="15.5" cy="12" r="2.5"/><path d="M7 18v3"/><path d="M17 18v3"/></symbol>\n  <symbol id="i-disk" viewBox="0 0 24 24"><line x1="22" y1="12" x2="2" y2="12"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/><line x1="6" y1="16" x2="6.01" y2="16"/><line x1="10" y1="16" x2="10.01" y2="16"/></symbol>\n  <symbol id="i-search" viewBox="0 0 24 24"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></symbol>\n  <symbol id="i-logout" viewBox="0 0 24 24"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></symbol>\n  <symbol id="i-dice" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="3"/><circle cx="8.5" cy="8.5" r="1.25" fill="currentColor" stroke="none"/><circle cx="15.5" cy="8.5" r="1.25" fill="currentColor" stroke="none"/><circle cx="12" cy="12" r="1.25" fill="currentColor" stroke="none"/><circle cx="8.5" cy="15.5" r="1.25" fill="currentColor" stroke="none"/><circle cx="15.5" cy="15.5" r="1.25" fill="currentColor" stroke="none"/></symbol>\n  <symbol id="i-arrow-up" viewBox="0 0 24 24"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></symbol>\n  <symbol id="i-chevron" viewBox="0 0 24 24"><polyline points="6 9 12 15 18 9"/></symbol>\n  <symbol id="i-check" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></symbol>\n  <symbol id="i-check-square" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="3"/><path d="m9 12 2 2 4-4"/></symbol>\n  <symbol id="i-list" viewBox="0 0 24 24"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></symbol>\n  <symbol id="i-cloud" viewBox="0 0 24 24"><path d="M17.5 19H9a7 7 0 1 1 6.71-9h1.79a4.5 4.5 0 1 1 0 9Z"/></symbol>\n  <symbol id="i-send" viewBox="0 0 24 24"><path d="m22 2-7 20-4-9-9-4Z"/><path d="M22 2 11 13"/></symbol>\n  <symbol id="i-panel-right" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="15" y1="3" x2="15" y2="21"/></symbol>\n</svg>\n\n<div class="app-shell" id="appShell">\n  <!-- ==================== 桌面端悬浮卡片式左侧导航（5 栏） ==================== -->\n  <aside class="app-sidebar" id="appSidebar" aria-label="主导航">\n    <div class="side-head">\n      <div class="brand" data-action="brand-home" role="button" tabindex="0" aria-label="返回工作台首页">\n        <span class="brand-mark" aria-hidden="true"><svg class="brand-mark-svg" viewBox="68 48 896 904" aria-hidden="true"><path d="M 642 238 A 308 308 0 1 0 464 786" fill="none" stroke="#377DF6" stroke-width="138" stroke-linecap="round"/><path d="M 304 338 A 182 182 0 0 0 284 568" fill="none" stroke="#64BAFF" stroke-width="44" stroke-linecap="round" opacity="0.92"/><path d="M 456 336 C 469 402, 490 423, 556 436 C 490 449, 469 470, 456 536 C 443 470, 422 449, 356 436 C 422 423, 443 402, 456 336 Z" fill="#F6BA2A"/><path d="M 786 352 L 558 688 L 904 688" fill="none" stroke="#4A90FF" stroke-width="116" stroke-linecap="round" stroke-linejoin="round"/><path d="M 786 352 L 786 888" fill="none" stroke="#2260DF" stroke-width="116" stroke-linecap="round"/></svg></span>\n        <div class="brand-text">\n          <span class="brand-title">G4 工作台</span>\n        </div>\n      </div>\n      <button type="button" id="refreshStatusBtn" class="side-icon-btn" data-action="reload-panel" title="刷新面板并保持当前位置" aria-label="刷新面板">\n        <svg class="ico ico-14" aria-hidden="true"><use href="#i-refresh"/></svg>\n      </button>\n    </div>\n\n    <nav class="side-nav-scroll" aria-label="功能分区">\n      <div class="side-nav-group">\n        <div class="side-nav-label">创作</div>\n        <button type="button" class="side-nav-btn active" data-action="nav-page" data-page="image" aria-current="page">\n          <svg class="ico" aria-hidden="true"><use href="#i-image"/></svg>\n          <span class="side-nav-text">3K 生图</span>\n        </button>\n        <button type="button" class="side-nav-btn" data-action="nav-page" data-page="video">\n          <svg class="ico" aria-hidden="true"><use href="#i-video"/></svg>\n          <span class="side-nav-text">高清视频</span>\n        </button>\n        <button type="button" class="side-nav-btn" data-action="nav-page" data-page="llm">\n          <svg class="ico" aria-hidden="true"><use href="#i-sparkles"/></svg>\n          <span class="side-nav-text">提示词大师</span>\n        </button>\n      </div>\n\n      <div class="side-nav-group">\n        <div class="side-nav-label">内容</div>\n        <button type="button" class="side-nav-btn" data-action="nav-page" data-page="works">\n          <svg class="ico" aria-hidden="true"><use href="#i-grid"/></svg>\n          <span class="side-nav-text">作品库</span>\n          <span class="tab-count" id="cnt_total_works_desk">0</span>\n        </button>\n      </div>\n\n      <div class="side-nav-group">\n        <div class="side-nav-label">系统</div>\n        <button type="button" class="side-nav-btn" data-action="nav-page" data-page="settings">\n          <svg class="ico" aria-hidden="true"><use href="#i-sliders"/></svg>\n          <span class="side-nav-text">参数设置</span>\n        </button>\n      </div>\n    </nav>\n\n    <a id="comfyExtLink" href="#" target="_blank" rel="noopener noreferrer" class="side-comfy-link" title="打开远端 ComfyUI 节点界面" hidden>\n      <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-external"/></svg>\n      <span class="control-label">ComfyUI</span>\n    </a>\n\n    <div class="side-footer">\n      <span class="side-util-note">跟随系统主题</span>\n      <button type="button" class="side-logout-btn" data-action="logout-panel" title="退出登录">\n        <svg class="ico ico-14" aria-hidden="true"><use href="#i-logout"/></svg>\n        <span>退出</span>\n      </button>\n    </div>\n  </aside>\n\n  <!-- ==================== 右侧主工作区（统一顶部实例状态卡片 + 主内容） ==================== -->\n  <div class="app-content" id="appContent">\n    <header class="top-status-card" id="topStatusCard" aria-label="Colab 实例状态、算力账号与硬件监控">\n      <div class="top-card-row1">\n        <div class="top-card-left">\n          <div class="mob-header-brand" data-action="brand-home" role="button" tabindex="0" aria-label="返回工作台首页">\n            <span class="brand-mark" aria-hidden="true"><svg class="brand-mark-svg" viewBox="68 48 896 904" aria-hidden="true"><path d="M 642 238 A 308 308 0 1 0 464 786" fill="none" stroke="#377DF6" stroke-width="138" stroke-linecap="round"/><path d="M 304 338 A 182 182 0 0 0 284 568" fill="none" stroke="#64BAFF" stroke-width="44" stroke-linecap="round" opacity="0.92"/><path d="M 456 336 C 469 402, 490 423, 556 436 C 490 449, 469 470, 456 536 C 443 470, 422 449, 356 436 C 422 423, 443 402, 456 336 Z" fill="#F6BA2A"/><path d="M 786 352 L 558 688 L 904 688" fill="none" stroke="#4A90FF" stroke-width="116" stroke-linecap="round" stroke-linejoin="round"/><path d="M 786 352 L 786 888" fill="none" stroke="#2260DF" stroke-width="116" stroke-linecap="round"/></svg></span>\n          </div>\n          <h1 class="page-header-title" id="pageHeaderTitle">3K 生图</h1>\n          <span class="page-header-sub" id="pageHeaderSub">参考图挂载与超清直出</span>\n        </div>\n\n        <div class="top-card-right">\n          <div class="top-compute-group" aria-label="实例状态与算力账号控制">\n            <span id="g4Badge" class="status-pill" role="button" tabindex="0" data-action="refresh-status" aria-busy="false" title="点击刷新 Colab 实例状态">\n              <span class="dot" aria-hidden="true"></span>\n              <span id="g4Text"><span class="status-state">检测中</span></span>\n            </span>\n            <button type="button" id="cuBadge" class="hdr-cu-btn" data-action="toggle-account-picker" aria-expanded="false" aria-controls="accountPicker" title="点击查看或切换 Colab 算力账号">\n              <svg class="ico ico-14 ico-gold" aria-hidden="true"><use href="#i-coins"/></svg>\n              <span id="cuAcctDesk" class="cu-acct-desk" hidden></span>\n              <span id="cuAcctMob" class="cu-acct-mob" hidden></span>\n              <span id="cuAcctSep" class="status-sep" hidden>·</span>\n              <span id="cuText">-- CU</span>\n              <svg class="ico cu-chevron" aria-hidden="true"><use href="#i-chevron"/></svg>\n            </button>\n            <div class="hdr-power-btns">\n              <button type="button" class="btn btn-sm mob-only-btn" data-action="reload-panel" title="刷新面板">\n                <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-refresh"/></svg>\n                <span class="control-label">刷新</span>\n              </button>\n              <button type="button" id="startG4Btn" class="btn btn-sm" data-power-btn="start" data-action="toggle-power" aria-label="正在检查实例电源状态" aria-busy="true" disabled>\n                <svg class="ico ico-14 ico-green" aria-hidden="true"><use href="#i-power"/></svg>\n                <span class="control-label">检查中</span>\n              </button>\n              <button type="button" id="warmupG4Btn" class="btn btn-sm" data-power-btn="warmup" aria-expanded="false" aria-controls="warmupPicker" data-action="warmup-all" aria-label="预热设备" aria-busy="false" disabled>\n                <svg class="ico ico-14 ico-orange" aria-hidden="true"><use href="#i-bolt"/></svg>\n                <span class="control-label">预热</span>\n              </button>\n            </div>\n          </div>\n\n          <span class="hdr-sep" aria-hidden="true"></span>\n\n          <div class="global-task-group">\n            <button type="button" id="globalStopMiniBtn" class="btn btn-xs btn-danger" data-action="cancel-job" disabled title="中止当前运行任务">\n              <span class="control-label">中止</span>\n            </button>\n            <button type="button" id="globalTaskPill" class="global-task-pill is-panel-open" data-action="toggle-task-panel" aria-expanded="true" aria-controls="taskPanelWrap" title="展开或收起实时任务队列与日志">\n              <span class="nav-run-dot" hidden aria-hidden="true"></span>\n              <span class="global-task-pill-text" id="globalTaskPillText">任务队列</span>\n              <svg class="ico ico-14" aria-hidden="true"><use href="#i-panel-right"/></svg>\n            </button>\n          </div>\n\n          <button type="button" class="mob-fold-top-btn" id="mobFoldTopBtn" data-action="toggle-mob-top-card" aria-expanded="true" aria-label="收起顶部状态卡片" title="收起/展开顶部状态卡片">\n            <svg class="ico" aria-hidden="true"><use href="#i-chevron"/></svg>\n          </button>\n        </div>\n      </div>\n\n    <div id="hwMetricsBar" class="topbar-hw" aria-label="G4 实例实时硬件资源占用">\n      <div class="hw-cell">\n        <div class="hw-head">\n          <span class="hw-label"><svg class="ico ico-sky" aria-hidden="true"><use href="#i-cpu"/></svg><span>CPU</span></span>\n          <span class="hw-val" id="hwValCpu">--</span>\n        </div>\n        <div class="hw-bar-track"><div class="hw-bar-fill" id="hwBarCpu"></div></div>\n      </div>\n      <div class="hw-cell">\n        <div class="hw-head">\n          <span class="hw-label"><svg class="ico ico-green" aria-hidden="true"><use href="#i-ram"/></svg><span>内存</span></span>\n          <span class="hw-val" id="hwValRam">--</span>\n        </div>\n        <div class="hw-bar-track"><div class="hw-bar-fill" id="hwBarRam"></div></div>\n      </div>\n      <div class="hw-cell">\n        <div class="hw-head">\n          <span class="hw-label"><svg class="ico ico-gold" aria-hidden="true"><use href="#i-gpu"/></svg><span>显存</span></span>\n          <span class="hw-val" id="hwValVram">--</span>\n        </div>\n        <div class="hw-bar-track"><div class="hw-bar-fill" id="hwBarVram"></div></div>\n      </div>\n      <div class="hw-cell">\n        <div class="hw-head">\n          <span class="hw-label"><svg class="ico ico-purple" aria-hidden="true"><use href="#i-disk"/></svg><span>硬盘</span></span>\n          <span class="hw-val" id="hwValDisk">--</span>\n        </div>\n        <div class="hw-bar-track"><div class="hw-bar-fill" id="hwBarDisk"></div></div>\n      </div>\n    </div>\n\n\n    </header>\n\n    <main class="app-stage" id="appStage" data-task-mode="docked">\n      <div class="app-pages" id="appPages">\n        <!-- 页面 1/2/3：创作工作区 -->\n        <section class="page-view active" id="pageCreation" data-page-group="creation">\n          <section class="card card-main">\n            <div id="modSwitcher" hidden aria-hidden="true"></div>\n      <div class="creation-body" id="creationBody" data-mod="image">\n        <div id="paramPanel" class="param-panel">\n          <!-- 模块 1：3K 生图参数 -->\n          <div id="modSecImage" class="mod-section active">\n            <div class="param-grid">\n              <div class="param-row">\n                <span class="param-label">尺寸</span>\n                <div class="seg-group" id="segImgRes" role="group" aria-label="生图尺寸">\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="imgRes" data-val="portrait" aria-pressed="true"><span class="control-label"><span class="option-value" id="lblResPortrait">2048×3072</span> <span class="option-hint">竖图</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="imgRes" data-val="landscape" aria-pressed="false"><span class="control-label"><span class="option-value" id="lblResLandscape">3072×2048</span> <span class="option-hint">横图</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="imgRes" data-val="square" aria-pressed="false"><span class="control-label"><span class="option-value" id="lblResSquare">2560×2560</span> <span class="option-hint">方图</span></span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">步数</span>\n                <div class="seg-group" id="segImgSteps" role="group" aria-label="生图步数">\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="imgSteps" data-val="tier1" aria-pressed="false"><span class="control-label"><span class="option-value" id="lblStepsTier1">13 步</span> <span class="option-hint" id="hintStepsTier1">(8+5)</span></span></button>\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="imgSteps" data-val="tier2" aria-pressed="true"><span class="control-label"><span class="option-value" id="lblStepsTier2">18 步</span> <span class="option-hint" id="hintStepsTier2">(12+6 默认)</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="imgSteps" data-val="tier3" aria-pressed="false"><span class="control-label"><span class="option-value" id="lblStepsTier3">24 步</span> <span class="option-hint" id="hintStepsTier3">(16+8)</span></span></button>\n                </div>\n              </div>\n            </div>\n\n            <div class="ref-strip-box">\n              <div class="ref-strip-head">\n                <div class="ref-head-info">\n                  <span class="ref-head-title"><svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-image"/></svg>参考图</span>\n                  <span id="imgRefStatus" class="ref-status-text">未挂载（点选下方缩略图可锁定人脸参考）</span>\n                </div>\n                <div class="ref-head-actions">\n                  <button type="button" id="imgRefClearBtn" class="btn btn-xs btn-danger" data-action="pick-ref" data-mod="image" data-path="" disabled><span class="control-label">取消挂载</span></button>\n                  <button type="button" class="btn btn-xs" data-action="trigger-upload"><svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-upload"/></svg><span class="control-label">上传图片</span></button>\n                </div>\n              </div>\n              <div id="imgRefList" class="ref-strip-list" role="region" aria-label="生图参考图列表"></div>\n            </div>\n          </div>\n\n          <!-- 模块 2：命令改图（保留隐藏兼容） -->\n          <div id="modSecEdit" hidden style="display:none" class="mod-section">\n            <div class="param-grid">\n              <div class="param-row">\n                <span class="param-label">步数</span>\n                <div class="seg-group" id="segEditSteps" role="group" aria-label="改图步数">\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="editSteps" data-val="8" aria-pressed="true"><span class="control-label"><span class="option-value">8 步</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="editSteps" data-val="12" aria-pressed="false"><span class="control-label"><span class="option-value">12 步</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="editSteps" data-val="16" aria-pressed="false"><span class="control-label"><span class="option-value">16 步</span></span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">画质</span>\n                <div class="seg-group" id="segEditRefine" role="group" aria-label="改后画质增强">\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="editRefine" data-val="0" aria-pressed="true"><span class="control-label"><span class="option-value">原生直出</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="editRefine" data-val="1" aria-pressed="false"><span class="control-label"><span class="option-value">同尺寸精修</span></span></button>\n                </div>\n              </div>\n            </div>\n            <div class="ref-strip-box">\n              <div class="ref-strip-head">\n                <div class="ref-head-info">\n                  <span class="ref-head-title">原图</span>\n                  <span id="editRefStatus" class="ref-status-text">未选择原图</span>\n                </div>\n                <div class="ref-head-actions">\n                  <button type="button" id="editRefClearBtn" class="btn btn-xs btn-danger" data-action="pick-ref" data-mod="edit" data-path="" disabled><span class="control-label">取消选择</span></button>\n                </div>\n              </div>\n              <div id="editRefList" class="ref-strip-list" role="region" aria-label="命令改图原图列表"></div>\n            </div>\n          </div>\n\n          <!-- 模块 3：高清视频参数 -->\n          <div id="modSecVideo" class="mod-section">\n            <div class="param-grid">\n              <div class="param-row">\n                <span class="param-label">清晰度</span>\n                <div class="seg-group" id="segVidQuality" role="group" aria-label="视频清晰度">\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="vidQuality" data-val="std" aria-pressed="true"><span class="control-label"><span class="option-value">标准 832p</span> <span class="option-hint">(极速直出)</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidQuality" data-val="hd" aria-pressed="false"><span class="control-label"><span class="option-value">超清 1088p</span> <span class="option-hint">(3D超分)</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidQuality" data-val="uhd2k" aria-pressed="false"><span class="control-label"><span class="option-value">2K 精修</span> <span class="option-hint">(超分+重绘)</span></span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">画幅</span>\n                <div class="seg-group" id="segVidRes" role="group" aria-label="视频画幅">\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="vidRes" data-val="832x1248" aria-pressed="true"><span class="control-label"><span class="option-value" id="lblVidResP">832×1248</span> <span class="option-hint">竖屏 2:3</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidRes" data-val="1248x832" aria-pressed="false"><span class="control-label"><span class="option-value" id="lblVidResL">1248×832</span> <span class="option-hint">横屏 3:2</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidRes" data-val="1024x1024" aria-pressed="false"><span class="control-label"><span class="option-value" id="lblVidResS">1024×1024</span> <span class="option-hint">方屏 1:1</span></span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">时长</span>\n                <div class="seg-group" id="segVidLen" role="group" aria-label="视频时长">\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="vidLen" data-val="192" aria-pressed="true"><span class="control-label"><span class="option-value">8.0 秒</span> <span class="option-hint">(192帧)</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidLen" data-val="243" aria-pressed="false"><span class="control-label"><span class="option-value">10.1 秒</span> <span class="option-hint">243帧</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidLen" data-val="124" aria-pressed="false"><span class="control-label"><span class="option-value">5.2 秒</span> <span class="option-hint">124帧</span></span></button>\n                </div>\n              </div>\n              <div class="param-row">\n                <span class="param-label">步数</span>\n                <div class="seg-group" id="segVidSteps" role="group" aria-label="视频步数">\n                  <button type="button" class="seg-btn active" data-action="set-seg" data-key="vidSteps" data-val="8" aria-pressed="true"><span class="control-label"><span class="option-value">8 步</span> <span class="option-hint">(标准声画)</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidSteps" data-val="12" aria-pressed="false"><span class="control-label"><span class="option-value">12 步</span> <span class="option-hint">(细节精绘)</span></span></button>\n                  <button type="button" class="seg-btn" data-action="set-seg" data-key="vidSteps" data-val="6" aria-pressed="false"><span class="control-label"><span class="option-value">6 步</span> <span class="option-hint">(极速)</span></span></button>\n                </div>\n              </div>\n            </div>\n            <div class="ref-strip-box">\n              <div class="ref-strip-head">\n                <div class="ref-head-info">\n                  <span class="ref-head-title"><svg class="ico ico-14 ico-purple" aria-hidden="true"><use href="#i-video"/></svg>首帧图</span>\n                  <span id="vidRefStatus" class="ref-status-text">未挂载（点选下方缩略图可作为视频第 1 帧）</span>\n                </div>\n                <div class="ref-head-actions">\n                  <button type="button" id="vidRefClearBtn" class="btn btn-xs btn-danger" data-action="pick-ref" data-mod="video" data-path="" disabled><span class="control-label">取消挂载</span></button>\n                  <button type="button" class="btn btn-xs" data-action="trigger-upload"><svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-upload"/></svg><span class="control-label">上传图片</span></button>\n                </div>\n              </div>\n              <div id="vidRefList" class="ref-strip-list" role="region" aria-label="视频首帧图列表"></div>\n            </div>\n          </div>\n\n          <!-- 模块 4：提示词大师对话区 -->\n          <div id="modSecLlm" class="mod-section">\n            <div class="chat-toolbar">\n              <span id="chatCountLabel" class="chat-count-label">对话记录 (0 轮 · Qwen3.8 27B)</span>\n              <button type="button" class="btn btn-xs btn-danger" data-action="clear-chat"><svg class="ico ico-14" aria-hidden="true"><use href="#i-plus"/></svg><span class="control-label">开启新对话</span></button>\n            </div>\n            <div class="chat-box" id="chatBox"></div>\n          </div>\n        </div>\n\n        <div class="prompt-label-row">\n          <label id="activePromptLabel" class="prompt-label" for="promptInputImage">生图提示词</label>\n          <div class="prompt-meta-right">\n            <span id="promptCharCount" class="prompt-char-badge">0 字</span>\n            <span class="prompt-shortcut-hint desk-only">Ctrl / Cmd + Enter 提交</span>\n          </div>\n        </div>\n        <textarea id="promptInputImage" class="mod-input active" aria-label="生图提示词" placeholder="在此输入或粘贴生图提示词（各模块输入框完全独立）..."></textarea>\n        <textarea id="promptInputEdit" hidden style="display:none" class="mod-input" aria-label="命令改图指令" placeholder="先选中一张原图，再输入改图指令..."></textarea>\n        <textarea id="promptInputVideo" class="mod-input" aria-label="视频分镜提示词" placeholder="在此输入或粘贴 24fps 声画视频提示词（支持动作、运镜、呼吸/台词与环境音效）..."></textarea>\n        <textarea id="promptInputLlm" class="mod-input" aria-label="提示词大师对话" placeholder="告诉提示词大师你想要的场景、角色穿搭或原位修改指令..."></textarea>\n\n        <div class="action-bar">\n          <button type="button" class="btn btn-danger" id="cancelBtn" data-action="cancel-job" disabled><span class="control-label">中止</span></button>\n          <button type="button" class="btn" data-action="clear-prompt"><span class="control-label">清空</span></button>\n          <span class="action-spacer" aria-hidden="true"></span>\n          <button type="button" class="btn btn-selected" id="randomRunBtn" data-action="toggle-random-picker" aria-haspopup="dialog" aria-expanded="false" aria-controls="randomCountPicker" title="选择随机生成 1～10 张 3K 作品（仅面板内生成）"><svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-dice"/></svg><span class="control-label">随机生图</span><svg class="ico rand-chevron" aria-hidden="true"><use href="#i-chevron"/></svg></button>\n          <button type="button" class="btn btn-primary" id="runBtn" data-action="submit-task"><span class="control-label">提交生成</span></button>\n          <div id="randomCountPicker" class="random-count-picker" role="dialog" aria-label="选择随机生图张数" tabindex="-1">\n  <div class="rand-pop-head">\n    <span class="rand-pop-title">随机生成张数</span>\n    <span class="rand-pop-sub">点选即刻开始 · 仅面板内生成</span>\n  </div>\n  <div class="rand-count-grid">\n    <button type="button" class="rand-count-btn is-last" data-action="pick-random-count" data-count="1" aria-label="随机生成 1 张"><span class="rc-num">1</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="2" aria-label="随机生成 2 张"><span class="rc-num">2</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="3" aria-label="随机生成 3 张"><span class="rc-num">3</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="4" aria-label="随机生成 4 张"><span class="rc-num">4</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="5" aria-label="随机生成 5 张"><span class="rc-num">5</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="6" aria-label="随机生成 6 张"><span class="rc-num">6</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="7" aria-label="随机生成 7 张"><span class="rc-num">7</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="8" aria-label="随机生成 8 张"><span class="rc-num">8</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="9" aria-label="随机生成 9 张"><span class="rc-num">9</span><span class="rc-unit">张</span></button>\n    <button type="button" class="rand-count-btn" data-action="pick-random-count" data-count="10" aria-label="随机生成 10 张"><span class="rc-num">10</span><span class="rc-unit">张</span></button>\n  </div>\n</div>\n        </div>\n      </div>\n\n          </section>\n        </section>\n\n        <!-- 页面 4：作品库（单一真实 DOM） -->\n        <section class="page-view" id="pageWorks" data-page-group="works">\n          <section class="card card-works works-body" id="worksBody">\n            <div class="works-top-card">\n              <div class="works-toolbar">\n                <div class="works-head-left">\n        <div class="tabs-bar" role="group" aria-label="多媒体作品库分类">\n          <button type="button" class="tab-pill active" data-action="switch-tab" data-tab="image" aria-pressed="true"><span class="control-label">3K 图片</span> <span class="tab-count" id="cnt_image">0</span></button>\n          <button type="button" class="tab-pill" data-action="switch-tab" data-tab="video" aria-pressed="false"><span class="control-label">高清视频</span> <span class="tab-count" id="cnt_video">0</span></button>\n          <button type="button" class="tab-pill" data-action="switch-tab" data-tab="upload_image" aria-pressed="false"><span class="control-label">上传素材</span> <span class="tab-count" id="cnt_upload_image">0</span></button>\n          <button type="button" class="tab-pill" data-action="switch-tab" data-tab="llm" aria-pressed="false"><span class="control-label">提示词历史</span> <span class="tab-count" id="cnt_llm">0</span></button>\n        </div>\n      </div>\n\n      <div class="works-filter-group">\n        <label class="search-wrap" for="gallerySearchInput">\n          <svg class="ico ico-14" aria-hidden="true"><use href="#i-search"/></svg>\n          <input type="search" id="gallerySearchInput" class="filter-search" placeholder="搜索提示词 / 文件名..." aria-label="搜索作品">\n        </label>\n        <div class="works-actions-inline" style="display:inline-flex;align-items:center;gap:6px;">\n          <div class="drive-sync-anchor" hidden>\n            <button type="button" id="driveSyncToolbarTrigger" class="btn btn-sm drive-sync-trigger" data-action="toggle-drive-sync-picker" aria-expanded="false" title="查看云盘状态、同步谷歌云盘或同步到 Telegram 频道">\n              <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-cloud"/></svg>\n              <span class="control-label">谷歌同步</span>\n              <svg class="ico ds-chevron" aria-hidden="true"><use href="#i-chevron"/></svg>\n            </button>\n            <section class="drive-sync-picker" aria-label="谷歌加密云盘与频道同步" aria-hidden="true" inert>\n              <div class="ds-pop-head">\n                <span class="ds-pop-title">\n                  <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-cloud"/></svg>\n                  <span>谷歌加密云同步</span>\n                </span>\n                <a class="ds-folder-link" data-ds-role="folder-link" target="_blank" rel="noopener noreferrer" hidden>\n                  <span>打开云盘</span>\n                  <svg class="ico ico-14" aria-hidden="true"><use href="#i-external"/></svg>\n                </a>\n              </div>\n              <div class="ds-stats-grid">\n                <div class="ds-stat-box">\n                  <span class="ds-stat-label">Google Drive 加密库</span>\n                  <span class="ds-stat-val" data-ds-role="drive-count">-- 张</span>\n                </div>\n                <div class="ds-stat-box">\n                  <span class="ds-stat-label" data-ds-role="channel-label">Telegram 频道</span>\n                  <span class="ds-stat-val" data-ds-role="channel-count">-- 张</span>\n                </div>\n              </div>\n              <div class="ds-status-note" data-ds-role="status-note">正在读取同步状态...</div>\n              <div class="ds-action-col">\n                <button type="button" class="ds-action-btn" data-action="sync-drive" data-ds-role="btn-drive">\n                  <div class="ds-action-left">\n                    <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-cloud"/></svg>\n                    <div class="ds-action-text">\n                      <span class="ds-action-title" data-ds-role="btn-drive-title">仅同步谷歌云盘</span>\n                      <span class="ds-action-sub">加密上传新图并同步删除项，不发频道</span>\n                    </div>\n                  </div>\n                </button>\n                <button type="button" class="ds-action-btn" data-action="sync-channel" data-ds-role="btn-channel">\n                  <div class="ds-action-left">\n                    <svg class="ico ico-14 ico-sky" aria-hidden="true"><use href="#i-send"/></svg>\n                    <div class="ds-action-text">\n                      <span class="ds-action-title" data-ds-role="btn-channel-title">同步到 TG 频道</span>\n                      <span class="ds-action-sub">先同步谷歌云，再按 6 张相册更新频道</span>\n                    </div>\n                  </div>\n                </button>\n              </div>\n            </section>\n          </div>\n          <button type="button" id="toggleBatchBtn" class="btn btn-sm" data-action="toggle-batch-mode" aria-pressed="false">\n            <svg class="ico ico-14" aria-hidden="true"><use href="#i-check-square"/></svg>\n            <span class="control-label" id="toggleBatchBtnText">批量选择</span>\n          </button>\n          <button type="button" class="btn btn-sm btn-danger" data-action="delete-all-tab">\n            <svg class="ico ico-14" aria-hidden="true"><use href="#i-trash"/></svg>\n            <span class="control-label">清空分类</span>\n          </button>\n        </div>\n      </div>\n    </div>\n\n    <div id="batchToolbar" class="batch-toolbar" hidden>\n      <div class="batch-left">\n        <span class="batch-count-pill" id="batchCountPill">\n          <span class="b-dot" aria-hidden="true"></span>\n          <span id="batchCountLabel">已选 <strong>0</strong> 项</span>\n        </span>\n        <span class="batch-sep" aria-hidden="true"></span>\n        <div class="batch-quick-group">\n          <button type="button" class="batch-elev-btn" data-action="batch-select-page"><span class="control-label">全选本页</span></button>\n          <button type="button" class="batch-elev-btn" id="batchSelectAllBtn" data-action="batch-select-filtered"><span class="control-label" id="batchSelectAllText">全选列表</span></button>\n          <button type="button" class="batch-elev-btn" id="batchClearBtn" data-action="batch-clear" disabled><span class="control-label">清空已选</span></button>\n        </div>\n      </div>\n      <div class="batch-right">\n        <button type="button" class="btn btn-xs btn-primary" id="batchDownloadBtn" data-action="batch-download" disabled>\n          <svg class="ico ico-14" aria-hidden="true"><use href="#i-download"/></svg>\n          <span class="control-label" id="batchDownloadText">打包下载</span>\n        </button>\n        <button type="button" class="btn btn-xs btn-danger" id="batchDeleteBtn" data-action="batch-delete" disabled>\n          <svg class="ico ico-14" aria-hidden="true"><use href="#i-trash"/></svg>\n          <span class="control-label" id="batchDeleteText">批量删除</span>\n        </button>\n        <button type="button" class="batch-elev-btn" data-action="toggle-batch-mode" title="退出批量选择">\n          <span class="control-label">完成</span>\n        </button>\n      </div>\n    </div>\n    </div>\n\n    <div class="works-gallery-card" id="worksGalleryScroll">\n    <div id="galleryPaginationTop" class="pagination-bar pagination-top" data-pos="top">\n      <div class="pagination-meta-left">\n        <span id="paginationInfoTop" class="pagination-info">共 0 项</span>\n        <div class="pagination-meta-right-mob" style="display:inline-flex;align-items:center;gap:6px;">\n          <button type="button" class="btn btn-xs gallery-refresh-btn" data-action="refresh-outputs" title="重新加载作品列表">\n            <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-refresh"/></svg>\n            <span class="control-label">刷新</span>\n          </button>\n          <div class="drive-sync-anchor works-only-sync" hidden>\n            <button type="button" id="driveSyncInlineTrigger" class="btn btn-xs gallery-refresh-btn drive-sync-trigger" data-action="toggle-drive-sync-picker" aria-expanded="false" title="谷歌加密云同步与频道同步">\n              <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-cloud"/></svg>\n              <span class="control-label">谷歌同步</span>\n              <svg class="ico ds-chevron" aria-hidden="true"><use href="#i-chevron"/></svg>\n            </button>\n            <section class="drive-sync-picker" aria-label="谷歌加密云盘与频道同步" aria-hidden="true" inert>\n              <div class="ds-pop-head">\n                <span class="ds-pop-title">\n                  <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-cloud"/></svg>\n                  <span>谷歌加密云同步</span>\n                </span>\n                <a class="ds-folder-link" data-ds-role="folder-link" target="_blank" rel="noopener noreferrer" hidden>\n                  <span>打开云盘</span>\n                  <svg class="ico ico-14" aria-hidden="true"><use href="#i-external"/></svg>\n                </a>\n              </div>\n              <div class="ds-stats-grid">\n                <div class="ds-stat-box">\n                  <span class="ds-stat-label">Google Drive 加密库</span>\n                  <span class="ds-stat-val" data-ds-role="drive-count">-- 张</span>\n                </div>\n                <div class="ds-stat-box">\n                  <span class="ds-stat-label" data-ds-role="channel-label">Telegram 频道</span>\n                  <span class="ds-stat-val" data-ds-role="channel-count">-- 张</span>\n                </div>\n              </div>\n              <div class="ds-status-note" data-ds-role="status-note">正在读取同步状态...</div>\n              <div class="ds-action-col">\n                <button type="button" class="ds-action-btn" data-action="sync-drive" data-ds-role="btn-drive">\n                  <div class="ds-action-left">\n                    <svg class="ico ico-14 ico-blue" aria-hidden="true"><use href="#i-cloud"/></svg>\n                    <div class="ds-action-text">\n                      <span class="ds-action-title" data-ds-role="btn-drive-title">仅同步谷歌云盘</span>\n                      <span class="ds-action-sub">加密上传新图并同步删除项，不发频道</span>\n                    </div>\n                  </div>\n                </button>\n                <button type="button" class="ds-action-btn" data-action="sync-channel" data-ds-role="btn-channel">\n                  <div class="ds-action-left">\n                    <svg class="ico ico-14 ico-sky" aria-hidden="true"><use href="#i-send"/></svg>\n                    <div class="ds-action-text">\n                      <span class="ds-action-title" data-ds-role="btn-channel-title">同步到 TG 频道</span>\n                      <span class="ds-action-sub">先同步谷歌云，再按 6 张相册更新频道</span>\n                    </div>\n                  </div>\n                </button>\n              </div>\n            </section>\n          </div>\n        </div>\n      </div>\n      <div id="paginationPagesTop" class="pagination-pages"></div>\n    </div>\n\n    <div id="galleryContent" class="gallery-grid"></div>\n\n    <div id="galleryPagination" class="pagination-bar" data-pos="bottom" hidden>\n      <span id="paginationInfo" class="pagination-info"></span>\n      <div id="paginationPages" class="pagination-pages"></div>\n    </div>\n    </div>\n\n          </section>\n        </section>\n\n        <!-- 页面 5：参数设置（整页展开，桌面端与侧栏等高齐底） -->\n        <section class="page-view" id="pageSettings" data-page-group="settings">\n          <div class="card settings-page-card" id="settingsShell">\n            <div class="settings-head">\n      <div class="seg-group full" id="settingsTabsBar" role="tablist" aria-label="参数设置分类">\n        <button type="button" class="seg-btn active" role="tab" aria-selected="true" data-action="switch-settings-tab" data-settings-tab="llm"><span class="control-label">提示词大师</span></button>\n        <button type="button" class="seg-btn" role="tab" aria-selected="false" data-action="switch-settings-tab" data-settings-tab="image_prompt"><span class="control-label"><span class="desk-only">生图</span>词条与前后缀</span></button>\n        <button type="button" class="seg-btn" role="tab" aria-selected="false" data-action="switch-settings-tab" data-settings-tab="image_workflow"><span class="control-label"><span class="desk-only">生图</span>工作流参数</span></button>\n      </div>\n    </div>\n\n    <div class="settings-body" id="settingsBody">\n      <!-- 页签 1：提示词大师 -->\n      <div id="settingsPane_llm" class="settings-pane active" role="tabpanel">\n        <div class="settings-block">\n          <div class="settings-field">\n            <label class="settings-label" for="cfgLlmSystemPrompt">\n              <span>系统提示词全文 (System Prompt)</span>\n              <span class="settings-hint">保存时自动同步更新 llm_default_system.txt</span>\n            </label>\n            <textarea id="cfgLlmSystemPrompt" class="settings-textarea" style="height:280px;" spellcheck="false"></textarea>\n          </div>\n          <div class="settings-field">\n            <label class="settings-label" for="cfgLlmMultiTurnHint">\n              <span>多轮原位修改附加提示 (Multi-turn Hint)</span>\n              <span class="settings-hint">多轮对话修改时自动追加在用户消息末尾（留空则不追加）</span>\n            </label>\n            <textarea id="cfgLlmMultiTurnHint" class="settings-textarea" style="height:68px;" spellcheck="false"></textarea>\n          </div>\n        </div>\n\n        <div class="settings-block">\n          <div class="settings-block-title">采样推理参数与引导前缀</div>\n          <div class="settings-grid-5">\n            <div class="settings-field">\n              <label class="settings-label" for="cfgLlmTemp"><span>温度 Temp</span><span class="settings-hint">0~2</span></label>\n              <input type="number" id="cfgLlmTemp" class="settings-input" step="0.05" min="0" max="2">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgLlmMaxTokens"><span>最大长度</span><span class="settings-hint">128~8192</span></label>\n              <input type="number" id="cfgLlmMaxTokens" class="settings-input" step="64" min="128" max="8192">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgLlmTopP"><span>Top-P</span><span class="settings-hint">0.05~1.0</span></label>\n              <input type="number" id="cfgLlmTopP" class="settings-input" step="0.05" min="0.05" max="1">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgLlmTopK"><span>Top-K</span><span class="settings-hint">1~200</span></label>\n              <input type="number" id="cfgLlmTopK" class="settings-input" step="1" min="1" max="200">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgLlmRepeatPenalty"><span>重复惩罚</span><span class="settings-hint">0.8~2.0</span></label>\n              <input type="number" id="cfgLlmRepeatPenalty" class="settings-input" step="0.01" min="0.8" max="2">\n            </div>\n          </div>\n          <div class="settings-field">\n            <label class="settings-label" for="cfgLlmCheckPrefix">\n              <span>自检引导前缀 Check Prefix</span>\n              <span class="settings-hint">单行输入，使用 \\n 表示换行</span>\n            </label>\n            <input type="text" id="cfgLlmCheckPrefix" class="settings-input" spellcheck="false">\n          </div>\n          <div class="settings-field">\n            <label class="settings-label" for="cfgLlmStripTerms">\n              <span>后置自动剔除词条 (Strip Terms)</span>\n              <span class="settings-hint">每行一个词条，生成后自动从结果文本中剔除（保留行内标点）</span>\n            </label>\n            <textarea id="cfgLlmStripTerms" class="settings-textarea" style="height:88px;" spellcheck="false"></textarea>\n          </div>\n        </div>\n      </div>\n\n      <!-- 页签 2：生图词条与前后缀 -->\n      <div id="settingsPane_image_prompt" class="settings-pane" role="tabpanel">\n        <div class="settings-block">\n          <label class="settings-check">\n            <input type="checkbox" id="cfgImgEnableSuffix">\n            <span>启用生图自动追加后缀（原提示词未包含完整后缀时自动追加）</span>\n          </label>\n          <div class="settings-field">\n            <label class="settings-label" for="cfgImgZhSuffix"><span>中文追加后缀</span></label>\n            <textarea id="cfgImgZhSuffix" class="settings-textarea" style="height:68px;" spellcheck="false"></textarea>\n          </div>\n          <div class="settings-field">\n            <label class="settings-label" for="cfgImgEnSuffix"><span>英文追加后缀</span></label>\n            <textarea id="cfgImgEnSuffix" class="settings-textarea" style="height:68px;" spellcheck="false"></textarea>\n          </div>\n\n          <div style="border-top:1px solid var(--line);padding-top:10px;display:flex;flex-direction:column;gap:8px;">\n            <label class="settings-check">\n              <input type="checkbox" id="cfgImgEnablePrefix">\n              <span>启用生图自动前置词</span>\n            </label>\n            <div class="settings-grid-2">\n              <div class="settings-field">\n                <label class="settings-label" for="cfgImgZhPrefix"><span>中文前置词</span></label>\n                <input type="text" id="cfgImgZhPrefix" class="settings-input" placeholder="留空不加前置词">\n              </div>\n              <div class="settings-field">\n                <label class="settings-label" for="cfgImgEnPrefix"><span>英文前置词</span></label>\n                <input type="text" id="cfgImgEnPrefix" class="settings-input" placeholder="Optional English prefix">\n              </div>\n            </div>\n          </div>\n        </div>\n\n        <div class="settings-block">\n          <div class="settings-block-head">\n            <label class="settings-check">\n              <input type="checkbox" id="cfgImgEnableClean">\n              <span>启用词条替换与旧尾巴清洗</span>\n            </label>\n            <button type="button" class="btn btn-xs btn-selected" data-action="add-replace-pair"><span class="control-label">+ 添加替换规则</span></button>\n          </div>\n          <div class="settings-hint">按从上到下顺序精确替换；右侧「替换为」留空即直接删除左侧原词。</div>\n          <div id="cfgReplacePairsList" class="replace-pairs-list"></div>\n\n          <details class="settings-fold">\n            <summary><svg class="ico fold-chevron" aria-hidden="true"><use href="#i-chevron"/></svg><span>高级：旧尾巴剥离正则表达式（每行一条，匹配部分替换为句号）</span></summary>\n            <textarea id="cfgImgStripRegexes" class="settings-textarea" style="height:86px;" spellcheck="false"></textarea>\n            <div class="settings-hint" style="margin-top:4px;">执行时内置容错保护，单条正则语法有误会自动跳过。</div>\n          </details>\n        </div>\n\n        <div class="settings-block">\n          <div class="settings-block-title">参考图挂载面部引导与裁剪框</div>\n          <div class="settings-field">\n            <label class="settings-label" for="cfgImgRefFacePrompt">\n              <span>参考图面部引导词模板</span>\n              <span class="settings-hint">用 {prompt} 代表清洗后的正文提示词（未写则自动拼在末尾）</span>\n            </label>\n            <textarea id="cfgImgRefFacePrompt" class="settings-textarea" style="height:68px;" spellcheck="false"></textarea>\n          </div>\n          <div class="settings-grid-6">\n            <div class="settings-field">\n              <label class="settings-label" for="cfgRefScaleW"><span>缩放宽</span></label>\n              <input type="number" id="cfgRefScaleW" class="settings-input" step="16" min="256" max="4096">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgRefScaleH"><span>缩放高</span></label>\n              <input type="number" id="cfgRefScaleH" class="settings-input" step="16" min="256" max="4096">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgRefCropW"><span>裁剪宽</span></label>\n              <input type="number" id="cfgRefCropW" class="settings-input" step="16" min="256" max="4096">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgRefCropH"><span>裁剪高</span></label>\n              <input type="number" id="cfgRefCropH" class="settings-input" step="16" min="256" max="4096">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgRefCropX"><span>偏移 X</span></label>\n              <input type="number" id="cfgRefCropX" class="settings-input" step="1" min="0" max="4096">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgRefCropY"><span>偏移 Y</span></label>\n              <input type="number" id="cfgRefCropY" class="settings-input" step="1" min="0" max="4096">\n            </div>\n          </div>\n        </div>\n      </div>\n\n      <!-- 页签 3：生图工作流参数 -->\n      <div id="settingsPane_image_workflow" class="settings-pane" role="tabpanel">\n        <div class="settings-block">\n          <div class="settings-block-head">\n            <label class="settings-check">\n              <input type="checkbox" id="cfgWfEnableHires">\n              <span>启用二阶段超清精绘 (HiRes Stage 2)</span>\n            </label>\n            <span class="settings-hint">关闭或二阶段步数为 0 时，直接按目标宽高跑一阶段直出</span>\n          </div>\n          <div class="settings-grid-5">\n            <div class="settings-field">\n              <label class="settings-label" for="cfgWfDenoise"><span>精绘幅度 Denoise</span><span class="settings-hint">0.01~1.0</span></label>\n              <input type="number" id="cfgWfDenoise" class="settings-input" step="0.01" min="0.01" max="1">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgWfCfg"><span>CFG 引导</span><span class="settings-hint">0.1~20</span></label>\n              <input type="number" id="cfgWfCfg" class="settings-input" step="0.1" min="0.1" max="20">\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgWfSampler"><span>采样器 Sampler</span></label>\n              <select id="cfgWfSampler" class="settings-select">\n                <option value="euler">euler</option>\n                <option value="euler_ancestral">euler_ancestral</option>\n                <option value="dpmpp_2m">dpmpp_2m</option>\n                <option value="dpmpp_2m_sde">dpmpp_2m_sde</option>\n                <option value="uni_pc">uni_pc</option>\n                <option value="heun">heun</option>\n                <option value="deis">deis</option>\n              </select>\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgWfScheduler"><span>调度器 Scheduler</span></label>\n              <select id="cfgWfScheduler" class="settings-select">\n                <option value="simple">simple</option>\n                <option value="normal">normal</option>\n                <option value="karras">karras</option>\n                <option value="exponential">exponential</option>\n                <option value="sgm_uniform">sgm_uniform</option>\n                <option value="beta">beta</option>\n                <option value="ddim_uniform">ddim_uniform</option>\n              </select>\n            </div>\n            <div class="settings-field">\n              <label class="settings-label" for="cfgWfUpscale"><span>放大插值 Upscale</span></label>\n              <select id="cfgWfUpscale" class="settings-select">\n                <option value="lanczos">lanczos</option>\n                <option value="bicubic">bicubic</option>\n                <option value="bilinear">bilinear</option>\n                <option value="area">area</option>\n                <option value="nearest-exact">nearest-exact</option>\n              </select>\n            </div>\n          </div>\n        </div>\n\n        <div class="settings-block">\n          <div class="settings-block-head">\n            <span class="settings-block-title">三种画幅尺寸配置（一阶段构图宽×高 → 二阶段目标宽×高）</span>\n            <span class="settings-hint">范围 256~4096，保存自动按 16 倍数对齐并同步主界面按钮</span>\n          </div>\n          <div class="res-matrix">\n            <div class="res-row-item">\n              <span class="row-tag">竖图</span>\n              <input type="number" id="cfgRes_portrait_s1w" class="settings-input" step="16" min="256" max="4096" aria-label="竖图一阶段宽">\n              <span class="row-sym">×</span>\n              <input type="number" id="cfgRes_portrait_s1h" class="settings-input" step="16" min="256" max="4096" aria-label="竖图一阶段高">\n              <span class="row-sym res-arrow-sep">→</span>\n              <input type="number" id="cfgRes_portrait_s2w" class="settings-input" step="16" min="256" max="4096" aria-label="竖图二阶段目标宽">\n              <span class="row-sym">×</span>\n              <input type="number" id="cfgRes_portrait_s2h" class="settings-input" step="16" min="256" max="4096" aria-label="竖图二阶段目标高">\n            </div>\n            <div class="res-row-item">\n              <span class="row-tag">横图</span>\n              <input type="number" id="cfgRes_landscape_s1w" class="settings-input" step="16" min="256" max="4096" aria-label="横图一阶段宽">\n              <span class="row-sym">×</span>\n              <input type="number" id="cfgRes_landscape_s1h" class="settings-input" step="16" min="256" max="4096" aria-label="横图一阶段高">\n              <span class="row-sym res-arrow-sep">→</span>\n              <input type="number" id="cfgRes_landscape_s2w" class="settings-input" step="16" min="256" max="4096" aria-label="横图二阶段目标宽">\n              <span class="row-sym">×</span>\n              <input type="number" id="cfgRes_landscape_s2h" class="settings-input" step="16" min="256" max="4096" aria-label="横图二阶段目标高">\n            </div>\n            <div class="res-row-item">\n              <span class="row-tag">方图</span>\n              <input type="number" id="cfgRes_square_s1w" class="settings-input" step="16" min="256" max="4096" aria-label="方图一阶段宽">\n              <span class="row-sym">×</span>\n              <input type="number" id="cfgRes_square_s1h" class="settings-input" step="16" min="256" max="4096" aria-label="方图一阶段高">\n              <span class="row-sym res-arrow-sep">→</span>\n              <input type="number" id="cfgRes_square_s2w" class="settings-input" step="16" min="256" max="4096" aria-label="方图二阶段目标宽">\n              <span class="row-sym">×</span>\n              <input type="number" id="cfgRes_square_s2h" class="settings-input" step="16" min="256" max="4096" aria-label="方图二阶段目标高">\n            </div>\n          </div>\n        </div>\n\n        <div class="settings-block">\n          <div class="settings-block-head">\n            <span class="settings-block-title">三档步数拆分配置（一阶段构图步数 + 二阶段精绘步数）</span>\n            <span class="settings-hint">一阶段 1~60 步，二阶段 0~40 步（同步主界面按钮）</span>\n          </div>\n          <div class="tier-matrix">\n            <div class="tier-row-item">\n              <span class="row-tag">第 1 档</span>\n              <input type="number" id="cfgTier_tier1_s1" class="settings-input" step="1" min="1" max="60" aria-label="第1档一阶段步数">\n              <span class="row-sym">+</span>\n              <input type="number" id="cfgTier_tier1_ref" class="settings-input" step="1" min="0" max="40" aria-label="第1档二阶段步数">\n              <span class="tier-total-pill" id="cfgTierTotal_tier1">= 13 步</span>\n            </div>\n            <div class="tier-row-item">\n              <span class="row-tag">第 2 档 (默认)</span>\n              <input type="number" id="cfgTier_tier2_s1" class="settings-input" step="1" min="1" max="60" aria-label="第2档一阶段步数">\n              <span class="row-sym">+</span>\n              <input type="number" id="cfgTier_tier2_ref" class="settings-input" step="1" min="0" max="40" aria-label="第2档二阶段步数">\n              <span class="tier-total-pill" id="cfgTierTotal_tier2">= 18 步</span>\n            </div>\n            <div class="tier-row-item">\n              <span class="row-tag">第 3 档 (精修)</span>\n              <input type="number" id="cfgTier_tier3_s1" class="settings-input" step="1" min="1" max="60" aria-label="第3档一阶段步数">\n              <span class="row-sym">+</span>\n              <input type="number" id="cfgTier_tier3_ref" class="settings-input" step="1" min="0" max="40" aria-label="第3档二阶段步数">\n              <span class="tier-total-pill" id="cfgTierTotal_tier3">= 24 步</span>\n            </div>\n          </div>\n        </div>\n      </div>\n    </div>\n\n\n            <div class="settings-foot">\n              <div class="settings-foot-left">\n                <button type="button" class="btn btn-sm btn-danger mob-only-btn" data-action="logout-panel"><svg class="ico ico-14" aria-hidden="true"><use href="#i-logout"/></svg><span class="control-label">退出登录</span></button>\n                <button type="button" class="btn btn-sm" data-action="reset-settings-section"><span class="control-label">恢复当前页默认</span></button>\n                <button type="button" class="btn btn-sm btn-danger" data-action="reset-settings-all"><span class="control-label">恢复全部默认</span></button>\n              </div>\n              <div class="settings-foot-right">\n                <button type="button" class="btn btn-sm" data-action="reload-settings-form"><span class="control-label">放弃修改</span></button>\n                <button type="button" class="btn btn-primary btn-sm" id="saveSettingsBtn" data-action="save-settings" style="height:34px;padding:0 16px;"><span class="control-label">保存并立即生效</span></button>\n              </div>\n            </div>\n          </div>\n        </section>\n      </div>\n\n      <!-- 实时任务队列与终端日志（创作页右侧常驻 / 作品库与设置页滑出抽屉） -->\n      <aside class="task-panel-wrap" id="taskPanelWrap" aria-label="实时任务队列与日志">\n        <section class="card card-progress" id="progressCard">\n          <div class="task-sheet-handle" aria-hidden="true"></div>\n          <div class="card-head">\n            <div class="card-title">\n              <svg class="ico ico-blue" aria-hidden="true"><use href="#i-list"/></svg>\n              <span>实时任务队列</span>\n            </div>\n            <div class="card-head-actions">\n              <span id="jobTimer">空闲待命</span>\n              <button type="button" class="btn btn-xs btn-danger" id="clearTasksBtn" data-action="clear-tasks" disabled><span class="control-label">清空记录</span></button>\n              <button type="button" class="btn btn-xs task-drawer-close" id="closeTaskDrawerBtn" data-action="close-task-drawer" aria-label="关闭任务抽屉" title="收起 (Esc)">\n                <svg class="ico ico-14" aria-hidden="true"><use href="#i-close"/></svg>\n              </button>\n            </div>\n          </div>\n\n          <div class="progress-box">\n            <div class="progress-top">\n              <span class="progress-stage" id="progStageText">空闲待命 · 随时可提交任务</span>\n              <span class="progress-eta" id="progEtaText">待命</span>\n            </div>\n            <div class="progress-track" id="progTrack" role="progressbar" aria-label="任务阶段进度" aria-valuemin="0" aria-valuemax="100" aria-valuetext="待命">\n              <div class="progress-fill" id="progFillBar"></div>\n            </div>\n          </div>\n\n          <div class="task-ledger" id="taskLedgerBox">\n            <div class="task-empty">暂无任务记录（提交后在此查看实时进度、排队与历史记录）</div>\n          </div>\n\n          <div class="log-head">\n            <span class="log-label">实时终端日志</span>\n          </div>\n          <div class="log-console" id="logBox">等待提交任务...</div>\n        </section>\n      </aside>\n    </main>\n  </div>\n\n  <!-- 手机端底部 5 栏圆角卡片导航（锁视口内 absolute 透明容器，绝不触发 Safari fixed 遮罩/抬高） -->\n  <nav class="mob-bottom-nav" id="mobileBottomNav" aria-label="移动端主导航">\n    <div class="mob-nav-card">\n      <div class="mob-nav-track">\n        <span class="mob-nav-glider" id="mobNavGlider" aria-hidden="true"></span>\n        <button type="button" class="mob-nav-item active" data-action="nav-page" data-page="image" aria-current="page">\n          <span class="mob-nav-icon-wrap"><svg class="ico" aria-hidden="true"><use href="#i-image"/></svg></span>\n          <span class="mob-nav-lbl">生图</span>\n        </button>\n        <button type="button" class="mob-nav-item" data-action="nav-page" data-page="video">\n          <span class="mob-nav-icon-wrap"><svg class="ico" aria-hidden="true"><use href="#i-video"/></svg></span>\n          <span class="mob-nav-lbl">视频</span>\n        </button>\n        <button type="button" class="mob-nav-item" data-action="nav-page" data-page="llm">\n          <span class="mob-nav-icon-wrap"><svg class="ico" aria-hidden="true"><use href="#i-sparkles"/></svg></span>\n          <span class="mob-nav-lbl">提示词</span>\n        </button>\n        <button type="button" class="mob-nav-item" data-action="nav-page" data-page="works">\n          <span class="mob-nav-icon-wrap"><svg class="ico" aria-hidden="true"><use href="#i-grid"/></svg></span>\n          <span class="mob-nav-lbl">作品库</span>\n        </button>\n        <button type="button" class="mob-nav-item" data-action="nav-page" data-page="settings">\n          <span class="mob-nav-icon-wrap"><svg class="ico" aria-hidden="true"><use href="#i-sliders"/></svg></span>\n          <span class="mob-nav-lbl">设置</span>\n        </button>\n      </div>\n    </div>\n  </nav>\n</div>\n\n<!-- 抽屉遮罩 -->\n<div class="task-drawer-backdrop" id="taskDrawerBackdrop" data-action="close-task-drawer" aria-hidden="true"></div>\n\n<!-- 全局算力账号管理与切换浮层 -->\n<section id="accountPicker" class="account-picker account-picker-fixed" aria-label="Colab 算力账号管理" aria-hidden="true" inert>\n  <div class="acct-head">\n    <div class="acct-head-left">\n      <span class="warmup-title">Colab 算力账号</span>\n      <span id="acctTotalCu" class="acct-total-cu"></span>\n    </div>\n    <button type="button" id="refreshAccountsBtn" class="btn btn-xs" data-action="refresh-accounts" title="实时刷新所有账号余额">\n      <svg class="ico ico-14" aria-hidden="true"><use href="#i-refresh"/></svg>\n      <span class="control-label">刷新余额</span>\n    </button>\n  </div>\n  <div id="acctList" class="acct-list"></div>\n  <div id="acctOauthBox" class="acct-oauth-box" hidden>\n    <div class="warmup-hint" style="margin:0">1. 点击打开 Google 官方授权页登录新号并复制授权码；2. 粘贴至下方完成绑定（云盘加密同步保持主号不变）。</div>\n    <div class="acct-oauth-row">\n      <a id="acctOauthLink" href="#" target="_blank" rel="noopener noreferrer" class="btn btn-xs btn-selected">\n        <svg class="ico ico-14" aria-hidden="true"><use href="#i-external"/></svg>\n        <span class="control-label">打开 Google 授权页</span>\n      </a>\n      <button type="button" class="btn btn-xs" data-action="copy-acct-oauth-url"><span class="control-label">复制链接</span></button>\n    </div>\n    <div class="acct-oauth-row">\n      <input type="text" id="acctOauthCodeInp" class="settings-input acct-oauth-inp" placeholder="粘贴 4/0A... 开头的授权码" autocomplete="off" spellcheck="false">\n      <button type="button" id="acctOauthSubmitBtn" class="btn btn-xs btn-primary" data-action="submit-acct-oauth"><span class="control-label">绑定账号</span></button>\n    </div>\n  </div>\n  <div class="dialog-actions">\n    <button type="button" id="toggleAcctOauthBtn" class="btn btn-xs" data-action="toggle-acct-oauth">\n      <svg class="ico ico-14" aria-hidden="true"><use href="#i-plus"/></svg>\n      <span class="control-label">登录新号</span>\n    </button>\n    <span id="acctPickerHint" class="warmup-hint" aria-live="polite"></span>\n    <button type="button" class="btn btn-sm" data-action="close-account-picker"><span class="control-label">收起</span></button>\n  </div>\n</section>\n\n<!-- 预热模型选择器浮层 -->\n    <section id="warmupPicker" class="warmup-picker warmup-picker-fixed" aria-label="选择预热项目" aria-hidden="true" inert>\n      <div class="warmup-title">选择预热项目</div>\n      <div class="warmup-options">\n        <label><input type="checkbox" data-warmup-target="v5_int8"><span id="warmupLblImage">Flux / DiT INT8</span></label>\n        <label><input type="checkbox" data-warmup-target="llm"><span id="warmupLblLlm">Qwen 27B GGUF (llama-server)</span></label>\n        <label><input type="checkbox" data-warmup-target="video"><span id="warmupLblVideo">24fps 声画视频</span></label>\n      </div>\n      <p class="warmup-hint">按需勾选要预热常驻的模型；同时勾选媒体（生图/视频）与 LLM 将双路并行加载。</p>\n      <div class="dialog-actions">\n        <span id="warmupHint" class="warmup-hint" aria-live="polite"></span>\n        <button type="button" class="btn btn-sm" data-action="close-warmup"><span class="control-label">收起</span></button>\n        <button type="button" id="startWarmupBtn" class="btn btn-primary btn-sm" data-action="start-warmup" disabled><span class="control-label">开始预热</span></button>\n      </div>\n    </section>\n\n<!-- 回到顶部悬浮圆钮 -->\n<div class="float-dock" id="floatDock">\n  <button type="button" id="backToTopBtn" class="back-top-btn" data-action="scroll-top" aria-label="快速回到顶部" title="回到顶部" hidden>\n    <svg class="ico ico-14" aria-hidden="true"><use href="#i-arrow-up"/></svg>\n  </button>\n</div>\n\n<input type="file" id="fileInputImage" accept="image/png,image/jpeg,image/webp" style="display:none">\n<form id="batchDownloadForm" method="POST" action="/api/download_batch" style="display:none">\n  <input type="hidden" name="payload" id="batchDownloadPayload" value="">\n</form>\n\n<!-- 全屏无损 3K 原图看图层 -->\n<div class="lightbox" id="lightbox" role="dialog" aria-modal="true" aria-hidden="true" aria-label="3K 原图全屏预览" tabindex="-1">\n  <div class="lightbox-hud" id="lightboxHud"></div>\n  <div class="lightbox-stage" id="lightboxStage">\n    <img id="lightboxImg" src="" alt="图片预览" draggable="false">\n  </div>\n  <button type="button" class="lightbox-nav prev" id="lightboxPrev" data-action="lightbox-prev" aria-label="上一张图片" title="上一张 (←)" hidden><span aria-hidden="true">‹</span></button>\n  <button type="button" class="lightbox-nav next" id="lightboxNext" data-action="lightbox-next" aria-label="下一张图片" title="下一张 (→)" hidden><span aria-hidden="true">›</span></button>\n</div>\n\n\n<!-- 二次确认对话框（统一视觉规范：写明确认删除「名称」？和删除后无法恢复。） -->\n<dialog id="confirmDialog" class="confirm-dialog">\n  <h2 id="confirmTitle">确认删除？</h2>\n  <p id="confirmText"></p>\n  <div class="dialog-actions">\n    <button type="button" class="btn btn-sm" id="cancelConfirm"><span class="control-label">取消</span></button>\n    <button type="button" class="btn btn-sm btn-danger" id="acceptConfirm"><span class="control-label">确认删除</span></button>\n  </div>\n</dialog>\n\n<dialog id="promptDialog" class="prompt-dialog" aria-labelledby="promptDialogTitle">\n  <div class="prompt-dialog-head">\n    <div class="prompt-dialog-titles">\n      <h2 class="prompt-dialog-title" id="promptDialogTitle">提示词详情</h2>\n      <span class="prompt-dialog-sub" id="promptDialogSub"></span>\n    </div>\n    <button type="button" class="btn settings-close-btn" data-action="close-prompt-dialog" aria-label="关闭"><svg class="ico ico-14" aria-hidden="true"><use href="#i-close"/></svg></button>\n  </div>\n  <div class="prompt-dialog-body" id="promptDialogBody"></div>\n  <div class="prompt-dialog-foot">\n    <button type="button" class="btn btn-sm" data-action="close-prompt-dialog"><span class="control-label">关闭</span></button>\n    <button type="button" class="btn btn-sm" id="promptDialogCopyBtn" data-action="copy-prompt-dialog"><span class="control-label">复制</span></button>\n    <button type="button" class="btn btn-sm btn-selected" id="promptDialogReuseBtn" data-action="reuse-prompt-dialog"><span class="control-label">复用</span></button>\n  </div>\n</dialog>\n\n<div id="zSelectMenu" class="z-select-menu" role="listbox" tabindex="-1"></div>\n<div class="toast" id="toast" role="status" aria-live="polite"></div>\n<script>\'use strict\';\n\n/* ==================== 外观主题：永久自动跟随系统深浅色 ==================== */\ntry { localStorage.removeItem(\'g4_studio_theme\'); } catch (_) {}\nconst sysDarkMedia = window.matchMedia(\'(prefers-color-scheme: dark)\');\nfunction isFinePointer() {\n  return window.matchMedia(\'(hover: hover) and (pointer: fine)\').matches;\n}\nfunction syncSystemTheme() {\n  const isDark = sysDarkMedia.matches;\n  document.documentElement.setAttribute(\'data-theme\', isDark ? \'dark\' : \'light\');\n  const mt = document.getElementById(\'metaThemeColor\');\n  if (mt) mt.setAttribute(\'content\', isDark ? \'#0B111C\' : \'#EEF1F6\');\n}\nsysDarkMedia.addEventListener(\'change\', syncSystemTheme);\nsyncSystemTheme();\n(function detectPwaStandalone() {\n  try {\n    const isPwa = window.navigator.standalone === true || window.matchMedia(\'(display-mode: standalone)\').matches;\n    document.documentElement.classList.toggle(\'pwa-standalone\', !!isPwa);\n  } catch (_) {}\n})();\n\n\n/* ==================== 核心状态与常量 ==================== */\nconst VALID_CREATION_MODS = [\'image\', \'video\', \'llm\'];\nlet currentView = \'creation\';\nlet currentMod = \'image\';\nlet currentTab = \'image\';\nlet outputsCache = { image: [], video: [], upload_image: [], llm: [], counts: {} };\nlet _outputsLoadedOnce = false;\nlet _restoringUiState = false;\nlet _pendingRestoreScrollY = null;\nlet chatHistory = [];\nlet tasksList = [];\nlet totalHistoryTasksCount = 0;\nlet deletableTasksCount = 0;\nlet showingAllHistoryTasks = false;\nconst taskDetailCache = new Map();\n\nlet galleryPage = 1;\nlet gallerySearchQuery = \'\';\nlet filteredGalleryItems = [];\nlet _galleryFilterCache = null;\nlet batchMode = false;\nconst batchSelectedNames = new Set();\nlet lastBatchClickIdx = -1;\n\nlet _lastRefStripSig = \'\';\nlet _lastGallerySig = \'\';\nlet _lastChatSig = \'\';\nlet _lastLogText = \'\';\nlet _lastTasksSig = \'\';\nconst expandedTaskIds = new Set();\nconst taskBusy = new Set();\n\nconst state = {\n  imgModel: \'v5_int8\',\n  imgRes: \'portrait\',\n  imgSteps: \'tier2\',\n  imgRefImage: \'\',\n  editSteps: \'8\',\n  editRefine: \'0\',\n  editRefImage: \'\',\n  vidQuality: \'std\',\n  vidRes: \'832x1248\',\n  vidLen: \'192\',\n  vidSteps: \'8\',\n  vidRefImage: \'\'\n};\n\nlet studioSettings = null;\nlet studioDefaults = null;\nlet currentSettingsTab = \'llm\';\nlet settingsSaving = false;\n\nconst INPUT_ID_MAP = {\n  image: \'promptInputImage\',\n  edit: \'promptInputEdit\',\n  video: \'promptInputVideo\',\n  llm: \'promptInputLlm\'\n};\nconst INPUT_LABEL_MAP = {\n  image: \'生图提示词\',\n  edit: \'命令改图指令\',\n  video: \'视频分镜提示词\',\n  llm: \'提示词大师对话\'\n};\nconst SEG_GROUP_MAP = {\n  imgModel: \'segImgModel\',\n  imgRes: \'segImgRes\',\n  imgSteps: \'segImgSteps\',\n  editSteps: \'segEditSteps\',\n  editRefine: \'segEditRefine\',\n  vidQuality: \'segVidQuality\',\n  vidRes: \'segVidRes\',\n  vidLen: \'segVidLen\',\n  vidSteps: \'segVidSteps\'\n};\n\nfunction el(tag, cls, text) {\n  const n = document.createElement(tag);\n  if (cls) n.className = cls;\n  if (text !== undefined) n.textContent = text;\n  return n;\n}\n\nfunction svgIcon(id, cls = \'ico ico-14\') {\n  const ns = \'http://www.w3.org/2000/svg\';\n  const s = document.createElementNS(ns, \'svg\');\n  s.setAttribute(\'class\', cls);\n  s.setAttribute(\'aria-hidden\', \'true\');\n  const u = document.createElementNS(ns, \'use\');\n  u.setAttribute(\'href\', \'#\' + id);\n  s.appendChild(u);\n  return s;\n}\n\nfunction setControlText(btn, text, iconId = \'\', iconCls = \'ico ico-14\') {\n  if (!btn) return;\n  const sig = `${iconId}|${iconCls}|${text}`;\n  if (btn.dataset.ctlSig === sig && btn.childNodes.length > 0) return;\n  btn.dataset.ctlSig = sig;\n  const nodes = [];\n  if (iconId) nodes.push(svgIcon(iconId, iconCls));\n  nodes.push(el(\'span\', \'control-label\', text));\n  btn.replaceChildren(...nodes);\n}\n\nlet toastTimer = null;\nfunction toast(text) {\n  const t = document.getElementById(\'toast\');\n  if (!t) return;\n  t.textContent = text;\n  t.classList.add(\'visible\');\n  clearTimeout(toastTimer);\n  toastTimer = setTimeout(() => t.classList.remove(\'visible\'), 2600);\n}\n\nlet confirmCallback = null;\nfunction confirmAction(title, text, callback, acceptText = \'确认删除\') {\n  const d = document.getElementById(\'confirmDialog\');\n  document.getElementById(\'confirmTitle\').textContent = title;\n  document.getElementById(\'confirmText\').textContent = text;\n  setControlText(document.getElementById(\'acceptConfirm\'), acceptText);\n  confirmCallback = callback;\n  if (!d.open) d.showModal();\n  d.focus({ preventScroll: true });\n}\n\ndocument.getElementById(\'cancelConfirm\').addEventListener(\'click\', () => {\n  document.getElementById(\'confirmDialog\').close();\n});\ndocument.getElementById(\'acceptConfirm\').addEventListener(\'click\', () => {\n  const fn = confirmCallback;\n  document.getElementById(\'confirmDialog\').close();\n  confirmCallback = null;\n  if (fn) fn();\n});\n\n\nfunction updatePromptCharCount() {\n  const badge = document.getElementById(\'promptCharCount\');\n  const inp = getActiveInputEl();\n  if (!badge || !inp) return;\n  const len = (inp.value || \'\').trim().length;\n  badge.textContent = `${len} 字`;\n}\n\nfunction getActiveInputEl() {\n  return document.getElementById(INPUT_ID_MAP[currentMod] || \'promptInputImage\');\n}\n\n/* ==================== 5 栏统一路由 + 实时任务栏（常驻/抽屉）+ 移动端内滚 ==================== */\nconst VALID_PAGES = [\'image\', \'video\', \'llm\', \'works\', \'settings\'];\nconst PAGE_META = {\n  image: { title: \'3K 生图\', sub: \'参考图挂载与超清直出\', group: \'creation\' },\n  video: { title: \'高清视频\', sub: \'24fps 声画视频生成与首帧图生视频\', group: \'creation\' },\n  llm: { title: \'提示词大师\', sub: \'多轮提示词构思与修改\', group: \'creation\' },\n  works: { title: \'作品库\', sub: \'3K 图片 · 高清视频 · 上传素材 · 提示词历史\', group: \'works\' },\n  settings: { title: \'参数设置\', sub: \'系统提示词 · 生图前后缀与清洗 · 工作流参数\', group: \'settings\' }\n};\nconst MOB_TOP_FOLD_KEY = \'g4_mob_top_card_folded_v4\';\nconst TASK_COLLAPSED_KEY = \'g4_task_panel_collapsed_v2\';\nlet currentPage = \'image\';\nlet mobTopCardFolded = false;\nlet taskPanelCollapsed = false;\nlet taskDrawerOpen = false;\nconst pageScrollMemory = { image: 0, video: 0, llm: 0, works: 0, settings: 0 };\n\nfunction applyMobTopCardFolded(folded, persist = true) {\n  mobTopCardFolded = !!folded;\n  const card = document.getElementById(\'topStatusCard\');\n  const btn = document.getElementById(\'mobFoldTopBtn\');\n  if (card) card.classList.toggle(\'mob-collapsed\', mobTopCardFolded);\n  if (btn) {\n    btn.setAttribute(\'aria-expanded\', mobTopCardFolded ? \'false\' : \'true\');\n    btn.setAttribute(\'aria-label\', mobTopCardFolded ? \'展开顶部状态卡片\' : \'收起顶部状态卡片\');\n  }\n  if (mobTopCardFolded) {\n    closeWarmupPicker(false);\n    setAccountPickerOpen(false);\n  }\n  if (persist) {\n    try { localStorage.setItem(MOB_TOP_FOLD_KEY, mobTopCardFolded ? \'1\' : \'0\'); } catch (_) {}\n  }\n}\ntry {\n  if (localStorage.getItem(MOB_TOP_FOLD_KEY) === \'1\') applyMobTopCardFolded(true, false);\n  taskPanelCollapsed = localStorage.getItem(TASK_COLLAPSED_KEY) === \'1\';\n} catch (_) {}\n\nfunction syncInactiveWorksMirror() {}\nfunction isCreationPage(page) { return page === \'image\' || page === \'video\' || page === \'llm\'; }\n\nfunction isMobileScrollShell() { return (window.innerWidth || 1280) <= 1024; }\nfunction getDesktopScrollEl(page = currentPage) {\n  if (page === \'works\') return document.getElementById(\'worksGalleryScroll\');\n  if (page === \'settings\') return document.getElementById(\'settingsBody\');\n  if (page === \'llm\') return document.getElementById(\'chatBox\');\n  return document.getElementById(\'paramPanel\');\n}\nfunction getMainScrollY(page = currentPage) {\n  if (isMobileScrollShell()) {\n    const c = document.getElementById(\'appContent\');\n    return c ? Math.max(0, c.scrollTop || 0) : 0;\n  }\n  const el = getDesktopScrollEl(page);\n  return el ? Math.max(0, el.scrollTop || 0) : 0;\n}\nfunction setMainScrollY(y, smooth = false, page = currentPage) {\n  const target = Math.max(0, y || 0);\n  const beh = (smooth && !window.matchMedia(\'(prefers-reduced-motion: reduce)\').matches) ? \'smooth\' : \'auto\';\n  if (isMobileScrollShell()) {\n    const c = document.getElementById(\'appContent\');\n    if (c) c.scrollTo({ top: target, behavior: beh });\n  } else {\n    const el = getDesktopScrollEl(page);\n    if (el) el.scrollTo({ top: target, behavior: beh });\n  }\n}\n\nlet _lastTaskRunningFlag = false;\nfunction syncNavRunningDots(isRunning, badgeText = \'\') {\n  _lastTaskRunningFlag = !!isRunning;\n  document.querySelectorAll(\'.nav-run-dot\').forEach(dot => { dot.hidden = !isRunning; });\n  const pill = document.getElementById(\'globalTaskPill\');\n  if (pill) pill.classList.toggle(\'is-running\', !!isRunning);\n  const stopMini = document.getElementById(\'globalStopMiniBtn\');\n  if (stopMini) {\n    if (isMobileScrollShell()) {\n      stopMini.hidden = !isRunning;\n      stopMini.disabled = !isRunning;\n    } else {\n      stopMini.hidden = false;\n      stopMini.disabled = !isRunning;\n    }\n  }\n  const txtEl = document.getElementById(\'globalTaskPillText\');\n  if (txtEl && badgeText) txtEl.textContent = badgeText;\n}\n\nfunction syncTaskPanelLayout() {\n  const stage = document.getElementById(\'appStage\');\n  const backdrop = document.getElementById(\'taskDrawerBackdrop\');\n  const pill = document.getElementById(\'globalTaskPill\');\n  if (!stage) return;\n  const docked = !isMobileScrollShell();\n  if (docked) taskDrawerOpen = false;\n  stage.dataset.taskMode = docked ? \'docked\' : \'drawer\';\n  stage.classList.toggle(\'task-collapsed\', docked && taskPanelCollapsed);\n  const isDrawerVis = !docked && taskDrawerOpen;\n  stage.classList.toggle(\'drawer-open\', isDrawerVis);\n  document.body.classList.toggle(\'task-docked-open\', docked && !taskPanelCollapsed);\n  if (backdrop) {\n    backdrop.classList.toggle(\'open\', isDrawerVis);\n    backdrop.setAttribute(\'aria-hidden\', isDrawerVis ? \'false\' : \'true\');\n  }\n  const stopMini = document.getElementById(\'globalStopMiniBtn\');\n  if (stopMini) {\n    stopMini.hidden = docked ? false : !_lastTaskRunningFlag;\n    stopMini.disabled = !_lastTaskRunningFlag;\n  }\n  if (pill) {\n    const expanded = docked ? !taskPanelCollapsed : isDrawerVis;\n    pill.setAttribute(\'aria-expanded\', expanded ? \'true\' : \'false\');\n    pill.classList.toggle(\'is-panel-open\', expanded);\n  }\n}\n\nfunction setTaskDrawerOpen(open) {\n  taskDrawerOpen = !!open;\n  syncTaskPanelLayout();\n}\n\nfunction toggleTaskPanel() {\n  if (!isMobileScrollShell()) {\n    taskPanelCollapsed = !taskPanelCollapsed;\n    try { localStorage.setItem(TASK_COLLAPSED_KEY, taskPanelCollapsed ? \'1\' : \'0\'); } catch (_) {}\n    syncTaskPanelLayout();\n    return;\n  }\n  setTaskDrawerOpen(!taskDrawerOpen);\n}\n\nfunction navigateTo(page, opts = {}) {\n  if (!VALID_PAGES.includes(page)) page = \'image\';\n  const prevPage = currentPage;\n  const initialRestore = !!opts.initialRestore;\n  if (!initialRestore && prevPage !== page) {\n    pageScrollMemory[prevPage] = getMainScrollY();\n  }\n  currentPage = page;\n  document.body.dataset.page = page;\n  currentView = (page === \'works\') ? \'works\' : \'creation\';\n  taskDrawerOpen = false;\n  closeAllDriveSyncPickers();\n  closeWarmupPicker(false);\n  setAccountPickerOpen(false);\n  closeZenoSelect(false);\n\n  const meta = PAGE_META[page] || PAGE_META.image;\n  const hTitle = document.getElementById(\'pageHeaderTitle\');\n  const hSub = document.getElementById(\'pageHeaderSub\');\n  if (hTitle) hTitle.textContent = meta.title;\n  if (hSub) hSub.textContent = meta.sub;\n\n  document.querySelectorAll(\'[data-action="nav-page"]\').forEach(btn => {\n    const isAct = btn.dataset.page === page;\n    btn.classList.toggle(\'active\', isAct);\n    if (isAct) btn.setAttribute(\'aria-current\', \'page\');\n    else btn.removeAttribute(\'aria-current\');\n  });\n  const mobGlider = document.getElementById(\'mobNavGlider\');\n  if (mobGlider) {\n    const pIdx = Math.max(0, VALID_PAGES.indexOf(page));\n    if (initialRestore) {\n      mobGlider.style.transition = \'none\';\n      mobGlider.style.transform = `translate3d(${pIdx * 100}%, 0, 0)`;\n      void mobGlider.offsetWidth;\n      mobGlider.style.transition = \'\';\n    } else {\n      mobGlider.style.transform = `translate3d(${pIdx * 100}%, 0, 0)`;\n    }\n  }\n  document.querySelectorAll(\'.page-view\').forEach(sec => {\n    sec.classList.toggle(\'active\', sec.dataset.pageGroup === meta.group);\n  });\n\n  if (isCreationPage(page)) {\n    applyCreationModuleUI(page);\n  } else if (page === \'works\') {\n    if (!_lastGallerySig) renderGallery(false);\n  } else if (page === \'settings\') {\n    ensureSettingsPagePopulated();\n  }\n\n  syncTaskPanelLayout();\n\n  if (!initialRestore) {\n    if (prevPage !== page && (isMobileScrollShell() || page !== \'llm\')) {\n      setMainScrollY(opts.scrollTop ? 0 : (pageScrollMemory[page] || 0), false, page);\n    }\n    if (!_restoringUiState) setTimeout(savePanelUiState, 60);\n  }\n  syncBackToTopVisibility();\n}\n\nfunction selectView(view, initialRestore = false) {\n  if (view === \'works\') navigateTo(\'works\', { initialRestore });\n  else navigateTo(VALID_CREATION_MODS.includes(currentMod) ? currentMod : \'image\', { initialRestore });\n}\nfunction animateViewSwipeTo(view) { selectView(view); }\n\ndocument.addEventListener(\'focusin\', (e) => {\n  if (!isMobileScrollShell()) return;\n  if (e.target && e.target.matches && e.target.matches(\'input, textarea, [contenteditable="true"]\')) {\n    document.getElementById(\'mobileBottomNav\')?.classList.add(\'kb-hidden\');\n  }\n});\ndocument.addEventListener(\'focusout\', () => {\n  setTimeout(() => {\n    const a = document.activeElement;\n    if (!a || !a.matches || !a.matches(\'input, textarea, [contenteditable="true"]\')) {\n      document.getElementById(\'mobileBottomNav\')?.classList.remove(\'kb-hidden\');\n    }\n  }, 120);\n});\n\nfunction syncBackToTopVisibility() {\n  const topBtn = document.getElementById(\'backToTopBtn\');\n  if (!topBtn) return;\n  if (!isMobileScrollShell() && currentPage === \'llm\') {\n    topBtn.hidden = true;\n    return;\n  }\n  const y = getMainScrollY();\n  topBtn.hidden = y <= 240;\n}\nconst _onMainContainerScroll = () => {\n  syncBackToTopVisibility();\n  scheduleSavePanelUiState();\n};\nwindow.addEventListener(\'scroll\', _onMainContainerScroll, { passive: true });\n[\'appContent\', \'worksGalleryScroll\', \'settingsBody\', \'paramPanel\'].forEach(id => {\n  document.getElementById(id)?.addEventListener(\'scroll\', _onMainContainerScroll, { passive: true });\n});\nwindow.addEventListener(\'resize\', () => {\n  syncTaskPanelLayout();\n  if (document.getElementById(\'warmupPicker\')?.classList.contains(\'open\')) positionWarmupPicker();\n  if (document.getElementById(\'accountPicker\')?.classList.contains(\'open\')) alignAccountPickerToBadge();\n});\nsyncBackToTopVisibility();\n\n/* 面板真实交互自动续期并升级闲置窗口至 60 分钟（实例未运行时后端自动忽略） */\nlet _lastPanelTouchTs = 0;\nfunction notifyPanelUserActivity(e) {\n  if (e && !e.isTrusted) return;\n  const now = Date.now();\n  if (now - _lastPanelTouchTs < 20000) return;\n  _lastPanelTouchTs = now;\n  fetch(\'/api/touch\', {\n    method: \'POST\',\n    headers: { \'Content-Type\': \'application/json\' },\n    body: \'{"source":"panel"}\'\n  }).catch(() => {});\n}\ndocument.addEventListener(\'pointerdown\', notifyPanelUserActivity, { passive: true });\ndocument.addEventListener(\'keydown\', notifyPanelUserActivity, { passive: true });\n\nfunction applyCreationModuleUI(mod) {\n  if (!VALID_CREATION_MODS.includes(mod)) mod = \'image\';\n  currentMod = mod;\n  document.getElementById(\'creationBody\')?.setAttribute(\'data-mod\', mod);\n\n  document.getElementById(\'modSecImage\').classList.toggle(\'active\', mod === \'image\');\n  document.getElementById(\'modSecEdit\').classList.toggle(\'active\', mod === \'edit\');\n  document.getElementById(\'modSecVideo\').classList.toggle(\'active\', mod === \'video\');\n  document.getElementById(\'modSecLlm\').classList.toggle(\'active\', mod === \'llm\');\n\n  document.getElementById(\'promptInputImage\').classList.toggle(\'active\', mod === \'image\');\n  document.getElementById(\'promptInputEdit\').classList.toggle(\'active\', mod === \'edit\');\n  document.getElementById(\'promptInputVideo\').classList.toggle(\'active\', mod === \'video\');\n  document.getElementById(\'promptInputLlm\').classList.toggle(\'active\', mod === \'llm\');\n\n  const lbl = document.getElementById(\'activePromptLabel\');\n  if (lbl) {\n    lbl.setAttribute(\'for\', INPUT_ID_MAP[mod] || \'promptInputImage\');\n    lbl.textContent = INPUT_LABEL_MAP[mod] || \'生图提示词\';\n  }\n  if (mod !== \'image\') closeRandomPicker(false);\n  const randBtn = document.getElementById(\'randomRunBtn\');\n  if (randBtn) randBtn.hidden = (mod !== \'image\');\n  const runBtn = document.getElementById(\'runBtn\');\n  if (runBtn) {\n    const runTxt = mod === \'llm\' ? \'发送构思\' : (mod === \'video\' ? \'生成视频\' : \'提交生成\');\n    setControlText(runBtn, runTxt);\n  }\n  if (mod === \'llm\') {\n    const cb = document.getElementById(\'chatBox\');\n    if (cb) cb.scrollTop = cb.scrollHeight;\n  }\n  updatePromptCharCount();\n}\n\nfunction selectModule(mod) {\n  if (mod === \'edit\') { toast(\'改图暂未启用\'); return; }\n  if (mod === \'works\') { navigateTo(\'works\'); return; }\n  if (!VALID_CREATION_MODS.includes(mod)) return;\n  navigateTo(mod);\n}\n\nfunction setSeg(key, val) {\n  const gid = SEG_GROUP_MAP[key];\n  const grp = gid ? document.getElementById(gid) : null;\n  if (grp && !Array.from(grp.querySelectorAll(\'.seg-btn\')).some(b => b.dataset.val === String(val))) return;\n  state[key] = val;\n  if (key === \'imgModel\' && !warmupCustomized) { warmupTargets = [val, \'llm\']; syncWarmupPicker(); }\n  if (key === \'vidQuality\') {\n    const qMap = {\n      std: [\'832×1248\', \'1248×832\', \'1024×1024\'],\n      hd: [\'1088×1632\', \'1632×1088\', \'1344×1344\'],\n      uhd2k: [\'1408×2112\', \'2112×1408\', \'1728×1728\']\n    }[val] || [\'832×1248\', \'1248×832\', \'1024×1024\'];\n    const pEl = document.getElementById(\'lblVidResP\');\n    const lEl = document.getElementById(\'lblVidResL\');\n    const sEl = document.getElementById(\'lblVidResS\');\n    if (pEl) pEl.textContent = qMap[0];\n    if (lEl) lEl.textContent = qMap[1];\n    if (sEl) sEl.textContent = qMap[2];\n  }\n  if (!grp) return;\n  grp.querySelectorAll(\'.seg-btn\').forEach(btn => {\n    const isAct = btn.dataset.val === String(val);\n    btn.classList.toggle(\'active\', isAct);\n    btn.setAttribute(\'aria-pressed\', isAct ? \'true\' : \'false\');\n  });\n}\n\n/* ==================== 参考图 / 首帧横向挂载条（使用 WebP 缩略图秒开） ==================== */\nfunction pickRefForMod(mod, path) {\n  if (mod === \'video\') {\n    state.vidRefImage = (state.vidRefImage === path) ? \'\' : path;\n  } else if (mod === \'edit\') {\n    state.editRefImage = (state.editRefImage === path) ? \'\' : path;\n  } else {\n    state.imgRefImage = (state.imgRefImage === path) ? \'\' : path;\n  }\n  syncRefSelectionUI();\n}\n\nfunction syncRefSelectionUI() {\n  const configs = [\n    [\'image\', \'imgRefImage\', \'imgRefStatus\', \'imgRefClearBtn\', \'imgRefList\', \'已挂载: \', \'未挂载（点选下方缩略图可锁定人脸参考）\'],\n    [\'edit\', \'editRefImage\', \'editRefStatus\', \'editRefClearBtn\', \'editRefList\', \'待改原图: \', \'未选择原图\'],\n    [\'video\', \'vidRefImage\', \'vidRefStatus\', \'vidRefClearBtn\', \'vidRefList\', \'已挂载首帧: \', \'未挂载（点选下方缩略图可作为视频第 1 帧）\']\n  ];\n  for (const [, stateKey, stId, clrId, listId, prefix, emptyMsg] of configs) {\n    const val = state[stateKey];\n    const stEl = document.getElementById(stId);\n    const clrEl = document.getElementById(clrId);\n    const listEl = document.getElementById(listId);\n    if (stEl) {\n      if (val) {\n        stEl.className = \'ref-status-text has-ref\';\n        stEl.textContent = prefix + val.split(\'/\').pop();\n      } else {\n        stEl.className = \'ref-status-text\';\n        stEl.textContent = emptyMsg;\n      }\n    }\n    if (clrEl) clrEl.disabled = !val;\n    if (listEl) {\n      listEl.querySelectorAll(\'.ref-thumb-card\').forEach(card => {\n        const isSel = card.dataset.path === val;\n        card.classList.toggle(\'selected\', isSel);\n        card.setAttribute(\'aria-pressed\', isSel ? \'true\' : \'false\');\n      });\n    }\n  }\n}\n\nlet _refAllSorted = [];\nconst REF_BATCH_SIZE = 40;\n\nfunction appendRefCards(box, mod, startIdx, endIdx) {\n  const roleLabel = mod === \'video\' ? \'视频首帧图\' : \'生图参考图\';\n  const stateKey = mod === \'video\' ? \'vidRefImage\' : (mod === \'edit\' ? \'editRefImage\' : \'imgRefImage\');\n  const selVal = state[stateKey];\n  const frag = document.createDocumentFragment();\n  for (let i = startIdx; i < endIdx && i < _refAllSorted.length; i++) {\n    const it = _refAllSorted[i];\n    const thumbUrl = it.thumb_url || it.url || (\'/files/\' + it.mod + \'/\' + encodeURIComponent(it.name || \'\'));\n    const isSel = (it.path || \'\') === selVal && !!selVal;\n    const card = el(\'div\', \'ref-thumb-card\' + (isSel ? \' selected\' : \'\'));\n    card.setAttribute(\'role\', \'button\');\n    card.setAttribute(\'tabindex\', \'0\');\n    card.setAttribute(\'aria-pressed\', isSel ? \'true\' : \'false\');\n    card.setAttribute(\'aria-label\', \'选择\' + roleLabel + \' \' + (it.name || \'\'));\n    card.dataset.action = \'pick-ref\';\n    card.dataset.mod = mod;\n    card.dataset.path = it.path || \'\';\n    const tagPrefix = it.mod === \'upload_image\' ? \'[上传] \' : \'\';\n    card.title = tagPrefix + (it.name || \'\') + (it.res ? \' (\' + it.res.replace(\'x\', \'×\') + \')\' : \'\');\n    const img = el(\'img\');\n    img.src = thumbUrl;\n    img.alt = it.name || \'\';\n    img.loading = \'lazy\';\n    img.decoding = \'async\';\n    card.append(img);\n    frag.append(card);\n  }\n  box.append(frag);\n  box.dataset.renderedCount = String(Math.min(endIdx, _refAllSorted.length));\n}\n\nfunction renderRefStripsIfNeeded() {\n  const upImgs = (outputsCache.upload_image || []).map(x => ({ ...x, tag: \'上传\', mod: \'upload_image\' }));\n  const genImgs = (outputsCache.image || []).map(x => ({ ...x, tag: \'\', mod: \'image\' }));\n  const allImgs = [...upImgs, ...genImgs].sort((a, b) =>\n    ((b.mtime || 0) - (a.mtime || 0)) || String(b.name || \'\').localeCompare(String(a.name || \'\'))\n  );\n  const sig = allImgs.map(x => (x.path || \'\') + \':\' + (x.mtime || 0)).join(\'|\');\n  if (sig === _lastRefStripSig) {\n    syncRefSelectionUI();\n    return;\n  }\n  _lastRefStripSig = sig;\n  _refAllSorted = allImgs;\n\n  const populateStrip = (listId, mod) => {\n    const box = document.getElementById(listId);\n    if (!box) return;\n    if (!box.dataset.scrollBound) {\n      box.dataset.scrollBound = \'1\';\n      box.addEventListener(\'scroll\', () => {\n        const cur = parseInt(box.dataset.renderedCount || \'0\', 10) || 0;\n        if (cur >= _refAllSorted.length) return;\n        if (box.scrollLeft + box.clientWidth >= box.scrollWidth - 260) {\n          appendRefCards(box, mod, cur, cur + REF_BATCH_SIZE);\n        }\n      }, { passive: true });\n    }\n    box.replaceChildren();\n    box.dataset.renderedCount = \'0\';\n    if (!allImgs.length) {\n      const hint = el(\'div\', \'\', \'点击右侧「上传图片」或先生成作品即可在此点选挂载\');\n      hint.style.cssText = \'font-size:12px;color:var(--muted);padding:8px 2px;\';\n      box.append(hint);\n      return;\n    }\n    const stateKey = mod === \'video\' ? \'vidRefImage\' : (mod === \'edit\' ? \'editRefImage\' : \'imgRefImage\');\n    const selVal = state[stateKey];\n    let initCount = REF_BATCH_SIZE;\n    if (selVal) {\n      const selIdx = allImgs.findIndex(x => x.path === selVal);\n      if (selIdx >= initCount) initCount = selIdx + 8;\n    }\n    appendRefCards(box, mod, 0, initCount);\n  };\n\n  populateStrip(\'imgRefList\', \'image\');\n  populateStrip(\'editRefList\', \'edit\');\n  populateStrip(\'vidRefList\', \'video\');\n  syncRefSelectionUI();\n}\n\n/* ==================== 本地图片上传 ==================== */\nfunction triggerUpload() {\n  const f = document.getElementById(\'fileInputImage\');\n  f.value = \'\';\n  f.click();\n}\n\ndocument.getElementById(\'fileInputImage\').addEventListener(\'change\', async (ev) => {\n  const file = ev.target.files && ev.target.files[0];\n  if (!file) return;\n  if (!file.type.startsWith(\'image/\')) { toast(\'请选择图片文件\'); return; }\n  if (!file.size || file.size > 200 * 1024 * 1024) { toast(\'文件大小无效或超过 200 MB 限制\'); return; }\n  toast(\'正在上传图片...\');\n  try {\n    const res = await fetch(\'/api/upload?filename=\' + encodeURIComponent(file.name), {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/octet-stream\' },\n      body: file\n    });\n    const data = await res.json();\n    if (!res.ok || !data.ok) throw new Error(data.message || \'上传失败\');\n    if (currentMod === \'video\') state.vidRefImage = data.path;\n    else if (currentMod === \'edit\') state.editRefImage = data.path;\n    else state.imgRefImage = data.path;\n    await loadOutputs(true);\n    toast(\'图片已上传并挂载\');\n  } catch (e) {\n    toast((e.message || \'上传失败\') + \'；未自动重传\');\n  }\n});\n\n\n/* ==================== 提示词大师对话渲染 ==================== */\nlet uiMetaConfig = { image_label: \'Flux / DiT INT8\', video_label: \'24fps 声画视频\', llm_label: \'Qwen 27B GGUF (llama-server)\', comfy_url: \'\', drive_sync_enabled: false, channel_sync_enabled: false };\n\nfunction syncComfyExternalLink(meta) {\n  const link = document.getElementById(\'comfyExtLink\');\n  if (!link) return;\n  const raw = String((meta && meta.comfy_url) || \'\').trim();\n  const low = raw.toLowerCase();\n  if (low === \'hide\' || low === \'off\' || low === \'none\' || low === \'false\') {\n    link.hidden = true;\n    link.style.display = \'none\';\n    return;\n  }\n  link.hidden = false;\n  link.style.display = \'\';\n  const host = location.hostname.includes(\':\') && !location.hostname.startsWith(\'[\') ? `[${location.hostname}]` : location.hostname;\n  link.href = /^https?:\\/\\//i.test(raw) ? raw : `http://${host}:18188/`;\n}\nsyncComfyExternalLink(uiMetaConfig);\n\nfunction applyUiMetaLabels(meta) {\n  if (!meta || typeof meta !== \'object\') return;\n  uiMetaConfig = { ...uiMetaConfig, ...meta };\n  syncComfyExternalLink(uiMetaConfig);\n  const elImg = document.getElementById(\'warmupLblImage\');\n  const elLlm = document.getElementById(\'warmupLblLlm\');\n  const elVid = document.getElementById(\'warmupLblVideo\');\n  if (elImg && uiMetaConfig.image_label) elImg.textContent = uiMetaConfig.image_label;\n  if (elLlm && uiMetaConfig.llm_label) elLlm.textContent = uiMetaConfig.llm_label;\n  if (elVid && uiMetaConfig.video_label) elVid.textContent = uiMetaConfig.video_label;\n  if (typeof meta.drive_sync_enabled === \'boolean\') {\n    applyDriveSyncUI();\n  }\n}\n\nfunction displayModelName(label) {\n  const name = String(label || \'\');\n  if (/Qwen.*27B|Qwen.*30B/i.test(name)) return \'Qwen 27B\';\n  if (/BF16/i.test(name)) return (uiMetaConfig.image_label || \'Flux / DiT\').replace(/\\s*INT8$/i, \'\') + \' BF16\';\n  if (/Flux|DiT|\\bV5\\b/i.test(name)) return uiMetaConfig.image_label || \'Flux / DiT INT8\';\n  if (/24fps|H3/i.test(name)) return uiMetaConfig.video_label || \'24fps 声画视频\';\n  return name;\n}\n\nfunction extractCleanPrompt(raw, lang) {\n  const txt = (raw || \'\').trim();\n  const lines = txt.split(/\\r?\\n/);\n  let mode = \'\';\n  const enLines = [], zhLines = [];\n  for (const line of lines) {\n    const t = line.trim();\n    if (/Prompt in English|英文详细提示词/i.test(t)) { mode = \'en\'; continue; }\n    if (/Prompt in Chinese|中文详细提示词/i.test(t)) { mode = \'zh\'; continue; }\n    if (/^---+$/.test(t) || /^###/.test(t)) continue;\n    if (mode === \'en\') enLines.push(line);\n    else if (mode === \'zh\') zhLines.push(line);\n  }\n  if (lang === \'en\' && enLines.join(\'\\n\').trim().length > 20) return enLines.join(\'\\n\').replace(/\\*\\*/g, \'\').trim();\n  if (lang === \'zh\' && zhLines.join(\'\\n\').trim().length > 20) return zhLines.join(\'\\n\').replace(/\\*\\*/g, \'\').trim();\n  return txt.replace(/^[\\s\\-#*]+/, \'\').trim();\n}\n\nlet optimisticLlmPending = null;\n\nfunction getPendingLlmTasks() {\n  const backendPending = (Array.isArray(tasksList) ? tasksList : [])\n    .filter(t => t && t.module === \'llm\' && (t.state === \'running\' || t.state === \'queued\') && t.user_msg)\n    .sort((a, b) => (a.jid || 0) - (b.jid || 0));\n  if (optimisticLlmPending) {\n    const matched = backendPending.some(t => t.user_msg === optimisticLlmPending.text);\n    if (matched) {\n      optimisticLlmPending = null;\n    } else {\n      return [...backendPending, {\n        jid: -1,\n        state: \'running\',\n        user_msg: optimisticLlmPending.text,\n        sent_time: optimisticLlmPending.time,\n        elapsed_sec: 0,\n        optimistic: true\n      }];\n    }\n  }\n  return backendPending;\n}\n\nfunction updateChatThinkingTimersOnly(pendingLlm) {\n  const cb = document.getElementById(\'chatBox\');\n  if (!cb) return;\n  const nodes = cb.querySelectorAll(\'.thinking-status-text\');\n  pendingLlm.forEach((pt, i) => {\n    const node = nodes[i];\n    if (!node) return;\n    if (pt.state === \'queued\') {\n      node.textContent = \'排队等待思考中…\';\n    } else {\n      const sec = typeof pt.elapsed_sec === \'number\' && pt.elapsed_sec > 0 ? ` (${pt.elapsed_sec.toFixed(1)} s)` : \'\';\n      node.textContent = `正在构思提示词…${sec}`;\n    }\n  });\n}\n\nfunction renderChatIfNeeded() {\n  const pendingLlm = getPendingLlmTasks();\n  const structSig = JSON.stringify(chatHistory) + \'::\' + JSON.stringify(pendingLlm.map(p => ({ jid: p.jid, state: p.state, text: p.user_msg })));\n  if (structSig === _lastChatSig) {\n    if (pendingLlm.length) updateChatThinkingTimersOnly(pendingLlm);\n    return;\n  }\n  _lastChatSig = structSig;\n\n  const lbl = document.getElementById(\'chatCountLabel\');\n  if (lbl) {\n    const rounds = Math.floor(chatHistory.length / 2);\n    lbl.textContent = pendingLlm.length\n      ? `对话记录 (${rounds} 轮 · 正在构思…)`\n      : `对话记录 (${rounds} 轮 · Qwen3.8 27B)`;\n  }\n  const cb = document.getElementById(\'chatBox\');\n  if (!cb) return;\n  cb.replaceChildren();\n  if (!chatHistory.length && !pendingLlm.length) {\n    const empty = el(\'div\', \'\', \'暂无对话记录。在下方输入框输入场景、角色或修改指令。\');\n    empty.style.cssText = \'color:var(--muted);font-size:12.5px;text-align:center;padding:28px 12px;\';\n    cb.append(empty);\n    return;\n  }\n\n  chatHistory.forEach((m, idx) => {\n    const item = el(\'div\', \'chat-msg \' + (m.role === \'user\' ? \'user\' : \'assistant\'));\n    const meta = el(\'div\', \'chat-meta\');\n    if (m.role === \'user\') {\n      meta.append(el(\'span\', \'\', \'你\' + (m.time ? \' · \' + m.time : \'\')));\n      item.append(meta, el(\'div\', \'\', m.content || \'\'));\n    } else {\n      const mName = displayModelName(m.model_label || \'Qwen3.8 27B\');\n      meta.append(el(\'span\', \'\', mName + (m.time ? \' · \' + m.time : \'\')));\n      const copyBtn = el(\'button\', \'btn btn-xs\');\n      copyBtn.type = \'button\';\n      copyBtn.dataset.action = \'copy-chat\';\n      copyBtn.dataset.idx = String(idx);\n      setControlText(copyBtn, \'复制全文\');\n      meta.append(copyBtn);\n\n      const actRow = el(\'div\', \'chat-meta-actions\');\n      const toImg = el(\'button\', \'btn btn-xs btn-selected\');\n      toImg.type = \'button\';\n      toImg.dataset.action = \'chat-to-mod\';\n      toImg.dataset.idx = String(idx);\n      toImg.dataset.target = \'image\';\n      setControlText(toImg, \'填入 3K 生图\', \'i-image\', \'ico ico-14\');\n\n      const toVid = el(\'button\', \'btn btn-xs\');\n      toVid.type = \'button\';\n      toVid.dataset.action = \'chat-to-mod\';\n      toVid.dataset.idx = String(idx);\n      toVid.dataset.target = \'video\';\n      setControlText(toVid, \'填入视频分镜\', \'i-video\', \'ico ico-14 ico-purple\');\n\n      actRow.append(toImg, toVid);\n      item.append(meta, actRow, el(\'div\', \'\', m.content || \'\'));\n    }\n    cb.append(item);\n  });\n\n  pendingLlm.forEach(pt => {\n    const uItem = el(\'div\', \'chat-msg user\');\n    const uMeta = el(\'div\', \'chat-meta\');\n    uMeta.append(el(\'span\', \'\', \'你\' + (pt.sent_time ? \' · \' + pt.sent_time : \'\')));\n    uItem.append(uMeta, el(\'div\', \'\', pt.user_msg || \'\'));\n    cb.append(uItem);\n\n    const aItem = el(\'div\', \'chat-msg assistant thinking\');\n    const aMeta = el(\'div\', \'chat-meta\');\n    aMeta.append(el(\'span\', \'\', \'Qwen3.8 27B\'));\n    const bodyRow = el(\'div\', \'thinking-inline\');\n    const dots = el(\'span\', \'thinking-dots\');\n    dots.append(el(\'i\'), el(\'i\'), el(\'i\'));\n    const sec = typeof pt.elapsed_sec === \'number\' && pt.elapsed_sec > 0 ? ` (${pt.elapsed_sec.toFixed(1)} s)` : \'\';\n    const txtSpan = el(\'span\', \'thinking-status-text\', pt.state === \'queued\' ? \'排队等待思考中…\' : `正在构思提示词…${sec}`);\n    bodyRow.append(dots, txtSpan);\n    aItem.append(aMeta, bodyRow);\n    cb.append(aItem);\n  });\n\n  cb.scrollTop = cb.scrollHeight;\n}\n\n/* ==================== 输入框提交、清空与中止 ==================== */\nfunction clearPrompt() {\n  const inp = getActiveInputEl();\n  if (!inp || !inp.value.trim()) {\n    if (inp) inp.value = \'\';\n    return;\n  }\n  confirmAction(\'清空当前输入？\', \'仅清空当前模块的输入框内容，其他模块和已选参考图保持不变。\', () => {\n    inp.value = \'\';\n    inp.focus({ preventScroll: true });\n  }, \'确认清空\');\n}\n\nlet submitting = false;\nlet _randomSubmitting = false;\nlet _randomPickerOpen = false;\nlet lastRandomCount = 1;\ntry {\n  const savedCnt = Number(localStorage.getItem(\'g4_last_random_count\'));\n  if (Number.isInteger(savedCnt) && savedCnt >= 1 && savedCnt <= 10) lastRandomCount = savedCnt;\n} catch (_) {}\n\nfunction syncRandomCountHighlight() {\n  document.querySelectorAll(\'#randomCountPicker .rand-count-btn\').forEach(b => {\n    b.classList.toggle(\'is-last\', Number(b.dataset.count) === lastRandomCount);\n  });\n}\nsyncRandomCountHighlight();\n\nfunction closeRandomPicker(restoreFocus = false) {\n  if (!_randomPickerOpen) return;\n  _randomPickerOpen = false;\n  const picker = document.getElementById(\'randomCountPicker\');\n  const btn = document.getElementById(\'randomRunBtn\');\n  if (btn) btn.setAttribute(\'aria-expanded\', \'false\');\n  if (picker) picker.classList.remove(\'open\');\n\n}\n\nfunction toggleRandomPicker() {\n  if (_randomSubmitting) return;\n  if (_randomPickerOpen) {\n    closeRandomPicker(true);\n    return;\n  }\n  closeStudioSelect(false);\n  closeWarmupPicker(false);\n  const picker = document.getElementById(\'randomCountPicker\');\n  const btn = document.getElementById(\'randomRunBtn\');\n  if (!picker || !btn) return;\n  syncRandomCountHighlight();\n  _randomPickerOpen = true;\n  btn.setAttribute(\'aria-expanded\', \'true\');\n  picker.classList.add(\'open\');\n\n}\n\ndocument.addEventListener(\'pointerdown\', (e) => {\n  if (!_randomPickerOpen) return;\n  if (e.target.closest(\'#randomCountPicker\') || e.target.closest(\'#randomRunBtn\')) return;\n  closeRandomPicker(false);\n});\n\nasync function submitRandomImage(count) {\n  const n = Math.max(1, Math.min(10, Number(count) || 1));\n  lastRandomCount = n;\n  try { localStorage.setItem(\'g4_last_random_count\', String(n)); } catch (_) {}\n  syncRandomCountHighlight();\n  closeRandomPicker(false);\n\n  if (_randomSubmitting) return;\n  _randomSubmitting = true;\n  const btn = document.getElementById(\'randomRunBtn\');\n  if (btn) btn.disabled = true;\n  try {\n    const res = await fetch(\'/api/run\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({\n        module: \'random\',\n        source: \'panel\',\n        count: n,\n        step_tier: state.imgSteps || \'tier2\'\n      })\n    });\n    const data = await res.json();\n    if (!res.ok || !data.ok) {\n      toast(data.message || \'提交随机生图失败\');\n    } else {\n      toast(data.message || `已启动 ${n} 张随机生图`);\n      pollJob();\n    }\n  } catch (e) {\n    toast(\'网络请求失败\');\n  } finally {\n    _randomSubmitting = false;\n    if (btn) btn.disabled = false;\n  }\n}\n\nasync function submitTask() {\n  if (submitting || currentView !== \'creation\') return;\n  const activeInput = getActiveInputEl();\n  const text = activeInput ? activeInput.value.trim() : \'\';\n  if (!text) {\n    toast(\'请先输入提示词或指令\');\n    if (activeInput) activeInput.focus();\n    return;\n  }\n  if (currentMod === \'edit\') { toast(\'改图暂未启用\'); return; }\n  const payload = { module: currentMod, text };\n  if (currentMod === \'image\') {\n    const resMode = [\'portrait\', \'landscape\', \'square\'].includes(state.imgRes) ? state.imgRes : \'portrait\';\n    const stepTier = [\'tier1\', \'tier2\', \'tier3\'].includes(state.imgSteps) ? state.imgSteps : \'tier2\';\n    const wf = studioSettings?.image_workflow;\n    const rCfg = wf?.resolutions?.[resMode] || (resMode === \'landscape\' ? { s2_w: 3072, s2_h: 2048 } : resMode === \'square\' ? { s2_w: 2560, s2_h: 2560 } : { s2_w: 2048, s2_h: 3072 });\n    const tCfg = wf?.step_tiers?.[stepTier] || (stepTier === \'tier2\' ? { s1: 12, ref: 6 } : stepTier === \'tier3\' ? { s1: 16, ref: 8 } : { s1: 8, ref: 5 });\n    const useHires = (wf ? wf.enable_hires !== false : true) && Number(tCfg.ref) > 0;\n    const totalSteps = useHires ? (Number(tCfg.s1) + Number(tCfg.ref)) : Number(tCfg.s1);\n    payload.res_mode = resMode;\n    payload.step_tier = stepTier;\n    payload.width = Number(rCfg.s2_w);\n    payload.height = Number(rCfg.s2_h);\n    payload.model = state.imgModel;\n    payload.steps = totalSteps;\n    payload.ref_image = state.imgRefImage || \'\';\n  } else if (currentMod === \'video\') {\n    const [w, h] = (state.vidRes || \'832x1248\').split(\'x\');\n    payload.width = parseInt(w, 10);\n    payload.height = parseInt(h, 10);\n    payload.length = parseInt(state.vidLen || \'192\', 10);\n    payload.steps = parseInt(state.vidSteps || \'8\', 10);\n    payload.quality = state.vidQuality || \'std\';\n    payload.ref_image = state.vidRefImage || \'\';\n  }\n\n  const runBtn = document.getElementById(\'runBtn\');\n  const isLlm = currentMod === \'llm\';\n  submitting = true;\n  if (runBtn) runBtn.disabled = true;\n  if (isLlm) {\n    const nowDt = new Date();\n    const hh = String(nowDt.getHours()).padStart(2, \'0\');\n    const mm = String(nowDt.getMinutes()).padStart(2, \'0\');\n    optimisticLlmPending = { text, time: `${hh}:${mm}` };\n    if (activeInput) activeInput.value = \'\';\n    renderChatIfNeeded();\n  }\n  try {\n    const res = await fetch(\'/api/run\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify(payload)\n    });\n    const data = await res.json();\n    if (!res.ok || !data.ok) throw new Error(data.message || \'提交失败\');\n    if (!isLlm) toast(data.message || \'任务已提交\');\n    await pollJob();\n    if (isLlm) {\n      optimisticLlmPending = null;\n      await loadChat();\n      renderChatIfNeeded();\n    }\n  } catch (e) {\n    if (isLlm) {\n      optimisticLlmPending = null;\n      if (activeInput && !activeInput.value.trim()) activeInput.value = text;\n      renderChatIfNeeded();\n    }\n    toast((e.message || \'提交失败\') + \'；未自动重试\');\n  } finally {\n    submitting = false;\n    if (runBtn) runBtn.disabled = false;\n  }\n}\n\nasync function cancelJob() {\n  try {\n    await fetch(\'/api/cancel\', { method: \'POST\' });\n    await pollJob();\n  } catch (_) {}\n}\n\n/* ==================== 顶栏设备状态与电源/预热控制 ==================== */\nconst powerTasks = {\n  start: {\n    submitting: false, pending: false, epoch: 0, completedJid: 0,\n    buttonId: \'startG4Btn\', name: \'开机\', busyText: \'开机中\',\n    ariaIdle: \'开机：仅启动实例，不加载模型\', ariaBusy: \'开机中：正在排队或启动实例\'\n  },\n  stop: {\n    submitting: false, pending: false, epoch: 0, completedJid: 0,\n    buttonId: \'startG4Btn\', name: \'关机\', busyText: \'关机中\',\n    ariaIdle: \'关机：关闭 Colab G4 实例\', ariaBusy: \'关机中：正在排队或关闭实例\'\n  },\n  warmup: {\n    submitting: false, pending: false, epoch: 0, completedJid: 0,\n    buttonId: \'warmupG4Btn\', name: \'预热\', busyText: \'预热中\',\n    ariaIdle: \'预热设备：选择已开机实例要预热的项目\', ariaBusy: \'预热中：正在排队或加载所选项目\'\n  }\n};\n\nlet instanceActive = null;\nlet powerJobsKnown = false;\nlet statusLoading = false;\nlet statusRequestSeq = 0;\nlet lastStatusAttempt = 0;\n\nfunction powerBusyTask() {\n  return [\'stop\', \'start\'].map(mod => powerTasks[mod]).find(task => task.submitting || task.pending);\n}\n\nlet _lastWarmupAnchorEl = null;\n\nfunction positionFixedPopover(box, anchorEl, maxW = 392) {\n  if (!box) return;\n  const vw = window.innerWidth || 1280;\n  const vh = window.innerHeight || 800;\n  const w = Math.min(maxW, vw - 20);\n  box.style.width = `${w}px`;\n  const r = anchorEl && anchorEl.offsetParent !== null ? anchorEl.getBoundingClientRect() : null;\n  if (!r) {\n    box.style.left = `${Math.round((vw - w) / 2)}px`;\n    box.style.top = \'72px\';\n    box.style.bottom = \'auto\';\n    box.style.transformOrigin = \'top center\';\n    return;\n  }\n  let left = r.right - w;\n  if (left < 10) left = Math.max(10, Math.min(r.left, vw - w - 10));\n  if (left + w > vw - 10) left = Math.max(10, vw - w - 10);\n  box.style.left = `${Math.round(left)}px`;\n  const h = box.offsetHeight || 220;\n  const top = Math.round(r.bottom + 8);\n  if (top + h <= vh - 16 || r.top < vh * 0.5) {\n    box.style.bottom = \'auto\';\n    box.style.top = `${top}px`;\n    box.style.transformOrigin = \'top right\';\n  } else {\n    box.style.top = \'auto\';\n    box.style.bottom = `${Math.round(vh - r.top + 8)}px`;\n    box.style.transformOrigin = \'bottom right\';\n  }\n}\n\nfunction positionWarmupPicker(anchorEl) {\n  const box = document.getElementById(\'warmupPicker\');\n  const anc = anchorEl || _lastWarmupAnchorEl || document.getElementById(\'warmupG4Btn\');\n  positionFixedPopover(box, anc, 384);\n}\n\nfunction syncRefreshStatusButton() {\n  const refreshBtn = document.getElementById(\'refreshStatusBtn\');\n  if (refreshBtn) {\n    refreshBtn.disabled = statusLoading;\n    refreshBtn.classList.toggle(\'is-spinning\', statusLoading);\n    refreshBtn.setAttribute(\'aria-busy\', statusLoading ? \'true\' : \'false\');\n  }\n  const g4 = document.getElementById(\'g4Badge\');\n  if (g4) g4.setAttribute(\'aria-busy\', statusLoading ? \'true\' : \'false\');\n}\n\nfunction syncPowerButton(mod) {\n  syncRefreshStatusButton();\n  const task = powerTasks[mod];\n  if (mod === \'warmup\') {\n    const busy = task.submitting || task.pending;\n    document.querySelectorAll(\'[data-power-btn="warmup"]\').forEach(btn => {\n      btn.disabled = busy || !!powerBusyTask() || statusLoading || !powerJobsKnown || instanceActive !== true;\n      btn.setAttribute(\'aria-busy\', busy ? \'true\' : \'false\');\n      btn.setAttribute(\'aria-label\', busy ? task.ariaBusy : task.ariaIdle);\n      setControlText(btn, busy ? task.busyText : task.name, \'i-bolt\', \'ico ico-14 ico-orange\');\n    });\n    syncWarmupPicker();\n    return;\n  }\n  const busyTask = powerBusyTask();\n  const busy = !!busyTask || statusLoading;\n  const known = powerJobsKnown && (instanceActive === true || instanceActive === false);\n  const isDanger = busyTask === powerTasks.stop || (!busyTask && !statusLoading && known && instanceActive);\n  const icoCls = isDanger ? \'ico ico-14 ico-red\' : \'ico ico-14 ico-green\';\n  document.querySelectorAll(\'[data-power-btn="start"]\').forEach(btn => {\n    btn.disabled = busy || !known;\n    btn.setAttribute(\'aria-busy\', busy ? \'true\' : \'false\');\n    btn.classList.toggle(\'btn-danger\', isDanger);\n    if (busyTask) {\n      setControlText(btn, busyTask.busyText, \'i-power\', icoCls);\n      btn.setAttribute(\'aria-label\', busyTask.ariaBusy);\n    } else if (statusLoading || !known) {\n      setControlText(btn, statusLoading ? \'检查中\' : \'未知\', \'i-power\', \'ico ico-14\');\n      btn.setAttribute(\'aria-label\', \'实例状态未确认，电源按钮已禁用\');\n    } else {\n      const action = instanceActive ? powerTasks.stop : powerTasks.start;\n      setControlText(btn, action.name, \'i-power\', icoCls);\n      btn.setAttribute(\'aria-label\', action.ariaIdle);\n    }\n  });\n  syncPowerButton(\'warmup\');\n}\n\nasync function toggleG4Power() {\n  if (powerBusyTask() || statusLoading) return;\n  if (!powerJobsKnown || (instanceActive !== true && instanceActive !== false)) return;\n  if (instanceActive) {\n    confirmAction(\'确认关闭 Colab 实例？\', \'关闭远端 GPU 实例；正在运行和排队的任务将自动暂停保全，下次开机后可手动继续。\', () => submitPowerTask(\'stop\'), \'确认关机\');\n    return;\n  }\n  return submitPowerTask(\'start\');\n}\n\nlet warmupCustomized = false;\nlet warmupTargets = [state.imgModel, \'llm\'];\nfunction syncWarmupPicker() {\n  document.querySelectorAll(\'[data-warmup-target]\').forEach(el => { el.checked = warmupTargets.includes(el.dataset.warmupTarget); });\n  const busy = powerTasks.warmup.submitting || powerTasks.warmup.pending || !!powerBusyTask() || statusLoading || !powerJobsKnown || instanceActive !== true;\n  document.getElementById(\'startWarmupBtn\').disabled = busy || !warmupTargets.length;\n  document.getElementById(\'warmupHint\').textContent = !warmupTargets.length ? \'请至少选择一项\' : (instanceActive !== true ? \'请先开机\' : \'\');\n}\nfunction setWarmupPickerOpen(open, restoreFocus = false, anchorEl = null) {\n  const box = document.getElementById(\'warmupPicker\');\n  if (!box) return;\n  if (open) {\n    _lastWarmupAnchorEl = anchorEl || _lastWarmupAnchorEl;\n    setAccountPickerOpen(false);\n    positionWarmupPicker(_lastWarmupAnchorEl);\n  }\n  box.classList.toggle(\'open\', open);\n  box.setAttribute(\'aria-hidden\', open ? \'false\' : \'true\');\n  if (\'inert\' in box) box.inert = !open;\n  document.querySelectorAll(\'[data-power-btn="warmup"]\').forEach(b => b.setAttribute(\'aria-expanded\', open ? \'true\' : \'false\'));\n}\nfunction closeWarmupPicker(restoreFocus = true) { setWarmupPickerOpen(false, restoreFocus); }\nfunction warmupAll(triggerBtn) {\n  if (instanceActive === false) { toast(\'请先开机；预热不会自动创建实例\'); return; }\n  if (instanceActive !== true || powerBusyTask() || !powerJobsKnown || statusLoading) return;\n  const box = document.getElementById(\'warmupPicker\');\n  const nextOpen = !box.classList.contains(\'open\');\n  setWarmupPickerOpen(nextOpen, false, triggerBtn || null);\n  if (nextOpen) syncWarmupPicker();\n}\n\nlet colabAccountsState = { active_email: \'\', drive_sync_email: \'\', accounts: [] };\nlet accountSwitching = false;\nlet accountLoading = false;\nlet currentOauthUrl = \'\';\n\nfunction alignAccountPickerToBadge() {\n  positionFixedPopover(document.getElementById(\'accountPicker\'), document.getElementById(\'cuBadge\'), 396);\n}\n\nfunction setAccountPickerOpen(open) {\n  const box = document.getElementById(\'accountPicker\');\n  const badge = document.getElementById(\'cuBadge\');\n  if (!box || !badge) return;\n  if (open) {\n    setWarmupPickerOpen(false, false);\n    alignAccountPickerToBadge();\n  }\n  box.classList.toggle(\'open\', open);\n  box.setAttribute(\'aria-hidden\', open ? \'false\' : \'true\');\n  if (\'inert\' in box) box.inert = !open;\n  badge.setAttribute(\'aria-expanded\', open ? \'true\' : \'false\');\n  if (!open) {\n    const ob = document.getElementById(\'acctOauthBox\');\n    if (ob) ob.hidden = true;\n  }\n}\nwindow.addEventListener(\'resize\', () => {\n  if (document.getElementById(\'accountPicker\')?.classList.contains(\'open\')) {\n    alignAccountPickerToBadge();\n  }\n});\n\nfunction toggleAccountPicker() {\n  const box = document.getElementById(\'accountPicker\');\n  if (!box) return;\n  const nextOpen = !box.classList.contains(\'open\');\n  setAccountPickerOpen(nextOpen);\n  if (nextOpen) {\n    renderColabAccountsList();\n    loadColabAccounts(false);\n  }\n}\n\nfunction renderColabAccountsList() {\n  const listEl = document.getElementById(\'acctList\');\n  const totEl = document.getElementById(\'acctTotalCu\');\n  if (!listEl) return;\n  const rows = Array.isArray(colabAccountsState?.accounts) ? colabAccountsState.accounts : [];\n  if (!rows.length) {\n    listEl.replaceChildren(el(\'div\', \'warmup-hint\', accountLoading ? \'正在读取账号与实时余额...\' : \'暂无已登录的 Colab 账号\'));\n    if (totEl) totEl.textContent = \'\';\n    return;\n  }\n  let sumCu = 0;\n  let hasNum = false;\n  rows.forEach(r => {\n    if (typeof r.balance_num === \'number\' && Number.isFinite(r.balance_num)) {\n      sumCu += r.balance_num;\n      hasNum = true;\n    }\n  });\n  if (totEl) {\n    totEl.textContent = rows.length > 1 && hasNum ? `共 ${rows.length} 个号 · 合计 ${sumCu.toFixed(2)} CU` : \'\';\n  }\n\n  const frag = document.createDocumentFragment();\n  rows.forEach(item => {\n    const btn = document.createElement(\'button\');\n    btn.type = \'button\';\n    btn.className = \'acct-row\' + (item.is_active ? \' is-active\' : \'\');\n    btn.dataset.action = \'switch-colab-account\';\n    btn.dataset.email = item.email || \'\';\n    btn.disabled = accountSwitching;\n\n    const main = el(\'div\', \'acct-main\');\n    const line1 = el(\'div\', \'acct-email-line\');\n    line1.append(el(\'span\', \'acct-email\', item.email || \'未知账号\'));\n    if (item.is_active) line1.append(el(\'span\', \'acct-tag primary\', \'当前使用\'));\n    if (item.is_drive_sync) line1.append(el(\'span\', \'acct-tag\', \'云盘同步\'));\n    main.append(line1);\n\n    const rateDisp = typeof item.rate === \'string\' && item.rate.endsWith(\'/hr\') ? item.rate.slice(0, -3) + \' CU/h\' : (item.rate || \'未知\');\n    const subTxt = item.active === true\n      ? `实例运行中 · ${rateDisp}`\n      : (item.active === false ? \'已关机 · 空闲备用\' : \'状态待刷新\');\n    main.append(el(\'div\', \'acct-sub\' + (item.active === true ? \' online\' : \'\'), subTxt));\n\n    const right = el(\'div\', \'acct-right\');\n    right.append(el(\'span\', \'acct-bal\', `${item.balance || \'未知\'} CU`));\n    const ns = \'http://www.w3.org/2000/svg\';\n    const chk = document.createElementNS(ns, \'svg\');\n    chk.setAttribute(\'class\', \'ico acct-check\');\n    chk.setAttribute(\'aria-hidden\', \'true\');\n    const u = document.createElementNS(ns, \'use\');\n    u.setAttribute(\'href\', \'#i-check\');\n    chk.append(u);\n    right.append(chk);\n\n    btn.append(main, right);\n    frag.append(btn);\n  });\n  listEl.replaceChildren(frag);\n}\n\nasync function loadColabAccounts(force = false) {\n  if (accountLoading && !force) return;\n  accountLoading = true;\n  const hint = document.getElementById(\'acctPickerHint\');\n  if (hint && force) hint.textContent = \'正在刷新余额...\';\n  renderColabAccountsList();\n  try {\n    const res = await fetch(force ? \'/api/colab_accounts?force=1\' : \'/api/colab_accounts\', { cache: \'no-store\' });\n    const data = await res.json().catch(() => ({}));\n    if (res.ok && data.ok) {\n      colabAccountsState = {\n        active_email: data.active_email || \'\',\n        drive_sync_email: data.drive_sync_email || \'\',\n        accounts: Array.isArray(data.accounts) ? data.accounts : [],\n      };\n      const cur = colabAccountsState.accounts.find(a => a.is_active);\n      if (cur) syncCuAccountBadge(cur.local_name, cur.short_label, cur.balance, cur.email);\n    }\n  } catch (_) {\n  } finally {\n    accountLoading = false;\n    if (hint && hint.textContent === \'正在刷新余额...\') hint.textContent = \'\';\n    renderColabAccountsList();\n  }\n}\n\nfunction syncCuAccountBadge(localName, shortLabel, balanceStr, fullEmail) {\n  const deskEl = document.getElementById(\'cuAcctDesk\');\n  const mobEl = document.getElementById(\'cuAcctMob\');\n  const sepEl = document.getElementById(\'cuAcctSep\');\n  const cuEl = document.getElementById(\'cuText\');\n  const badgeEl = document.getElementById(\'cuBadge\');\n  const hasAcct = Boolean(localName || shortLabel);\n  if (deskEl) {\n    deskEl.textContent = localName || shortLabel || \'\';\n    deskEl.hidden = !hasAcct;\n  }\n  if (mobEl) {\n    mobEl.textContent = shortLabel || localName || \'\';\n    mobEl.hidden = !hasAcct;\n  }\n  if (sepEl) sepEl.hidden = !hasAcct;\n  if (cuEl && balanceStr) cuEl.textContent = `${balanceStr} CU`;\n  if (badgeEl && fullEmail) {\n    badgeEl.title = `当前算力账号: ${fullEmail}（点击切换或查看全部账号）`;\n  }\n}\n\nasync function requestSwitchColabAccount(email, stopCurrent = false) {\n  if (!email || accountSwitching) return;\n  const curActive = (Array.isArray(colabAccountsState?.accounts) ? colabAccountsState.accounts : []).find(a => a.is_active);\n  if (curActive && curActive.email === email) {\n    toast(`当前已在使用 ${email}`);\n    return;\n  }\n  if (!stopCurrent && (instanceActive === true || curActive?.active === true)) {\n    confirmAction(\n      \'切换前先关闭当前实例？\',\n      `当前账号 (${curActive?.email || \'原账号\'}) 仍有运行中的 Colab 实例。为避免原账号在后台持续扣费，将先自动关机确认释放后再切换至 ${email}。`,\n      () => requestSwitchColabAccount(email, true),\n      \'关机并切换\'\n    );\n    return;\n  }\n  accountSwitching = true;\n  const hint = document.getElementById(\'acctPickerHint\');\n  if (hint) hint.textContent = stopCurrent ? \'正在安全关闭当前实例并切换账号...\' : \'正在切换算力账号...\';\n  renderColabAccountsList();\n  try {\n    const res = await fetch(\'/api/colab_accounts/switch\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ email, stop_current: stopCurrent }),\n    });\n    const data = await res.json().catch(() => ({}));\n    if (res.status === 409 && data.need_stop_confirm && !stopCurrent) {\n      accountSwitching = false;\n      if (hint) hint.textContent = \'\';\n      renderColabAccountsList();\n      confirmAction(\n        \'切换前先关闭当前实例？\',\n        data.message || \'当前账号仍有运行中的实例，是否先自动关机再切换？\',\n        () => requestSwitchColabAccount(email, true),\n        \'关机并切换\'\n      );\n      return;\n    }\n    if (!res.ok || !data.ok) {\n      toast(data.message || \'切换账号失败\');\n      return;\n    }\n    if (data.accounts_state && Array.isArray(data.accounts_state.accounts)) {\n      colabAccountsState = data.accounts_state;\n    }\n    toast(data.message || `已切换至 ${email}`);\n    await loadColabStatus(true);\n    await pollJob();\n  } catch (err) {\n    toast(err.message || \'网络异常，切换失败\');\n  } finally {\n    accountSwitching = false;\n    if (hint) hint.textContent = \'\';\n    renderColabAccountsList();\n  }\n}\n\nasync function toggleAccountOauthBox() {\n  const box = document.getElementById(\'acctOauthBox\');\n  const inp = document.getElementById(\'acctOauthCodeInp\');\n  const link = document.getElementById(\'acctOauthLink\');\n  const hint = document.getElementById(\'acctPickerHint\');\n  if (!box) return;\n  if (!box.hidden) {\n    box.hidden = true;\n    return;\n  }\n  if (hint) hint.textContent = \'正在生成 Google 授权链接...\';\n  try {\n    const res = await fetch(\'/api/colab_accounts/oauth_start\', { method: \'POST\' });\n    const data = await res.json().catch(() => ({}));\n    if (!res.ok || !data.ok || !data.auth_url) throw new Error(data.message || \'生成授权链接失败\');\n    currentOauthUrl = data.auth_url;\n    if (link) link.href = (typeof currentOauthUrl === \'string\' && /^https:\\/\\//i.test(currentOauthUrl)) ? currentOauthUrl : \'#\';\n    if (inp) inp.value = \'\';\n    box.hidden = false;\n    setTimeout(() => inp?.focus(), 60);\n  } catch (err) {\n    toast(err.message || \'生成授权链接失败\');\n  } finally {\n    if (hint) hint.textContent = \'\';\n  }\n}\n\nasync function submitAccountOauthCode() {\n  const inp = document.getElementById(\'acctOauthCodeInp\');\n  const btn = document.getElementById(\'acctOauthSubmitBtn\');\n  const box = document.getElementById(\'acctOauthBox\');\n  const code = (inp?.value || \'\').trim();\n  if (!code) {\n    toast(\'请先粘贴 Google 授权码\');\n    inp?.focus();\n    return;\n  }\n  if (btn) btn.disabled = true;\n  try {\n    const res = await fetch(\'/api/colab_accounts/oauth_complete\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ code }),\n    });\n    const data = await res.json().catch(() => ({}));\n    if (!res.ok || !data.ok) throw new Error(data.message || \'授权验证失败\');\n    if (inp) inp.value = \'\';\n    if (box) box.hidden = true;\n    if (data.accounts_state && Array.isArray(data.accounts_state.accounts)) {\n      colabAccountsState = data.accounts_state;\n    } else {\n      await loadColabAccounts(true);\n    }\n    renderColabAccountsList();\n    await loadColabStatus(true);\n    toast(data.message || \'新账号登录成功\');\n  } catch (err) {\n    toast(err.message || \'授权验证失败\');\n  } finally {\n    if (btn) btn.disabled = false;\n  }\n}\n\ndocument.addEventListener(\'pointerdown\', (e) => {\n  const box = document.getElementById(\'warmupPicker\');\n  if (box && box.classList.contains(\'open\') && !e.target.closest(\'#warmupPicker\') && !e.target.closest(\'#warmupG4Btn\')) {\n    setWarmupPickerOpen(false, false);\n  }\n  const abox = document.getElementById(\'accountPicker\');\n  if (abox && abox.classList.contains(\'open\') && !e.target.closest(\'#accountPicker\') && !e.target.closest(\'#cuBadge\')) {\n    setAccountPickerOpen(false);\n  }\n  if (document.querySelector(\'.drive-sync-picker.open\') && !e.target.closest(\'.drive-sync-anchor\')) {\n    closeAllDriveSyncPickers();\n  }\n});\ndocument.addEventListener(\'change\', e => {\n  const el = e.target.closest(\'[data-warmup-target]\');\n  if (!el) return;\n  warmupCustomized = true;\n  const key = el.dataset.warmupTarget;\n  warmupTargets = el.checked ? [...warmupTargets.filter(t => t !== key), key] : warmupTargets.filter(t => t !== key);\n  syncWarmupPicker();\n  scheduleSavePanelUiState();\n});\n\nasync function submitPowerTask(mod) {\n  const task = powerTasks[mod];\n  if (task.submitting || task.pending) return;\n  if (powerBusyTask() || statusLoading || !powerJobsKnown) return;\n  if (mod === \'warmup\' && instanceActive !== true) {\n    toast(\'请先开机；预热不会自动创建实例\');\n    return;\n  }\n  if (mod === \'start\' && instanceActive !== false) return;\n  if (mod === \'stop\' && instanceActive !== true) return;\n  if (mod === \'warmup\' && !warmupTargets.length) { toast(\'请至少选择一项\'); return; }\n  const selectedTargets = [...warmupTargets];\n  task.submitting = true;\n  task.epoch += 1;\n  if (mod !== \'warmup\') {\n    powerJobsKnown = false;\n    instanceActive = null;\n    statusRequestSeq += 1;\n    statusLoading = false;\n  }\n  syncPowerButton(mod);\n  _lastTasksSig = \'\';\n  renderTaskLedger(tasksList);\n  try {\n    const res = await fetch(\'/api/run\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify(mod === \'warmup\' ? { module: mod, targets: selectedTargets } : { module: mod })\n    });\n    const data = await res.json();\n    if (!res.ok || !data.ok) throw new Error(data.message || \'请求失败\');\n    task.epoch += 1;\n    task.pending = true;\n    if (mod === \'warmup\') setWarmupPickerOpen(false, false);\n    toast(data.message || (task.name + \'请求已受理\'));\n    await pollJob();\n  } catch (e) {\n    task.epoch += 1;\n    task.pending = false;\n    toast(task.name + \'异常：\' + (e.message || \'网络异常\'));\n    if (mod !== \'warmup\') {\n      pollJob();\n      loadColabStatus(false);\n    }\n  } finally {\n    task.submitting = false;\n    syncPowerButton(mod);\n    _lastTasksSig = \'\';\n    renderTaskLedger(tasksList);\n  }\n}\n\n/* ==================== 任务轮询（精简轻量轮询 + 按需拉取详情） ==================== */\nlet lastCompletedSeq = null;\nlet jobPollSeq = 0;\nlet lastJobResponseSeq = 0;\n\nfunction formatSec(s) {\n  if (typeof s !== \'number\' || !Number.isFinite(s) || s <= 0) return \'未知\';\n  if (s < 1) return \'<1 s\';\n  s = Math.round(s);\n  if (s >= 60) return `${Math.floor(s / 60)}m ${String(s % 60).padStart(2, \'0\')}s`;\n  return `${s} s`;\n}\n\nlet clearingAllTasks = false;\nasync function clearAllTasks() {\n  if (clearingAllTasks || !powerJobsKnown || powerBusyTask()) return;\n  clearingAllTasks = true;\n  const btn = document.getElementById(\'clearTasksBtn\');\n  if (btn) btn.disabled = true;\n  try {\n    const res = await fetch(\'/api/task_action\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ action: \'clear_all\' })\n    });\n    const data = await res.json();\n    if (!res.ok || !data.ok) throw new Error(data.message || \'清空失败\');\n    expandedTaskIds.clear();\n    taskDetailCache.clear();\n    showingAllHistoryTasks = false;\n    _lastTasksSig = \'\';\n    toast(data.message || \'任务记录已清空\');\n  } catch (e) {\n    toast(e.message || \'清空失败\');\n  } finally {\n    clearingAllTasks = false;\n    await pollJob();\n  }\n}\n\nasync function executeTaskAction(jid, action) {\n  if (taskBusy.has(jid) || !powerJobsKnown || powerBusyTask()) return;\n  const task = tasksList.find(t => t.jid === jid);\n  if (!task) return;\n  if (action === \'delete\' && (task.can_delete !== true || task.state === \'running\' || task.state === \'queued\')) return;\n  if (action === \'stop\' && task.can_stop !== true) return;\n  if (action === \'resume\' && task.can_resume !== true) return;\n  taskBusy.add(jid);\n  _lastTasksSig = \'\';\n  renderTaskLedger(tasksList);\n  try {\n    const res = await fetch(\'/api/task_action\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ jid, action })\n    });\n    const data = await res.json();\n    if (!res.ok || !data.ok) throw new Error(data.message || \'操作未被受理\');\n    if (action === \'delete\') taskDetailCache.delete(jid);\n    toast(data.message || (action === \'delete\' ? `任务 #${jid} 已删除` : \'操作已受理\'));\n  } catch (e) {\n    toast(e.message || \'操作失败\');\n  } finally {\n    taskBusy.delete(jid);\n    _lastTasksSig = \'\';\n    await pollJob();\n  }\n}\n\nasync function ensureTaskDetailLoaded(jid) {\n  const t = tasksList.find(x => x.jid === jid);\n  if (!t) return;\n  if (t.full_prompt || t.cmd_s || taskDetailCache.has(jid)) return;\n  try {\n    const res = await fetch(\'/api/task_detail?jid=\' + encodeURIComponent(jid), { cache: \'no-store\' });\n    if (!res.ok) return;\n    const data = await res.json();\n    if (data && data.ok) {\n      taskDetailCache.set(jid, { full_prompt: data.full_prompt || \'\', cmd_s: data.cmd_s || \'\' });\n      _lastTasksSig = \'\';\n      renderTaskLedger(tasksList);\n    }\n  } catch (_) {}\n}\n\nfunction renderTaskLedger(tasks) {\n  const ledgerEl = document.getElementById(\'taskLedgerBox\');\n  if (!ledgerEl) return;\n  const prevScroll = ledgerEl.scrollTop;\n  const focused = document.activeElement;\n  const focusTask = ledgerEl.contains(focused)\n    ? { jid: focused.dataset.jid, action: focused.dataset.action } : null;\n  ledgerEl.replaceChildren();\n\n  if (!tasks.length) {\n    ledgerEl.append(el(\'div\', \'task-empty\', \'暂无任务记录（提交后在此查看实时进度、排队与历史记录）\'));\n    return;\n  }\n\n  tasks.forEach(t => {\n    const isOpen = expandedTaskIds.has(t.jid);\n    const busy = taskBusy.has(t.jid) || !powerJobsKnown || !!powerBusyTask();\n    const wrap = el(\'div\', \'task-item-wrap \' + (t.state || \'\'));\n    wrap.dataset.jid = String(t.jid);\n\n    const row = el(\'div\', \'task-row\');\n    row.dataset.action = \'toggle-task\';\n    row.dataset.jid = String(t.jid);\n\n    const badgeEl = el(\'span\', \'task-badge\');\n    badgeEl.append(el(\'span\', \'s-dot\'), el(\'span\', \'\', t.badge || t.state || \'\'));\n\n    row.append(\n      badgeEl,\n      el(\'span\', \'task-name\', \'#\' + t.jid + \' \' + (t.label || \'任务\')),\n      el(\'span\', \'task-preview\', (t.resume_disabled_reason ? t.resume_disabled_reason + \' · \' : \'\') + (t.preview || \'\')),\n      el(\'span\', \'task-time\', t.time_info || \'\')\n    );\n\n    const actions = el(\'div\', \'task-actions\');\n    const detailBtn = el(\'button\', \'task-mini-btn\');\n    detailBtn.type = \'button\';\n    detailBtn.dataset.action = \'toggle-task\';\n    detailBtn.dataset.jid = String(t.jid);\n    detailBtn.setAttribute(\'aria-expanded\', isOpen ? \'true\' : \'false\');\n    setControlText(detailBtn, isOpen ? \'收起\' : \'详情\');\n    actions.append(detailBtn);\n\n    if (t.can_stop === true) {\n      const stopBtn = el(\'button\', \'task-mini-btn stop\');\n      stopBtn.type = \'button\';\n      stopBtn.disabled = busy;\n      stopBtn.dataset.action = \'task-stop\';\n      stopBtn.dataset.jid = String(t.jid);\n      setControlText(stopBtn, \'终止\');\n      actions.append(stopBtn);\n    }\n    if (t.can_resume === true) {\n      const resBtn = el(\'button\', \'task-mini-btn resume\');\n      resBtn.type = \'button\';\n      resBtn.disabled = busy;\n      resBtn.dataset.action = \'task-resume\';\n      resBtn.dataset.jid = String(t.jid);\n      setControlText(resBtn, \'继续\');\n      actions.append(resBtn);\n    }\n    if (t.can_delete === true && t.state !== \'running\' && t.state !== \'queued\') {\n      const delBtn = el(\'button\', \'task-mini-btn delete\');\n      delBtn.type = \'button\';\n      delBtn.disabled = busy;\n      delBtn.dataset.action = \'task-delete\';\n      delBtn.dataset.jid = String(t.jid);\n      setControlText(delBtn, \'删除\');\n      actions.append(delBtn);\n    }\n\n    row.append(actions);\n    wrap.append(row);\n\n    if (isOpen) {\n      const cached = taskDetailCache.get(t.jid);\n      const fpText = t.full_prompt || cached?.full_prompt || t.preview || \'(加载中...)\';\n      const cmdText = t.cmd_s || cached?.cmd_s || \'\';\n      const detailBox = el(\'div\', \'task-detail-box open\');\n      detailBox.append(\n        el(\'strong\', \'\', \'完整提示词 / 内容：\\n\'),\n        document.createTextNode(fpText)\n      );\n      if (cmdText) {\n        const cmdLine = el(\'div\', \'\', \'执行命令: \' + cmdText);\n        cmdLine.style.cssText = \'margin-top:6px;font-size:11px;color:var(--muted);\';\n        detailBox.append(cmdLine);\n      }\n      wrap.append(detailBox);\n    }\n\n    ledgerEl.append(wrap);\n  });\n\n  const shownHistory = tasks.filter(t => t.state !== \'running\' && t.state !== \'queued\').length;\n  if (!showingAllHistoryTasks && totalHistoryTasksCount > shownHistory) {\n    const moreWrap = el(\'div\', \'task-more-row\');\n    const moreBtn = el(\'button\', \'btn btn-xs\');\n    moreBtn.type = \'button\';\n    moreBtn.dataset.action = \'load-all-tasks\';\n    setControlText(moreBtn, `展开全部历史任务（当前显示 ${shownHistory} / 共 ${totalHistoryTasksCount} 条）`);\n    moreWrap.append(moreBtn);\n    ledgerEl.append(moreWrap);\n  }\n\n  ledgerEl.scrollTop = prevScroll;\n  if (focusTask) {\n    const replacement = Array.from(ledgerEl.querySelectorAll(\'button[data-action]\'))\n      .find(btn => btn.dataset.jid === focusTask.jid && btn.dataset.action === focusTask.action && !btn.disabled);\n    if (replacement) replacement.focus({ preventScroll: true });\n  }\n}\n\nlet _pollJobInFlight = false;\nasync function pollJob() {\n  if (_pollJobInFlight) return false;\n  if (document.hidden && !submitting) return false;\n  _pollJobInFlight = true;\n  const pollSeq = ++jobPollSeq;\n  const controller = new AbortController();\n  const timeout = setTimeout(() => controller.abort(), 10000);\n  const powerEpochs = { start: powerTasks.start.epoch, stop: powerTasks.stop.epoch, warmup: powerTasks.warmup.epoch };\n  try {\n    const url = showingAllHistoryTasks ? \'/api/job?limit=0\' : \'/api/job?limit=80\';\n    const res = await fetch(url, { signal: controller.signal, cache: \'no-store\' });\n    if (res.status === 401) { window.location.replace(\'/\'); return false; }\n    if (!res.ok) throw new Error(\'任务状态获取失败\');\n    const st = await res.json();\n    if (pollSeq < lastJobResponseSeq) return false;\n    lastJobResponseSeq = pollSeq;\n\n    let refreshInstance = false;\n    if (powerEpochs.start === powerTasks.start.epoch && powerEpochs.stop === powerTasks.stop.epoch) powerJobsKnown = true;\n    for (const mod of [\'start\', \'stop\', \'warmup\']) {\n      const task = powerTasks[mod];\n      if (powerEpochs[mod] === task.epoch) {\n        task.pending = !!st[mod + \'_pending\'];\n        syncPowerButton(mod);\n      }\n      if ((st[mod + \'_completed_jid\'] || 0) > task.completedJid) {\n        task.completedJid = st[mod + \'_completed_jid\'];\n        refreshInstance = true;\n      }\n    }\n    syncPowerButton(\'start\');\n    if (refreshInstance) {\n      instanceActive = null;\n      syncPowerButton(\'start\');\n      setTimeout(() => loadColabStatus(false), 31000);\n    } else if (!statusLoading && Date.now() - lastStatusAttempt >= 30000) {\n      loadColabStatus(false);\n    }\n\n    const cancelBtn = document.getElementById(\'cancelBtn\');\n    const timer = document.getElementById(\'jobTimer\');\n    const logBox = document.getElementById(\'logBox\');\n    const stageEl = document.getElementById(\'progStageText\');\n    const etaEl = document.getElementById(\'progEtaText\');\n    const fillEl = document.getElementById(\'progFillBar\');\n\n    if (cancelBtn) cancelBtn.disabled = !st.running;\n    const qCnt = st.queued_count || 0;\n    const aCnt = st.active_count || 0;\n    const isTaskActive = !!(st.running || aCnt > 0 || qCnt > 0);\n    const pillProg = st.progress || {};\n    const pCnt = st.paused_count || 0;\n    totalHistoryTasksCount = typeof st.total_history_count === \'number\' ? st.total_history_count : 0;\n    deletableTasksCount = typeof st.deletable_count === \'number\' ? st.deletable_count : 0;\n    const pillPct = (typeof pillProg.percent === \'number\' && Number.isFinite(pillProg.percent))\n      ? Math.max(0, Math.min(99, Math.round(pillProg.percent))) : null;\n    let pillTxt = \'任务队列\';\n    if (isTaskActive) {\n      if (pillPct !== null) pillTxt = `运行 ${aCnt} · ${pillPct}%` + (qCnt > 0 ? ` (+${qCnt})` : \'\');\n      else pillTxt = `运行 ${aCnt}` + (qCnt > 0 ? ` · 排队 ${qCnt}` : \' · 执行中\');\n    } else if (totalHistoryTasksCount > 0) {\n      pillTxt = `任务队列 (${totalHistoryTasksCount})`;\n    }\n    syncNavRunningDots(isTaskActive, pillTxt);\n\n    if (timer) {\n      if (st.running) {\n        const elapsedTxt = typeof st.elapsed === \'number\' ? ` · ${st.elapsed.toFixed(1)} s` : \'\';\n        timer.textContent = `运行 ${aCnt} · 排队 ${qCnt} · 暂停 ${pCnt}${elapsedTxt}`;\n      } else {\n        timer.textContent = `空闲 · 暂停 ${pCnt} · 历史 ${totalHistoryTasksCount}`;\n      }\n    }\n\n    const prog = st.progress || {};\n    const pct = typeof prog.percent === \'number\' && Number.isFinite(prog.percent)\n      ? Math.max(0, Math.min(prog.scope === \'phase\' ? 99 : 100, prog.percent))\n      : null;\n    const eta = typeof prog.eta_sec === \'number\' && Number.isFinite(prog.eta_sec) ? prog.eta_sec : null;\n    const indeterminate = prog.indeterminate === true || (st.running && pct === null);\n    const stageText = prog.stage_text || prog.phase_text || (st.running ? \'等待任务进度\' : \'空闲待命 · 随时可提交任务\');\n    if (stageEl) stageEl.textContent = stageText;\n    if (fillEl) {\n      fillEl.classList.toggle(\'indeterminate\', indeterminate);\n      fillEl.classList.toggle(\'tone-warn\', !indeterminate && pct !== null && pct > 60 && pct < 100);\n      fillEl.classList.toggle(\'tone-blue\', !indeterminate && pct === 100);\n      if (!indeterminate) fillEl.style.width = pct !== null ? `${pct}%` : \'0%\';\n      else fillEl.style.width = \'\';\n    }\n    const trackEl = document.getElementById(\'progTrack\');\n    if (trackEl) {\n      if (pct !== null && !indeterminate) trackEl.setAttribute(\'aria-valuenow\', String(pct));\n      else trackEl.removeAttribute(\'aria-valuenow\');\n      trackEl.setAttribute(\'aria-valuetext\', stageText);\n    }\n    if (etaEl) {\n      const parts = [];\n      if (pct !== null && !indeterminate) parts.push((prog.scope === \'phase\' ? \'采样 \' : \'\') + pct + \'%\');\n      if (typeof prog.step_value === \'number\' && typeof prog.step_max === \'number\') parts.push(`${prog.step_value}/${prog.step_max} 步`);\n      if (st.running && eta !== null && eta > 0) parts.push(`剩余 ${formatSec(eta)}`);\n      if (!parts.length) parts.push(st.running ? \'执行中\' : \'待命\');\n      etaEl.textContent = parts.join(\' · \');\n    }\n\n    tasksList = Array.isArray(st.tasks) ? st.tasks : [];\n    const clearTasksBtn = document.getElementById(\'clearTasksBtn\');\n    if (clearTasksBtn) {\n      const hasDeletable = deletableTasksCount > 0 || tasksList.some(t => t && t.can_delete === true);\n      clearTasksBtn.disabled = !hasDeletable || !powerJobsKnown || !!powerBusyTask() || clearingAllTasks;\n    }\n    const tSig = JSON.stringify(tasksList) + \'::\' + totalHistoryTasksCount + \'::\' + showingAllHistoryTasks + \'::\' + Array.from(expandedTaskIds).join(\',\') + \'::\' + Array.from(taskBusy).join(\',\') + \'::\' + powerJobsKnown + \'::\' + !!powerBusyTask();\n    if (tSig !== _lastTasksSig) {\n      _lastTasksSig = tSig;\n      renderTaskLedger(tasksList);\n    }\n\n    const newLog = st.log_text || \'等待提交任务...\';\n    if (logBox && newLog !== _lastLogText) {\n      const atBottom = (logBox.scrollHeight - logBox.scrollTop - logBox.clientHeight) < 36;\n      const readingScroll = logBox.scrollTop;\n      _lastLogText = newLog;\n      logBox.textContent = newLog;\n      logBox.scrollTop = atBottom ? logBox.scrollHeight : readingScroll;\n    }\n\n    const curSeq = st.completed_seq || 0;\n    if (lastCompletedSeq === null) {\n      lastCompletedSeq = curSeq;\n    } else if (curSeq !== lastCompletedSeq) {\n      lastCompletedSeq = curSeq;\n      await loadChat();\n      await loadOutputs(false);\n    }\n    renderChatIfNeeded();\n    return true;\n  } catch (e) {\n    if (pollSeq < lastJobResponseSeq) return false;\n    lastJobResponseSeq = pollSeq;\n    powerJobsKnown = false;\n    syncPowerButton(\'start\');\n    return false;\n  } finally {\n    clearTimeout(timeout);\n    _pollJobInFlight = false;\n  }\n}\n\ndocument.addEventListener(\'visibilitychange\', () => {\n  if (!document.hidden) pollJob();\n});\n\nasync function loadColabStatus(force = false) {\n  const seq = ++statusRequestSeq;\n  const showBusy = force || instanceActive === null;\n  if (showBusy) {\n    statusLoading = true;\n    syncPowerButton(\'start\');\n  }\n  lastStatusAttempt = Date.now();\n  let st = { active: null, balance: \'未知\', rate: \'未知\', email: \'\', local_name: \'\', short_label: \'\' };\n  try {\n    const res = await fetch(force ? \'/api/status?force=1\' : \'/api/status\', { cache: \'no-store\' });\n    if (!res.ok) throw new Error(\'状态检查失败\');\n    const data = await res.json();\n    st = {\n      active: data.active === true ? true : (data.active === false ? false : null),\n      balance: data.balance || \'未知\',\n      rate: data.rate || \'未知\',\n      email: data.email || \'\',\n      local_name: data.local_name || \'\',\n      short_label: data.short_label || \'\'\n    };\n  } catch (_) {}\n  if (seq !== statusRequestSeq) return;\n  instanceActive = st.active;\n  statusLoading = false;\n  const g4 = document.getElementById(\'g4Badge\');\n  const g4Text = document.getElementById(\'g4Text\');\n  const cu = document.getElementById(\'cuText\');\n  if (g4) {\n    g4.className = st.active === true ? \'status-pill on\' : (st.active === false ? \'status-pill off\' : \'status-pill\');\n  }\n  const rateDisplay = typeof st.rate === \'string\' && st.rate.endsWith(\'/hr\') ? st.rate.slice(0, -3) + \' CU/h\' : st.rate;\n  if (g4Text) {\n    const stateStr = st.active === true ? \'在线\' : (st.active === false ? \'已关机\' : \'未知\');\n    g4Text.replaceChildren(\n      el(\'span\', \'status-state\', stateStr),\n      el(\'span\', \'status-sep\', \'·\'),\n      el(\'span\', \'status-meta\', rateDisplay)\n    );\n  }\n  if (cu) cu.textContent = `${st.balance} CU`;\n  if (st.local_name || st.short_label) {\n    syncCuAccountBadge(st.local_name, st.short_label, st.balance, st.email);\n  }\n  syncPowerButton(\'start\');\n  if (st.active === true) {\n    pollHwMetrics();\n  } else if (st.active === false) {\n    renderHwMetricsSnapshot(null);\n  }\n}\n\n/* ==================== G4 实时 CPU / 内存 / 显存 / 硬盘常驻监控（永不隐藏零闪现；关机显示 --，刷新首帧秒显缓存） ==================== */\nconst HW_METRICS_CACHE_KEY = \'g4_hw_metrics_last_v1\';\nlet _metricsBusy = false;\n\nfunction applyStudioBar(barEl, pct) {\n  if (!barEl) return;\n  if (typeof pct !== \'number\' || !Number.isFinite(pct)) {\n    barEl.style.width = \'0%\';\n    barEl.className = \'hw-bar-fill\';\n    return;\n  }\n  const clamped = Math.max(0, Math.min(100, pct));\n  barEl.style.width = `${clamped}%`;\n  barEl.className = \'hw-bar-fill\' + (clamped > 90 ? \' danger\' : (clamped > 60 ? \' warn\' : \'\'));\n}\n\nfunction setHwTextWithSub(valEl, mainTxt, subTxt = \'\') {\n  if (!valEl) return;\n  const sig = `${mainTxt}|${subTxt}`;\n  if (valEl.dataset.hwSig === sig && valEl.childNodes.length > 0) return;\n  valEl.dataset.hwSig = sig;\n  if (!subTxt) {\n    valEl.textContent = mainTxt;\n    return;\n  }\n  valEl.replaceChildren(\n    document.createTextNode(mainTxt),\n    el(\'span\', \'hw-val-sub\', subTxt)\n  );\n}\n\nfunction renderHwMetricsSnapshot(m) {\n  const bar = document.getElementById(\'hwMetricsBar\');\n  if (!bar) return;\n  if (!m || !m.online) {\n    bar.setAttribute(\'data-offline\', \'true\');\n    setHwTextWithSub(document.getElementById(\'hwValCpu\'), \'--\');\n    setHwTextWithSub(document.getElementById(\'hwValRam\'), \'--\');\n    setHwTextWithSub(document.getElementById(\'hwValVram\'), \'--\');\n    setHwTextWithSub(document.getElementById(\'hwValDisk\'), \'--\');\n    applyStudioBar(document.getElementById(\'hwBarCpu\'), null);\n    applyStudioBar(document.getElementById(\'hwBarRam\'), null);\n    applyStudioBar(document.getElementById(\'hwBarVram\'), null);\n    applyStudioBar(document.getElementById(\'hwBarDisk\'), null);\n    try { sessionStorage.removeItem(HW_METRICS_CACHE_KEY); } catch (_) {}\n    return;\n  }\n  bar.removeAttribute(\'data-offline\');\n\n  if (typeof m.cpu_pct === \'number\') {\n    const coreSub = m.cpu_cores ? `${m.cpu_cores} 核` : \'\';\n    setHwTextWithSub(document.getElementById(\'hwValCpu\'), `${m.cpu_pct.toFixed(1)}%`, coreSub);\n    applyStudioBar(document.getElementById(\'hwBarCpu\'), m.cpu_pct);\n  } else {\n    setHwTextWithSub(document.getElementById(\'hwValCpu\'), \'--\');\n    applyStudioBar(document.getElementById(\'hwBarCpu\'), null);\n  }\n\n  if (typeof m.ram_used_gb === \'number\' && typeof m.ram_total_gb === \'number\') {\n    setHwTextWithSub(document.getElementById(\'hwValRam\'), `${m.ram_used_gb.toFixed(1)} / ${m.ram_total_gb.toFixed(0)} GB`, `${Math.round(m.ram_pct || 0)}%`);\n    applyStudioBar(document.getElementById(\'hwBarRam\'), m.ram_pct);\n  } else {\n    setHwTextWithSub(document.getElementById(\'hwValRam\'), \'--\');\n    applyStudioBar(document.getElementById(\'hwBarRam\'), null);\n  }\n\n  if (typeof m.vram_used_gb === \'number\' && typeof m.vram_total_gb === \'number\') {\n    setHwTextWithSub(document.getElementById(\'hwValVram\'), `${m.vram_used_gb.toFixed(1)} / ${m.vram_total_gb.toFixed(0)} GB`, `${Math.round(m.vram_pct || 0)}%`);\n    applyStudioBar(document.getElementById(\'hwBarVram\'), m.vram_pct);\n  } else {\n    setHwTextWithSub(document.getElementById(\'hwValVram\'), \'--\');\n    applyStudioBar(document.getElementById(\'hwBarVram\'), null);\n  }\n\n  if (typeof m.disk_used_gb === \'number\' && typeof m.disk_total_gb === \'number\') {\n    setHwTextWithSub(document.getElementById(\'hwValDisk\'), `${m.disk_used_gb.toFixed(1)} / ${m.disk_total_gb.toFixed(0)} GB`, `${Math.round(m.disk_pct || 0)}%`);\n    applyStudioBar(document.getElementById(\'hwBarDisk\'), m.disk_pct);\n  } else {\n    setHwTextWithSub(document.getElementById(\'hwValDisk\'), \'--\');\n    applyStudioBar(document.getElementById(\'hwBarDisk\'), null);\n  }\n\n  try { sessionStorage.setItem(HW_METRICS_CACHE_KEY, JSON.stringify(m)); } catch (_) {}\n}\n\n// 首帧同步水合最近一次硬件快照，网页刷新瞬间数值与进度条也不闪跳\n(function hydrateHwMetricsSync() {\n  try {\n    const cached = JSON.parse(sessionStorage.getItem(HW_METRICS_CACHE_KEY) || \'null\');\n    if (cached && cached.online) renderHwMetricsSnapshot(cached);\n    else renderHwMetricsSnapshot(null);\n  } catch (_) {\n    renderHwMetricsSnapshot(null);\n  }\n})();\n\nasync function pollHwMetrics() {\n  if (instanceActive === false) {\n    renderHwMetricsSnapshot(null);\n    return;\n  }\n  if (document.hidden || _metricsBusy) return;\n  _metricsBusy = true;\n  try {\n    const res = await fetch(\'/api/metrics\', { cache: \'no-store\' });\n    if (!res.ok) return;\n    const m = await res.json();\n    if (!m || !m.online || instanceActive === false) {\n      renderHwMetricsSnapshot(null);\n      return;\n    }\n    renderHwMetricsSnapshot(m);\n  } catch (_) {\n  } finally {\n    _metricsBusy = false;\n  }\n}\n\nsetInterval(pollHwMetrics, 3000);\ndocument.addEventListener(\'visibilitychange\', () => {\n  if (!document.hidden) pollHwMetrics();\n});\n\nasync function loadChat() {\n  try {\n    const res = await fetch(\'/api/chat\', { cache: \'no-store\' });\n    if (!res.ok) return;\n    const data = await res.json();\n    if (Array.isArray(data)) {\n      chatHistory = data;\n      renderChatIfNeeded();\n    }\n  } catch (_) {}\n}\n\n/* ==================== 作品库全量渲染 + 整行自适应分页 + 模型/搜索过滤 ==================== */\nfunction getAdaptivePageSize() {\n  if (currentTab === \'llm\') return 20;\n  const w = window.innerWidth || 1280;\n  if (w > 1220) return 24; // 4 列 × 6 行\n  if (w > 880) return 24;  // 3 列 × 8 行\n  return 24;               // 2 列 × 12 行（4/3/2 最小公倍数 24，任何断点都严格整行无残尾）\n}\n\nfunction updateTabCounts() {\n  const counts = outputsCache.counts || {};\n  let total = 0;\n  [\'image\', \'video\', \'upload_image\', \'llm\'].forEach(k => {\n    const len = typeof counts[k] === \'number\' ? counts[k] : (outputsCache[k] || []).length;\n    total += len;\n    const cEl = document.getElementById(\'cnt_\' + k);\n    if (cEl) cEl.textContent = String(len);\n  });\n  const totalValStr = String(typeof counts.total === \'number\' ? counts.total : total);\n  const totalDesk = document.getElementById(\'cnt_total_works_desk\');\n  if (totalDesk) totalDesk.textContent = totalValStr;\n  const totalStandalone = document.getElementById(\'cnt_total_works_standalone\');\n  if (totalStandalone) totalStandalone.textContent = totalValStr;\n\n  document.querySelectorAll(\'[data-mirror-id^="cnt_"]\').forEach(mEl => {\n    const srcEl = document.getElementById(mEl.dataset.mirrorId);\n    if (srcEl) mEl.textContent = srcEl.textContent;\n  });\n  document.querySelectorAll(\'.tabs-bar .tab-pill\').forEach(btn => {\n    const isAct = btn.dataset.tab === currentTab;\n    btn.classList.toggle(\'active\', isAct);\n    btn.setAttribute(\'aria-pressed\', isAct ? \'true\' : \'false\');\n  });\n}\n\nasync function loadOutputs(forceRender) {\n  try {\n    const res = await fetch(\'/api/outputs\', { cache: \'no-store\' });\n    if (!res.ok) return;\n    const data = await res.json();\n    if (data && typeof data === \'object\') {\n      outputsCache = data;\n      _outputsLoadedOnce = true;\n      updateTabCounts();\n      renderRefStripsIfNeeded();\n      renderGallery(forceRender);\n      loadDriveSyncStatus();\n    }\n  } catch (_) {}\n}\n\n/* ==================== Google Drive 加密同步 & Telegram 频道同步控制 ==================== */\nlet driveSyncState = null;\nlet _driveSyncPollTimer = null;\nlet _driveSyncSubmitting = false;\n\nfunction alignDriveSyncPicker(box) {\n  const anchor = box?.parentElement;\n  if (!box || !anchor) return;\n  box.style.left = \'auto\';\n  box.style.right = \'0px\';\n  const aRect = anchor.getBoundingClientRect();\n  const vw = document.documentElement.clientWidth || window.innerWidth || 390;\n  const host = (!isMobileScrollShell() && box.closest(\'.card-works\')) || null;\n  const hRect = host ? host.getBoundingClientRect() : null;\n  const minLeft = hRect ? Math.round(hRect.left + 12) : 10;\n  const maxRight = hRect ? Math.round(hRect.right - 12) : (vw - 10);\n  const w = Math.min(308, Math.max(220, maxRight - minLeft));\n  const idealLeft = aRect.right - w;\n  if (idealLeft < minLeft) {\n    box.style.right = \'auto\';\n    box.style.left = `${Math.round(minLeft - aRect.left)}px`;\n  } else if (aRect.right > maxRight) {\n    box.style.left = \'auto\';\n    box.style.right = `${Math.round(aRect.right - maxRight)}px`;\n  }\n}\n\nfunction closeAllDriveSyncPickers() {\n  document.querySelectorAll(\'.drive-sync-anchor\').forEach(anc => {\n    const box = anc.querySelector(\'.drive-sync-picker\');\n    const btn = anc.querySelector(\'.drive-sync-trigger\');\n    if (box) {\n      box.classList.remove(\'open\');\n      box.setAttribute(\'aria-hidden\', \'true\');\n      if (\'inert\' in box) box.inert = true;\n    }\n    if (btn) btn.setAttribute(\'aria-expanded\', \'false\');\n  });\n}\n\nfunction toggleDriveSyncPicker(triggerBtn) {\n  const anc = triggerBtn?.closest(\'.drive-sync-anchor\');\n  const box = anc?.querySelector(\'.drive-sync-picker\');\n  if (!anc || !box) return;\n  const willOpen = !box.classList.contains(\'open\');\n  closeAllDriveSyncPickers();\n  if (willOpen) {\n    setWarmupPickerOpen(false, false);\n    setAccountPickerOpen(false);\n    alignDriveSyncPicker(box);\n    box.classList.add(\'open\');\n    box.setAttribute(\'aria-hidden\', \'false\');\n    if (\'inert\' in box) box.inert = false;\n    triggerBtn.setAttribute(\'aria-expanded\', \'true\');\n    loadDriveSyncStatus(true);\n  }\n}\n\nwindow.addEventListener(\'resize\', () => {\n  document.querySelectorAll(\'.drive-sync-picker.open\').forEach(box => alignDriveSyncPicker(box));\n});\n\nfunction setTriggerLabelAndChevron(btn, text, iconId, iconCls) {\n  if (!btn) return;\n  const sig = `${iconId}|${iconCls}|${text}|chev`;\n  if (btn.dataset.ctlSig === sig && btn.childNodes.length > 0) return;\n  btn.dataset.ctlSig = sig;\n  btn.replaceChildren(\n    svgIcon(iconId, iconCls),\n    el(\'span\', \'control-label\', text),\n    svgIcon(\'i-chevron\', \'ico ds-chevron\')\n  );\n}\n\nfunction applyDriveSyncUI() {\n  const st = driveSyncState;\n  const dsEnabled = st ? (st.enabled !== false) : !!uiMetaConfig.drive_sync_enabled;\n  const chEnabled = st ? !!st.channel_enabled : !!uiMetaConfig.channel_sync_enabled;\n\n  document.querySelectorAll(\'.drive-sync-anchor\').forEach(anc => {\n    anc.hidden = !dsEnabled;\n  });\n  if (!dsEnabled) {\n    closeAllDriveSyncPickers();\n    return;\n  }\n\n  const task = (st && st.task) || {};\n  const isRunning = !!task.running || _driveSyncSubmitting;\n  const runMode = task.mode || \'\';\n\n  const upCnt = Number(st?.drive_uploaded || 0);\n  const pendUp = Number(st?.pending_upload || 0);\n  const pendDel = Number(st?.pending_delete || 0);\n  const errCnt = Number(st?.errors || 0);\n  const chCnt = Number(st?.channel_synced || 0);\n  const pendTotal = pendUp + pendDel;\n\n  document.querySelectorAll(\'.drive-sync-trigger\').forEach(btn => {\n    btn.classList.toggle(\'is-spinning\', isRunning);\n    if (isRunning) {\n      setTriggerLabelAndChevron(\n        btn,\n        runMode === \'channel\' ? \'推频道中\' : \'同步中\',\n        \'i-refresh\',\n        \'ico ico-14 ico-blue\'\n      );\n    } else if (pendTotal > 0) {\n      setTriggerLabelAndChevron(btn, `谷歌同步 (${pendTotal})`, \'i-cloud\', \'ico ico-14 ico-orange\');\n    } else {\n      setTriggerLabelAndChevron(btn, \'谷歌同步\', \'i-cloud\', \'ico ico-14 ico-blue\');\n    }\n  });\n\n  document.querySelectorAll(\'.drive-sync-picker\').forEach(box => {\n    const fl = box.querySelector(\'[data-ds-role="folder-link"]\');\n    if (fl) {\n      if (st?.folder_url) {\n        fl.href = (typeof st.folder_url === \'string\' && /^https:\\/\\//i.test(st.folder_url)) ? st.folder_url : \'#\';\n        fl.hidden = false;\n      } else {\n        fl.hidden = true;\n      }\n    }\n    const dc = box.querySelector(\'[data-ds-role="drive-count"]\');\n    if (dc) dc.textContent = st ? `${upCnt} 张` : \'-- 张\';\n\n    const cl = box.querySelector(\'[data-ds-role="channel-label"]\');\n    const cc = box.querySelector(\'[data-ds-role="channel-count"]\');\n    const chanStatBox = cc?.closest(\'.ds-stat-box\');\n    const statsGrid = box.querySelector(\'.ds-stats-grid\');\n    if (chanStatBox) chanStatBox.hidden = !chEnabled;\n    if (statsGrid) statsGrid.style.gridTemplateColumns = chEnabled ? \'repeat(2, minmax(0, 1fr))\' : \'minmax(0, 1fr)\';\n    if (cl) cl.textContent = st?.channel_username ? `TG 频道 ${st.channel_username}` : \'Telegram 频道\';\n    if (cc) cc.textContent = st ? `${chCnt} 张` : \'-- 张\';\n\n    const note = box.querySelector(\'[data-ds-role="status-note"]\');\n    if (note) {\n      note.classList.remove(\'is-syncing\', \'is-pending\', \'is-error\');\n      if (isRunning) {\n        note.classList.add(\'is-syncing\');\n        note.textContent = task.progress\n          ? task.progress.replace(/^[⏳🔄⬆️✅❌]+\\s*/, \'\')\n          : (runMode === \'channel\' ? \'正在将谷歌云图片同步至频道...\' : \'正在加密同步本地作品到 Google Drive...\');\n      } else if (errCnt > 0) {\n        note.classList.add(\'is-error\');\n        note.textContent = `有 ${errCnt} 项同步异常，点击下方按钮可立即重试`;\n      } else if (pendTotal > 0) {\n        note.classList.add(\'is-pending\');\n        const pts = [];\n        if (pendUp > 0) pts.push(`待上传 ${pendUp} 张`);\n        if (pendDel > 0) pts.push(`待移入回收站 ${pendDel} 张`);\n        note.textContent = pts.join(\' · \');\n      } else if (st) {\n        const acctPart = st.account_email ? `账号 ${st.account_email.split(\'@\')[0]}` : \'AES-256-GCM 加密\';\n        note.textContent = `${acctPart} · ${st.updated_str ? \'最近同步 \' + st.updated_str : \'已实时对齐\'}`;\n      } else {\n        note.textContent = \'正在读取同步状态...\';\n      }\n    }\n\n    const bDrive = box.querySelector(\'[data-ds-role="btn-drive"]\');\n    const bDriveTitle = box.querySelector(\'[data-ds-role="btn-drive-title"]\');\n    if (bDrive) {\n      const busyD = isRunning && runMode === \'drive\';\n      bDrive.disabled = isRunning;\n      bDrive.classList.toggle(\'is-busy\', busyD);\n      if (bDriveTitle) bDriveTitle.textContent = busyD ? \'正在同步谷歌云盘...\' : (chEnabled ? \'仅同步谷歌云盘\' : \'立即同步谷歌云盘\');\n    }\n\n    const bChan = box.querySelector(\'[data-ds-role="btn-channel"]\');\n    const bChanTitle = box.querySelector(\'[data-ds-role="btn-channel-title"]\');\n    if (bChan) {\n      bChan.hidden = !chEnabled;\n      bChan.style.display = chEnabled ? \'\' : \'none\';\n      const busyC = isRunning && runMode === \'channel\';\n      bChan.disabled = isRunning || !chEnabled;\n      bChan.classList.toggle(\'is-busy\', busyC);\n      if (bChanTitle) bChanTitle.textContent = busyC ? \'正在同步到 TG 频道...\' : \'同步到 TG 频道\';\n    }\n  });\n}\n\nasync function loadDriveSyncStatus(silent = true) {\n  try {\n    const res = await fetch(\'/api/drive_sync\', { cache: \'no-store\' });\n    if (!res.ok) return null;\n    const data = await res.json();\n    if (data && data.ok) {\n      const prevRunning = !!(driveSyncState && driveSyncState.task && driveSyncState.task.running);\n      driveSyncState = data;\n      applyDriveSyncUI();\n      const nowRunning = !!(data.task && data.task.running);\n      if (nowRunning) {\n        scheduleDriveSyncPoll();\n      } else {\n        stopDriveSyncPoll();\n        if (prevRunning && data.task && data.task.last_message) {\n          toast(data.task.last_message);\n        }\n      }\n      return data;\n    }\n  } catch (_) {\n    if (!silent) toast(\'读取谷歌同步状态失败\');\n  }\n  return null;\n}\n\nfunction scheduleDriveSyncPoll() {\n  if (_driveSyncPollTimer) return;\n  _driveSyncPollTimer = setInterval(() => {\n    loadDriveSyncStatus(true);\n  }, 1500);\n}\n\nfunction stopDriveSyncPoll() {\n  if (_driveSyncPollTimer) {\n    clearInterval(_driveSyncPollTimer);\n    _driveSyncPollTimer = null;\n  }\n}\n\nasync function triggerDriveSyncAction(mode) {\n  if (_driveSyncSubmitting || (driveSyncState?.task?.running)) {\n    toast(\'同步任务正在执行中，请稍候\');\n    return;\n  }\n  _driveSyncSubmitting = true;\n  if (!driveSyncState) driveSyncState = { ok: true, task: { running: true, mode } };\n  else driveSyncState.task = { ...(driveSyncState.task || {}), running: true, mode };\n  applyDriveSyncUI();\n  try {\n    const res = await fetch(\'/api/drive_sync\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ mode })\n    });\n    const data = await res.json().catch(() => ({}));\n    _driveSyncSubmitting = false;\n    if (data && typeof data === \'object\' && data.drive_uploaded !== undefined) {\n      driveSyncState = { ...data, ok: true };\n    }\n    if (!res.ok || data.ok === false) {\n      applyDriveSyncUI();\n      toast(data.message || \'同步启动失败\');\n      if (data.task?.running) scheduleDriveSyncPoll();\n      return;\n    }\n    applyDriveSyncUI();\n    toast(data.message || (mode === \'channel\' ? \'已开始同步到频道\' : \'已开始同步谷歌云\'));\n    scheduleDriveSyncPoll();\n  } catch (err) {\n    _driveSyncSubmitting = false;\n    if (driveSyncState?.task) driveSyncState.task.running = false;\n    applyDriveSyncUI();\n    toast(err.message || \'网络请求失败\');\n  }\n}\n\nfunction queryMirrorAll(idName) {\n  return document.querySelectorAll(`#${idName}, [data-mirror-id="${idName}"]`);\n}\n\nfunction syncBatchToolbarUI() {\n  queryMirrorAll(\'batchToolbar\').forEach(bar => { bar.hidden = !batchMode; });\n  queryMirrorAll(\'toggleBatchBtn\').forEach(toggleBtn => {\n    toggleBtn.classList.toggle(\'btn-selected\', batchMode);\n    toggleBtn.setAttribute(\'aria-pressed\', batchMode ? \'true\' : \'false\');\n  });\n  queryMirrorAll(\'toggleBatchBtnText\').forEach(toggleTxt => {\n    toggleTxt.textContent = batchMode ? \'退出批量\' : \'批量选择\';\n  });\n  if (!batchMode) return;\n\n  // Prune any stale names no longer in currentTab\n  const validSet = new Set((outputsCache[currentTab] || []).map(it => it.name));\n  for (const n of Array.from(batchSelectedNames)) {\n    if (!validSet.has(n)) batchSelectedNames.delete(n);\n  }\n  const cnt = batchSelectedNames.size;\n  queryMirrorAll(\'batchCountPill\').forEach(pill => pill.classList.toggle(\'has-sel\', cnt > 0));\n  queryMirrorAll(\'batchCountLabel\').forEach(lbl => {\n    lbl.replaceChildren(\'已选 \', el(\'strong\', \'\', String(cnt)), \' 项\');\n  });\n  queryMirrorAll(\'batchClearBtn\').forEach(b => { b.disabled = cnt === 0; });\n  queryMirrorAll(\'batchDownloadBtn\').forEach(b => { b.disabled = cnt === 0; });\n  queryMirrorAll(\'batchDeleteBtn\').forEach(b => { b.disabled = cnt === 0; });\n  queryMirrorAll(\'batchDownloadText\').forEach(t => { t.textContent = cnt > 0 ? `打包下载 (${cnt})` : \'打包下载\'; });\n  queryMirrorAll(\'batchDeleteText\').forEach(t => { t.textContent = cnt > 0 ? `批量删除 (${cnt})` : \'批量删除\'; });\n  queryMirrorAll(\'batchSelectAllText\').forEach(t => { t.textContent = `全选列表 (${filteredGalleryItems.length})`; });\n}\n\nfunction toggleBatchMode(forceVal) {\n  batchMode = typeof forceVal === \'boolean\' ? forceVal : !batchMode;\n  if (!batchMode) {\n    batchSelectedNames.clear();\n    lastBatchClickIdx = -1;\n  }\n  syncBatchToolbarUI();\n  renderGallery(true);\n  if (!_restoringUiState) savePanelUiState();\n}\n\nfunction toggleBatchItem(globalIdx, shiftKey) {\n  const item = filteredGalleryItems[globalIdx];\n  if (!item || !item.name) return;\n  const shouldSelect = !batchSelectedNames.has(item.name);\n  if (shiftKey && lastBatchClickIdx >= 0 && lastBatchClickIdx < filteredGalleryItems.length) {\n    const lo = Math.min(lastBatchClickIdx, globalIdx);\n    const hi = Math.max(lastBatchClickIdx, globalIdx);\n    for (let i = lo; i <= hi; i++) {\n      const nm = filteredGalleryItems[i]?.name;\n      if (!nm) continue;\n      if (shouldSelect) batchSelectedNames.add(nm);\n      else batchSelectedNames.delete(nm);\n    }\n  } else {\n    if (shouldSelect) batchSelectedNames.add(item.name);\n    else batchSelectedNames.delete(item.name);\n  }\n  lastBatchClickIdx = globalIdx;\n  syncBatchToolbarUI();\n  renderGallery(true);\n}\n\nfunction batchSelectCurrentPage() {\n  const pageSize = getAdaptivePageSize();\n  const pageStart = (galleryPage - 1) * pageSize;\n  const pageItems = filteredGalleryItems.slice(pageStart, pageStart + pageSize);\n  const allPageSelected = pageItems.length > 0 && pageItems.every(it => batchSelectedNames.has(it.name));\n  pageItems.forEach(it => {\n    if (!it.name) return;\n    if (allPageSelected) batchSelectedNames.delete(it.name);\n    else batchSelectedNames.add(it.name);\n  });\n  syncBatchToolbarUI();\n  renderGallery(true);\n}\n\nfunction batchSelectAllFiltered() {\n  const allSelected = filteredGalleryItems.length > 0 && filteredGalleryItems.every(it => batchSelectedNames.has(it.name));\n  filteredGalleryItems.forEach(it => {\n    if (!it.name) return;\n    if (allSelected) batchSelectedNames.delete(it.name);\n    else batchSelectedNames.add(it.name);\n  });\n  syncBatchToolbarUI();\n  renderGallery(true);\n}\n\nfunction submitBatchDownload() {\n  const names = Array.from(batchSelectedNames);\n  if (!names.length) { toast(\'请先勾选要下载的作品\'); return; }\n  const form = document.getElementById(\'batchDownloadForm\');\n  const inp = document.getElementById(\'batchDownloadPayload\');\n  if (!form || !inp) return;\n  inp.value = JSON.stringify({ module: currentTab, names });\n  form.submit();\n  toast(`正在打包下载 ${names.length} 个文件...`);\n}\n\nfunction submitBatchDelete() {\n  const names = Array.from(batchSelectedNames);\n  if (!names.length) { toast(\'请先勾选要删除的作品\'); return; }\n  confirmAction(\n    `确认批量删除已选的 ${names.length} 项作品？`,\n    \'删除后无法恢复（3K 图片将同步移入 Google Drive 回收站）。\',\n    async () => {\n      try {\n        const selectedSet = new Set(names);\n        const removedPaths = new Set(\n          (outputsCache[currentTab] || []).filter(it => selectedSet.has(it.name)).map(it => it.path)\n        );\n        const res = await fetch(\'/api/delete\', {\n          method: \'POST\',\n          headers: { \'Content-Type\': \'application/json\' },\n          body: JSON.stringify({ module: currentTab, names })\n        });\n        const data = await res.json();\n        if (!res.ok || !data.ok) throw new Error(data.message || \'批量删除失败\');\n        for (const key of [\'imgRefImage\', \'editRefImage\', \'vidRefImage\']) {\n          if (removedPaths.has(state[key])) state[key] = \'\';\n        }\n        batchSelectedNames.clear();\n        lastBatchClickIdx = -1;\n        await loadOutputs(true);\n        syncBatchToolbarUI();\n        toast(`已删除 ${data.removed ?? names.length} 项作品`);\n      } catch (e) {\n        toast(e.message || \'批量删除失败\');\n      }\n    },\n    \'确认批量删除\'\n  );\n}\n\nfunction switchTab(mod) {\n  if (currentTab === mod) return;\n  currentTab = mod;\n  galleryPage = 1;\n  batchSelectedNames.clear();\n  lastBatchClickIdx = -1;\n  updateTabCounts();\n  syncBatchToolbarUI();\n  renderGallery(true);\n}\n\nfunction deleteFileWithConfirm(mod, name, path) {\n  confirmAction(`确认删除「${name}」？`, \'删除后无法恢复（图片分类将同步移入云盘回收站）。\', async () => {\n    try {\n      const res = await fetch(\'/api/delete\', {\n        method: \'POST\',\n        headers: { \'Content-Type\': \'application/json\' },\n        body: JSON.stringify({ module: mod, name })\n      });\n      const data = await res.json();\n      if (!res.ok || !data.ok) throw new Error(data.message || \'删除失败\');\n      if (state.imgRefImage === path) state.imgRefImage = \'\';\n      if (state.editRefImage === path) state.editRefImage = \'\';\n      if (state.vidRefImage === path) state.vidRefImage = \'\';\n      await loadOutputs(true);\n      toast(\'已删除\');\n    } catch (e) {\n      toast(e.message || \'删除失败\');\n    }\n  });\n}\n\nfunction deleteAllInTab() {\n  const items = outputsCache[currentTab] || [];\n  if (!items.length) { toast(\'当前分类为空\'); return; }\n  const tabNameMap = { image: \'3K 图片\', video: \'高清视频\', upload_image: \'上传素材\', llm: \'提示词历史\' };\n  confirmAction(`确认清空「${tabNameMap[currentTab] || currentTab}」？`, `将删除该分类下全部 ${items.length} 个文件，删除后无法恢复。`, async () => {\n    try {\n      const res = await fetch(\'/api/delete\', {\n        method: \'POST\',\n        headers: { \'Content-Type\': \'application/json\' },\n        body: JSON.stringify({ module: currentTab, all: true })\n      });\n      const data = await res.json();\n      if (!res.ok || !data.ok) throw new Error(data.message || \'清空失败\');\n      const removedPaths = new Set(items.map(it => it.path));\n      for (const key of [\'imgRefImage\', \'editRefImage\', \'vidRefImage\']) {\n        if (removedPaths.has(state[key])) state[key] = \'\';\n      }\n      galleryPage = 1;\n      await loadOutputs(true);\n      toast(\'当前分类已清空\');\n    } catch (e) {\n      toast(e.message || \'清空失败\');\n    }\n  }, \'确认清空\');\n}\n\nfunction computeFilteredItems() {\n  const raw = outputsCache[currentTab] || [];\n  const q = gallerySearchQuery.trim().toLowerCase();\n  const cached = _galleryFilterCache;\n  if (cached && cached.raw === raw && cached.tab === currentTab &&\n      cached.query === q) return cached.items;\n  const items = raw.filter(it => {\n    if (q) {\n      const hay = ((it.name || \'\') + \'\\n\' + (it.prompt || \'\') + \'\\n\' + (it.preview_text || \'\') + \'\\n\' + (it.model_label || \'\')).toLowerCase();\n      if (!hay.includes(q)) return false;\n    }\n    return true;\n  });\n  // API refreshes replace the arrays; page/selection changes leave them untouched.\n  _galleryFilterCache = { raw, tab: currentTab, query: q, items };\n  return items;\n}\n\nfunction renderPaginationBar(totalItems, pageSize, totalPages) {\n  const bars = [\n    [\'galleryPaginationTop\', \'paginationInfoTop\', \'paginationPagesTop\'],\n    [\'galleryPagination\', \'paginationInfo\', \'paginationPages\']\n  ];\n  const pageNums = [];\n  if (totalPages > 1) {\n    for (let p = 1; p <= totalPages; p++) {\n      if (p === 1 || p === totalPages || Math.abs(p - galleryPage) <= 1) {\n        pageNums.push(p);\n      }\n    }\n  }\n  const startIdx = (galleryPage - 1) * pageSize + 1;\n  const endIdx = Math.min(totalItems, galleryPage * pageSize);\n  const infoText = (totalItems <= pageSize && totalPages <= 1)\n    ? (totalItems > 0 ? `共 ${totalItems} 项（全部展示）` : \'\')\n    : `第 ${startIdx}–${endIdx} 项 / 共 ${totalItems} 项（第 ${galleryPage}/${totalPages} 页）`;\n\n  bars.forEach(([barId, infoId, pagesId]) => {\n    const bar = document.getElementById(barId);\n    const info = document.getElementById(infoId);\n    const pages = document.getElementById(pagesId);\n    if (!bar || !info || !pages) return;\n    const isTopBar = barId === \'galleryPaginationTop\';\n    if (totalItems === 0) {\n      bar.hidden = !isTopBar;\n      if (isTopBar) info.textContent = \'当前分类暂无作品\';\n      pages.replaceChildren();\n      return;\n    }\n    bar.hidden = false;\n    info.textContent = infoText;\n    pages.replaceChildren();\n    if (totalItems <= pageSize && totalPages <= 1) return;\n\n    const addPageBtn = (label, targetPage, disabled = false, active = false) => {\n      const b = el(\'button\', \'page-btn\' + (active ? \' active\' : \'\'), label);\n      b.type = \'button\';\n      b.disabled = disabled;\n      b.dataset.action = \'goto-page\';\n      b.dataset.page = String(targetPage);\n      pages.append(b);\n    };\n\n    addPageBtn(\'上一页\', galleryPage - 1, galleryPage <= 1);\n    let prevP = 0;\n    for (const p of pageNums) {\n      if (prevP && p - prevP > 1) {\n        pages.append(el(\'span\', \'page-ellipsis\', \'…\'));\n      }\n      addPageBtn(String(p), p, false, p === galleryPage);\n      prevP = p;\n    }\n    addPageBtn(\'下一页\', galleryPage + 1, galleryPage >= totalPages);\n  });\n}\n\nfunction renderGallery(force) {\n  filteredGalleryItems = computeFilteredItems();\n  const pageSize = getAdaptivePageSize();\n  const totalItems = filteredGalleryItems.length;\n  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));\n  if (_outputsLoadedOnce && galleryPage > totalPages) galleryPage = totalPages;\n  if (galleryPage < 1) galleryPage = 1;\n\n  const pageStart = (galleryPage - 1) * pageSize;\n  const pageItems = filteredGalleryItems.slice(pageStart, pageStart + pageSize);\n\n  if (batchMode) syncBatchToolbarUI();\n\n  const selKey = batchMode ? (\'B1:\' + Array.from(batchSelectedNames).sort().join(\',\')) : \'B0\';\n  const sig = [\n    currentTab, galleryPage, pageSize, gallerySearchQuery, selKey,\n    totalItems, pageItems.map(x => (x.name || \'\') + \'_\' + (x.mtime || \'\')).join(\'|\')\n  ].join(\'::\');\n  if (!force && sig === _lastGallerySig) return;\n  _lastGallerySig = sig;\n\n  const box = document.getElementById(\'galleryContent\');\n  if (!box) return;\n  box.className = (currentTab === \'llm\') ? \'gallery-grid single-col\' : \'gallery-grid\';\n  box.replaceChildren();\n\n  renderPaginationBar(totalItems, pageSize, totalPages);\n\n  if (!pageItems.length) {\n    const msg = gallerySearchQuery.trim()\n      ? \'没有匹配当前筛选条件的作品\'\n      : \'当前分类暂无文件\';\n    const empty = el(\'div\', \'\', msg);\n    empty.style.cssText = \'color:var(--muted);font-size:13px;padding:42px 12px;text-align:center;grid-column:1/-1;\';\n    box.append(empty);\n    syncInactiveWorksMirror();\n    return;\n  }\n\n  pageItems.forEach((item, localIdx) => {\n    const globalFilteredIdx = pageStart + localIdx;\n    const fileName = item.name || \'\';\n    const srcMod = item.source_mod || currentTab;\n    const rawFileUrl = item.url || (\'/files/\' + srcMod + \'/\' + encodeURIComponent(fileName));\n    const thumbUrl = item.thumb_url || rawFileUrl;\n    const isSelected = batchMode && batchSelectedNames.has(fileName);\n\n    const card = el(\'div\', \'media-item\' + (currentTab === \'llm\' ? \' llm-card\' : \'\') + (batchMode ? \' batch-selectable\' : \'\') + (isSelected ? \' batch-selected\' : \'\'));\n    if (batchMode) {\n      card.dataset.action = \'batch-toggle-item\';\n      card.dataset.idx = String(globalFilteredIdx);\n    }\n\n    const makeBatchBadge = () => {\n      const badge = el(\'span\', \'batch-check-badge\');\n      badge.append(svgIcon(\'i-check\', \'ico ico-14\'));\n      return badge;\n    };\n\n    if (currentTab === \'image\' || currentTab === \'upload_image\') {\n      const mName = (currentTab === \'upload_image\') ? \'上传素材\' : displayModelName(item.model_label || uiMetaConfig.image_label || \'Flux / DiT INT8\');\n      const titleLine = mName + (item.steps ? ` · ${item.steps}步` : \'\');\n      const metaParts = [];\n      if (item.res) metaParts.push(item.res.replace(\'x\', \'×\'));\n      if (item.time_str) metaParts.push(item.time_str);\n      if (item.size_kb) metaParts.push(`${(item.size_kb / 1024).toFixed(1)} MB`);\n      const metaLine = metaParts.join(\' · \');\n\n      // 画面保持 100% 纯净无遮挡角标；模型与步数统一展示在下方标题栏\n      const thumbWrap = el(\'div\', \'media-thumb-wrap\');\n      if (!batchMode) {\n        thumbWrap.setAttribute(\'role\', \'button\');\n        thumbWrap.tabIndex = 0;\n        thumbWrap.setAttribute(\'aria-label\', \'全屏查看 3K 原图 \' + fileName);\n        thumbWrap.dataset.action = \'open-lightbox\';\n        thumbWrap.dataset.idx = String(globalFilteredIdx);\n      }\n\n      const img = el(\'img\');\n      img.src = thumbUrl;\n      img.alt = fileName;\n      img.loading = localIdx < 12 ? \'eager\' : \'lazy\';\n      img.decoding = \'sync\';\n      img.addEventListener(\'error\', () => {\n        if (img.src !== location.origin + rawFileUrl && !img.dataset.fellback) {\n          img.dataset.fellback = \'1\';\n          img.src = rawFileUrl;\n        }\n      });\n\n      thumbWrap.append(img);\n      if (batchMode) thumbWrap.append(makeBatchBadge());\n      card.append(thumbWrap);\n\n      const footer = el(\'div\', \'media-footer\');\n      const topRow = el(\'div\', \'media-top-row\');\n      const info = el(\'div\', \'media-info\');\n      info.append(\n        el(\'strong\', \'\', titleLine),\n        el(\'span\', \'\', metaLine)\n      );\n\n      const iconActions = el(\'div\', \'media-icon-actions\');\n      const dlLink = el(\'a\', \'btn btn-xs\');\n      dlLink.href = rawFileUrl;\n      dlLink.download = fileName;\n      dlLink.title = \'下载无损原图\';\n      setControlText(dlLink, \'原图\', \'i-download\', \'ico ico-14 ico-blue\');\n\n      const delBtn = el(\'button\', \'btn btn-xs btn-danger\');\n      delBtn.type = \'button\';\n      delBtn.dataset.action = \'delete-file\';\n      delBtn.dataset.mod = currentTab;\n      delBtn.dataset.name = fileName;\n      delBtn.dataset.path = item.path || \'\';\n      setControlText(delBtn, \'删除\');\n\n      iconActions.append(dlLink, delBtn);\n      topRow.append(info, iconActions);\n      footer.append(topRow);\n      card.append(footer);\n\n      if (item.prompt) {\n        const pBox = el(\'div\', \'img-prompt-box\');\n        const pHead = el(\'div\', \'img-prompt-head\');\n\n        const pMeta = el(\'span\', \'img-prompt-meta\', `提示词 · ${item.prompt.trim().length}字`);\n        if (!batchMode) {\n          pMeta.dataset.action = \'open-prompt-dialog\';\n          pMeta.dataset.idx = String(globalFilteredIdx);\n        }\n\n        const pActs = el(\'div\', \'img-prompt-actions\');\n\n        const cpBtn = el(\'button\', \'btn btn-xs\');\n        cpBtn.type = \'button\';\n        cpBtn.dataset.action = \'copy-img-prompt\';\n        cpBtn.dataset.idx = String(globalFilteredIdx);\n        setControlText(cpBtn, \'复制\');\n\n        const ruBtn = el(\'button\', \'btn btn-xs btn-selected\');\n        ruBtn.type = \'button\';\n        ruBtn.dataset.action = \'reuse-img-spec\';\n        ruBtn.dataset.idx = String(globalFilteredIdx);\n        setControlText(ruBtn, \'复用\');\n\n        pActs.append(cpBtn, ruBtn);\n        pHead.append(pMeta, pActs);\n\n        const pText = el(\'div\', \'img-prompt-text\', item.prompt);\n        if (!batchMode) {\n          pText.setAttribute(\'role\', \'button\');\n          pText.tabIndex = 0;\n          pText.dataset.action = \'open-prompt-dialog\';\n          pText.dataset.idx = String(globalFilteredIdx);\n        }\n        pBox.append(pHead, pText);\n        card.append(pBox);\n      }\n    } else if (currentTab === \'video\') {\n      const thumbWrap = el(\'div\', \'media-thumb-wrap video-wrap\');\n      const v = el(\'video\');\n      v.src = rawFileUrl;\n      v.controls = !batchMode;\n      v.preload = \'metadata\';\n      v.loop = true;\n      v.playsInline = true;\n      thumbWrap.append(v);\n      if (batchMode) thumbWrap.append(makeBatchBadge());\n\n      const vTitle = displayModelName(item.model_label || uiMetaConfig.video_label || \'24fps 声画视频\') + (item.steps ? ` · ${item.steps}步` : \'\');\n      const footer = el(\'div\', \'media-footer\');\n      const topRow = el(\'div\', \'media-top-row\');\n      const info = el(\'div\', \'media-info\');\n      info.append(\n        el(\'strong\', \'\', vTitle),\n        el(\'span\', \'\', (item.time_str || \'\') + (item.size_kb ? ` · ${(item.size_kb / 1024).toFixed(1)} MB` : \'\'))\n      );\n      const iconActions = el(\'div\', \'media-icon-actions\');\n      const dlLink = el(\'a\', \'btn btn-xs\');\n      dlLink.href = rawFileUrl;\n      dlLink.download = fileName;\n      setControlText(dlLink, \'下载\', \'i-download\', \'ico ico-14 ico-blue\');\n      const delBtn = el(\'button\', \'btn btn-xs btn-danger\');\n      delBtn.type = \'button\';\n      delBtn.dataset.action = \'delete-file\';\n      delBtn.dataset.mod = \'video\';\n      delBtn.dataset.name = fileName;\n      delBtn.dataset.path = item.path || \'\';\n      setControlText(delBtn, \'删除\');\n      iconActions.append(dlLink, delBtn);\n      topRow.append(info, iconActions);\n      footer.append(topRow);\n      card.append(thumbWrap, footer);\n    } else {\n      if (batchMode) card.append(makeBatchBadge());\n      if (item.preview_text) {\n        card.append(el(\'pre\', \'text-card\', item.preview_text));\n      }\n      const footer = el(\'div\', \'media-footer\');\n      const topRow = el(\'div\', \'media-top-row\');\n      const info = el(\'div\', \'media-info\');\n      info.append(el(\'strong\', \'\', item.time_str || fileName), el(\'span\', \'\', (item.size_kb || 0) + \' KB · \' + fileName));\n      const iconActions = el(\'div\', \'media-icon-actions\');\n\n      const cpBtn = el(\'button\', \'btn btn-xs\');\n      cpBtn.type = \'button\';\n      cpBtn.dataset.action = \'copy-llm-file\';\n      cpBtn.dataset.idx = String(globalFilteredIdx);\n      setControlText(cpBtn, \'复制\');\n\n      const ruBtn = el(\'button\', \'btn btn-xs btn-selected\');\n      ruBtn.type = \'button\';\n      ruBtn.dataset.action = \'reuse-llm-file\';\n      ruBtn.dataset.idx = String(globalFilteredIdx);\n      setControlText(ruBtn, \'填入生图\');\n\n      const delBtn = el(\'button\', \'btn btn-xs btn-danger\');\n      delBtn.type = \'button\';\n      delBtn.dataset.action = \'delete-file\';\n      delBtn.dataset.mod = \'llm\';\n      delBtn.dataset.name = fileName;\n      delBtn.dataset.path = item.path || \'\';\n      setControlText(delBtn, \'删除\');\n      iconActions.append(cpBtn, ruBtn, delBtn);\n      topRow.append(info, iconActions);\n      footer.append(topRow);\n      card.append(footer);\n    }\n\n    box.append(card);\n  });\n  syncInactiveWorksMirror();\n  applyDriveSyncUI();\n}\n\nlet searchDebounce = null;\ndocument.getElementById(\'gallerySearchInput\')?.addEventListener(\'input\', (e) => {\n  clearTimeout(searchDebounce);\n  searchDebounce = setTimeout(() => {\n    gallerySearchQuery = e.target.value || \'\';\n    galleryPage = 1;\n    renderGallery(true);\n    savePanelUiState();\n  }, 150);\n});\n\n/* ==================== 全屏无损 3K 原图看图层（Lightbox） ==================== */\nlet currentLightboxIdx = -1;\nlet currentLightboxList = [];\nlet lbScale = 1, lbX = 0, lbY = 0, lbDrag = null, lbLastFocusEl = null, lbPushedState = false, lbClosedAt = 0, lbLoadSeq = 0;\ndocument.addEventListener(\'click\', (ev) => {\n  if (lbClosedAt && (Date.now() - lbClosedAt < 450)) {\n    ev.preventDefault();\n    ev.stopPropagation();\n    ev.stopImmediatePropagation();\n  }\n}, true);\n\nfunction applyLbTransform() {\n  const imgEl = document.getElementById(\'lightboxImg\');\n  if (!imgEl) return;\n  imgEl.style.transform = `translate(${lbX}px, ${lbY}px) scale(${lbScale})`;\n}\n\nfunction clearLightboxImageBuffer() {\n  ++lbLoadSeq;\n  const imgEl = document.getElementById(\'lightboxImg\');\n  if (!imgEl) return;\n  imgEl.style.visibility = \'hidden\';\n  imgEl.style.width = \'\';\n  imgEl.style.height = \'\';\n  imgEl.removeAttribute(\'src\');\n}\n\nfunction applyExactLbBoxFromRes(imgEl, resStr) {\n  if (!imgEl) return;\n  const m = /^(\\d+)[x×](\\d+)$/i.exec(String(resStr || \'\').trim());\n  if (!m) {\n    imgEl.style.width = \'\';\n    imgEl.style.height = \'\';\n    return;\n  }\n  const rw = Number(m[1]), rh = Number(m[2]);\n  if (!(rw > 0 && rh > 0)) return;\n  imgEl.style.width = `min(96vw, ${rw}px, calc(min(96dvh, ${rh}px) * ${rw} / ${rh}))`;\n  imgEl.style.height = `min(96dvh, ${rh}px, calc(min(96vw, ${rw}px) * ${rh} / ${rw}))`;\n}\n\nfunction zoomLbAtPoint(factor, clientX, clientY) {\n  const nextScale = Math.min(8, Math.max(0.25, lbScale * factor));\n  if (nextScale === lbScale) return;\n  const stage = document.getElementById(\'lightboxStage\');\n  const rect = stage.getBoundingClientRect();\n  const cx = clientX - (rect.left + rect.width / 2);\n  const cy = clientY - (rect.top + rect.height / 2);\n  const ratio = nextScale / lbScale;\n  lbX = cx - (cx - lbX) * ratio;\n  lbY = cy - (cy - lbY) * ratio;\n  lbScale = nextScale;\n  applyLbTransform();\n}\n\nfunction showLightboxItem() {\n  const seq = ++lbLoadSeq;\n  lbDrag = null;\n  lbScale = 1;\n  lbX = 0;\n  lbY = 0;\n  applyLbTransform();\n  const imgEl = document.getElementById(\'lightboxImg\');\n  const hudEl = document.getElementById(\'lightboxHud\');\n  const total = currentLightboxList.length;\n  const canStep = total > 1;\n  const prevBtn = document.getElementById(\'lightboxPrev\');\n  const nextBtn = document.getElementById(\'lightboxNext\');\n  if (prevBtn) prevBtn.hidden = !canStep;\n  if (nextBtn) nextBtn.hidden = !canStep;\n  if (!(currentLightboxIdx >= 0 && currentLightboxIdx < total) || !imgEl) return;\n\n  const it = currentLightboxList[currentLightboxIdx];\n  const srcMod = it.source_mod || currentTab;\n  const rawUrl = it.url || (\'/files/\' + srcMod + \'/\' + encodeURIComponent(it.name || \'\'));\n  const thumbUrl = it.thumb_url || \'\';\n  const mLbl = displayModelName(it.model_label || \'\');\n  const resLbl = it.res ? it.res.replace(\'x\', \'×\') : \'\';\n  const baseHud = [`${currentLightboxIdx + 1} / ${total}`, mLbl, resLbl];\n\n  // 先彻底清空上一张帧缓冲，绝不残存旧图；再用已缓存的同比例缩略图 0ms 瞬时垫底，后台解码完无缝切换 3K 原图\n  imgEl.style.visibility = \'hidden\';\n  imgEl.removeAttribute(\'src\');\n  imgEl.alt = it.name || \'图片预览\';\n  applyExactLbBoxFromRes(imgEl, it.res);\n\n  if (thumbUrl && thumbUrl !== rawUrl) {\n    imgEl.src = thumbUrl;\n    imgEl.style.visibility = \'visible\';\n    if (hudEl) hudEl.textContent = [...baseHud, \'加载原图中…\'].filter(Boolean).join(\' · \');\n\n    const hi = new Image();\n    hi.decoding = \'async\';\n    hi.src = rawUrl;\n    const swapToRaw = () => {\n      if (seq !== lbLoadSeq) return;\n      imgEl.src = rawUrl;\n      imgEl.style.visibility = \'visible\';\n      if (!it.res) {\n        imgEl.style.width = \'\';\n        imgEl.style.height = \'\';\n      }\n      if (hudEl) hudEl.textContent = [...baseHud, \'3K 原图\'].filter(Boolean).join(\' · \');\n    };\n    if (typeof hi.decode === \'function\') {\n      hi.decode().then(swapToRaw).catch(() => {\n        if (hi.complete && hi.naturalWidth > 0) swapToRaw();\n        else hi.onload = swapToRaw;\n      });\n    } else {\n      hi.onload = swapToRaw;\n    }\n  } else {\n    if (hudEl) hudEl.textContent = [...baseHud, \'原图\'].filter(Boolean).join(\' · \');\n    const hi = new Image();\n    hi.decoding = \'async\';\n    hi.src = rawUrl;\n    const showDirect = () => {\n      if (seq !== lbLoadSeq) return;\n      imgEl.src = rawUrl;\n      imgEl.style.visibility = \'visible\';\n    };\n    if (typeof hi.decode === \'function\') {\n      hi.decode().then(showDirect).catch(() => { hi.onload = showDirect; showDirect(); });\n    } else {\n      hi.onload = showDirect;\n    }\n  }\n\n  // 后台静默预取相邻原图，左右切图更快\n  if (total > 1) {\n    const nextIt = currentLightboxList[(currentLightboxIdx + 1) % total];\n    if (nextIt?.url) { const pre = new Image(); pre.decoding = \'async\'; pre.src = nextIt.url; }\n  }\n}\n\nfunction openLightbox(idx, triggerEl) {\n  currentLightboxList = filteredGalleryItems.length ? filteredGalleryItems : (outputsCache[currentTab] || []);\n  if (!currentLightboxList.length) return;\n  currentLightboxIdx = typeof idx === \'number\' ? idx : 0;\n  lbLastFocusEl = triggerEl || document.activeElement;\n  clearLightboxImageBuffer();\n  const lb = document.getElementById(\'lightbox\');\n  if (!lb.classList.contains(\'open\')) {\n    lb.classList.add(\'open\');\n    lb.setAttribute(\'aria-hidden\', \'false\');\n    document.body.classList.add(\'lightbox-lock\');\n    try {\n      history.pushState({ g4Lightbox: true }, \'\');\n      lbPushedState = true;\n    } catch (_) {}\n  }\n  showLightboxItem();\n  lb.focus({ preventScroll: true });\n}\n\nfunction closeLightboxFromUI() {\n  const lb = document.getElementById(\'lightbox\');\n  if (!lb.classList.contains(\'open\')) return;\n  lbClosedAt = Date.now();\n  lb.classList.remove(\'open\');\n  lb.setAttribute(\'aria-hidden\', \'true\');\n  document.body.classList.remove(\'lightbox-lock\');\n  clearLightboxImageBuffer();\n  if (lbPushedState) {\n    lbPushedState = false;\n    try { history.back(); } catch (_) {}\n  }\n\n  lbLastFocusEl = null;\n}\n\nwindow.addEventListener(\'popstate\', () => {\n  const lb = document.getElementById(\'lightbox\');\n  if (lb && lb.classList.contains(\'open\')) {\n    lbPushedState = false;\n    lb.classList.remove(\'open\');\n    lb.setAttribute(\'aria-hidden\', \'true\');\n    document.body.classList.remove(\'lightbox-lock\');\n    clearLightboxImageBuffer();\n\n  }\n});\n\nfunction lightboxContentRect() {\n  const img = document.getElementById(\'lightboxImg\');\n  if (!img.naturalWidth || !img.naturalHeight) return null;\n  const r = img.getBoundingClientRect();\n  const fit = Math.min(r.width / img.naturalWidth, r.height / img.naturalHeight);\n  const w = img.naturalWidth * fit, h = img.naturalHeight * fit;\n  return {\n    left: r.left + (r.width - w) / 2, right: r.left + (r.width + w) / 2,\n    top: r.top + (r.height - h) / 2, bottom: r.top + (r.height + h) / 2\n  };\n}\nfunction lightboxOutside(x, y) {\n  const r = lightboxContentRect();\n  return !r || x < r.left || x > r.right || y < r.top || y > r.bottom;\n}\nfunction stepLightbox(delta) {\n  if (currentLightboxList.length < 2) return;\n  currentLightboxIdx = (currentLightboxIdx + delta + currentLightboxList.length) % currentLightboxList.length;\n  showLightboxItem();\n}\n\n(function initPureLightbox() {\n  const lb = document.getElementById(\'lightbox\');\n  const stage = document.getElementById(\'lightboxStage\');\n  const activePointers = new Map();\n  let lbPinch = null;\n\n  stage.addEventListener(\'wheel\', (e) => {\n    if (!lb.classList.contains(\'open\')) return;\n    e.preventDefault();\n    zoomLbAtPoint(e.deltaY < 0 ? 1.18 : 1 / 1.18, e.clientX, e.clientY);\n  }, { passive: false });\n\n  function startPinchFromPointers() {\n    const pts = Array.from(activePointers.values());\n    if (pts.length < 2) return;\n    const [p1, p2] = pts;\n    lbDrag = null;\n    stage.classList.remove(\'dragging\');\n    lbPinch = {\n      dist: Math.max(12, Math.hypot(p2.x - p1.x, p2.y - p1.y)),\n      cx: (p1.x + p2.x) / 2,\n      cy: (p1.y + p2.y) / 2,\n      baseScale: lbScale,\n      baseX: lbX,\n      baseY: lbY\n    };\n  }\n\n  stage.addEventListener(\'pointerdown\', (e) => {\n    if (e.button !== undefined && e.button !== 0) return;\n    activePointers.set(e.pointerId, { x: e.clientX, y: e.clientY, type: e.pointerType });\n    try { stage.setPointerCapture(e.pointerId); } catch (_) {}\n    if (activePointers.size >= 2) {\n      startPinchFromPointers();\n      return;\n    }\n    lbPinch = null;\n    const zoomed = lbScale > 1.01 || Math.hypot(lbX, lbY) > 3;\n    lbDrag = {\n      id: e.pointerId, x: e.clientX, y: e.clientY, px: lbX, py: lbY,\n      outside: lightboxOutside(e.clientX, e.clientY), maxMove: 0,\n      pan: zoomed || e.pointerType !== \'touch\', multiTouched: false\n    };\n    stage.classList.add(\'dragging\');\n  });\n\n  stage.addEventListener(\'pointermove\', (e) => {\n    if (activePointers.has(e.pointerId)) {\n      activePointers.set(e.pointerId, { x: e.clientX, y: e.clientY, type: e.pointerType });\n    }\n    if (activePointers.size >= 2 && lbPinch) {\n      const pts = Array.from(activePointers.values());\n      const [p1, p2] = pts;\n      const newDist = Math.max(12, Math.hypot(p2.x - p1.x, p2.y - p1.y));\n      const newCx = (p1.x + p2.x) / 2;\n      const newCy = (p1.y + p2.y) / 2;\n      const nextScale = Math.min(8, Math.max(0.5, lbPinch.baseScale * (newDist / lbPinch.dist)));\n      const rect = stage.getBoundingClientRect();\n      const originX = lbPinch.cx - (rect.left + rect.width / 2);\n      const originY = lbPinch.cy - (rect.top + rect.height / 2);\n      const ratio = nextScale / lbPinch.baseScale;\n      lbX = originX - (originX - lbPinch.baseX) * ratio + (newCx - lbPinch.cx);\n      lbY = originY - (originY - lbPinch.baseY) * ratio + (newCy - lbPinch.cy);\n      lbScale = nextScale;\n      applyLbTransform();\n      return;\n    }\n    if (!lbDrag || lbDrag.id !== e.pointerId) return;\n    const dx = e.clientX - lbDrag.x, dy = e.clientY - lbDrag.y;\n    lbDrag.maxMove = Math.max(lbDrag.maxMove, Math.hypot(dx, dy));\n    if (lbDrag.pan && lbDrag.maxMove > 10) {\n      lbX = lbDrag.px + dx;\n      lbY = lbDrag.py + dy;\n      applyLbTransform();\n    }\n  });\n\n  function endPointer(e, cancelled) {\n    const hadPinch = !!lbPinch || activePointers.size >= 2;\n    activePointers.delete(e.pointerId);\n    if (activePointers.size >= 2) {\n      startPinchFromPointers();\n      return;\n    }\n    lbPinch = null;\n    if (activePointers.size === 1) {\n      const [[remId, remPt]] = Array.from(activePointers.entries());\n      const zoomed = lbScale > 1.01 || Math.hypot(lbX, lbY) > 3;\n      lbDrag = {\n        id: remId, x: remPt.x, y: remPt.y, px: lbX, py: lbY,\n        outside: false, maxMove: 999,\n        pan: zoomed || remPt.type !== \'touch\', multiTouched: true\n      };\n      stage.classList.add(\'dragging\');\n      return;\n    }\n    if (lbScale < 1) {\n      lbScale = 1; lbX = 0; lbY = 0; applyLbTransform();\n    }\n    if (!lbDrag || lbDrag.id !== e.pointerId) {\n      lbDrag = null;\n      stage.classList.remove(\'dragging\');\n      return;\n    }\n    const d = lbDrag;\n    lbDrag = null;\n    stage.classList.remove(\'dragging\');\n    if (cancelled || hadPinch || d.multiTouched) return;\n    const dx = e.clientX - d.x, dy = e.clientY - d.y;\n    const moved = Math.max(d.maxMove, Math.hypot(dx, dy));\n    if (moved <= 10 && d.outside && lightboxOutside(e.clientX, e.clientY)) {\n      closeLightboxFromUI();\n      return;\n    }\n    if (!d.pan && Math.abs(dx) >= 60 && Math.abs(dx) > Math.abs(dy) * 1.6) {\n      stepLightbox(dx < 0 ? 1 : -1);\n    }\n  }\n\n  stage.addEventListener(\'touchend\', (e) => {\n    if (lb.classList.contains(\'open\')) e.preventDefault();\n  }, { passive: false });\n  stage.addEventListener(\'pointerup\', e => {\n    if (lb.classList.contains(\'open\')) e.preventDefault();\n    endPointer(e, false);\n  });\n  stage.addEventListener(\'pointercancel\', e => endPointer(e, true));\n  stage.addEventListener(\'lostpointercapture\', e => endPointer(e, true));\n  stage.addEventListener(\'dblclick\', (e) => {\n    if (lightboxOutside(e.clientX, e.clientY)) return;\n    if (Math.abs(lbScale - 1) > 0.05 || Math.hypot(lbX, lbY) > 5) {\n      lbScale = 1; lbX = 0; lbY = 0; applyLbTransform();\n    } else zoomLbAtPoint(2.2, e.clientX, e.clientY);\n  });\n})();\n\n/* ==================== 全局参数与提示词设置弹窗及主界面实时联动 ==================== */\nconst SETTINGS_TAB_LABELS = {\n  llm: \'提示词大师\',\n  image_prompt: \'生图词条与前后缀\',\n  image_workflow: \'生图工作流参数\'\n};\n\nfunction applyStudioSettingsToMainUI(cfg) {\n  if (!cfg || !cfg.image_workflow) return;\n  const wf = cfg.image_workflow;\n  const res = wf.resolutions || {};\n  const setResText = (id, r) => {\n    const node = document.getElementById(id);\n    if (node && r && r.s2_w && r.s2_h) node.textContent = `${r.s2_w}×${r.s2_h}`;\n  };\n  setResText(\'lblResPortrait\', res.portrait);\n  setResText(\'lblResLandscape\', res.landscape);\n  setResText(\'lblResSquare\', res.square);\n\n  const tiers = wf.step_tiers || {};\n  const enableHires = wf.enable_hires !== false;\n  [\n    [\'tier1\', \'lblStepsTier1\', \'hintStepsTier1\', false],\n    [\'tier2\', \'lblStepsTier2\', \'hintStepsTier2\', true],\n    [\'tier3\', \'lblStepsTier3\', \'hintStepsTier3\', false]\n  ].forEach(([tk, valId, hintId, isDef]) => {\n    const t = tiers[tk];\n    if (!t) return;\n    const s1 = Number(t.s1) || 8;\n    const ref = Number(t.ref) || 0;\n    const useHires = enableHires && ref > 0;\n    const tot = useHires ? (s1 + ref) : s1;\n    const vEl = document.getElementById(valId);\n    const hEl = document.getElementById(hintId);\n    if (vEl) vEl.textContent = `${tot} 步`;\n    if (hEl) {\n      hEl.textContent = useHires\n        ? `(${s1}+${ref}${isDef ? \' 默认\' : \'\'})`\n        : `(直出${isDef ? \' 默认\' : \'\'})`;\n    }\n  });\n}\n\nfunction createReplacePairRow(fromVal = \'\', toVal = \'\') {\n  const row = el(\'div\', \'replace-pair-row\');\n  const fromInp = el(\'input\', \'settings-input pair-from\');\n  fromInp.type = \'text\';\n  fromInp.placeholder = \'要查找的原词\';\n  fromInp.value = fromVal;\n  fromInp.spellcheck = false;\n\n  const arrow = el(\'span\', \'replace-arrow\', \'→\');\n\n  const toInp = el(\'input\', \'settings-input pair-to\');\n  toInp.type = \'text\';\n  toInp.placeholder = \'替换为（留空即删除）\';\n  toInp.value = toVal;\n  toInp.spellcheck = false;\n\n  const delBtn = el(\'button\', \'btn btn-xs btn-danger\');\n  delBtn.type = \'button\';\n  delBtn.dataset.action = \'remove-replace-pair\';\n  delBtn.title = \'删除此条规则\';\n  setControlText(delBtn, \'删除\');\n\n  row.append(fromInp, arrow, toInp, delBtn);\n  return row;\n}\n\nfunction updateModalTierTotals() {\n  const enableHires = !!document.getElementById(\'cfgWfEnableHires\')?.checked;\n  [\'tier1\', \'tier2\', \'tier3\'].forEach(tk => {\n    const s1 = parseInt(document.getElementById(`cfgTier_${tk}_s1`)?.value || \'0\', 10) || 0;\n    const ref = parseInt(document.getElementById(`cfgTier_${tk}_ref`)?.value || \'0\', 10) || 0;\n    const useHires = enableHires && ref > 0;\n    const tot = useHires ? (s1 + ref) : s1;\n    const pill = document.getElementById(`cfgTierTotal_${tk}`);\n    if (pill) pill.textContent = useHires ? `= ${tot} 步` : `= ${tot} 步 (直出)`;\n  });\n}\n\nfunction populateSettingsForm(cfg) {\n  if (!cfg) return;\n  const llm = cfg.llm || {};\n  document.getElementById(\'cfgLlmSystemPrompt\').value = llm.system_prompt || \'\';\n  document.getElementById(\'cfgLlmMultiTurnHint\').value = llm.multi_turn_hint ?? \'\';\n  document.getElementById(\'cfgLlmTemp\').value = llm.temperature ?? 0.6;\n  document.getElementById(\'cfgLlmMaxTokens\').value = llm.max_tokens ?? 1600;\n  document.getElementById(\'cfgLlmTopP\').value = llm.top_p ?? 0.9;\n  document.getElementById(\'cfgLlmTopK\').value = llm.top_k ?? 20;\n  document.getElementById(\'cfgLlmRepeatPenalty\').value = llm.repeat_penalty ?? 1.05;\n  document.getElementById(\'cfgLlmCheckPrefix\').value = String(llm.check_prefix ?? \'\').replace(/\\r?\\n/g, \'\\\\n\');\n  document.getElementById(\'cfgLlmStripTerms\').value = Array.isArray(llm.strip_terms) ? llm.strip_terms.join(\'\\n\') : \'\';\n\n  const ip = cfg.image_prompt || {};\n  document.getElementById(\'cfgImgEnableSuffix\').checked = ip.enable_suffix !== false;\n  document.getElementById(\'cfgImgZhSuffix\').value = ip.zh_suffix ?? \'\';\n  document.getElementById(\'cfgImgEnSuffix\').value = ip.en_suffix ?? \'\';\n  document.getElementById(\'cfgImgEnablePrefix\').checked = !!ip.enable_prefix;\n  document.getElementById(\'cfgImgZhPrefix\').value = ip.zh_prefix ?? \'\';\n  document.getElementById(\'cfgImgEnPrefix\').value = ip.en_prefix ?? \'\';\n  document.getElementById(\'cfgImgEnableClean\').checked = ip.enable_clean !== false;\n  document.getElementById(\'cfgImgStripRegexes\').value = Array.isArray(ip.strip_regexes) ? ip.strip_regexes.join(\'\\n\') : \'\';\n\n  const pairsBox = document.getElementById(\'cfgReplacePairsList\');\n  pairsBox.replaceChildren();\n  const pairs = Array.isArray(ip.replace_pairs) ? ip.replace_pairs : [];\n  pairs.forEach(p => {\n    pairsBox.append(createReplacePairRow(p.from ?? \'\', p.to ?? \'\'));\n  });\n\n  document.getElementById(\'cfgImgRefFacePrompt\').value = ip.ref_face_prompt ?? \'\';\n  document.getElementById(\'cfgRefScaleW\').value = ip.ref_scale_w ?? 768;\n  document.getElementById(\'cfgRefScaleH\').value = ip.ref_scale_h ?? 1152;\n  document.getElementById(\'cfgRefCropW\').value = ip.ref_crop_w ?? 512;\n  document.getElementById(\'cfgRefCropH\').value = ip.ref_crop_h ?? 448;\n  document.getElementById(\'cfgRefCropX\').value = ip.ref_crop_x ?? 128;\n  document.getElementById(\'cfgRefCropY\').value = ip.ref_crop_y ?? 0;\n\n  const wf = cfg.image_workflow || {};\n  document.getElementById(\'cfgWfEnableHires\').checked = wf.enable_hires !== false;\n  document.getElementById(\'cfgWfDenoise\').value = wf.hires_denoise ?? 0.16;\n  document.getElementById(\'cfgWfCfg\').value = wf.cfg ?? 1.0;\n  document.getElementById(\'cfgWfSampler\').value = wf.sampler_name || \'euler\';\n  document.getElementById(\'cfgWfScheduler\').value = wf.scheduler || \'simple\';\n  document.getElementById(\'cfgWfUpscale\').value = wf.upscale_method || \'lanczos\';\n\n  const res = wf.resolutions || {};\n  [\'portrait\', \'landscape\', \'square\'].forEach(rk => {\n    const r = res[rk] || {};\n    document.getElementById(`cfgRes_${rk}_s1w`).value = r.s1_w ?? 1024;\n    document.getElementById(`cfgRes_${rk}_s1h`).value = r.s1_h ?? 1536;\n    document.getElementById(`cfgRes_${rk}_s2w`).value = r.s2_w ?? 2048;\n    document.getElementById(`cfgRes_${rk}_s2h`).value = r.s2_h ?? 3072;\n  });\n\n  const tiers = wf.step_tiers || {};\n  [\'tier1\', \'tier2\', \'tier3\'].forEach(tk => {\n    const t = tiers[tk] || {};\n    document.getElementById(`cfgTier_${tk}_s1`).value = t.s1 ?? 8;\n    document.getElementById(`cfgTier_${tk}_ref`).value = t.ref ?? 5;\n  });\n  updateModalTierTotals();\n  syncAllStudioSelects();\n}\n\nfunction collectSettingsForm() {\n  const pairs = [];\n  document.querySelectorAll(\'#cfgReplacePairsList .replace-pair-row\').forEach(row => {\n    const fromVal = row.querySelector(\'.pair-from\')?.value ?? \'\';\n    const toVal = row.querySelector(\'.pair-to\')?.value ?? \'\';\n    if (fromVal !== \'\') pairs.push({ from: fromVal, to: toVal });\n  });\n  const num = (id) => Number(document.getElementById(id).value);\n  return {\n    llm: {\n      system_prompt: document.getElementById(\'cfgLlmSystemPrompt\').value,\n      multi_turn_hint: document.getElementById(\'cfgLlmMultiTurnHint\').value,\n      check_prefix: document.getElementById(\'cfgLlmCheckPrefix\').value.replace(/\\\\n/g, \'\\n\'),\n      temperature: num(\'cfgLlmTemp\'),\n      max_tokens: num(\'cfgLlmMaxTokens\'),\n      top_p: num(\'cfgLlmTopP\'),\n      top_k: num(\'cfgLlmTopK\'),\n      repeat_penalty: num(\'cfgLlmRepeatPenalty\'),\n      strip_terms: document.getElementById(\'cfgLlmStripTerms\').value.split(/\\r?\\n/).filter(s => s.trim() !== \'\')\n    },\n    image_prompt: {\n      enable_clean: document.getElementById(\'cfgImgEnableClean\').checked,\n      strip_regexes: document.getElementById(\'cfgImgStripRegexes\').value.split(/\\r?\\n/).map(s => s.trim()).filter(Boolean),\n      replace_pairs: pairs,\n      enable_prefix: document.getElementById(\'cfgImgEnablePrefix\').checked,\n      zh_prefix: document.getElementById(\'cfgImgZhPrefix\').value,\n      en_prefix: document.getElementById(\'cfgImgEnPrefix\').value,\n      enable_suffix: document.getElementById(\'cfgImgEnableSuffix\').checked,\n      zh_suffix: document.getElementById(\'cfgImgZhSuffix\').value,\n      en_suffix: document.getElementById(\'cfgImgEnSuffix\').value,\n      ref_face_prompt: document.getElementById(\'cfgImgRefFacePrompt\').value,\n      ref_scale_w: num(\'cfgRefScaleW\'),\n      ref_scale_h: num(\'cfgRefScaleH\'),\n      ref_crop_w: num(\'cfgRefCropW\'),\n      ref_crop_h: num(\'cfgRefCropH\'),\n      ref_crop_x: num(\'cfgRefCropX\'),\n      ref_crop_y: num(\'cfgRefCropY\')\n    },\n    image_workflow: {\n      enable_hires: document.getElementById(\'cfgWfEnableHires\').checked,\n      hires_denoise: num(\'cfgWfDenoise\'),\n      cfg: num(\'cfgWfCfg\'),\n      sampler_name: document.getElementById(\'cfgWfSampler\').value,\n      scheduler: document.getElementById(\'cfgWfScheduler\').value,\n      upscale_method: document.getElementById(\'cfgWfUpscale\').value,\n      resolutions: {\n        portrait: { s1_w: num(\'cfgRes_portrait_s1w\'), s1_h: num(\'cfgRes_portrait_s1h\'), s2_w: num(\'cfgRes_portrait_s2w\'), s2_h: num(\'cfgRes_portrait_s2h\') },\n        landscape: { s1_w: num(\'cfgRes_landscape_s1w\'), s1_h: num(\'cfgRes_landscape_s1h\'), s2_w: num(\'cfgRes_landscape_s2w\'), s2_h: num(\'cfgRes_landscape_s2h\') },\n        square: { s1_w: num(\'cfgRes_square_s1w\'), s1_h: num(\'cfgRes_square_s1h\'), s2_w: num(\'cfgRes_square_s2w\'), s2_h: num(\'cfgRes_square_s2h\') }\n      },\n      step_tiers: {\n        tier1: { s1: num(\'cfgTier_tier1_s1\'), ref: num(\'cfgTier_tier1_ref\') },\n        tier2: { s1: num(\'cfgTier_tier2_s1\'), ref: num(\'cfgTier_tier2_ref\') },\n        tier3: { s1: num(\'cfgTier_tier3_s1\'), ref: num(\'cfgTier_tier3_ref\') }\n      }\n    }\n  };\n}\n\nfunction switchSettingsTab(tab) {\n  if (!SETTINGS_TAB_LABELS[tab]) return;\n  currentSettingsTab = tab;\n  document.querySelectorAll(\'#settingsTabsBar [data-settings-tab]\').forEach(btn => {\n    const isAct = btn.dataset.settingsTab === tab;\n    btn.classList.toggle(\'active\', isAct);\n    btn.setAttribute(\'aria-selected\', isAct ? \'true\' : \'false\');\n  });\n  [\'llm\', \'image_prompt\', \'image_workflow\'].forEach(k => {\n    const pane = document.getElementById(\'settingsPane_\' + k);\n    if (pane) pane.classList.toggle(\'active\', k === tab);\n  });\n  const body = document.getElementById(\'settingsBody\');\n  if (body) body.scrollTop = 0;\n}\n\nasync function loadStudioSettings() {\n  try {\n    const res = await fetch(\'/api/settings\', { cache: \'no-store\' });\n    if (!res.ok) return null;\n    const data = await res.json();\n    if (data && data.ok && data.settings) {\n      studioSettings = data.settings;\n      if (data.defaults) studioDefaults = data.defaults;\n      if (data.ui_meta) applyUiMetaLabels(data.ui_meta);\n      applyStudioSettingsToMainUI(studioSettings);\n      return studioSettings;\n    }\n  } catch (_) {}\n  return null;\n}\n\nasync function ensureSettingsPagePopulated(forceFetch = false) {\n  const latest = (forceFetch || !studioSettings) ? await loadStudioSettings() : studioSettings;\n  populateSettingsForm(latest || studioSettings || studioDefaults);\n}\nasync function openSettingsDialog() {\n  navigateTo(\'settings\');\n  await ensureSettingsPagePopulated(true);\n}\nfunction closeSettingsDialog() { closeZenoSelect(false); }\nasync function saveStudioSettings() {\n  if (settingsSaving) return;\n  const saveBtn = document.getElementById(\'saveSettingsBtn\');\n  settingsSaving = true;\n  if (saveBtn) saveBtn.disabled = true;\n  try {\n    const payload = collectSettingsForm();\n    const res = await fetch(\'/api/settings\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify(payload)\n    });\n    const data = await res.json();\n    if (!res.ok || !data.ok) throw new Error(data.message || \'参数校验失败\');\n    studioSettings = data.settings;\n    populateSettingsForm(studioSettings);\n    applyStudioSettingsToMainUI(studioSettings);\n    toast(data.message || \'参数与提示词已保存，立即生效\');\n  } catch (e) {\n    toast(e.message || \'保存失败\');\n  } finally {\n    settingsSaving = false;\n    if (saveBtn) saveBtn.disabled = false;\n  }\n}\n\nfunction requestResetSettings(section) {\n  const isAll = section === \'all\';\n  const title = isAll ? \'恢复全部默认参数？\' : `恢复「${SETTINGS_TAB_LABELS[section] || section}」默认？`;\n  const desc = isAll\n    ? \'将提示词大师、生图词条前后缀、生图工作流三页参数全部恢复为出厂默认值并立即生效。\'\n    : `仅将当前「${SETTINGS_TAB_LABELS[section] || section}」页恢复为出厂默认值并立即保存生效，其余页签保持不变。`;\n  confirmAction(title, desc, async () => {\n    try {\n      const res = await fetch(\'/api/settings/reset\', {\n        method: \'POST\',\n        headers: { \'Content-Type\': \'application/json\' },\n        body: JSON.stringify({ section })\n      });\n      const data = await res.json();\n      if (!res.ok || !data.ok) throw new Error(data.message || \'恢复默认失败\');\n      studioSettings = data.settings;\n      populateSettingsForm(studioSettings);\n      applyStudioSettingsToMainUI(studioSettings);\n      toast(isAll ? \'已恢复全部默认参数并生效\' : `已恢复「${SETTINGS_TAB_LABELS[section]}」默认参数`);\n    } catch (e) {\n      toast(e.message || \'恢复默认失败\');\n    }\n  }, \'确认恢复\');\n}\n\n(function initSettingsFormEvents() {\n  [\'cfgWfEnableHires\', \'cfgTier_tier1_s1\', \'cfgTier_tier1_ref\', \'cfgTier_tier2_s1\', \'cfgTier_tier2_ref\', \'cfgTier_tier3_s1\', \'cfgTier_tier3_ref\'].forEach(id => {\n    const node = document.getElementById(id);\n    if (node) {\n      node.addEventListener(\'input\', updateModalTierTotals);\n      node.addEventListener(\'change\', updateModalTierTotals);\n    }\n  });\n})();\n\n/* ==================== 自绘浮层下拉框控制器（零原生 OS 下拉菜单、兼容 Top-Layer 弹窗） ==================== */\nconst zSelectRegistry = new Map();\nlet activeZSelect = null;\n\nfunction enhanceStudioSelect(sel) {\n  if (!sel || zSelectRegistry.has(sel)) return;\n  const wrap = el(\'div\', \'z-select\');\n  const trigger = el(\'button\', \'z-select-trigger\');\n  trigger.type = \'button\';\n  trigger.setAttribute(\'aria-haspopup\', \'listbox\');\n  trigger.setAttribute(\'aria-expanded\', \'false\');\n  if (sel.getAttribute(\'aria-label\')) {\n    trigger.setAttribute(\'aria-label\', sel.getAttribute(\'aria-label\'));\n  } else if (sel.id) {\n    const assocLbl = document.querySelector(`label[for="${sel.id}"]`);\n    if (assocLbl) {\n      assocLbl.addEventListener(\'click\', (ev) => {\n        ev.preventDefault();\n        trigger.focus();\n        openStudioSelect(sel);\n      });\n    }\n  }\n  const lblSpan = el(\'span\', \'z-select-label\', \'\');\n  const arrowSvg = svgIcon(\'i-chevron\', \'ico z-select-arrow\');\n  trigger.append(lblSpan, arrowSvg);\n  wrap.append(trigger);\n\n  sel.classList.add(\'z-native-select-hidden\');\n  sel.insertAdjacentElement(\'afterend\', wrap);\n\n  const entry = { sel, wrap, trigger, lblSpan };\n  zSelectRegistry.set(sel, entry);\n  syncStudioSelect(sel);\n\n  trigger.addEventListener(\'click\', (e) => {\n    e.stopPropagation();\n    if (activeZSelect === entry) closeStudioSelect(true);\n    else openStudioSelect(sel);\n  });\n  trigger.addEventListener(\'keydown\', (e) => {\n    if (e.key === \'ArrowDown\' || e.key === \'ArrowUp\' || e.key === \'Enter\' || e.key === \' \') {\n      e.preventDefault();\n      openStudioSelect(sel);\n    }\n  });\n}\n\nfunction syncStudioSelect(sel) {\n  const entry = zSelectRegistry.get(sel);\n  if (!entry) return;\n  entry.wrap.hidden = !!sel.hidden;\n  const opt = sel.options[sel.selectedIndex] || sel.options[0];\n  entry.lblSpan.textContent = opt ? opt.textContent : \'\';\n  entry.trigger.disabled = !!sel.disabled;\n}\n\nfunction syncAllStudioSelects() {\n  zSelectRegistry.forEach((_, sel) => syncStudioSelect(sel));\n}\n\nfunction scrollMenuOptionIntoView(menu, item) {\n  if (!menu || !item) return;\n  const top = item.offsetTop;\n  const bottom = top + item.offsetHeight;\n  if (top < menu.scrollTop) {\n    menu.scrollTop = Math.max(0, top - 4);\n  } else if (bottom > menu.scrollTop + menu.clientHeight) {\n    menu.scrollTop = bottom - menu.clientHeight + 4;\n  }\n}\n\nfunction positionStudioMenu(menu, entry, inDialog) {\n  const r = entry.trigger.getBoundingClientRect();\n  const vw = window.innerWidth;\n  const vh = window.innerHeight;\n  const menuH = Math.min(268, menu.scrollHeight || 220);\n  const spaceBelow = vh - r.bottom - 12;\n  const flipUp = spaceBelow < menuH && r.top > spaceBelow;\n\n  if (!inDialog) {\n    // Pure in-flow absolute positioning inside .z-select: zero fixed/Top-Layer so Mobile Safari never triggers top/bottom bar masks!\n    menu.classList.remove(\'in-dialog\');\n    menu.classList.toggle(\'flip-up\', flipUp);\n    menu.style.top = \'\';\n    menu.style.bottom = \'\';\n    menu.style.left = \'0px\';\n    menu.style.minWidth = `${Math.round(Math.max(r.width, 152))}px`;\n    menu.style.maxWidth = `${Math.max(180, vw - 20)}px`;\n    const mr = menu.getBoundingClientRect();\n    if (mr.right > vw - 10) {\n      menu.style.left = `${Math.round(vw - 10 - mr.right)}px`;\n    }\n    return;\n  }\n\n  menu.classList.add(\'in-dialog\');\n  menu.classList.remove(\'flip-up\');\n  const minW = Math.max(r.width, 152);\n  menu.style.minWidth = `${Math.round(minW)}px`;\n  menu.style.maxWidth = `${Math.max(180, vw - 20)}px`;\n  let left = r.left;\n  const actualW = Math.max(minW, menu.offsetWidth || minW);\n  if (left + actualW > vw - 10) left = Math.max(10, vw - actualW - 10);\n  menu.style.left = `${Math.round(left)}px`;\n  if (flipUp) {\n    menu.style.top = \'auto\';\n    menu.style.bottom = `${Math.round(vh - r.top + 5)}px`;\n  } else {\n    menu.style.bottom = \'auto\';\n    menu.style.top = `${Math.round(r.bottom + 5)}px`;\n  }\n}\n\nfunction closeStudioSelect(restoreFocus = false) {\n  if (!activeZSelect) return;\n  const prev = activeZSelect;\n  activeZSelect = null;\n  prev.wrap.classList.remove(\'open\');\n  prev.trigger.setAttribute(\'aria-expanded\', \'false\');\n  const menu = document.getElementById(\'zSelectMenu\');\n  if (menu) {\n    menu.classList.remove(\'open\', \'in-dialog\', \'flip-up\');\n    menu.replaceChildren();\n  }\n\n}\n\nfunction openStudioSelect(sel) {\n  const entry = zSelectRegistry.get(sel);\n  const menu = document.getElementById(\'zSelectMenu\');\n  if (!entry || !menu || sel.disabled || sel.hidden) return;\n  if (activeZSelect && activeZSelect !== entry) closeStudioSelect(false);\n\n  menu.replaceChildren();\n  const opts = Array.from(sel.options);\n  let activeIdx = Math.max(0, sel.selectedIndex);\n\n  opts.forEach((o, idx) => {\n    const isSel = idx === sel.selectedIndex;\n    const btn = el(\'button\', \'z-select-opt\' + (isSel ? \' selected\' : \'\'));\n    btn.type = \'button\';\n    btn.setAttribute(\'role\', \'option\');\n    btn.setAttribute(\'aria-selected\', isSel ? \'true\' : \'false\');\n    btn.dataset.idx = String(idx);\n    const txt = el(\'span\', \'z-select-label\', o.textContent || o.value);\n    const chk = svgIcon(\'i-check\', \'ico z-select-opt-check\');\n    btn.append(txt, chk);\n    btn.addEventListener(\'click\', (ev) => {\n      ev.stopPropagation();\n      if (sel.selectedIndex !== idx) {\n        sel.selectedIndex = idx;\n        sel.value = o.value;\n        syncStudioSelect(sel);\n        sel.dispatchEvent(new Event(\'change\', { bubbles: true }));\n      }\n      closeStudioSelect(true);\n    });\n    menu.append(btn);\n  });\n\n  const dlg = entry.trigger.closest(\'dialog[open]\');\n  const host = dlg || entry.wrap;\n  if (menu.parentElement !== host) host.appendChild(menu);\n\n  activeZSelect = entry;\n  entry.wrap.classList.add(\'open\');\n  entry.trigger.setAttribute(\'aria-expanded\', \'true\');\n  menu.classList.add(\'open\');\n  positionStudioMenu(menu, entry, !!dlg);\n\n  const selNode = menu.children[activeIdx];\n  if (selNode) {\n    selNode.classList.add(\'focused\');\n    scrollMenuOptionIntoView(menu, selNode);\n  }\n  if (isFinePointer()) {\n    menu.focus({ preventScroll: true });\n  }\n}\n\ndocument.querySelectorAll(\'select.settings-select\').forEach(enhanceStudioSelect);\n\ndocument.getElementById(\'zSelectMenu\')?.addEventListener(\'keydown\', (e) => {\n  if (!activeZSelect) return;\n  const menu = e.currentTarget;\n  const items = Array.from(menu.querySelectorAll(\'.z-select-opt\'));\n  if (!items.length) return;\n  let cur = items.findIndex(x => x.classList.contains(\'focused\'));\n  if (cur < 0) cur = Math.max(0, activeZSelect.sel.selectedIndex);\n  if (e.key === \'Escape\' || e.key === \'Tab\') {\n    e.preventDefault();\n    closeStudioSelect(true);\n    return;\n  }\n  if (e.key === \'ArrowDown\' || e.key === \'ArrowUp\') {\n    e.preventDefault();\n    items[cur]?.classList.remove(\'focused\');\n    cur = (cur + (e.key === \'ArrowDown\' ? 1 : -1) + items.length) % items.length;\n    items[cur]?.classList.add(\'focused\');\n    scrollMenuOptionIntoView(menu, items[cur]);\n    return;\n  }\n  if (e.key === \'Enter\' || e.key === \' \') {\n    e.preventDefault();\n    items[cur]?.click();\n  }\n});\n\ndocument.addEventListener(\'pointerdown\', (e) => {\n  if (!activeZSelect) return;\n  if (e.target.closest(\'#zSelectMenu\') || activeZSelect.wrap.contains(e.target)) return;\n  closeStudioSelect(false);\n});\nwindow.addEventListener(\'resize\', () => { if (activeZSelect) closeStudioSelect(false); });\ndocument.addEventListener(\'scroll\', (e) => {\n  if (!activeZSelect) return;\n  const menu = document.getElementById(\'zSelectMenu\');\n  if (!menu || !menu.classList.contains(\'in-dialog\')) return;\n  if (e.target === menu || menu.contains(e.target)) return;\n  closeStudioSelect(false);\n}, true);\n\n\nasync function copyTextSafe(str) {\n  if (!str) return false;\n  try {\n    if (navigator.clipboard && typeof navigator.clipboard.writeText === \'function\') {\n      await navigator.clipboard.writeText(str);\n      return true;\n    }\n  } catch (_) {}\n  try {\n    const ta = document.createElement(\'textarea\');\n    ta.value = str;\n    ta.setAttribute(\'readonly\', \'\');\n    ta.style.cssText = \'position:fixed;top:-9999px;left:-9999px;opacity:0;\';\n    document.body.appendChild(ta);\n    ta.select();\n    ta.setSelectionRange(0, str.length);\n    const ok = document.execCommand(\'copy\');\n    document.body.removeChild(ta);\n    return ok;\n  } catch (_) {\n    return false;\n  }\n}\n\nlet _promptDialogIdx = -1;\nlet _activeSelText = \'\';\n\nfunction resetPromptDialogCopyBtn() {\n  const cpBtn = document.getElementById(\'promptDialogCopyBtn\');\n  if (!cpBtn) return;\n  cpBtn.dataset.hasSelection = \'0\';\n  cpBtn.classList.remove(\'btn-primary\');\n  setControlText(cpBtn, \'复制\');\n  _activeSelText = \'\';\n}\n\nfunction openPromptDialog(idx) {\n  const item = filteredGalleryItems[idx];\n  if (!item || !item.prompt) return;\n  const dlg = document.getElementById(\'promptDialog\');\n  if (!dlg) return;\n  _promptDialogIdx = idx;\n\n  const mName = (currentTab === \'upload_image\') ? \'上传素材\' : displayModelName(item.model_label || uiMetaConfig.image_label || \'Flux / DiT INT8\');\n  const titleLine = mName + (item.steps ? ` · ${item.steps}步` : \'\');\n  const subParts = [];\n  if (item.res) subParts.push(item.res.replace(\'x\', \'×\'));\n  if (item.time_str) subParts.push(item.time_str);\n  subParts.push(`${item.prompt.trim().length} 字`);\n\n  document.getElementById(\'promptDialogTitle\').textContent = titleLine;\n  document.getElementById(\'promptDialogSub\').textContent = subParts.join(\' · \');\n  const bodyEl = document.getElementById(\'promptDialogBody\');\n  bodyEl.textContent = item.prompt;\n  bodyEl.scrollTop = 0;\n  resetPromptDialogCopyBtn();\n\n  if (!dlg.open) dlg.showModal();\n  dlg.focus({ preventScroll: true });\n}\n\nfunction closePromptDialog() {\n  const dlg = document.getElementById(\'promptDialog\');\n  if (dlg && dlg.open) dlg.close();\n}\n\n(function initPromptDialogEvents() {\n  const dlg = document.getElementById(\'promptDialog\');\n  const cpBtn = document.getElementById(\'promptDialogCopyBtn\');\n  if (cpBtn) {\n    cpBtn.addEventListener(\'pointerdown\', (ev) => {\n      if (cpBtn.dataset.hasSelection === \'1\') ev.preventDefault();\n    });\n  }\n  if (dlg) {\n    dlg.addEventListener(\'click\', (ev) => {\n      if (ev.target === dlg) closePromptDialog();\n    });\n    dlg.addEventListener(\'close\', () => {\n      resetPromptDialogCopyBtn();\n      try { window.getSelection()?.removeAllRanges(); } catch (_) {}\n    });\n  }\n})();\n\ndocument.addEventListener(\'selectionchange\', () => {\n  const dlg = document.getElementById(\'promptDialog\');\n  if (!dlg || !dlg.open) return;\n  const sel = window.getSelection();\n  const str = sel ? sel.toString().trim() : \'\';\n  let insideBody = false;\n  if (str && sel.rangeCount > 0) {\n    const anc = sel.anchorNode instanceof Element ? sel.anchorNode : sel.anchorNode?.parentElement;\n    const foc = sel.focusNode instanceof Element ? sel.focusNode : sel.focusNode?.parentElement;\n    insideBody = !!(anc?.closest?.(\'#promptDialogBody\') && foc?.closest?.(\'#promptDialogBody\'));\n  }\n  const cpBtn = document.getElementById(\'promptDialogCopyBtn\');\n  if (!cpBtn) return;\n  if (insideBody && str) {\n    _activeSelText = str;\n    if (cpBtn.dataset.hasSelection !== \'1\') {\n      cpBtn.dataset.hasSelection = \'1\';\n      cpBtn.classList.add(\'btn-primary\');\n      setControlText(cpBtn, \'复制选中\');\n    }\n  } else if (cpBtn.dataset.hasSelection === \'1\') {\n    resetPromptDialogCopyBtn();\n  }\n});\n\n/* ==================== 全局键盘与点击事件委托 ==================== */\n[\'promptInputImage\', \'promptInputEdit\', \'promptInputVideo\', \'promptInputLlm\'].forEach(id => {\n  const inp = document.getElementById(id);\n  if (inp) {\n    inp.addEventListener(\'keydown\', (e) => {\n      if ((e.ctrlKey || e.metaKey) && e.key === \'Enter\') {\n        e.preventDefault();\n        submitTask();\n      }\n    });\n  }\n});\n\nwindow.addEventListener(\'keydown\', (e) => {\n  const lb = document.getElementById(\'lightbox\');\n  if (lb && lb.classList.contains(\'open\')) {\n    if (e.key === \'Escape\') {\n      e.preventDefault();\n      closeLightboxFromUI();\n    } else if (e.key === \'ArrowLeft\' && currentLightboxList.length > 1) {\n      e.preventDefault();\n      stepLightbox(-1);\n    } else if (e.key === \'ArrowRight\' && currentLightboxList.length > 1) {\n      e.preventDefault();\n      stepLightbox(1);\n    }\n    return;\n  }\n  if (e.key === \'Escape\' && _randomPickerOpen) {\n    e.preventDefault();\n    closeRandomPicker(true);\n    return;\n  }\n  const ap = document.getElementById(\'accountPicker\');\n  if (e.key === \'Escape\' && ap && ap.classList.contains(\'open\')) {\n    e.preventDefault();\n    setAccountPickerOpen(false);\n    return;\n  }\n  if (e.key === \'Escape\' && document.querySelector(\'.drive-sync-picker.open\')) {\n    e.preventDefault();\n    closeAllDriveSyncPickers();\n    return;\n  }\n  if (e.key === \'Escape\' && taskDrawerOpen) { e.preventDefault(); setTaskDrawerOpen(false); return; }\n  const wp = document.getElementById(\'warmupPicker\');\n  if (e.key === \'Escape\' && wp && wp.classList.contains(\'open\')) {\n    e.preventDefault();\n    closeWarmupPicker(true);\n  }\n});\n\ndocument.addEventListener(\'pointerdown\', (e) => {\n  const wp = document.getElementById(\'warmupPicker\');\n  if (wp && wp.classList.contains(\'open\') && !wp.contains(e.target) && !e.target.closest(\'[data-power-btn="warmup"]\')) closeWarmupPicker(false);\n  const ap = document.getElementById(\'accountPicker\');\n  if (ap && ap.classList.contains(\'open\') && !ap.contains(e.target) && !e.target.closest(\'#cuBadge\')) setAccountPickerOpen(false);\n}, { passive: true });\n\ndocument.addEventListener(\'keydown\', (e) => {\n  const control = e.target.closest(\'[role="button"][data-action]\');\n  if (!control || (e.key !== \'Enter\' && e.key !== \' \')) return;\n  e.preventDefault();\n  control.click();\n});\n\ndocument.addEventListener(\'click\', async (e) => {\n  if (e.target.closest(\'a[download]\')) return;\n  const target = e.target.closest(\'[data-action]\');\n  if (!target || target.disabled) return;\n  const action = target.dataset.action;\n\n  switch (action) {\n    case \'brand-home\':\n      navigateTo(\'image\', { scrollTop: true });\n      break;\n    case \'nav-page\':\n      navigateTo(target.dataset.page || \'image\');\n      break;\n    case \'toggle-task-panel\':\n      toggleTaskPanel();\n      break;\n    case \'close-task-drawer\':\n      setTaskDrawerOpen(false);\n      break;\n    case \'toggle-mob-top-card\':\n      applyMobTopCardFolded(!mobTopCardFolded, true);\n      break;\n    case \'reload-settings-form\':\n      ensureSettingsPagePopulated(true).then(() => toast(\'已重新加载已保存的参数\'));\n      break;\n    case \'logout-panel\':\n      confirmAction(\'确认退出登录？\', \'将清除当前登录状态并立即吊销旧会话凭证。\', async () => {\n        try {\n          await fetch(\'/api/logout\', {\n            method: \'POST\',\n            headers: { \'Content-Type\': \'application/json\' },\n            body: \'{}\'\n          });\n        } catch (_) {}\n        window.location.replace(\'/\');\n      }, \'确认退出\');\n      break;\n    case \'select-view\': {\n      const v = target.dataset.view;\n      if (v && v !== currentView) selectView(v);\n      break;\n    }\n    case \'mob-nav-toggle\':\n      animateViewSwipeTo(currentView === \'works\' ? \'creation\' : \'works\');\n      break;\n    case \'toggle-mob-topbar\':\n      applyMobTopCardFolded(!mobTopCardFolded, true);\n      break;\n    case \'toggle-mob-works-bar\':\n      break;\n      break;\n    case \'toggle-view\':\n      selectView(currentView === \'works\' ? \'creation\' : \'works\');\n      break;\n    case \'select-mod\':\n      selectModule(target.dataset.mod);\n      break;\n    case \'set-seg\':\n      setSeg(target.dataset.key, target.dataset.val);\n      break;\n    case \'pick-ref\':\n      pickRefForMod(target.dataset.mod, target.dataset.path || \'\');\n      break;\n    case \'trigger-upload\':\n      triggerUpload();\n      break;\n    case \'clear-chat\':\n      if (!chatHistory.length) { toast(\'当前对话已为空\'); break; }\n      confirmAction(\'确认清空当前对话历史？\', \'将清空服务器当前对话轮次；作品库中已保存的提示词历史文件不受影响。\', async () => {\n        try {\n          const res = await fetch(\'/api/chat/clear\', { method: \'POST\' });\n          const data = await res.json();\n          if (!res.ok || !data.ok) throw new Error(data.message || \'清空失败\');\n          await loadChat();\n          toast(\'对话已清空\');\n        } catch (err) { toast(err.message || \'清空失败\'); }\n      }, \'确认清空\');\n      break;\n    case \'copy-chat\': {\n      const msg = chatHistory[Number(target.dataset.idx)];\n      if (msg?.content) {\n        try { await navigator.clipboard.writeText(msg.content.trim()); toast(\'已复制对话内容\'); } catch (_) {}\n      }\n      break;\n    }\n    case \'chat-to-mod\': {\n      const msg = chatHistory[Number(target.dataset.idx)];\n      if (!msg?.content) break;\n      const modKey = target.dataset.target || \'image\';\n      const destInput = document.getElementById(INPUT_ID_MAP[modKey] || \'promptInputImage\');\n      if (destInput) destInput.value = extractCleanPrompt(msg.content, \'zh\');\n      selectModule(modKey);\n      if (destInput) {\n        destInput.scrollIntoView({ behavior: \'smooth\', block: \'center\' });\n        setTimeout(() => destInput.focus({ preventScroll: true }), 160);\n      }\n      toast(\'已填入\' + (modKey === \'video\' ? \'视频分镜提示词\' : \'生图提示词\'));\n      break;\n    }\n    case \'cancel-job\':\n      cancelJob();\n      break;\n    case \'clear-prompt\':\n      clearPrompt();\n      break;\n    case \'toggle-random-picker\':\n      toggleRandomPicker();\n      break;\n    case \'pick-random-count\':\n      submitRandomImage(Number(target.dataset.count));\n      break;\n    case \'submit-task\':\n      submitTask();\n      break;\n    case \'reload-panel\':\n      reloadPanelKeepPlace(target);\n      break;\n    case \'refresh-status\':\n      if (statusLoading) break;\n      loadColabStatus(true);\n      if (document.getElementById(\'accountPicker\')?.classList.contains(\'open\')) {\n        loadColabAccounts(true);\n      }\n      break;\n    case \'toggle-account-picker\':\n      toggleAccountPicker();\n      break;\n    case \'close-account-picker\':\n      setAccountPickerOpen(false);\n      break;\n    case \'refresh-accounts\':\n      loadColabAccounts(true);\n      loadColabStatus(true);\n      break;\n    case \'switch-colab-account\':\n      requestSwitchColabAccount(target.dataset.email || \'\', false);\n      break;\n    case \'toggle-acct-oauth\':\n      toggleAccountOauthBox();\n      break;\n    case \'copy-acct-oauth-url\':\n      if (currentOauthUrl) {\n        try { await navigator.clipboard.writeText(currentOauthUrl); toast(\'已复制 Google 授权链接\'); } catch (_) {}\n      }\n      break;\n    case \'submit-acct-oauth\':\n      submitAccountOauthCode();\n      break;\n    case \'toggle-power\':\n      toggleG4Power();\n      break;\n    case \'close-warmup\':\n      closeWarmupPicker();\n      break;\n    case \'start-warmup\':\n      submitPowerTask(\'warmup\');\n      break;\n    case \'warmup-all\':\n      warmupAll(target);\n      break;\n    case \'open-settings\':\n      openSettingsDialog();\n      break;\n    case \'close-settings\':\n      closeSettingsDialog();\n      break;\n    case \'switch-settings-tab\':\n      switchSettingsTab(target.dataset.settingsTab);\n      break;\n    case \'add-replace-pair\': {\n      const list = document.getElementById(\'cfgReplacePairsList\');\n      if (list) {\n        const row = createReplacePairRow(\'\', \'\');\n        list.append(row);\n        row.scrollIntoView({ behavior: \'smooth\', block: \'nearest\' });\n        row.querySelector(\'.pair-from\')?.focus();\n      }\n      break;\n    }\n    case \'remove-replace-pair\': {\n      const row = target.closest(\'.replace-pair-row\');\n      if (row) row.remove();\n      break;\n    }\n    case \'save-settings\':\n      saveStudioSettings();\n      break;\n    case \'reset-settings-section\':\n      requestResetSettings(currentSettingsTab);\n      break;\n    case \'reset-settings-all\':\n      requestResetSettings(\'all\');\n      break;\n    case \'switch-tab\':\n      switchTab(target.dataset.tab);\n      break;\n    case \'scroll-top\':\n      setMainScrollY(0, true);\n      break;\n    case \'goto-page\': {\n      const p = Number(target.dataset.page);\n      const fromBottom = target.closest(\'.pagination-bar\')?.dataset.pos === \'bottom\';\n      if (Number.isFinite(p) && p >= 1 && p !== galleryPage) {\n        galleryPage = p;\n        renderGallery(true);\n        if (fromBottom) {\n          if (isMobileScrollShell()) {\n            const wb = document.getElementById(\'worksBody\');\n            const cEl = document.getElementById(\'appContent\');\n            const topY = wb\n              ? Math.max(0, Math.round(wb.getBoundingClientRect().top - (cEl ? cEl.getBoundingClientRect().top : 0) + getMainScrollY() - 10))\n              : 0;\n            setMainScrollY(topY, true);\n          } else {\n            setMainScrollY(0, true, \'works\');\n          }\n        }\n        savePanelUiState();\n      }\n      break;\n    }\n    case \'refresh-outputs\':\n      loadOutputs(true).then(() => toast(\'作品库已刷新\'));\n      break;\n    case \'toggle-drive-sync-picker\':\n      toggleDriveSyncPicker(target);\n      break;\n    case \'sync-drive\':\n      triggerDriveSyncAction(\'drive\');\n      break;\n    case \'sync-channel\':\n      triggerDriveSyncAction(\'channel\');\n      break;\n    case \'delete-all-tab\':\n      deleteAllInTab();\n      break;\n    case \'toggle-batch-mode\':\n      toggleBatchMode();\n      break;\n    case \'batch-toggle-item\':\n      toggleBatchItem(Number(target.dataset.idx), !!e.shiftKey);\n      break;\n    case \'batch-select-page\':\n      batchSelectCurrentPage();\n      break;\n    case \'batch-select-filtered\':\n      batchSelectAllFiltered();\n      break;\n    case \'batch-clear\':\n      batchSelectedNames.clear();\n      lastBatchClickIdx = -1;\n      syncBatchToolbarUI();\n      renderGallery(true);\n      break;\n    case \'batch-download\':\n      submitBatchDownload();\n      break;\n    case \'batch-delete\':\n      submitBatchDelete();\n      break;\n    case \'delete-file\':\n      deleteFileWithConfirm(target.dataset.mod, target.dataset.name, target.dataset.path);\n      break;\n    case \'copy-img-prompt\': {\n      const it = filteredGalleryItems[Number(target.dataset.idx)];\n      if (it?.prompt) {\n        const ok = await copyTextSafe(it.prompt);\n        if (ok) toast(\'已复制整段提示词\');\n      }\n      break;\n    }\n    case \'reuse-img-spec\': {\n      const it = filteredGalleryItems[Number(target.dataset.idx)];\n      if (!it || !it.prompt) break;\n      const destInput = document.getElementById(\'promptInputImage\');\n      if (destInput) destInput.value = it.prompt;\n      selectModule(\'image\');\n      updatePromptCharCount();\n      if (currentView !== \'creation\') animateViewSwipeTo(\'creation\');\n      else if (window.innerWidth <= 1024 && destInput) destInput.scrollIntoView({ behavior: \'smooth\', block: \'center\' });\n      toast(\'已复用提示词到 3K 生图\');\n      break;\n    }\n    case \'copy-llm-file\': {\n      const it = filteredGalleryItems[Number(target.dataset.idx)];\n      if (it?.preview_text) {\n        try { await navigator.clipboard.writeText(it.preview_text.trim()); toast(\'已复制提示词全文\'); } catch (_) {}\n      }\n      break;\n    }\n    case \'reuse-llm-file\': {\n      const it = filteredGalleryItems[Number(target.dataset.idx)];\n      if (!it?.preview_text) break;\n      const destInput = document.getElementById(\'promptInputImage\');\n      if (destInput) destInput.value = extractCleanPrompt(it.preview_text, \'zh\');\n      selectModule(\'image\');\n      updatePromptCharCount();\n      if (currentView !== \'creation\') animateViewSwipeTo(\'creation\');\n      else if (window.innerWidth <= 1024 && destInput) destInput.scrollIntoView({ behavior: \'smooth\', block: \'center\' });\n      toast(\'已填入 3K 生图输入框\');\n      break;\n    }\n    case \'open-prompt-dialog\':\n      openPromptDialog(Number(target.dataset.idx));\n      break;\n    case \'close-prompt-dialog\':\n      closePromptDialog();\n      break;\n    case \'copy-prompt-dialog\': {\n      if (target.dataset.hasSelection === \'1\' && _activeSelText) {\n        const ok = await copyTextSafe(_activeSelText);\n        if (ok) toast(\'已复制选中片段\');\n        break;\n      }\n      const it = filteredGalleryItems[_promptDialogIdx];\n      if (it?.prompt) {\n        const ok = await copyTextSafe(it.prompt);\n        if (ok) toast(\'已复制整段提示词\');\n      }\n      break;\n    }\n    case \'reuse-prompt-dialog\': {\n      const it = filteredGalleryItems[_promptDialogIdx];\n      if (!it || !it.prompt) break;\n      const destInput = document.getElementById(\'promptInputImage\');\n      if (destInput) destInput.value = it.prompt;\n      closePromptDialog();\n      selectModule(\'image\');\n      updatePromptCharCount();\n      if (currentView !== \'creation\') animateViewSwipeTo(\'creation\');\n      toast(\'已复用提示词到 3K 生图\');\n      break;\n    }\n    case \'open-lightbox\':\n      openLightbox(Number(target.dataset.idx), target);\n      break;\n    case \'lightbox-prev\':\n      e.stopPropagation();\n      stepLightbox(-1);\n      break;\n    case \'lightbox-next\':\n      e.stopPropagation();\n      stepLightbox(1);\n      break;\n    case \'toggle-task\': {\n      if (e.target.closest(\'.task-actions\') && !target.classList.contains(\'task-mini-btn\')) break;\n      const jid = Number(target.dataset.jid);\n      if (expandedTaskIds.has(jid)) {\n        expandedTaskIds.delete(jid);\n      } else {\n        expandedTaskIds.add(jid);\n        ensureTaskDetailLoaded(jid);\n      }\n      _lastTasksSig = \'\';\n      renderTaskLedger(tasksList);\n      break;\n    }\n    case \'load-all-tasks\':\n      showingAllHistoryTasks = true;\n      pollJob();\n      break;\n    case \'task-stop\':\n      executeTaskAction(Number(target.dataset.jid), \'stop\');\n      break;\n    case \'task-resume\':\n      executeTaskAction(Number(target.dataset.jid), \'resume\');\n      break;\n    case \'clear-tasks\': {\n      const cnt = deletableTasksCount || tasksList.filter(x => x && x.can_delete === true).length;\n      if (!cnt || !powerJobsKnown || powerBusyTask()) break;\n      confirmAction(\n        \'确认清空任务记录？\',\n        `将移除全部 ${cnt} 条已结束或已暂停的任务记录（正在运行和排队中的任务保留，作品库文件与对话不受影响）。`,\n        () => clearAllTasks(),\n        \'确认清空\'\n      );\n      break;\n    }\n    case \'task-delete\': {\n      const jid = Number(target.dataset.jid);\n      const t = tasksList.find(x => x.jid === jid);\n      if (!t || t.can_delete !== true || t.state === \'running\' || t.state === \'queued\') break;\n      confirmAction(\n        `确认删除任务记录 #${jid}？`,\n        `删除「${t.label || \'任务\'}」记录后无法恢复（已生成的作品文件不受影响）。`,\n        () => executeTaskAction(jid, \'delete\'),\n        \'确认删除\'\n      );\n      break;\n    }\n  }\n});\n\nconst PANEL_UI_STATE_KEY = \'g4_panel_ui_state_v1\';\nif (\'scrollRestoration\' in history) {\n  try { history.scrollRestoration = \'manual\'; } catch (_) {}\n}\n\nfunction savePanelUiState(extra = null) {\n  if (_restoringUiState) return;\n  try {\n    const payload = {\n      navPage: currentPage,\n      view: currentView,\n      mod: currentMod,\n      tab: currentTab,\n      page: galleryPage,\n      search: gallerySearchQuery,\n      scrollY: Math.round(getMainScrollY()),\n      segs: {\n        imgRes: state.imgRes,\n        imgSteps: state.imgSteps,\n        vidQuality: state.vidQuality,\n        vidRes: state.vidRes,\n        vidLen: state.vidLen,\n        vidSteps: state.vidSteps\n      },\n      warmupCustomized,\n      warmupTargets,\n      drafts: {\n        image: document.getElementById(\'promptInputImage\')?.value || \'\',\n        video: document.getElementById(\'promptInputVideo\')?.value || \'\',\n        llm: document.getElementById(\'promptInputLlm\')?.value || \'\'\n      },\n      forceStatus: !!(extra && extra.forceStatus),\n      ts: Date.now()\n    };\n    sessionStorage.setItem(PANEL_UI_STATE_KEY, JSON.stringify(payload));\n  } catch (_) {}\n}\n\nfunction reloadPanelKeepPlace(btnEl) {\n  savePanelUiState({ forceStatus: true });\n  if (btnEl) {\n    btnEl.classList.add(\'is-spinning\');\n    btnEl.disabled = true;\n  }\n  const topRefreshBtn = document.getElementById(\'refreshStatusBtn\');\n  if (topRefreshBtn) {\n    topRefreshBtn.disabled = true;\n    topRefreshBtn.classList.add(\'is-spinning\');\n  }\n  window.location.reload();\n}\n\nlet _uiSaveTimer = null;\nfunction scheduleSavePanelUiState() {\n  if (_restoringUiState) return;\n  clearTimeout(_uiSaveTimer);\n  _uiSaveTimer = setTimeout(() => savePanelUiState(), 180);\n}\n\nwindow.addEventListener(\'pagehide\', () => savePanelUiState());\n[\'promptInputImage\', \'promptInputVideo\', \'promptInputLlm\'].forEach(id => {\n  document.getElementById(id)?.addEventListener(\'input\', () => { updatePromptCharCount(); scheduleSavePanelUiState(); });\n});\n\nlet _forceStatusOnBoot = false;\n(function restorePanelUiStateSync() {\n  let saved = null;\n  try {\n    saved = JSON.parse(sessionStorage.getItem(PANEL_UI_STATE_KEY) || \'null\');\n  } catch (_) {}\n  if (!saved || typeof saved !== \'object\') return;\n\n  _restoringUiState = true;\n  try {\n    _forceStatusOnBoot = !!saved.forceStatus;\n    if (saved.warmupCustomized && Array.isArray(saved.warmupTargets)) {\n      const validW = saved.warmupTargets.filter(k => [\'v5_int8\', \'video\', \'llm\'].includes(k));\n      if (validW.length) {\n        warmupCustomized = true;\n        warmupTargets = validW;\n        syncWarmupPicker();\n      }\n    }\n    if (saved.segs && typeof saved.segs === \'object\') {\n      [\'imgRes\', \'imgSteps\', \'vidQuality\', \'vidRes\', \'vidLen\', \'vidSteps\'].forEach(k => {\n        if (saved.segs[k]) setSeg(k, saved.segs[k]);\n      });\n    }\n    if (saved.drafts && typeof saved.drafts === \'object\') {\n      if (typeof saved.drafts.image === \'string\') {\n        const elImg = document.getElementById(\'promptInputImage\');\n        if (elImg) elImg.value = saved.drafts.image;\n      }\n      if (typeof saved.drafts.video === \'string\') {\n        const elVid = document.getElementById(\'promptInputVideo\');\n        if (elVid) elVid.value = saved.drafts.video;\n      }\n      if (typeof saved.drafts.llm === \'string\') {\n        const elLlm = document.getElementById(\'promptInputLlm\');\n        if (elLlm) elLlm.value = saved.drafts.llm;\n      }\n    }\n    if (saved.mod && VALID_CREATION_MODS.includes(saved.mod)) {\n      selectModule(saved.mod);\n    }\n    if (saved.tab && [\'image\', \'video\', \'upload_image\', \'llm\'].includes(saved.tab)) {\n      currentTab = saved.tab;\n      updateTabCounts();\n    }\n    if (typeof saved.search === \'string\') {\n      gallerySearchQuery = saved.search;\n      const sInp = document.getElementById(\'gallerySearchInput\');\n      if (sInp) sInp.value = saved.search;\n    }\n    if (Number.isInteger(saved.page) && saved.page >= 1) {\n      galleryPage = saved.page;\n    }\n    const restoredNavPage = (typeof saved.navPage === \'string\' && VALID_PAGES.includes(saved.navPage))\n      ? saved.navPage\n      : (typeof saved.page === \'string\' && VALID_PAGES.includes(saved.page) ? saved.page : \'\');\n    if (restoredNavPage) {\n      navigateTo(restoredNavPage, { initialRestore: true });\n    } else if (saved.view === \'works\') {\n      navigateTo(\'works\', { initialRestore: true });\n    } else if (saved.mod && VALID_CREATION_MODS.includes(saved.mod)) {\n      navigateTo(saved.mod, { initialRestore: true });\n    }\n    if (typeof saved.scrollY === \'number\' && saved.scrollY > 0) {\n      _pendingRestoreScrollY = saved.scrollY;\n    }\n  } finally {\n    _restoringUiState = false;\n  }\n  savePanelUiState({ forceStatus: false });\n  updatePromptCharCount();\n})();\nnavigateTo(currentPage, { initialRestore: true });\nupdatePromptCharCount();\n\nfunction applyPendingScrollRestore() {\n  if (!_pendingRestoreScrollY) return;\n  const targetY = _pendingRestoreScrollY;\n  _pendingRestoreScrollY = null;\n  requestAnimationFrame(() => {\n    setMainScrollY(targetY, false);\n    syncBackToTopVisibility();\n  });\n}\n\nloadStudioSettings().then(cfg => { if (cfg) populateSettingsForm(cfg); });\nloadChat();\npollHwMetrics();\nloadDriveSyncStatus(true);\nsetInterval(() => { if (!document.hidden && !_driveSyncPollTimer) loadDriveSyncStatus(true); }, 20000);\nloadOutputs(true).then(() => {\n  if (currentView === \'works\') applyPendingScrollRestore();\n});\npollJob().then(() => {\n  if (currentView === \'creation\') applyPendingScrollRestore();\n  if (_forceStatusOnBoot || !lastStatusAttempt) {\n    loadColabStatus(_forceStatusOnBoot);\n  }\n});\nsetInterval(pollJob, 1500);\n</script>\n</body>\n</html>'

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
        except SettingsValidationError as exc:
            self._send_json({"ok": False, "message": str(exc)}, 400)
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

        if parsed.path == "/api/settings":
            data = self._read_json_body(max_bytes=2 * 1024 * 1024, required=True)
            if data is None:
                return
            saved = save_studio_settings(data)
            touch_if_running(PANEL_IDLE_MIN)
            self._send_json({
                "ok": True,
                "settings": saved,
                "message": "参数与提示词已保存，立即生效",
            })
            return

        if parsed.path == "/api/settings/reset":
            data = self._read_json_body(max_bytes=65536, required=False)
            if data is None:
                return
            sec = str(data.get("section") or "all").strip()
            saved = reset_studio_settings(sec)
            touch_if_running(PANEL_IDLE_MIN)
            self._send_json({
                "ok": True,
                "settings": saved,
                "message": "已恢复默认参数并立即生效",
            })
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
