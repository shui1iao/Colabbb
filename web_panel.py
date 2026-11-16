#!/usr/bin/env python3
"""Colabbb Web 创作工作台，媒体与提示词双通道 FIFO 队列。"""
import hmac, zipfile
import base64, collections, copy, glob, hashlib, math, json, mimetypes, tempfile, os, re, shlex, signal, subprocess, sys, threading, time, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from stat import S_ISREG

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
remove_synced_image = os.remove

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
    ip["strip_regexes"] = [x.strip() for x in ip["strip_regexes"] if x.strip()]
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

def load_chat_history():
    try:
        if os.path.exists(CHAT_FILE):
            data = json.load(open(CHAT_FILE, "r", encoding="utf-8"))
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []

def save_chat_history(msgs):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(CHAT_FILE, "w", encoding="utf-8") as fh:
        json.dump(msgs[-40:], fh, ensure_ascii=False, indent=2)

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
                while not self.queues[lane]:
                    self.cond.wait()
                self._sync_generation()
                if not self.queues[lane]:
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
            if it["module"] in ("image", "edit", "video"):
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
                "video": "准备视频模型与首帧", "llm": "准备提示词引擎（无 token 进度）",
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
        elif "全层常驻 GPU" in text or "引擎已就绪" in text:
            phase = "LLM 模型已加载，等待推理结果（无 token 进度）" if item["module"] == "llm" else "模型服务已就绪，等待预热完成确认"
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
            for src in (list(self.active_procs.values()),
                        [it for q in self.queues.values() for it in q],
                        list(self.paused.values()),
                        list(self.recent_done)):
                for it in src:
                    if it.get("jid") == jid:
                        return {
                            "ok": True,
                            "jid": jid,
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
        "import json\n"
        "from colab_cli.common import state\n"
        "try:\n"
        "    assignments = state.client.list_assignments()\n"
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


_STATUS_CACHE = {"ts": 0.0, "data": None}
_STATUS_LOCK = threading.Lock()
STATUS_TTL = 30.0

def get_colab_status(force: bool = False):
    # One authoritative billing read provides balance, hourly rate and active count.
    # Never infer power/billing from a live tunnel or extend stale data with it.
    with _STATUS_LOCK:
        now = time.time()
        if not force and _STATUS_CACHE["data"] is not None and (now - _STATUS_CACHE["ts"] < STATUS_TTL):
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
        _STATUS_CACHE["ts"] = time.time()
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
                threading.Thread(target=_bg_gen_thumb, args=(mod, fname), daemon=True).start()
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

threading.Thread(target=cleanup_stale_temp_files, daemon=True).start()


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
            if STATUS_CACHE.get("data", {}).get("active") is True:
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


LIVE_UI_HTML_PATH = os.path.join(STATE_DIR, "panel_ui.html")
_LIVE_UI_CACHE = {"mtime": -1, "bytes": None}


def get_live_html_bytes() -> bytes:
    try:
        if os.path.isfile(LIVE_UI_HTML_PATH):
            st = os.stat(LIVE_UI_HTML_PATH)
            if st.st_mtime_ns != _LIVE_UI_CACHE["mtime"] or _LIVE_UI_CACHE["bytes"] is None:
                data = open(LIVE_UI_HTML_PATH, "rb").read()
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
_AUTH_CACHE = {"mtime": -1, "doc": None}
_LOGIN_ATTEMPTS = {}
_LOGIN_LOCK = threading.Lock()


def _get_auth_config() -> dict:
    try:
        st = os.stat(PANEL_AUTH_FILE)
        if st.st_mtime_ns != _AUTH_CACHE["mtime"] or _AUTH_CACHE["doc"] is None:
            with open(PANEL_AUTH_FILE, "r", encoding="utf-8") as fh:
                _AUTH_CACHE["doc"] = json.load(fh)
                _AUTH_CACHE["mtime"] = st.st_mtime_ns
        return _AUTH_CACHE["doc"] or {}
    except Exception:
        return {}


def _make_session_cookie(username: str) -> str:
    cfg = _get_auth_config()
    sec = bytes.fromhex(cfg.get("session_secret_hex") or "00" * 32)
    exp = int(time.time()) + SESSION_TTL_SEC
    payload = f"{username}|{exp}"
    sig = hmac.new(sec, payload.encode("utf-8"), hashlib.sha256).hexdigest()
    raw = f"{payload}|{sig}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _verify_session_cookie(token: str) -> bool:
    if not token or len(token) > 512:
        return False
    cfg = _get_auth_config()
    sec_hex = cfg.get("session_secret_hex") or ""
    expected_user = cfg.get("username") or "admin"
    if not sec_hex:
        return False
    try:
        pad = "=" * ((4 - len(token) % 4) % 4)
        decoded = base64.urlsafe_b64decode((token + pad).encode("ascii")).decode("utf-8")
        user, exp_s, sig = decoded.split("|", 2)
        if not hmac.compare_digest(user, expected_user):
            return False
        if int(exp_s) < int(time.time()):
            return False
        expected_sig = hmac.new(bytes.fromhex(sec_hex), f"{user}|{exp_s}".encode("utf-8"), hashlib.sha256).hexdigest()
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


def _check_login_rate_limit(ip_key: str) -> bool:
    now = time.time()
    with _LOGIN_LOCK:
        hist = [ts for ts in _LOGIN_ATTEMPTS.get(ip_key, []) if now - ts < 300]
        _LOGIN_ATTEMPTS[ip_key] = hist
        return len(hist) < 10


def _record_login_failure(ip_key: str, success: bool):
    now = time.time()
    with _LOGIN_LOCK:
        if success:
            _LOGIN_ATTEMPTS.pop(ip_key, None)
        else:
            hist = [ts for ts in _LOGIN_ATTEMPTS.get(ip_key, []) if now - ts < 300]
            hist.append(now)
            _LOGIN_ATTEMPTS[ip_key] = hist

LOGIN_VIEW_TEMPLATE = '<!doctype html>\n<html lang="zh-CN">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n  <meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'sha256-KLNzgjEDQkQ7X/QMaSvAIjv1o+9VUoretBVEl7irTQk=\'; style-src \'unsafe-inline\'; img-src \'self\' data:; connect-src \'self\'; form-action \'self\'; base-uri \'none\'">\n  <title>登录 · G4 工作台</title>\n  <link rel="icon" type="image/svg+xml" href="/g4-icon.svg?v=g5">\n  <link rel="icon" type="image/png" sizes="64x64" href="/favicon.png?v=g5">\n  <link rel="shortcut icon" href="/favicon.ico?v=g5">\n  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v=g5">\n  <style>\n    :root {\n      color-scheme: light dark;\n      --bg: #EEF1F6;\n      --surface: #FFFFFF;\n      --card-2: rgba(20, 28, 40, 0.048);\n      --hover: rgba(20, 28, 40, 0.078);\n      --press: rgba(20, 28, 40, 0.12);\n      --fg: #161A22;\n      --fg-secondary: #3B4454;\n      --muted: #647084;\n      --line: rgba(15, 23, 42, 0.075);\n      --line-strong: rgba(15, 23, 42, 0.14);\n      --blue: #0071E3;\n      --blue-hover: #0062C7;\n      --blue-soft: rgba(0, 113, 227, 0.10);\n      --blue-border: rgba(0, 113, 227, 0.36);\n      --red: #DC2626;\n      --red-soft: rgba(220, 38, 38, 0.095);\n      --shadow-card: 0 1px 3px rgba(15, 23, 42, 0.05), 0 12px 32px -10px rgba(15, 23, 42, 0.08);\n      --autofill-bg: #F4F6FA;\n      --ease: cubic-bezier(0.2, 0.8, 0.2, 1);\n    }\n    html[data-theme="dark"] {\n      color-scheme: dark;\n      --bg: #0B111C;\n      --surface: #131B29;\n      --card-2: rgba(255, 255, 255, 0.055);\n      --hover: rgba(255, 255, 255, 0.09);\n      --press: rgba(255, 255, 255, 0.135);\n      --fg: #F3F6FA;\n      --fg-secondary: #C8D2E1;\n      --muted: #8B98AD;\n      --line: rgba(255, 255, 255, 0.075);\n      --line-strong: rgba(255, 255, 255, 0.15);\n      --blue: #3B82F6;\n      --blue-hover: #2563EB;\n      --blue-soft: rgba(59, 130, 246, 0.16);\n      --blue-border: rgba(59, 130, 246, 0.44);\n      --red: #F85149;\n      --red-soft: rgba(248, 81, 73, 0.14);\n      --shadow-card: 0 1px 2px rgba(0, 0, 0, 0.26), 0 18px 44px -10px rgba(0, 0, 0, 0.52);\n      --autofill-bg: #182233;\n    }\n    *, *::before, *::after, *:focus, *:focus-visible {\n      box-sizing: border-box;\n      outline: none !important;\n      -webkit-tap-highlight-color: transparent;\n    }\n    html, body { height: 100%; margin: 0; }\n    body {\n      min-height: 100dvh;\n      padding: 20px 16px calc(24px + env(safe-area-inset-bottom, 0px));\n      background: var(--bg);\n      color: var(--fg);\n      font-family: "Segoe UI Variable Text", "Segoe UI", ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei UI", sans-serif;\n      font-size: 13px;\n      line-height: 1.48;\n      display: flex;\n      align-items: center;\n      justify-content: center;\n      text-rendering: optimizeLegibility;\n    }\n    @media (-webkit-min-device-pixel-ratio: 2), (min-resolution: 2dppx) {\n      body { -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; }\n    }\n    .login-card {\n      width: 100%;\n      max-width: 384px;\n      background: var(--surface);\n      border: 1px solid var(--line);\n      border-radius: 14px;\n      padding: 24px 24px 20px;\n      box-shadow: var(--shadow-card);\n      display: flex;\n      flex-direction: column;\n      gap: 18px;\n    }\n    @keyframes shakeCard {\n      0%, 100% { transform: translateX(0); }\n      22% { transform: translateX(-5px); }\n      44% { transform: translateX(5px); }\n      66% { transform: translateX(-3px); }\n      88% { transform: translateX(3px); }\n    }\n    .login-card.shake { animation: shakeCard 280ms var(--ease); }\n    .login-head {\n      display: flex;\n      align-items: center;\n      justify-content: space-between;\n      gap: 12px;\n      padding-bottom: 16px;\n      border-bottom: 1px solid var(--line);\n    }\n    .brand-row { display: flex; align-items: center; gap: 11px; min-width: 0; }\n    .brand-mark {\n      width: 38px;\n      height: 38px;\n      flex: 0 0 38px;\n      background: transparent;\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      user-select: none;\n    }\n    .brand-mark-svg {\n      width: 100%;\n      height: 100%;\n      display: block;\n      overflow: visible;\n    }\n    .brand-info { display: flex; flex-direction: column; gap: 2px; min-width: 0; }\n    .brand-title { font-size: 15px; font-weight: 600; letter-spacing: -0.015em; color: var(--fg); line-height: 1.25; white-space: nowrap; }\n    .brand-sub { font-size: 11.5px; color: var(--muted); line-height: 1.25; white-space: nowrap; }\n    .guard-pill {\n      height: 24px;\n      padding: 0 8px;\n      border-radius: 6px;\n      background: var(--card-2);\n      color: var(--muted);\n      font-size: 11px;\n      font-weight: 500;\n      display: inline-flex;\n      align-items: center;\n      gap: 4px;\n      flex-shrink: 0;\n      user-select: none;\n    }\n    .ico { width: 15px; height: 15px; flex-shrink: 0; stroke: currentColor; stroke-width: 1.75; stroke-linecap: round; stroke-linejoin: round; fill: none; }\n    .ico-13 { width: 13px; height: 13px; }\n    .login-form { display: flex; flex-direction: column; gap: 13px; margin: 0; }\n    .field-group { display: flex; flex-direction: column; gap: 6px; }\n    .field-label { font-size: 12px; font-weight: 500; color: var(--fg-secondary); user-select: none; }\n    .input-shell {\n      height: 38px;\n      padding: 0 6px 0 11px;\n      border-radius: 8px;\n      background: var(--card-2);\n      box-shadow: inset 0 0 0 1px var(--line);\n      display: flex;\n      align-items: center;\n      gap: 8px;\n      transition: background-color 130ms var(--ease), box-shadow 130ms var(--ease);\n    }\n    .input-shell .field-ico { color: var(--muted); transition: color 130ms var(--ease); }\n    .input-shell:focus-within {\n      background: var(--surface);\n      box-shadow: inset 0 0 0 1.5px var(--blue-border), 0 0 0 3px var(--blue-soft);\n    }\n    .input-shell:focus-within .field-ico { color: var(--blue); }\n    .field-input {\n      flex: 1;\n      min-width: 0;\n      width: 100%;\n      height: 100%;\n      border: 0;\n      background: transparent;\n      color: var(--fg);\n      font-family: inherit;\n      font-size: 13px;\n      padding: 0;\n    }\n    .field-input::placeholder { color: var(--muted); opacity: 0.72; }\n    .field-input:-webkit-autofill,\n    .field-input:-webkit-autofill:hover,\n    .field-input:-webkit-autofill:focus {\n      -webkit-text-fill-color: var(--fg);\n      -webkit-box-shadow: 0 0 0 1000px var(--autofill-bg) inset;\n      border-radius: 6px;\n      transition: background-color 9999s ease-out 0s;\n    }\n    .eye-btn {\n      width: 28px;\n      height: 28px;\n      flex: 0 0 28px;\n      padding: 0;\n      border: 0;\n      border-radius: 6px;\n      background: transparent;\n      color: var(--muted);\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      cursor: pointer;\n      transition: background-color 130ms var(--ease), color 130ms var(--ease);\n    }\n    .eye-btn:active { background: var(--press); }\n    .error-box {\n      display: none;\n      align-items: center;\n      gap: 7px;\n      padding: 8px 11px;\n      border-radius: 8px;\n      background: var(--red-soft);\n      color: var(--red);\n      font-size: 12px;\n      font-weight: 500;\n      line-height: 1.4;\n    }\n    .error-box.visible { display: flex; }\n    .submit-btn {\n      margin-top: 3px;\n      width: 100%;\n      height: 38px;\n      padding: 0 16px;\n      border: 0;\n      border-radius: 8px;\n      background: var(--blue);\n      color: #FFFFFF;\n      font-family: inherit;\n      font-size: 13px;\n      font-weight: 500;\n      display: inline-flex;\n      align-items: center;\n      justify-content: center;\n      gap: 7px;\n      cursor: pointer;\n      user-select: none;\n      transition: background-color 130ms var(--ease), opacity 130ms var(--ease);\n    }\n    .submit-btn:active:not(:disabled) { background: var(--blue-hover); }\n    .submit-btn:disabled { opacity: 0.56; cursor: not-allowed; }\n    .login-foot { font-size: 11px; color: var(--muted); text-align: center; padding-top: 2px; user-select: none; }\n    @keyframes spin { to { transform: rotate(360deg); } }\n    .spin { animation: spin 560ms linear infinite; }\n    @media (hover: hover) and (pointer: fine) {\n      .input-shell:hover:not(:focus-within) { background: var(--hover); box-shadow: inset 0 0 0 1px var(--line-strong); }\n      .eye-btn:hover { background: var(--hover); color: var(--fg); }\n      .submit-btn:hover:not(:disabled) { background: var(--blue-hover); }\n    }\n  </style>\n</head>\n<body>\n  <svg xmlns="http://www.w3.org/2000/svg" style="display:none" aria-hidden="true">\n    <symbol id="i-lock" viewBox="0 0 24 24"><rect x="4" y="11" width="16" height="10" rx="2.5"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></symbol>\n    <symbol id="i-user" viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"/><path d="M4.5 20a7.5 7.5 0 0 1 15 0"/></symbol>\n    <symbol id="i-eye" viewBox="0 0 24 24"><path d="M2 12s3.6-6.5 10-6.5S22 12 22 12s-3.6 6.5-10 6.5S2 12 2 12z"/><circle cx="12" cy="12" r="2.8"/></symbol>\n    <symbol id="i-eye-off" viewBox="0 0 24 24"><path d="M17.94 17.94A10.4 10.4 0 0 1 12 18.5C5.6 18.5 2 12 2 12a18.8 18.8 0 0 1 4.4-5.26M9.9 5.73A9.8 9.8 0 0 1 12 5.5c6.4 0 10 6.5 10 6.5a18.8 18.8 0 0 1-2.55 3.68"/><line x1="3" y1="3" x2="21" y2="21"/></symbol>\n    <symbol id="i-alert" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><line x1="12" y1="8" x2="12" y2="12.5"/><circle cx="12" cy="16" r="0.6" fill="currentColor"/></symbol>\n    <symbol id="i-spin" viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/></symbol>\n  </svg>\n\n  <main class="login-card" id="loginCard">\n    <div class="login-head">\n      <div class="brand-row">\n        <span class="brand-mark" aria-hidden="true"><svg class="brand-mark-svg" viewBox="68 48 896 904" aria-hidden="true"><defs><linearGradient id="bm_bright" gradientUnits="userSpaceOnUse" x1="150" y1="100" x2="880" y2="900"><stop offset="0%" stop-color="#9BD4FF"/><stop offset="46%" stop-color="#4A90FF"/><stop offset="100%" stop-color="#1D54E0"/></linearGradient><linearGradient id="bm_main" gradientUnits="userSpaceOnUse" x1="180" y1="120" x2="880" y2="920"><stop offset="0%" stop-color="#75BEFF"/><stop offset="50%" stop-color="#377DF6"/><stop offset="100%" stop-color="#1745BE"/></linearGradient><linearGradient id="bm_deep" gradientUnits="userSpaceOnUse" x1="180" y1="140" x2="860" y2="940"><stop offset="0%" stop-color="#4B92FF"/><stop offset="55%" stop-color="#2260DF"/><stop offset="100%" stop-color="#123597"/></linearGradient><linearGradient id="bm_gold" gradientUnits="userSpaceOnUse" x1="350" y1="120" x2="850" y2="850"><stop offset="0%" stop-color="#FFF0A8"/><stop offset="48%" stop-color="#FACC3E"/><stop offset="100%" stop-color="#E59213"/></linearGradient><linearGradient id="bm_cyan" gradientUnits="userSpaceOnUse" x1="200" y1="120" x2="820" y2="850"><stop offset="0%" stop-color="#B8ECFF"/><stop offset="50%" stop-color="#58B4FF"/><stop offset="100%" stop-color="#256AF0"/></linearGradient><mask id="bm_knock" maskUnits="userSpaceOnUse" x="-100" y="-100" width="1224" height="1224"><rect x="-100" y="-100" width="1224" height="1224" fill="#fff"/><path d="M 786 352 L 558 688 L 904 688 M 786 352 L 786 888" fill="none" stroke="#000" stroke-width="174" stroke-linecap="round" stroke-linejoin="round"/></mask></defs><g mask="url(#bm_knock)"><path d="M 642 238 A 308 308 0 1 0 464 786" fill="none" stroke="url(#bm_main)" stroke-width="138" stroke-linecap="round"/><path d="M 304 338 A 182 182 0 0 0 284 568" fill="none" stroke="url(#bm_cyan)" stroke-width="44" stroke-linecap="round" opacity="0.9"/></g><path d="M 456 336 C 469 402, 490 423, 556 436 C 490 449, 469 470, 456 536 C 443 470, 422 449, 356 436 C 422 423, 443 402, 456 336 Z" fill="url(#bm_gold)"/><path d="M 786 352 L 558 688 L 904 688" fill="none" stroke="url(#bm_bright)" stroke-width="116" stroke-linecap="round" stroke-linejoin="round"/><path d="M 786 352 L 786 888" fill="none" stroke="url(#bm_deep)" stroke-width="116" stroke-linecap="round"/></svg></span>\n        <div class="brand-info">\n          <span class="brand-title">G4 工作台</span>\n          <span class="brand-sub">请输入账号密码验证身份</span>\n        </div>\n      </div>\n      <span class="guard-pill">\n        <svg class="ico ico-13" aria-hidden="true"><use href="#i-lock"/></svg>\n        <span>访问保护</span>\n      </span>\n    </div>\n\n    <form class="login-form" id="loginForm" autocomplete="on" novalidate>\n      <div class="field-group">\n        <label class="field-label" for="loginUser">账号</label>\n        <div class="input-shell">\n          <svg class="ico field-ico" aria-hidden="true"><use href="#i-user"/></svg>\n          <input class="field-input" id="loginUser" name="username" type="text" autocomplete="username" autocapitalize="none" spellcheck="false" placeholder="输入管理账号" required>\n        </div>\n      </div>\n\n      <div class="field-group">\n        <label class="field-label" for="loginPass">密码</label>\n        <div class="input-shell">\n          <svg class="ico field-ico" aria-hidden="true"><use href="#i-lock"/></svg>\n          <input class="field-input" id="loginPass" name="password" type="password" autocomplete="current-password" placeholder="输入访问密码" required>\n          <button type="button" class="eye-btn" id="togglePwdBtn" aria-label="显示密码" title="显示/隐藏密码">\n            <svg class="ico" aria-hidden="true"><use id="eyeUse" href="#i-eye"/></svg>\n          </button>\n        </div>\n      </div>\n\n      <div class="error-box" id="loginErr" role="alert">\n        <svg class="ico" aria-hidden="true"><use href="#i-alert"/></svg>\n        <span id="loginErrText">账号或密码错误</span>\n      </div>\n\n      <button type="submit" class="submit-btn" id="loginSubmitBtn">\n        <span id="submitBtnLabel">进入工作台</span>\n      </button>\n    </form><p class="hint"><!--INIT_NOTICE--></p>\n\n    <div class="login-foot">登录凭证加密校验 · 当前设备保持 30 天免重复登录</div>\n  </main>\n\n  <script>\'use strict\';\nconst sysDark = window.matchMedia(\'(prefers-color-scheme: dark)\');\nfunction syncTheme() {\n  const dark = sysDark.matches;\n  document.documentElement.setAttribute(\'data-theme\', dark ? \'dark\' : \'light\');\n}\nsysDark.addEventListener(\'change\', syncTheme);\nsyncTheme();\n\nconst userInp = document.getElementById(\'loginUser\');\nconst passInp = document.getElementById(\'loginPass\');\nconst eyeBtn = document.getElementById(\'togglePwdBtn\');\nconst eyeUse = document.getElementById(\'eyeUse\');\nconst errBox = document.getElementById(\'loginErr\');\nconst errTxt = document.getElementById(\'loginErrText\');\nconst card = document.getElementById(\'loginCard\');\nconst subBtn = document.getElementById(\'loginSubmitBtn\');\n\nif (window.matchMedia(\'(hover: hover) and (pointer: fine)\').matches) {\n  userInp?.focus();\n}\n\neyeBtn?.addEventListener(\'click\', () => {\n  const show = passInp.type === \'password\';\n  passInp.type = show ? \'text\' : \'password\';\n  eyeUse?.setAttribute(\'href\', show ? \'#i-eye-off\' : \'#i-eye\');\n  eyeBtn.setAttribute(\'aria-label\', show ? \'隐藏密码\' : \'显示密码\');\n});\n\nfunction showErr(msg) {\n  errTxt.textContent = msg || \'账号或密码错误\';\n  errBox.classList.add(\'visible\');\n  card.classList.remove(\'shake\');\n  void card.offsetWidth;\n  card.classList.add(\'shake\');\n}\n\n[userInp, passInp].forEach(el => el?.addEventListener(\'input\', () => {\n  errBox.classList.remove(\'visible\');\n}));\n\nlet busy = false;\ndocument.getElementById(\'loginForm\')?.addEventListener(\'submit\', async (e) => {\n  e.preventDefault();\n  if (busy) return;\n  const username = (userInp.value || \'\').trim();\n  const password = passInp.value || \'\';\n  if (!username || !password) {\n    showErr(\'请输入账号和密码\');\n    (!username ? userInp : passInp).focus();\n    return;\n  }\n  busy = true;\n  subBtn.disabled = true;\n  errBox.classList.remove(\'visible\');\n  const ns = \'http://www.w3.org/2000/svg\';\n  const sp = document.createElementNS(ns, \'svg\');\n  sp.setAttribute(\'class\', \'ico spin\');\n  const u = document.createElementNS(ns, \'use\');\n  u.setAttribute(\'href\', \'#i-spin\');\n  sp.appendChild(u);\n  const st = document.createElement(\'span\');\n  st.textContent = \'正在验证...\';\n  subBtn.replaceChildren(sp, st);\n\n  try {\n    const res = await fetch(\'/api/login\', {\n      method: \'POST\',\n      headers: { \'Content-Type\': \'application/json\' },\n      body: JSON.stringify({ username, password })\n    });\n    const data = await res.json().catch(() => ({}));\n    if (!res.ok || !data.ok) {\n      showErr(data.message || \'账号或密码不正确\');\n      busy = false;\n      subBtn.disabled = false;\n      const lbl = document.createElement(\'span\');\n      lbl.textContent = \'进入工作台\';\n      subBtn.replaceChildren(lbl);\n      passInp.select();\n      return;\n    }\n    window.location.replace(\'/\');\n  } catch (_) {\n    showErr(\'网络连接异常，请重试\');\n    busy = false;\n    subBtn.disabled = false;\n    const lbl = document.createElement(\'span\');\n    lbl.textContent = \'进入工作台\';\n    subBtn.replaceChildren(lbl);\n  }\n});\n</script>\n</body>\n</html>'

# 权威 UI 只存储一份；运行时保持与磁盘模板字节一致。
HTML_PAGE = open(os.path.join(BASE_DIR, 'ui', 'panel_ui.html'), encoding='utf-8').read()

class PanelHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _client_real_ip(self) -> str:
        xff = (self.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        return xff or (self.client_address[0] if self.client_address else "unknown")

    def _is_authenticated(self) -> bool:
        peer_ip = self.client_address[0] if self.client_address else ""
        if peer_ip in ("127.0.0.1", "::1", "::ffff:127.0.0.1") and not self.headers.get("X-Forwarded-For"):
            ipc_hdr = (self.headers.get("X-G4-Internal") or "").strip()
            cfg_tok = (_get_auth_config().get("internal_ipc_token") or "").strip()
            if ipc_hdr and cfg_tok and hmac.compare_digest(ipc_hdr, cfg_tok):
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
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
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
                data = open(fp, "rb").read()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "public, max-age=86400")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)
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

        if path == "/api/outputs":
            self._send_json(list_outputs())
            return

        if path == "/api/chat":
            self._send_json(load_chat_history())
            return

        if path == "/api/settings":
            self._send_json({
                "ok": True,
                "settings": load_studio_settings(),
                "defaults": DEFAULT_STUDIO_SETTINGS,
                "ui_meta": {"image_label": MODEL_LABELS["v5_int8"], "video_label": MODEL_LABELS["video"], "llm_label": MODEL_LABELS["llm"], "comfy_url": os.environ.get("COLABBB_COMFY_URL", "")},
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
            self.send_header("Content-Length", str(st.st_size))
            self.end_headers()
            with open(serve_path, "rb") as fh:
                while True:
                    chunk = fh.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
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
            if self.headers.get("If-None-Match") == etag:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.send_header("Cache-Control", "private, max-age=86400")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("ETag", etag)
            self.send_header("Cache-Control", "private, max-age=86400")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(fsize))
            self.end_headers()
            with open(full_path, "rb") as fh:
                while True:
                    chunk = fh.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
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

        if parsed.path == "/api/login":
            ip_key = self._client_real_ip()
            if not _check_login_rate_limit(ip_key):
                self._send_json({"ok": False, "message": "尝试次数过多，请 5 分钟后再试"}, 429)
                return
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 16384:
                self._send_json({"ok": False, "message": "请求无效"}, 400)
                return
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            data = json.loads(raw or "{}")
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
            self.wfile.write(body)
            return

        if parsed.path == "/api/logout":
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
            self.wfile.write(body)
            return

        if not self._is_authenticated():
            self._send_json({"ok": False, "auth_required": True, "message": "未登录或会话已过期"}, 401)
            return

        if parsed.path == "/api/touch":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="ignore") if length > 0 else "{}"
            data = json.loads(raw or "{}")
            touch_if_running(_resolve_source_idle_minutes(data.get("source") if isinstance(data, dict) else None))
            self._send_json({"ok": True})
            return

        if parsed.path == "/api/cancel":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="ignore") if length > 0 else "{}"
            data = json.loads(raw or "{}")
            touch_if_running(_resolve_source_idle_minutes(data.get("source") if isinstance(data, dict) else None))
            ok, msg = JOB.cancel_job()
            self._send_json({"ok": ok, "message": msg})
            return

        if parsed.path == "/api/task_action":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            data = json.loads(raw or "{}")
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
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="ignore") if length > 0 else "{}"
            data = json.loads(raw or "{}")
            touch_if_running(_resolve_source_idle_minutes(data.get("source") if isinstance(data, dict) else None))
            save_chat_history([])
            self._send_json({"ok": True})
            return

        if parsed.path == "/api/settings":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 2 * 1024 * 1024:
                self._send_json({"ok": False, "message": "提交的配置内容为空或超出大小限制"}, 400)
                return
            raw = self.rfile.read(length).decode("utf-8", errors="replace")
            try:
                data = json.loads(raw)
            except Exception:
                self._send_json({"ok": False, "message": "JSON 格式解析失败"}, 400)
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
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else "{}"
            try:
                data = json.loads(raw or "{}")
            except Exception:
                self._send_json({"ok": False, "message": "JSON 格式解析失败"}, 400)
                return
            if not isinstance(data, dict):
                self._send_json({"ok": False, "message": "请求体必须为 JSON 对象"}, 400)
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
            mod_dir_name = "upload_image"
            target_dir = os.path.abspath(os.path.join(OUT_ROOT, mod_dir_name))
            os.makedirs(target_dir, exist_ok=True)
            fname = f"up_{time.time_ns()}_{os.urandom(4).hex()}_{safe_base}"
            full_path = os.path.join(target_dir, fname)
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 200 * 1024 * 1024:
                self._send_json({"ok": False, "message": "文件大小无效或超过 200MB 限制"}, 400)
                return
            remaining = length
            with open(full_path, "wb") as fh:
                while remaining > 0:
                    chunk = self.rfile.read(min(65536, remaining))
                    if not chunk:
                        break
                    fh.write(chunk)
                    remaining -= len(chunk)
            if remaining:
                os.remove(full_path)
                self._send_json({"ok": False, "message": "上传内容不完整，未保留文件"}, 400)
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
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            data = json.loads(raw or "{}")
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
                    self.wfile.write(chunk)
            return

        if parsed.path == "/api/run":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="ignore")
            data = json.loads(raw or "{}")
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
                if raw_sub in ("start", "stop", "warmup"):
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
                ref_img = (data.get("ref_image") or "").strip()
                if ref_img and os.path.isfile(ref_img):
                    cmd_list.extend(["--ref-image", ref_img])
                res_lbl = f"{actual_w}×{actual_h}"
                stage_tag = "3K生图" if use_hires else "直出生图"
                ok, msg = JOB.start_job(cmd_list, "image", meta_label=f"{stage_tag} [{MODEL_LABELS[img_mod].replace(' ', '-')}] ({res_lbl}·{st_val}步)", steps=max(1, st_val), prompt_preview=clean_text, idle_minutes=req_idle_min)
                self._send_json({"ok": ok, "message": msg})
                return

            if mod == "edit":
                ref_img = (data.get("ref_image") or "").strip()
                if not ref_img or not os.path.isfile(ref_img):
                    self._send_json({"ok": False, "message": "请先选择或上传一张要修改的原图"}, 400)
                    return
                clean_text = re.sub(r"^[\s\-#*]+", "", text).strip() or text.strip()
                os.makedirs(STATE_DIR, exist_ok=True)
                pfile = os.path.join(STATE_DIR, f"edit_req_prompt_{time.time_ns()}_{os.urandom(4).hex()}.txt")
                with open(pfile, "w", encoding="utf-8") as fh:
                    fh.write(clean_text)
                st_val = int(data.get("steps", 8))
                refine_3k = bool(data.get("refine_3k", False))
                cmd_list = [
                    COLAB_AI_BIN, "image",
                    "--prompt-file", pfile,
                    "--model", "krea2edit",
                    "--steps", str(st_val),
                    "--ref-image", ref_img,
                ]
                if refine_3k:
                    cmd_list.append("--refine-3k")
                tot_s = st_val + (5 if refine_3k else 0)
                r_tag = "+同尺寸精修" if refine_3k else "直出"
                ok, msg = JOB.start_job(cmd_list, "edit", meta_label=f"命令改图 [Krea2Edit {r_tag}] ({tot_s}步)", steps=tot_s, prompt_preview=clean_text, idle_minutes=req_idle_min)
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
                ref_img = (data.get("ref_image") or "").strip()
                if ref_img and os.path.isfile(ref_img):
                    cmd_list.extend(["--ref-image", ref_img])
                import remote_video as _rv_mod
                _, _, out_w, out_h, q_val = _rv_mod.resolve_h3_stage_dims(vw, vh, q_val, ref_img if (ref_img and os.path.isfile(ref_img)) else "")
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
                if isinstance(sys_file_in, str) and sys_file_in.strip():
                    real_sf = os.path.realpath(sys_file_in.strip())
                    if real_sf.startswith(real_state + os.sep) and os.path.isfile(real_sf):
                        cmd_list.extend(["--system-file", real_sf])
                elif isinstance(custom_sys, str) and custom_sys.strip():
                    sys_file = os.path.join(STATE_DIR, f"llm_sys_{time.time_ns()}_{os.urandom(4).hex()}.txt")
                    with open(sys_file, "w", encoding="utf-8") as fh:
                        fh.write(custom_sys.strip())
                    cmd_list.extend(["--system-file", sys_file])
                out_file_in = data.get("out_file")
                if isinstance(out_file_in, str) and out_file_in.strip():
                    real_of = os.path.realpath(out_file_in.strip())
                    if real_of.startswith(real_state + os.sep):
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
    srv = ThreadingHTTPServer((os.environ.get('COLABBB_PANEL_HOST', '0.0.0.0'), PORT), PanelHandler)
    print(f'[Colabbb] Listening on port {PORT}', flush=True)
    srv.serve_forever()

if __name__ == "__main__":
    main()
