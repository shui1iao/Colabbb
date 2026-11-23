#!/usr/bin/env python3
"""
Colabbb 远端 ComfyUI 3K 二阶段生图执行器
- 支持一阶段直出与二阶段 3K 潜空间放大精绘 (1024x1536 -> 2048x3072 / 3072x2048 / 2560x2560)
- 支持挂载参考图特征保持、动态提示词前后缀与工作流参数热覆盖
- 所有模型仓库、权重文件名与反向 SSH 隧道参数均通过环境变量配置，零硬编码凭据
"""
import argparse, base64, copy, fcntl, glob, hashlib, json, os, re, shutil, socket, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
from functools import wraps

os.environ["HF_HUB_DISABLE_XET"] = "1"
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
try:
    import google.colab.userdata as _ud
    _ud.get = lambda *a, **k: None
except Exception:
    pass

if os.environ.get("COLAB_AI_ARGS_JSON"):
    sys.argv = [sys.argv[0]] + json.loads(os.environ["COLAB_AI_ARGS_JSON"])

COMFY_DIR = os.environ.get("COLABBB_COMFY_DIR", "/content/ComfyUI")
COMFY_TUNNEL_PORT = int(os.environ.get("COLABBB_COMFY_TUNNEL_PORT", "28188"))

IMAGE_LABEL = os.environ.get("COLABBB_IMAGE_LABEL", "Flux / DiT INT8").strip()
IMAGE_UNET_REPO = os.environ.get("COLABBB_IMAGE_UNET_REPO", "Comfy-Org/Krea-2").strip()
IMAGE_UNET_FILE = os.environ.get("COLABBB_IMAGE_UNET_FILE", "krea2_dev_fp8_e4m3fn.safetensors").strip()
IMAGE_UNET_PATH = os.environ.get("COLABBB_IMAGE_UNET_PATH", f"split_files/diffusion_models/{IMAGE_UNET_FILE}").strip()
IMAGE_UNET_SIZE = int(os.environ.get("COLABBB_IMAGE_UNET_SIZE", "0"))
IMAGE_UNET_SHA256 = os.environ.get("COLABBB_IMAGE_UNET_SHA256", "").strip()

IMAGE_BF16_REPO = os.environ.get("COLABBB_IMAGE_BF16_REPO", IMAGE_UNET_REPO).strip()
IMAGE_BF16_FILE = os.environ.get("COLABBB_IMAGE_BF16_FILE", "krea2_dev_bf16.safetensors").strip()
IMAGE_BF16_PATH = os.environ.get("COLABBB_IMAGE_BF16_PATH", f"split_files/diffusion_models/{IMAGE_BF16_FILE}").strip()

IMAGE_CLIP_REPO = os.environ.get("COLABBB_IMAGE_CLIP_REPO", "Comfy-Org/Krea-2").strip()
IMAGE_CLIP_FILE = os.environ.get("COLABBB_IMAGE_CLIP_FILE", "qwen_3_4b.safetensors").strip()
IMAGE_CLIP_PATH = os.environ.get("COLABBB_IMAGE_CLIP_PATH", f"split_files/text_encoders/{IMAGE_CLIP_FILE}").strip()
IMAGE_CLIP_TYPE = os.environ.get("COLABBB_IMAGE_CLIP_TYPE", "krea2").strip()

IMAGE_VAE_REPO = os.environ.get("COLABBB_IMAGE_VAE_REPO", "Comfy-Org/Krea-2").strip()
IMAGE_VAE_FILE = os.environ.get("COLABBB_IMAGE_VAE_FILE", "qwen_image_vae.safetensors").strip()
IMAGE_VAE_PATH = os.environ.get("COLABBB_IMAGE_VAE_PATH", f"vae/{IMAGE_VAE_FILE}").strip()

DEFAULT_NEG = "artifacts, washed-out colors, low quality, low resolution, blurry, indistinct, deformed, bad anatomy, extra digits, watermark"

VALID_SAMPLERS = ("euler", "euler_ancestral", "dpmpp_2m", "dpmpp_2m_sde", "uni_pc", "heun", "deis")
VALID_SCHEDULERS = ("simple", "normal", "karras", "exponential", "sgm_uniform", "beta", "ddim_uniform")
VALID_UPSCALE_METHODS = ("lanczos", "bicubic", "bilinear", "area", "nearest-exact")

DEFAULT_IMAGE_SETTINGS = {
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


def _deep_merge_dict(base: dict, extra: dict) -> dict:
    out = copy.deepcopy(base)
    if not isinstance(extra, dict):
        return out
    for k, v in extra.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge_dict(out[k], v)
        elif k in out:
            out[k] = copy.deepcopy(v)
    return out


def load_runtime_studio_settings() -> dict:
    cfg = copy.deepcopy(DEFAULT_IMAGE_SETTINGS)
    raw = os.environ.get("COLAB_AI_SETTINGS_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                cfg = _deep_merge_dict(cfg, parsed)
        except Exception:
            pass
    ip = cfg["image_prompt"]
    ip["enable_clean"] = bool(ip.get("enable_clean", True))
    ip["enable_prefix"] = bool(ip.get("enable_prefix", False))
    ip["enable_suffix"] = bool(ip.get("enable_suffix", True))
    ip["strip_regexes"] = [x for x in (ip.get("strip_regexes") or []) if isinstance(x, str) and x.strip()]
    ip["replace_pairs"] = [
        {"from": str(it["from"]), "to": str(it.get("to", ""))}
        for it in (ip.get("replace_pairs") or [])
        if isinstance(it, dict) and isinstance(it.get("from"), str) and it["from"] != ""
    ]
    for pk in ("ref_scale_w", "ref_scale_h", "ref_crop_w", "ref_crop_h"):
        try:
            ip[pk] = _align16(ip[pk])
        except Exception:
            ip[pk] = DEFAULT_IMAGE_SETTINGS["image_prompt"][pk]
    for ok in ("ref_crop_x", "ref_crop_y"):
        try:
            ip[ok] = max(0, min(4096, int(ip[ok])))
        except Exception:
            ip[ok] = DEFAULT_IMAGE_SETTINGS["image_prompt"][ok]

    wf = cfg["image_workflow"]
    wf["enable_hires"] = bool(wf.get("enable_hires", True))
    try:
        wf["hires_denoise"] = max(0.01, min(1.0, float(wf.get("hires_denoise", 0.16))))
    except Exception:
        wf["hires_denoise"] = 0.16
    try:
        wf["cfg"] = max(0.1, min(20.0, float(wf.get("cfg", 1.0))))
    except Exception:
        wf["cfg"] = 1.0
    if wf.get("sampler_name") not in VALID_SAMPLERS:
        wf["sampler_name"] = "euler"
    if wf.get("scheduler") not in VALID_SCHEDULERS:
        wf["scheduler"] = "simple"
    if wf.get("upscale_method") not in VALID_UPSCALE_METHODS:
        wf["upscale_method"] = "lanczos"
    for rk, rdef in DEFAULT_IMAGE_SETTINGS["image_workflow"]["resolutions"].items():
        rcur = wf["resolutions"].get(rk) if isinstance(wf.get("resolutions"), dict) else None
        if not isinstance(rcur, dict):
            wf["resolutions"][rk] = dict(rdef)
        else:
            for dk in ("s1_w", "s1_h", "s2_w", "s2_h"):
                try:
                    rcur[dk] = _align16(rcur.get(dk, rdef[dk]))
                except Exception:
                    rcur[dk] = rdef[dk]
    for tk, tdef in DEFAULT_IMAGE_SETTINGS["image_workflow"]["step_tiers"].items():
        tcur = wf["step_tiers"].get(tk) if isinstance(wf.get("step_tiers"), dict) else None
        if not isinstance(tcur, dict):
            wf["step_tiers"][tk] = dict(tdef)
        else:
            try:
                tcur["s1"] = max(1, min(60, int(tcur.get("s1", tdef["s1"]))))
            except Exception:
                tcur["s1"] = tdef["s1"]
            try:
                tcur["ref"] = max(0, min(40, int(tcur.get("ref", tdef["ref"]))))
            except Exception:
                tcur["ref"] = tdef["ref"]
    return cfg


COMMON_ENC_VAE = [
    (IMAGE_CLIP_REPO, IMAGE_CLIP_PATH, "text_encoders", IMAGE_CLIP_FILE),
    (IMAGE_VAE_REPO, IMAGE_VAE_PATH, "vae", IMAGE_VAE_FILE),
]

WEIGHT_VERIFY = {}
if IMAGE_UNET_SIZE > 0 or IMAGE_UNET_SHA256:
    WEIGHT_VERIFY[IMAGE_UNET_FILE] = (IMAGE_UNET_SIZE, IMAGE_UNET_SHA256, IMAGE_LABEL)

MODEL_SPECS = {
    "v5_int8": [(IMAGE_UNET_REPO, IMAGE_UNET_PATH, "diffusion_models", IMAGE_UNET_FILE), *COMMON_ENC_VAE],
    "v5_bf16": [(IMAGE_BF16_REPO, IMAGE_BF16_PATH, "diffusion_models", IMAGE_BF16_FILE), *COMMON_ENC_VAE],
}

UNET_BY_KEY = {
    "v5_int8": (IMAGE_UNET_FILE, IMAGE_LABEL),
    "v5_bf16": (IMAGE_BF16_FILE, f"{IMAGE_LABEL} BF16"),
}


def _verify_unet_file(path, local_name, check_hash=True):
    spec = WEIGHT_VERIFY.get(local_name)
    if not spec:
        return
    exp_size, exp_sha, lbl = spec
    if exp_size > 0 and os.path.getsize(path) != exp_size:
        raise RuntimeError(f"{lbl} 文件大小校验失败 (got={os.path.getsize(path)}, want={exp_size})")
    if check_hash and exp_sha:
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            for block in iter(lambda: fh.read(8 * 1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest().lower() != exp_sha.lower():
            raise RuntimeError(f"{lbl} 完整 SHA256 校验失败")
        print(f"[image] {lbl} 精确大小与完整 SHA256 校验通过", flush=True)


def _resolve_civitai_url(ver_id: str, file_id: str) -> str:
    civ_tok = os.environ.get("CIVITAI_TOKEN", "").strip()
    civ_entry = f"https://civitai.com/api/download/models/{ver_id}?fileId={file_id}"
    civ_api_url = civ_entry + ("&token=" + urllib.parse.quote(civ_tok, safe="") if civ_tok else "")

    class _NoRedir(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            self.loc = newurl
            return None

    _nr = _NoRedir()
    _op = urllib.request.build_opener(_nr)
    try:
        _op.open(urllib.request.Request(civ_api_url, headers={"User-Agent": "Mozilla/5.0"}), timeout=20)
    except Exception:
        pass
    return getattr(_nr, "loc", civ_api_url)


def _resolve_download_url(repo: str, hf_path: str) -> str:
    if repo.startswith(("http://", "https://")):
        return repo
    if repo.startswith("CIVITAI:"):
        parts = repo.split(":")
        if len(parts) >= 3:
            return _resolve_civitai_url(parts[1], parts[2])
    return f"https://huggingface.co/{repo}/resolve/main/{hf_path}"


def ensure_tunnel(port: int = 8188):
    tunnel_key_b64 = os.environ["COLABBB_TUNNEL_KEY_B64"].strip()
    tunnel_host = os.environ["COLABBB_TUNNEL_HOST"].strip()
    tunnel_port = os.environ.get("COLABBB_TUNNEL_PORT", "22").strip() or "22"
    tunnel_user = os.environ.get("COLABBB_TUNNEL_USER", "root").strip() or "root"
    if not tunnel_key_b64 or not tunnel_host or tunnel_host.startswith("your-"):
        return

    key_path = "/content/.colabbb_tunnel_key_img"
    tmp_kp = f"{key_path}.{os.getpid()}"
    with open(tmp_kp, "wb") as fh:
        fh.write(base64.b64decode(tunnel_key_b64))
    os.chmod(tmp_kp, 0o600)
    os.replace(tmp_kp, key_path)
    if subprocess.run(["pgrep", "-f", f"{COMFY_TUNNEL_PORT}:127.0.0.1:{port}"], capture_output=True, timeout=10).returncode == 0:
        return
    host_pub = os.environ.get("COLABBB_TUNNEL_HOST_PUBKEY", "").strip()
    if host_pub:
        kh_path = "/tmp/.colabbb_known_hosts"
        kh_tmp = f"{kh_path}.{os.getpid()}"
        host_entry = tunnel_host if tunnel_port == "22" else f"[{tunnel_host}]:{tunnel_port}"
        with open(kh_tmp, "w", encoding="utf-8") as kh:
            kh.write(f"{host_entry} {host_pub}\n")
        os.chmod(kh_tmp, 0o600)
        os.replace(kh_tmp, kh_path)
        host_opts = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={kh_path}"]
    else:
        host_opts = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "UserKnownHostsFile=/tmp/.colabbb_known_hosts"]
    subprocess.Popen([
        "ssh", "-N", "-T",
        *host_opts,
        "-o", "ServerAliveInterval=20",
        "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes",
        "-i", key_path,
        "-p", tunnel_port,
        "-R", f"127.0.0.1:{COMFY_TUNNEL_PORT}:127.0.0.1:{port}",
        f"{tunnel_user}@{tunnel_host}",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


def _ensure_aria2():
    if shutil.which("aria2c"):
        return
    with open("/content/.colab-ai-apt.lock", "a+") as lfh:
        fcntl.flock(lfh, fcntl.LOCK_EX)
        try:
            if not shutil.which("aria2c"):
                subprocess.run(["apt-get", "update", "-qq"], check=False)
                subprocess.run(["apt-get", "install", "-y", "-qq", "aria2"], check=True)
        finally:
            fcntl.flock(lfh, fcntl.LOCK_UN)


def _initialization_guard(fn):
    @wraps(fn)
    def guarded(*args, **kwargs):
        with open("/content/.colab-ai-image-init.lock", "a+") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                return fn(*args, **kwargs)
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
    return guarded


def _service_present(port, pattern):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return subprocess.run(["pgrep", "-f", pattern], capture_output=True).returncode == 0


def _ensure_http_service(port, health, command, log_path, timeout, pattern, cwd=None, prepare=None):
    if health():
        return
    proc = None
    if not _service_present(port, pattern):
        if prepare:
            prepare()
        with open(log_path, "a") as log:
            proc = subprocess.Popen(command, cwd=cwd, stdout=log, stderr=log, start_new_session=True)
        print("[lifecycle] model service started once; waiting for readiness", flush=True)
    else:
        print("[lifecycle] existing service loading/unhealthy; waiting without kill/restart", flush=True)
    for _ in range(timeout):
        if health():
            return
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"模型服务启动失败(code={proc.returncode})，未自动重启")
        time.sleep(1)
    raise RuntimeError("模型服务就绪超时；未kill或重复启动，请检查技术日志")


@_initialization_guard
def ensure_models_and_comfy(model_keys, port: int = 8188):
    _ensure_aria2()
    pip_proc = None
    if not os.path.exists(os.path.join(COMFY_DIR, "main.py")):
        if os.path.exists(COMFY_DIR):
            shutil.rmtree(COMFY_DIR, ignore_errors=True)
        print("[image] 正在克隆最新版 ComfyUI 并后台并发安装依赖...", flush=True)
        subprocess.run(["git", "clone", "--depth", "1", "https://github.com/comfyanonymous/ComfyUI.git", COMFY_DIR], check=True)
        pip_proc = subprocess.Popen(
            ["uv", "pip", "install", "--system", "-q", "-r", f"{COMFY_DIR}/requirements.txt", "av>=14.2.0"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
    else:
        sd_py = os.path.join(COMFY_DIR, "comfy", "sd.py")
        if os.path.exists(sd_py) and "KREA2" not in open(sd_py, "r", encoding="utf-8", errors="ignore").read():
            subprocess.run(["git", "-C", COMFY_DIR, "pull", "--ff-only"], check=False)

    protected_files = {
        os.path.join(COMFY_DIR, "models", subdir, local_name)
        for mk in model_keys
        for _, _, subdir, local_name in MODEL_SPECS.get(mk, MODEL_SPECS["v5_int8"])
    }
    for old_vid in (
        glob.glob(os.path.join(COMFY_DIR, "models", "**", "*Wan2.2*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "*wan2.2*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "wan_2.1_vae*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "umt5_xxl*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "clip_vision_h*"), recursive=True)
    ):
        if old_vid in protected_files:
            continue
        if os.path.isfile(old_vid):
            try:
                os.remove(old_vid)
            except Exception:
                pass

    seen = set()
    active_dls = []
    t_dl_start = time.time()
    use_shm_img = False
    try:
        mem_avail_b = 0
        with open("/proc/meminfo", "r") as mfh:
            for ln in mfh:
                if ln.startswith("MemAvailable:"):
                    mem_avail_b = int(ln.split()[1]) * 1024
                    break
        shm_free_b = shutil.disk_usage("/dev/shm").free if os.path.isdir("/dev/shm") else 0
        if shm_free_b >= int(42 * (1024 ** 3)) and mem_avail_b >= int(62 * (1024 ** 3)):
            use_shm_img = True
            print(f"[image] 启用 /dev/shm 内存盘加速生图权重写入 (空闲 {shm_free_b // (1024**3)}GiB)", flush=True)
    except Exception:
        use_shm_img = False

    hf_tok = os.environ.get("HF_TOKEN", "").strip()
    for mk in model_keys:
        for repo, hf_path, subdir, local_name in MODEL_SPECS.get(mk, MODEL_SPECS["v5_int8"]):
            if (subdir, local_name) in seen:
                continue
            seen.add((subdir, local_name))
            comfy_model_dir = os.path.join(COMFY_DIR, "models", subdir)
            os.makedirs(comfy_model_dir, exist_ok=True)
            comfy_link_path = os.path.join(comfy_model_dir, local_name)
            if os.path.lexists(comfy_link_path) and not os.path.exists(comfy_link_path):
                try:
                    os.unlink(comfy_link_path)
                except OSError:
                    pass

            est_size = WEIGHT_VERIFY.get(local_name, (14_000_000_000,))[0] or 14_000_000_000
            if os.path.exists(comfy_link_path):
                target_dir = os.path.dirname(os.path.realpath(comfy_link_path))
                target_file = os.path.realpath(comfy_link_path)
            elif use_shm_img and (shutil.disk_usage("/dev/shm").free >= (est_size + int(22 * (1024 ** 3)))):
                target_dir = os.path.join("/dev/shm/colab_comfy_models", subdir)
                os.makedirs(target_dir, exist_ok=True)
                target_file = os.path.join(target_dir, local_name)
                if not os.path.lexists(comfy_link_path):
                    os.symlink(target_file, comfy_link_path)
            else:
                target_dir = comfy_model_dir
                target_file = comfy_link_path

            cache_exists = os.path.exists(target_file)
            cache_size = os.path.getsize(target_file) if cache_exists else None
            cache_incomplete = os.path.exists(target_file + ".aria2")
            has_verify = subdir == "diffusion_models" and local_name in WEIGHT_VERIFY
            if cache_exists and cache_size > 10_000_000 and not cache_incomplete:
                if has_verify:
                    _verify_unet_file(target_file, local_name, check_hash=False)
                continue
            cache_reasons = []
            if not cache_exists:
                cache_reasons.append("文件缺失")
            elif cache_size <= 10_000_000:
                cache_reasons.append(f"文件过小（本地大小={cache_size}字节，要求>10000000字节）")
            if cache_incomplete:
                cache_reasons.append("存在同路径.aria2未完成标记")
            print(f"[image] 缓存未命中: {local_name}；原因: {'；'.join(cache_reasons)}", flush=True)
            url = _resolve_download_url(repo, hf_path)
            print(f"[image] 正在并发极速拉取 {local_name} ...", flush=True)
            aria_cmd = [
                "aria2c", "-x", "16", "-s", "10", "-k", "16M", "--file-allocation=none", "--disk-cache=64M",
                "--user-agent=Mozilla/5.0", "--console-log-level=error", "--summary-interval=0",
                "--allow-overwrite=true",
            ]
            if hf_tok and "huggingface.co" in url:
                aria_cmd.append(f"--header=Authorization: Bearer {hf_tok}")
            aria_cmd.extend(["-d", target_dir, "-o", local_name, url])
            dl_p = subprocess.Popen(aria_cmd)
            active_dls.append((local_name, target_file, has_verify, dl_p, time.time()))

    while any(p.poll() is None for _, _, _, p, _ in active_dls):
        time.sleep(6)
        running_names = [
            f"{nm}({int(round(os.path.getsize(tf)/1e6, 0)) if os.path.exists(tf) else 0}MB)"
            for nm, tf, _, p, _ in active_dls if p.poll() is None
        ]
        if running_names:
            print(f"[image] 正在拉取 {', '.join(running_names)} ... ({int(time.time()-t_dl_start)}s)", flush=True)

    for local_name, target_file, has_verify, dl_p, t0 in active_dls:
        rc = dl_p.wait()
        if rc != 0:
            raise RuntimeError(f"下载失败: {local_name} (code={rc})")
        if has_verify:
            _verify_unet_file(target_file, local_name, check_hash=True)
        print(f"[image] 已就绪: {local_name} ({time.time() - t0:.1f}s)", flush=True)

    if pip_proc is not None:
        _, pip_err = pip_proc.communicate()
        if pip_proc.returncode != 0:
            raise RuntimeError(f"ComfyUI 依赖安装失败: {(pip_err or '')[-400:]}")
    else:
        try:
            from av.video.reformatter import ColorPrimaries
        except Exception:
            subprocess.run(["uv", "pip", "install", "--system", "-q", "-U", "av>=14.2.0"], check=False)

    def healthy():
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/system_stats", timeout=2).read()
            info = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/object_info/UNETLoader", timeout=2).read().decode())
            return "UNETLoader" in info
        except Exception:
            return False

    _ensure_http_service(
        port, healthy,
        [sys.executable, "main.py", "--listen", "127.0.0.1", "--port", str(port), "--highvram"],
        "/content/comfyui.log", 90, "main.py.*--port " + str(port), cwd=COMFY_DIR
    )
    ensure_tunnel(port)


def _clean_prompt_terms(prompt: str, img_prompt_cfg: dict = None) -> str:
    p = prompt.strip()
    cfg = img_prompt_cfg if isinstance(img_prompt_cfg, dict) else load_runtime_studio_settings()["image_prompt"]
    if not cfg.get("enable_clean", True):
        return p
    for rx in (cfg.get("strip_regexes") or []):
        if isinstance(rx, str) and rx:
            try:
                p = re.sub(rx, "。", p)
            except re.error as exc:
                print(f"[image] 忽略无效的清理正则 {rx!r}: {exc}", flush=True)
    for pair in (cfg.get("replace_pairs") or []):
        if isinstance(pair, dict):
            old_s = pair.get("from", "")
            new_s = pair.get("to", "")
            if isinstance(old_s, str) and old_s and isinstance(new_s, str):
                p = p.replace(old_s, new_s)
    return p.strip()


def _apply_prompt_affixes(prompt: str, img_prompt_cfg: dict = None) -> str:
    cfg = img_prompt_cfg if isinstance(img_prompt_cfg, dict) else load_runtime_studio_settings()["image_prompt"]
    p = _clean_prompt_terms((prompt or "").lstrip("-# \n\r\t").strip(), cfg)
    if not p:
        return ""
    has_zh = bool(re.search(r"[\u4e00-\u9fff]", p))
    if cfg.get("enable_prefix", False):
        pref = ((cfg.get("zh_prefix") if has_zh else cfg.get("en_prefix")) or "").strip()
        if pref and not p.startswith(pref):
            if has_zh:
                sep = "" if pref[-1] in "，。；、：,.;:" else "，"
                p = f"{pref}{sep}{p}"
            else:
                sep = " " if pref[-1] in ",.;:" else ", "
                p = f"{pref}{sep}{p}"
    if cfg.get("enable_suffix", True):
        if has_zh:
            zh_suf = (cfg.get("zh_suffix") or "").strip()
            if zh_suf and zh_suf not in p:
                p = p.rstrip("。 ") + "。" + zh_suf.lstrip("。 ")
        else:
            en_suf = (cfg.get("en_suffix") or "").strip()
            if en_suf and en_suf.lower() not in p.lower():
                p = p.rstrip(". ") + ". " + en_suf.lstrip(". ")
    return p


def _format_ref_face_prompt(clean_p: str, img_prompt_cfg: dict) -> str:
    tpl = (img_prompt_cfg.get("ref_face_prompt") or "").strip()
    if not tpl:
        return clean_p
    if "{prompt}" in tpl:
        return tpl.replace("{prompt}", clean_p)
    return f"{tpl.rstrip()} {clean_p}"


def _attach_ref_images(wf: dict, prompt_str: str, ref_images: list, settings: dict = None) -> str:
    cur_settings = settings if isinstance(settings, dict) else load_runtime_studio_settings()
    ip = cur_settings["image_prompt"]
    clean_p = _apply_prompt_affixes(prompt_str, ip)
    ref_images = [p for p in (ref_images or []) if p and os.path.exists(p)]
    if not ref_images:
        wf["6"] = {"class_type": "CLIPTextEncode", "inputs": {"clip": ["11", 0], "text": clean_p}}
        wf["13"] = {"class_type": "ConditioningZeroOut", "inputs": {"conditioning": ["6", 0]}}
        return "6"
    in_dir = os.path.join(COMFY_DIR, "input")
    os.makedirs(in_dir, exist_ok=True)
    face_p = _format_ref_face_prompt(clean_p, ip)
    edit_inputs = {"clip": ["11", 0], "prompt": face_p}
    scale_w = int(ip.get("ref_scale_w", 768))
    scale_h = int(ip.get("ref_scale_h", 1152))
    crop_w = int(ip.get("ref_crop_w", 512))
    crop_h = int(ip.get("ref_crop_h", 448))
    crop_x = int(ip.get("ref_crop_x", 128))
    crop_y = int(ip.get("ref_crop_y", 0))
    for idx, rpath in enumerate(ref_images[:3], start=1):
        fname = f"imgref_{int(time.time()*1000)}_{idx}_{os.path.basename(rpath)}"
        shutil.copy2(rpath, os.path.join(in_dir, fname))
        nid_load = str(100 + idx)
        nid_scale = str(110 + idx)
        nid_crop = str(120 + idx)
        wf[nid_load] = {"class_type": "LoadImage", "inputs": {"image": fname}}
        wf[nid_scale] = {"class_type": "ImageScale", "inputs": {"image": [nid_load, 0], "upscale_method": "lanczos", "width": scale_w, "height": scale_h, "crop": "center"}}
        wf[nid_crop] = {"class_type": "ImageCrop", "inputs": {"image": [nid_scale, 0], "width": crop_w, "height": crop_h, "x": crop_x, "y": crop_y}}
        edit_inputs[f"image{idx}"] = [nid_crop, 0]
    wf["52"] = {"class_type": "TextEncodeQwenImageEditPlus", "inputs": edit_inputs}
    wf["13"] = {"class_type": "ConditioningZeroOut", "inputs": {"conditioning": ["52", 0]}}
    print(f"[image] 已通过多模态视觉编码挂载参考图进行生图: {[os.path.basename(x) for x in ref_images[:3]]}", flush=True)
    return "52"


def _resolve_res_and_steps(wf_cfg: dict, base_width: int, base_height: int, steps: int, res_mode: str = "", step_tier: str = ""):
    rm = (res_mode or "").strip().lower()
    if rm not in ("portrait", "landscape", "square"):
        if base_height > base_width:
            rm = "portrait"
        elif base_width > base_height:
            rm = "landscape"
        else:
            rm = "square"
    rcfg = wf_cfg["resolutions"][rm]
    s1_w, s1_h = int(rcfg["s1_w"]), int(rcfg["s1_h"])
    s2_w, s2_h = int(rcfg["s2_w"]), int(rcfg["s2_h"])

    enable_hires = bool(wf_cfg.get("enable_hires", True))
    st = (step_tier or "").strip().lower()
    if st not in ("tier1", "tier2", "tier3"):
        tiers = wf_cfg["step_tiers"]
        def _tot(tk):
            t = tiers[tk]
            return int(t["s1"]) + (int(t["ref"]) if enable_hires and int(t["ref"]) > 0 else 0)
        if steps <= 0 or steps == _tot("tier2"):
            st = "tier2"
        elif steps == _tot("tier1"):
            st = "tier1"
        elif steps == _tot("tier3"):
            st = "tier3"
        elif steps <= 13:
            st = "tier1"
        elif steps <= 18:
            st = "tier2"
        else:
            st = "tier3"
    tcfg = wf_cfg["step_tiers"][st]
    s1_steps = int(tcfg["s1"])
    ref_steps = int(tcfg["ref"])
    use_hires = enable_hires and ref_steps > 0
    total_steps = (s1_steps + ref_steps) if use_hires else s1_steps
    return s1_w, s1_h, s2_w, s2_h, s1_steps, ref_steps, total_steps, use_hires


def run_single_stage(unet_name: str, label: str, prefix: str, prompt: str, width: int, height: int, steps: int, cfg: float, seed: int, out_dir: str, lora_name: str = "", lora_strength: float = 1.0, ref_images=None, port: int = 8188):
    cur_settings = load_runtime_studio_settings()
    wf_cfg = cur_settings["image_workflow"]
    before_files = set(glob.glob(f"{COMFY_DIR}/output/**/*.*", recursive=True))
    model_ref = ["10", 0]
    wf = {
        "10": {"class_type": "UNETLoader", "inputs": {"unet_name": unet_name, "weight_dtype": "default"}},
        "11": {"class_type": "CLIPLoader", "inputs": {"clip_name": IMAGE_CLIP_FILE, "type": IMAGE_CLIP_TYPE, "device": "default"}},
        "12": {"class_type": "VAELoader", "inputs": {"vae_name": IMAGE_VAE_FILE}},
        "5": {"class_type": "EmptyLatentImage", "inputs": {"width": width, "height": height, "batch_size": 1}},
    }
    pos_node = _attach_ref_images(wf, prompt, ref_images, settings=cur_settings)
    if lora_name:
        wf["15"] = {"class_type": "LoraLoaderModelOnly", "inputs": {"model": ["10", 0], "lora_name": lora_name, "strength_model": float(lora_strength)}}
        model_ref = ["15", 0]
    wf["3"] = {
        "class_type": "KSampler",
        "inputs": {
            "model": model_ref,
            "positive": [pos_node, 0],
            "negative": ["13", 0],
            "latent_image": ["5", 0],
            "seed": seed,
            "steps": steps,
            "cfg": cfg,
            "sampler_name": wf_cfg.get("sampler_name", "euler"),
            "scheduler": wf_cfg.get("scheduler", "simple"),
            "denoise": 1.0,
        },
    }
    wf["8"] = {"class_type": "VAEDecode", "inputs": {"samples": ["3", 0], "vae": ["12", 0]}}
    wf["29"] = {"class_type": "SaveImage", "inputs": {"images": ["8", 0], "filename_prefix": prefix}}
    return _submit_and_collect(wf, label, width, height, steps, seed, out_dir, prefix, before_files, port)


def build_hires_workflow(unet_name: str, prompt: str, base_width: int, base_height: int, steps: int, cfg: float, seed: int, ref_images=None, lora_name: str = "", lora_strength: float = 1.0, settings: dict = None, res_mode: str = "", step_tier: str = "", copy_refs: bool = True):
    cur_settings = settings if isinstance(settings, dict) else load_runtime_studio_settings()
    wf_cfg = cur_settings["image_workflow"]
    ip = cur_settings["image_prompt"]
    s1_w, s1_h, s2_w, s2_h, s1_steps, ref_steps, total_steps, use_hires = _resolve_res_and_steps(
        wf_cfg, base_width, base_height, steps, res_mode=res_mode, step_tier=step_tier
    )
    eff_cfg = float(cfg) if cfg and cfg > 0 else float(wf_cfg.get("cfg", 1.0))
    sampler_name = wf_cfg.get("sampler_name", "euler")
    scheduler = wf_cfg.get("scheduler", "simple")
    upscale_method = wf_cfg.get("upscale_method", "lanczos")
    hires_denoise = float(wf_cfg.get("hires_denoise", 0.16))

    init_w, init_h = (s1_w, s1_h) if use_hires else (s2_w, s2_h)
    wf = {
        "10": {"class_type": "UNETLoader", "inputs": {"unet_name": unet_name, "weight_dtype": "default"}},
        "11": {"class_type": "CLIPLoader", "inputs": {"clip_name": IMAGE_CLIP_FILE, "type": IMAGE_CLIP_TYPE, "device": "default"}},
        "12": {"class_type": "VAELoader", "inputs": {"vae_name": IMAGE_VAE_FILE}},
        "5": {"class_type": "EmptyLatentImage", "inputs": {"width": init_w, "height": init_h, "batch_size": 1}},
    }
    if copy_refs:
        pos_node = _attach_ref_images(wf, prompt, ref_images, settings=cur_settings)
    else:
        clean_p = _apply_prompt_affixes(prompt, ip)
        wf["6"] = {"class_type": "CLIPTextEncode", "inputs": {"clip": ["11", 0], "text": clean_p}}
        wf["13"] = {"class_type": "ConditioningZeroOut", "inputs": {"conditioning": ["6", 0]}}
        pos_node = "6"
    model_ref = ["10", 0]
    if lora_name:
        wf["15"] = {"class_type": "LoraLoaderModelOnly", "inputs": {"model": ["10", 0], "lora_name": lora_name, "strength_model": float(lora_strength)}}
        model_ref = ["15", 0]
    wf["3"] = {
        "class_type": "KSampler",
        "inputs": {
            "model": model_ref,
            "positive": [pos_node, 0],
            "negative": ["13", 0],
            "latent_image": ["5", 0],
            "seed": seed,
            "steps": s1_steps,
            "cfg": eff_cfg,
            "sampler_name": sampler_name,
            "scheduler": scheduler,
            "denoise": 1.0,
        },
    }
    wf["8"] = {"class_type": "VAEDecode", "inputs": {"samples": ["3", 0], "vae": ["12", 0]}}
    if use_hires:
        wf["20"] = {"class_type": "ImageScale", "inputs": {"image": ["8", 0], "upscale_method": upscale_method, "width": s2_w, "height": s2_h, "crop": "disabled"}}
        wf["21"] = {"class_type": "VAEEncode", "inputs": {"pixels": ["20", 0], "vae": ["12", 0]}}
        wf["22"] = {
            "class_type": "KSampler",
            "inputs": {
                "model": model_ref,
                "positive": [pos_node, 0],
                "negative": ["13", 0],
                "latent_image": ["21", 0],
                "seed": seed + 1,
                "steps": ref_steps,
                "cfg": eff_cfg,
                "sampler_name": sampler_name,
                "scheduler": scheduler,
                "denoise": hires_denoise,
            },
        }
        wf["23"] = {"class_type": "VAEDecode", "inputs": {"samples": ["22", 0], "vae": ["12", 0]}}
        wf["29"] = {"class_type": "SaveImage", "inputs": {"images": ["23", 0], "filename_prefix": "img3k"}}
    else:
        wf["29"] = {"class_type": "SaveImage", "inputs": {"images": ["8", 0], "filename_prefix": "img3k"}}
    return wf, s2_w, s2_h, total_steps, s1_steps, ref_steps, use_hires


build_krea2_hires_workflow = build_hires_workflow


def run_hires_generation(prompt: str, base_width: int, base_height: int, steps: int, cfg: float, seed: int, out_dir: str, ref_images=None, lora_name: str = "", lora_strength: float = 1.0, port: int = 8188, model_key: str = "v5_int8", res_mode: str = "", step_tier: str = ""):
    unet_name, unet_label = UNET_BY_KEY.get(model_key, (IMAGE_UNET_FILE, IMAGE_LABEL))
    before_files = set(glob.glob(f"{COMFY_DIR}/output/**/*.*", recursive=True))
    wf, s2_w, s2_h, total_steps, s1_steps, ref_steps, use_hires = build_hires_workflow(
        unet_name, prompt, base_width, base_height, steps, cfg, seed,
        ref_images=ref_images, lora_name=lora_name, lora_strength=lora_strength,
        res_mode=res_mode, step_tier=step_tier, copy_refs=True,
    )
    stage_desc = f"二阶段精绘 ({s1_steps}+{ref_steps}={total_steps}步)" if use_hires else f"一阶段直出 ({total_steps}步)"
    return _submit_and_collect(wf, f"{unet_label} {stage_desc}", s2_w, s2_h, total_steps, seed, out_dir, "img3k", before_files, port)


def _submit_and_collect(wf: dict, label: str, width: int, height: int, steps: int, seed: int, out_dir: str, prefix: str, before_files: set, port: int = 8188):
    t0 = time.time()
    payload = json.dumps({"prompt": wf}).encode("utf-8")
    req = urllib.request.Request(f"http://127.0.0.1:{port}/prompt", data=payload, headers={"Content-Type": "application/json"})
    try:
        resp = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"ComfyUI /prompt HTTP {e.code}: {err_body}")
    prompt_id = resp["prompt_id"]
    print(f"[image] 已提交 {label} 任务 (prompt_id={prompt_id}, {width}x{height}, steps={steps})，正在生成...", flush=True)

    poll_deadline = time.monotonic() + 600.0
    while True:
        if time.monotonic() > poll_deadline:
            raise RuntimeError(f"ComfyUI 生图轮询超时 (>600s, prompt_id={prompt_id})")
        time.sleep(0.8)
        hist = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/history/{prompt_id}", timeout=15).read().decode())
        if prompt_id in hist:
            entry = hist[prompt_id]
            status = entry.get("status", {})
            if status.get("status_str") == "error":
                raise RuntimeError(f"ComfyUI 生图报错: {json.dumps(status, ensure_ascii=False)}")
            found = []
            for node_out in entry.get("outputs", {}).values():
                for val in node_out.values():
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict) and "filename" in item:
                                src = os.path.join(COMFY_DIR, "output", item.get("subfolder", ""), item["filename"])
                                if os.path.exists(src):
                                    found.append(src)
            if not found:
                after_files = set(glob.glob(f"{COMFY_DIR}/output/**/*.*", recursive=True))
                found = sorted(after_files - before_files)
            if found:
                src = found[-1]
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, f"{prefix}_{int(time.time())}_{seed}.png")
                shutil.copy2(src, out_path)
                print(f"[image] {label} 生成完成 ({time.time() - t0:.2f}s, {width}x{height}, steps={steps}, seed={seed}): {out_path}", flush=True)
                if out_dir != "/tmp/warmup_out":
                    print(f"COLAB_AI_OUTPUT_FILE={out_path}", flush=True)
                return out_path
            break
    raise RuntimeError("ComfyUI 未返回生成的图片")


def normalize_model_key(raw: str, has_ref: bool = False) -> str:
    m = (raw or "v5_int8").lower().strip()
    if m in ("edit", "image_edit"):
        raise SystemExit("改图模块暂未启用")
    if m in ("v5_int8", "v5-int8", "v5_int8_convrot", "flux_int8", "dit_int8", "default", ""):
        return "v5_int8"
    if m in ("v5_bf16", "v5-bf16", "v5", "flux_bf16", "dit_bf16"):
        return "v5_bf16"
    raise SystemExit("未知生图模型键；请使用 v5_int8 (默认) 或通过环境变量配置自定义权重")


def main():
    p = argparse.ArgumentParser(description="Colabbb 3K 二阶段生图执行器")
    p.add_argument("prompt_pos", nargs="*", help="提示词")
    p.add_argument("--prompt", "-p", default="", help="生图提示词")
    p.add_argument("--prompt-file", "-f", default="", help="从文件读取生图提示词")
    p.add_argument("--negative-prompt", "-n", default="", help="反向提示词")
    p.add_argument("--ref-image", "--image", "-i", dest="ref_images", action="append", default=[], help="参考图路径")
    p.add_argument("--denoise", type=float, default=1.0, help="重绘幅度")
    p.add_argument("--refine-3k", action="store_true", help="开启二阶段精绘兼容开关")
    p.add_argument("--ref-boost", type=float, default=4.0, help="参考锁定强度")
    p.add_argument("--model", "-m", default="v5_int8", help="生图模型键 (默认 v5_int8)")
    p.add_argument("--steps", "-s", type=int, default=0, help="推理步数")
    p.add_argument("--guidance", "-g", type=float, default=0.0, help="CFG")
    p.add_argument("--width", "-W", type=int, default=3072, help="宽度")
    p.add_argument("--height", "-H", type=int, default=2048, help="高度")
    p.add_argument("--res-mode", default="", help="画幅档位 portrait|landscape|square")
    p.add_argument("--step-tier", default="", help="步数档位 tier1|tier2|tier3")
    p.add_argument("--seed", type=int, default=-1, help="随机种子 (-1 为随机)")
    p.add_argument("--num-images", type=int, default=1, help="生成张数")
    p.add_argument("--out-dir", default="/content/outputs/image", help="远端输出目录")
    p.add_argument("--warmup-only", action="store_true", help="预下载并启动 ComfyUI 显存预热")
    args = p.parse_args()

    ref_images = [p for p in (args.ref_images or []) if p and os.path.exists(p)]
    if args.warmup_only:
        raw_mks = [x.strip() for x in (args.model or "v5_int8").split(",") if x.strip()]
        mks = [normalize_model_key(x, False) for x in raw_mks]
        for x in mks:
            if x != "v5_int8":
                raise SystemExit("预热仅接受主生图模型键 v5_int8")
        ensure_models_and_comfy(mks)
        unet, label = UNET_BY_KEY[mks[0]]
        lbl_all = " + ".join(UNET_BY_KEY[k][1] for k in mks)
        print(f"[image] 已就绪权重 ({lbl_all})，正在预热 {label} ...", flush=True)
        run_single_stage(unet, f"{label} 预热", "warmup_img", "a sunlit architectural studio", 256, 256, 1, 1.0, 1, "/tmp/warmup_out")
        shutil.rmtree("/tmp/warmup_out", ignore_errors=True)
        print("COLAB_AI_WARMUP_OK=image", flush=True)
        return

    mk = normalize_model_key(args.model, False)
    ensure_models_and_comfy([mk])

    prompt = args.prompt or " ".join(args.prompt_pos).strip()
    if args.prompt_file and os.path.exists(args.prompt_file):
        with open(args.prompt_file, "r", encoding="utf-8") as fh:
            prompt = fh.read().strip()
    prompt = prompt.lstrip("-# \n\r\t").strip()
    if not prompt:
        raise SystemExit("请提供生图提示词")

    width = (args.width // 16) * 16
    height = (args.height // 16) * 16
    seed = args.seed if args.seed >= 0 else int(time.time() * 1000) % (2**31 - 1)
    for i in range(args.num_images):
        cur_seed = seed + i
        steps = args.steps if args.steps > 0 else 0
        cfg = args.guidance if args.guidance > 0 else 0.0
        run_hires_generation(
            prompt, width, height, steps, cfg, cur_seed, args.out_dir,
            ref_images=ref_images, model_key=mk,
            res_mode=args.res_mode, step_tier=args.step_tier,
        )


if __name__ == "__main__":
    main()
