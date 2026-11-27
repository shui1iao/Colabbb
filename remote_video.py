#!/usr/bin/env python3
"""
Colabbb 远端 24fps 声画同步高清视频执行器
- 支持严格首帧图 I2V / 首尾帧过渡 FL2VA / 纯文生音视频 T2VA
- 支持三档视频画质：std (标准 832p) / hd (超清 1088p 3D 潜空间超分) / uhd2k (2K 精修)
- 所有模型仓库、权重文件名与反向 SSH 隧道参数均支持环境变量覆盖，零硬编码凭据
"""
import argparse, math, struct, base64, fcntl, glob, json, os, shutil, socket, subprocess, sys, time, urllib.parse, urllib.request
from functools import wraps

os.environ["HF_HUB_DISABLE_XET"] = "1"
if os.environ.get("COLAB_AI_ARGS_JSON"):
    sys.argv = [sys.argv[0]] + json.loads(os.environ["COLAB_AI_ARGS_JSON"])

COMFY_DIR = os.environ.get("COLABBB_COMFY_DIR", "/content/ComfyUI")
COMFY_TUNNEL_PORT = int(os.environ.get("COLABBB_COMFY_TUNNEL_PORT", "28188"))

H3_UNET_REPO = os.environ.get("COLABBB_VIDEO_UNET_REPO", "Comfy-Org/MiniMax-H3").strip()
H3_UNET_NAME = os.environ.get("COLABBB_VIDEO_UNET_FILE", "minimax_h3_fp8_e4m3fn.safetensors").strip()
H3_UNET_PATH = os.environ.get("COLABBB_VIDEO_UNET_PATH", H3_UNET_NAME).strip()

H3_CLIP_REPO = os.environ.get("COLABBB_VIDEO_CLIP_REPO", "Comfy-Org/MiniMax-H3").strip()
H3_CLIP_NAME = os.environ.get("COLABBB_VIDEO_CLIP_FILE", "qwen_3_vl_32b_minimax_h3_fp8_e4m3fn.safetensors").strip()
H3_CLIP_PATH = os.environ.get("COLABBB_VIDEO_CLIP_PATH", f"text_encoders/{H3_CLIP_NAME}").strip()

H3_VIDEO_VAE_REPO = os.environ.get("COLABBB_VIDEO_VAE_REPO", "Comfy-Org/MiniMax-H3").strip()
H3_VIDEO_VAE_NAME = os.environ.get("COLABBB_VIDEO_VAE_FILE", "minimax_h3_video_vae_fp16.safetensors").strip()
H3_VIDEO_VAE_PATH = os.environ.get("COLABBB_VIDEO_VAE_PATH", f"vae/{H3_VIDEO_VAE_NAME}").strip()

H3_AUDIO_VAE_REPO = os.environ.get("COLABBB_AUDIO_VAE_REPO", "Comfy-Org/MiniMax-H3").strip()
H3_AUDIO_VAE_NAME = os.environ.get("COLABBB_AUDIO_VAE_FILE", "minimax_h3_audio_vae_fp32.safetensors").strip()
H3_AUDIO_VAE_PATH = os.environ.get("COLABBB_AUDIO_VAE_PATH", f"vae/{H3_AUDIO_VAE_NAME}").strip()

H3_UPSCALER_REPO = os.environ.get("COLABBB_VIDEO_UPSCALER_REPO", "LBH-123-AI/Minimax_h3_latent_Upscaler").strip()
H3_UPSCALER_NAME = os.environ.get("COLABBB_VIDEO_UPSCALER_FILE", "minimax_h3_latent_upscaler_3d_conv_v1_bf16.safetensors").strip()
H3_UPSCALER_PATH = os.environ.get("COLABBB_VIDEO_UPSCALER_PATH", f"minimax_h3_latent_upscaler_3d_conv_v1/{H3_UPSCALER_NAME}").strip()

QUALITY_SCALE_AREA = {
    "std": 832 * 1248,       # ~1.04 MP (832x1248 / 1248x832 / 1024x1024)
    "hd": 1088 * 1632,       # ~1.78 MP (1088x1632 / 1632x1088 / 1344x1344)
    "uhd2k": 1408 * 2112,    # ~2.97 MP (1408x2112 / 2112x1408 / 1728x1728)
}

VIDEO_MODEL_SPECS = [
    (H3_UNET_REPO, H3_UNET_PATH, "diffusion_models", H3_UNET_NAME, int(os.environ.get("COLABBB_VIDEO_UNET_SIZE", "0"))),
    (H3_CLIP_REPO, H3_CLIP_PATH, "text_encoders", H3_CLIP_NAME, int(os.environ.get("COLABBB_VIDEO_CLIP_SIZE", "0"))),
    (H3_VIDEO_VAE_REPO, H3_VIDEO_VAE_PATH, "vae", H3_VIDEO_VAE_NAME, int(os.environ.get("COLABBB_VIDEO_VAE_SIZE", "0"))),
    (H3_AUDIO_VAE_REPO, H3_AUDIO_VAE_PATH, "vae", H3_AUDIO_VAE_NAME, int(os.environ.get("COLABBB_AUDIO_VAE_SIZE", "0"))),
    (H3_UPSCALER_REPO, H3_UPSCALER_PATH, "latent_upscale_models", H3_UPSCALER_NAME, int(os.environ.get("COLABBB_VIDEO_UPSCALER_SIZE", "0"))),
]


def align_h3_dim(v: float, lo: int = 256, hi: int = 2560) -> int:
    return max(lo, min(hi, int(round(float(v) / 32.0)) * 32))


def _probe_image_size(path: str):
    try:
        with open(path, "rb") as fh:
            head = fh.read(64)
            if len(head) >= 24 and head[:8] == b"\x89PNG\r\n\x1a\n" and head[12:16] == b"IHDR":
                return struct.unpack(">II", head[16:24])
            if len(head) >= 30 and head[:4] == b"RIFF" and head[8:12] == b"WEBP":
                chunk = head[12:16]
                if chunk == b"VP8 " and len(head) >= 30:
                    w, h = struct.unpack("<HH", head[26:30])
                    return (w & 0x3FFF), (h & 0x3FFF)
                if chunk == b"VP8L" and len(head) >= 25:
                    b0, b1, b2, b3 = head[21:25]
                    return (1 + (((b1 & 0x3F) << 8) | b0)), (1 + (((b3 & 0xF) << 10) | (b2 << 2) | ((b1 & 0xC0) >> 6)))
                if chunk == b"VP8X" and len(head) >= 30:
                    w = 1 + int.from_bytes(head[24:27], "little")
                    h = 1 + int.from_bytes(head[27:30], "little")
                    return w, h
            if len(head) >= 4 and head[:2] == b"\xff\xd8":
                fh.seek(2)
                while True:
                    b = fh.read(1)
                    if not b:
                        break
                    while b == b"\xff":
                        b = fh.read(1)
                    if not b:
                        break
                    m = b[0]
                    if m in (0xD8, 0xD9):
                        continue
                    seg_len_b = fh.read(2)
                    if len(seg_len_b) < 2:
                        break
                    seg_len = struct.unpack(">H", seg_len_b)[0]
                    if m in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                        sof = fh.read(5)
                        if len(sof) == 5:
                            h, w = struct.unpack(">HH", sof[1:5])
                            return w, h
                        break
                    fh.seek(max(0, seg_len - 2), 1)
    except Exception:
        pass
    return None


def resolve_h3_stage_dims(width: int, height: int, quality: str = "std", img_path: str = "") -> tuple[int, int, int, int, str]:
    q = (quality or "std").strip().lower()
    if q not in QUALITY_SCALE_AREA:
        q = "std"
    sz = _probe_image_size(img_path) if img_path else None
    if sz and sz[0] > 0 and sz[1] > 0:
        ratio = float(sz[0]) / float(sz[1])
    else:
        ratio = float(max(32, width)) / float(max(32, height))

    # Stage 1 always stays on the native ~1.04MP training grid
    if abs(ratio - 1.0) < 0.06:
        s1_w, s1_h = 1024, 1024
        s2_map = {"std": (1024, 1024), "hd": (1344, 1344), "uhd2k": (1728, 1728)}
        s2_w, s2_h = s2_map[q]
    elif abs(ratio - (2.0 / 3.0)) < 0.04:
        s1_w, s1_h = 832, 1248
        s2_map = {"std": (832, 1248), "hd": (1088, 1632), "uhd2k": (1408, 2112)}
        s2_w, s2_h = s2_map[q]
    elif abs(ratio - (3.0 / 2.0)) < 0.04:
        s1_w, s1_h = 1248, 832
        s2_map = {"std": (1248, 832), "hd": (1632, 1088), "uhd2k": (2112, 1408)}
        s2_w, s2_h = s2_map[q]
    else:
        s1_area = QUALITY_SCALE_AREA["std"]
        s1_w = align_h3_dim(math.sqrt(s1_area * ratio))
        s1_h = align_h3_dim(math.sqrt(s1_area / ratio))
        if q == "std":
            s2_w, s2_h = s1_w, s1_h
        else:
            s2_area = QUALITY_SCALE_AREA[q]
            s2_w = align_h3_dim(math.sqrt(s2_area * ratio))
            s2_h = align_h3_dim(math.sqrt(s2_area / ratio))
    return s1_w, s1_h, s2_w, s2_h, q


def match_image_aspect_canvas(img_path: str, width: int, height: int) -> tuple[int, int]:
    s1_w, s1_h, _, _, _ = resolve_h3_stage_dims(width, height, "std", img_path)
    return s1_w, s1_h


def align_h3_length(n: int) -> int:
    n = max(5, int(n))
    while n % 17 != 5:
        n += 1
    return n


def ensure_tunnel(port: int = 8188):
    tunnel_key_b64 = os.environ["COLABBB_TUNNEL_KEY_B64"].strip()
    tunnel_host = os.environ["COLABBB_TUNNEL_HOST"].strip()
    tunnel_port = os.environ.get("COLABBB_TUNNEL_PORT", "22").strip() or "22"
    tunnel_user = os.environ.get("COLABBB_TUNNEL_USER", "root").strip() or "root"
    if not tunnel_key_b64 or not tunnel_host or tunnel_host.startswith("your-"):
        return

    key_path = "/content/.colabbb_tunnel_key_vid"
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


def _build_download_url(repo: str, hf_path: str) -> str:
    if repo.startswith(("http://", "https://")):
        return repo
    return f"https://huggingface.co/{repo}/resolve/main/{hf_path}"


@_initialization_guard
def ensure_video_models_and_comfy(port: int = 8188):
    _ensure_aria2()
    pip_proc = None
    if not os.path.exists(os.path.join(COMFY_DIR, "main.py")):
        if os.path.exists(COMFY_DIR):
            shutil.rmtree(COMFY_DIR, ignore_errors=True)
        print("[video] 正在克隆最新版 ComfyUI 并后台并发安装依赖...", flush=True)
        subprocess.run(["git", "clone", "--depth", "1", "https://github.com/comfyanonymous/ComfyUI.git", COMFY_DIR], check=True)
        pip_proc = subprocess.Popen(
            ["uv", "pip", "install", "--system", "-q", "-r", f"{COMFY_DIR}/requirements.txt", "av>=14.2.0"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
    else:
        h3_node_py = os.path.join(COMFY_DIR, "comfy_extras", "nodes_minimax_h3.py")
        if not os.path.exists(h3_node_py):
            subprocess.run(["git", "-C", COMFY_DIR, "pull", "--ff-only"], check=False)

    protected = {
        os.path.join(COMFY_DIR, "models", subdir, local_name)
        for _, _, subdir, local_name, _ in VIDEO_MODEL_SPECS
    }
    for old_w in (
        glob.glob(os.path.join(COMFY_DIR, "models", "**", "*Wan2.2*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "*wan2.2*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "wan_2.1_vae*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "umt5_xxl*"), recursive=True)
        + glob.glob(os.path.join(COMFY_DIR, "models", "**", "clip_vision_h*"), recursive=True)
    ):
        if old_w in protected:
            continue
        if os.path.isfile(old_w):
            try:
                os.remove(old_w)
            except Exception:
                pass

    hf_tok = os.environ.get("HF_TOKEN", "").strip()
    active_dls = []
    t_dl_start = time.time()
    for repo, hf_path, subdir, local_name, exp_size in VIDEO_MODEL_SPECS:
        target_dir = os.path.join(COMFY_DIR, "models", subdir)
        os.makedirs(target_dir, exist_ok=True)
        target_file = os.path.join(target_dir, local_name)
        if os.path.exists(target_file) and not os.path.exists(target_file + ".aria2"):
            cur_sz = os.path.getsize(target_file)
            if (exp_size > 0 and cur_sz == exp_size) or (exp_size <= 0 and cur_sz > 10_000_000):
                continue
        url = _build_download_url(repo, hf_path)
        size_hint = f" ({exp_size / (1024**3):.2f} GiB)" if exp_size > 0 else ""
        print(f"[video] 正在并发极速拉取 {local_name}{size_hint} ...", flush=True)
        aria_cmd = [
            "aria2c", "-x", "16", "-s", "10", "-k", "16M", "--file-allocation=none", "--disk-cache=64M",
            "--user-agent=Mozilla/5.0", "--console-log-level=error", "--summary-interval=0",
            "--allow-overwrite=true",
        ]
        if hf_tok and "huggingface.co" in url:
            aria_cmd.append(f"--header=Authorization: Bearer {hf_tok}")
        aria_cmd.extend(["-d", target_dir, "-o", local_name, url])
        dl_p = subprocess.Popen(aria_cmd)
        active_dls.append((local_name, target_file, exp_size, dl_p, time.time()))

    while any(p.poll() is None for _, _, _, p, _ in active_dls):
        time.sleep(6)
        running_names = [
            f"{nm}({int(round(os.path.getsize(tf)/1e6, 0)) if os.path.exists(tf) else 0}MB)"
            for nm, tf, _, p, _ in active_dls if p.poll() is None
        ]
        if running_names:
            print(f"[video] 正在拉取 {', '.join(running_names)} ... ({int(time.time() - t_dl_start)}s)", flush=True)

    for local_name, target_file, exp_size, dl_p, t0 in active_dls:
        rc = dl_p.wait()
        if rc != 0:
            raise RuntimeError(f"视频权重下载失败: {local_name} (code={rc})")
        got_sz = os.path.getsize(target_file) if os.path.exists(target_file) else 0
        if exp_size > 0 and got_sz != exp_size:
            raise RuntimeError(f"视频权重大小校验失败: {local_name} (got={got_sz}, want={exp_size})")
        if got_sz <= 10_000_000:
            raise RuntimeError(f"视频权重文件异常过小: {local_name} (got={got_sz})")
        print(f"[video] 已就绪: {local_name} ({time.time() - t0:.1f}s)", flush=True)

    if pip_proc is not None:
        _, pip_err = pip_proc.communicate()
        if pip_proc.returncode != 0:
            raise RuntimeError(f"ComfyUI 依赖安装失败: {(pip_err or '')[-400:]}")

    def healthy():
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/system_stats", timeout=2).read()
            info = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/object_info/MiniMaxH3ImageToVideo", timeout=2).read().decode())
            return "MiniMaxH3ImageToVideo" in info
        except Exception:
            return False

    try:
        _vout = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits"],
            text=True, timeout=5,
        ).strip().splitlines()
        _vgb = float(_vout[0].strip()) / 1024.0 if _vout else 96.0
    except Exception:
        _vgb = 96.0
    if _vgb < 60.0:
        try:
            _ul_req = urllib.request.Request(
                "http://127.0.0.1:8189/unload",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(_ul_req, timeout=180).read()
        except Exception:
            pass
    _vram_args = ["--highvram"] if _vgb >= 60.0 else ([] if _vgb >= 28.0 else ["--lowvram"])
    _ensure_http_service(
        port, healthy,
        [sys.executable, "main.py", "--listen", "127.0.0.1", "--port", str(port), *_vram_args],
        "/content/comfyui.log", 110, "main.py.*--port " + str(port), cwd=COMFY_DIR
    )
    ensure_tunnel(port)


def build_h3_workflow(
    prompt: str,
    width: int,
    height: int,
    length: int,
    steps: int,
    seed: int,
    start_img_name: str = "",
    last_img_name: str = "",
    filename_prefix: str = "h3_hd",
    quality: str = "std",
    out_width: int = 0,
    out_height: int = 0,
) -> dict:
    s1_w = align_h3_dim(width)
    s1_h = align_h3_dim(height)
    q = (quality or "std").strip().lower()
    if q not in ("std", "hd", "uhd2k"):
        q = "std"
    s2_w = align_h3_dim(out_width) if out_width > 0 else s1_w
    s2_h = align_h3_dim(out_height) if out_height > 0 else s1_h
    length = align_h3_length(length)
    steps = max(1, min(30, int(steps)))

    wf = {
        "6": {"class_type": "UNETLoader", "inputs": {"unet_name": H3_UNET_NAME, "weight_dtype": "default"}},
        "120": {"class_type": "MiniMaxH3SigmaShift", "inputs": {"model": ["6", 0], "shift_video": 9.0, "shift_audio": 4.0}},
        "13": {"class_type": "CLIPLoader", "inputs": {"clip_name": H3_CLIP_NAME, "type": "minimax", "device": "default"}},
        "11": {"class_type": "VAELoader", "inputs": {"vae_name": H3_VIDEO_VAE_NAME}},
        "24": {"class_type": "VAELoader", "inputs": {"vae_name": H3_AUDIO_VAE_NAME}},
        "104": {
            "class_type": "MiniMaxH3ImageToVideo",
            "inputs": {
                "clip": ["13", 0],
                "vae": ["11", 0],
                "prompt": prompt,
                "width": s1_w,
                "height": s1_h,
                "length": length,
            },
        },
        "16": {"class_type": "BasicGuider", "inputs": {"model": ["120", 0], "conditioning": ["104", 0]}},
        "9": {"class_type": "BasicScheduler", "inputs": {"model": ["120", 0], "scheduler": "simple", "steps": steps, "denoise": 1.0}},
        "17": {"class_type": "KSamplerSelect", "inputs": {"sampler_name": "euler"}},
        "15": {"class_type": "RandomNoise", "inputs": {"noise_seed": seed}},
        "14": {
            "class_type": "SamplerCustomAdvanced",
            "inputs": {
                "noise": ["15", 0],
                "guider": ["16", 0],
                "sampler": ["17", 0],
                "sigmas": ["9", 0],
                "latent_image": ["104", 1],
            },
        },
    }
    if start_img_name:
        wf["204"] = {"class_type": "LoadImage", "inputs": {"image": start_img_name}}
        wf["104"]["inputs"]["first_frame"] = ["204", 0]
    if last_img_name:
        wf["205"] = {"class_type": "LoadImage", "inputs": {"image": last_img_name}}
        wf["104"]["inputs"]["last_frame"] = ["205", 0]

    final_video_samples = ["14", 1]
    if q in ("hd", "uhd2k") and (s2_w != s1_w or s2_h != s1_h):
        wf["179"] = {"class_type": "LatentUpscaleModelLoader", "inputs": {"model_name": H3_UPSCALER_NAME}}
        wf["180"] = {
            "class_type": "MiniMaxH3LatentUpscaleWithModel",
            "inputs": {
                "model": ["179", 0],
                "samples": ["14", 1],
                "width": s2_w,
                "height": s2_h,
            },
        }
        if q == "uhd2k":
            wf["105"] = {
                "class_type": "MiniMaxH3ImageToVideo",
                "inputs": {
                    "clip": ["13", 0],
                    "vae": ["11", 0],
                    "prompt": prompt,
                    "width": s2_w,
                    "height": s2_h,
                    "length": length,
                },
            }
            if start_img_name:
                wf["105"]["inputs"]["first_frame"] = ["204", 0]
            if last_img_name:
                wf["105"]["inputs"]["last_frame"] = ["205", 0]
            wf["216"] = {"class_type": "BasicGuider", "inputs": {"model": ["120", 0], "conditioning": ["105", 0]}}
            wf["209"] = {"class_type": "BasicScheduler", "inputs": {"model": ["120", 0], "scheduler": "simple", "steps": 3, "denoise": 0.375}}
            wf["215"] = {"class_type": "RandomNoise", "inputs": {"noise_seed": seed + 1}}
            wf["214"] = {
                "class_type": "SamplerCustomAdvanced",
                "inputs": {
                    "noise": ["215", 0],
                    "guider": ["216", 0],
                    "sampler": ["17", 0],
                    "sigmas": ["209", 0],
                    "latent_image": ["180", 0],
                },
            }
            final_video_samples = ["214", 1]
        else:
            final_video_samples = ["180", 0]

    wf["8"] = {"class_type": "VAEDecode", "inputs": {"samples": final_video_samples, "vae": ["11", 0]}}
    wf["23"] = {"class_type": "VAEDecodeAudio", "inputs": {"samples": ["14", 1], "vae": ["24", 0]}}
    wf["60"] = {"class_type": "CreateVideo", "inputs": {"images": ["8", 0], "audio": ["23", 0], "fps": 24.0}}
    wf["61"] = {
        "class_type": "SaveVideo",
        "inputs": {
            "video": ["60", 0],
            "filename_prefix": filename_prefix,
            "format": "mp4",
        },
    }
    return wf


build_wan22_workflow = build_h3_workflow


def _run_comfy_video_job(wf: dict, out_dir: str, seed: int, width: int, height: int, length: int, steps: int, quality: str = "std", is_warmup: bool = False):
    t0 = time.time()
    req = urllib.request.Request(
        "http://127.0.0.1:8188/prompt",
        data=json.dumps({"prompt": wf}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=25).read().decode())
    prompt_id = resp["prompt_id"]
    q_tag = {"std": "标准832p", "hd": "超清1088p", "uhd2k": "2K精修"}.get(quality, "标准")
    if is_warmup:
        print(f"[video] 正在预热 24fps 声画视频引擎至显存 (prompt_id={prompt_id}) ...", flush=True)
    else:
        print(
            f"[video] 已提交 24fps 声画视频 {q_tag}任务 (prompt_id={prompt_id}, 输出 {width}x{height}, {length}帧/{round(length/24.0, 1)}s @24fps, {steps}步)...",
            flush=True,
        )

    poll_deadline = time.monotonic() + 1800.0
    while True:
        if time.monotonic() > poll_deadline:
            raise RuntimeError(f"ComfyUI 视频生成轮询超时 (>1800s, prompt_id={prompt_id})")
        time.sleep(1.2)
        hist = json.loads(urllib.request.urlopen(f"http://127.0.0.1:8188/history/{prompt_id}", timeout=15).read().decode())
        if prompt_id in hist:
            entry = hist[prompt_id]
            status = entry.get("status", {})
            if status.get("status_str") == "error":
                raise RuntimeError(f"ComfyUI 视频生成报错: {json.dumps(status, ensure_ascii=False)}")
            for node_out in entry.get("outputs", {}).values():
                for val in node_out.values():
                    if isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict) and "filename" in item:
                                fname = item["filename"]
                                sub = item.get("subfolder", "")
                                src_f = os.path.join(COMFY_DIR, "output", sub, fname)
                                if is_warmup:
                                    try:
                                        os.remove(src_f)
                                    except OSError:
                                        pass
                                    print(f"[video] 24fps 声画视频显存预热就绪 ({time.time() - t0:.1f}s)", flush=True)
                                    return None
                                os.makedirs(out_dir, exist_ok=True)
                                out_f = os.path.join(out_dir, f"h3_{int(time.time())}_{seed}.mp4")
                                shutil.copy2(src_f, out_f)
                                dt = time.time() - t0
                                print(
                                    f"[video] 24fps 声画视频 {q_tag}生成完成 ({dt:.1f}s, {width}x{height}, {length}帧) -> {out_f}",
                                    flush=True,
                                )
                                print(f"COLAB_AI_OUTPUT_FILE={out_f}", flush=True)
                                return out_f
            raise RuntimeError(f"ComfyUI 视频任务已完成但未产出任何输出文件 (prompt_id={prompt_id})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("prompt_pos", nargs="*")
    p.add_argument("--prompt", "-p", default="")
    p.add_argument("--prompt-file", "-f", default="")
    p.add_argument("--ref-image", "--image", "-i", action="append", dest="ref_images", default=[])
    p.add_argument("--width", "-W", type=int, default=832)
    p.add_argument("--height", "-H", type=int, default=1248)
    p.add_argument("--length", "-l", type=int, default=192)
    p.add_argument("--steps", "-s", type=int, default=8)
    p.add_argument("--quality", "-q", default="std", help="std | hd | uhd2k")
    p.add_argument("--seed", type=int, default=-1)
    p.add_argument("--out-dir", default="/content/outputs/video")
    p.add_argument("--warmup-only", action="store_true")
    args = p.parse_args()

    ensure_video_models_and_comfy()
    if args.warmup_only:
        wf_warm = build_h3_workflow("a quiet sunlit room", 256, 256, 5, 1, 1, filename_prefix="warmup_h3", quality="std")
        _run_comfy_video_job(wf_warm, "/tmp/warmup_video_out", 1, 256, 256, 5, 1, quality="std", is_warmup=True)
        shutil.rmtree("/tmp/warmup_video_out", ignore_errors=True)
        print("COLAB_AI_WARMUP_OK=video", flush=True)
        return

    prompt = args.prompt or " ".join(args.prompt_pos).strip()
    if args.prompt_file and os.path.exists(args.prompt_file):
        prompt = open(args.prompt_file, "r", encoding="utf-8").read().strip()
    prompt = prompt.lstrip("-# \n\r\t").strip()
    if not prompt:
        raise SystemExit("请提供视频生成提示词")

    length = align_h3_length(args.length)
    steps = max(4, min(30, int(args.steps)))
    seed = args.seed if args.seed >= 0 else int(time.time() * 1000) % (2**31 - 1)

    start_img_name = ""
    last_img_name = ""
    ref_images = [x for x in (args.ref_images or []) if x and os.path.exists(x)]
    first_ref = ref_images[0] if ref_images else ""
    s1_w, s1_h, s2_w, s2_h, q = resolve_h3_stage_dims(args.width, args.height, args.quality, first_ref)

    if ref_images:
        in_dir = os.path.join(COMFY_DIR, "input")
        os.makedirs(in_dir, exist_ok=True)
        start_img_name = f"vref_{int(time.time()*1000)}_{os.path.basename(ref_images[0])}"
        shutil.copy2(ref_images[0], os.path.join(in_dir, start_img_name))
        print(f"[video] 已挂载首帧参考图: {os.path.basename(ref_images[0])} (构图 {s1_w}x{s1_h} -> 输出 {s2_w}x{s2_h})", flush=True)
        if len(ref_images) >= 2:
            last_img_name = f"vlast_{int(time.time()*1000)}_{os.path.basename(ref_images[1])}"
            shutil.copy2(ref_images[1], os.path.join(in_dir, last_img_name))
            print(f"[video] 已挂载尾帧过渡图: {os.path.basename(ref_images[1])}", flush=True)

    wf = build_h3_workflow(prompt, s1_w, s1_h, length, steps, seed, start_img_name, last_img_name, quality=q, out_width=s2_w, out_height=s2_h)
    tot_steps = steps + (3 if q == "uhd2k" else 0)
    _run_comfy_video_job(wf, args.out_dir, seed, s2_w, s2_h, length, tot_steps, quality=q, is_warmup=False)


if __name__ == "__main__":
    main()
