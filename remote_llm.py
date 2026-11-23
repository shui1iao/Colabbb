#!/usr/bin/env python3
"""
Colabbb 远端本地 LLM 提示词服务执行器
- 引擎: 官方 CUDA 预编译 llama-server (-ngl 999 全层 GPU 常驻显存)
- 独立常驻 HTTP 服务 127.0.0.1:8189 -> 反向 SSH 隧道回连宿主机 127.0.0.1:28189
- 支持 <核对> 自检剥离、<提示词> 标签精准提取与多轮原位修改
- 所有模型仓库、权重文件名与隧道凭据均通过环境变量配置，零硬编码凭据
"""
import argparse, base64, fcntl, json, os, socket, subprocess, sys, time, urllib.request
from functools import wraps

if os.environ.get("COLAB_AI_ARGS_JSON"):
    sys.argv = [sys.argv[0]] + json.loads(os.environ["COLAB_AI_ARGS_JSON"])

LLM_TUNNEL_PORT = int(os.environ.get("COLABBB_LLM_TUNNEL_PORT", "28189"))

LLM_SERVER_CODE = r'''#!/usr/bin/env python3
import fcntl, glob, json, os, re, shutil, subprocess, sys, tarfile, threading, time, urllib.error, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8189
BACKEND_PORT = 18189

DEFAULT_LLM_REPO = os.environ.get("COLABBB_LLM_REPO", "bartowski/Qwen_Qwen3-30B-A3B-GGUF").strip()
DEFAULT_LLM_FILE = os.environ.get("COLABBB_LLM_FILE", "Qwen_Qwen3-30B-A3B-Q4_K_M.gguf").strip()
DEFAULT_LLM_LABEL = os.environ.get("COLABBB_LLM_LABEL", "Qwen 27B GGUF (llama-server)").strip()

MODEL_SPECS = {
    "qwen27b": {
        "repo": DEFAULT_LLM_REPO,
        "filename": DEFAULT_LLM_FILE,
        "dir": "/content/models/colabbb_llm_gguf",
        "label": DEFAULT_LLM_LABEL,
    },
}
MODEL_ALIASES = {
    "default": "qwen27b",
    "qwen27b_q8": "qwen27b",
    "qwen30b": "qwen27b",
    "qwen14b": "qwen27b",
    "qwen8b": "qwen27b",
    "gemma12b": "qwen27b",
}

DEFAULT_SYSTEM = (
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
)
DEFAULT_MULTI_TURN_HINT = "（多轮修改要求：请以上一版提示词为底稿，仅在对应元素原本所在的句子位置直接做原位替换修改，未要求改动的部分保持原有细节不变，确保全篇连贯自然、无重复与前后冲突。）"
DEFAULT_CHECK_PREFIX = "<think>\n\n</think>\n\n<提示词>\n"
DEFAULT_STRIP_TERMS = [
    "画面真实无滤镜，", "，画面真实无滤镜", "画面真实无滤镜",
    "不过度磨皮，", "（不过度磨皮）",
]

LLM_LOCK = threading.Lock()
CURRENT_KEY = "qwen27b"
BACKEND_PROC = None
LLAMA_BIN = None

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

def _cleanup_legacy_models(keep_dir: str):
    models_root = "/content/models"
    if not os.path.isdir(models_root):
        return
    for name in os.listdir(models_root):
        full = os.path.join(models_root, name)
        if os.path.abspath(full) != os.path.abspath(keep_dir) and ("Qwen3-30B" in name or name in ("qwen38_gguf", "qwen38_rvn_bf16")):
            shutil.rmtree(full, ignore_errors=True)

def _pick_fast_llm_dir(default_dir: str, filename: str, need_gb: float = 17.5) -> str:
    try:
        shm_dir = "/dev/shm/colabbb_llm_gguf"
        shm_dst = os.path.join(shm_dir, filename)
        if os.path.exists(shm_dst) and os.path.getsize(shm_dst) > 100_000_000 and not os.path.exists(shm_dst + ".aria2"):
            return shm_dir
        disk_dst = os.path.join(default_dir, filename)
        if os.path.exists(disk_dst) and os.path.getsize(disk_dst) > 100_000_000 and not os.path.exists(disk_dst + ".aria2"):
            return default_dir
        mem_avail = 0
        with open("/proc/meminfo", "r") as mfh:
            for ln in mfh:
                if ln.startswith("MemAvailable:"):
                    mem_avail = int(ln.split()[1]) * 1024
                    break
        shm_free = shutil.disk_usage("/dev/shm").free if os.path.isdir("/dev/shm") else 0
        if shm_free >= int((need_gb + 4) * (1024 ** 3)) and mem_avail >= int((need_gb + 18) * (1024 ** 3)):
            os.makedirs(shm_dir, exist_ok=True)
            print(f"[llm_server] 启用 /dev/shm 内存盘极速拉取 LLM 权重 (空闲 {shm_free // (1024**3)}GiB，免云盘竞争)", flush=True)
            return shm_dir
    except Exception:
        pass
    return default_dir

def _download_with_heartbeat(url: str, target_dir: str, filename: str, label: str):
    _ensure_aria2()
    target_dir = _pick_fast_llm_dir(target_dir, filename)
    os.makedirs(target_dir, exist_ok=True)
    dst = os.path.join(target_dir, filename)
    if os.path.exists(dst) and os.path.getsize(dst) > 100_000_000 and not os.path.exists(dst + ".aria2"):
        return dst
    print(f"[llm_server] 正在极速拉取 {label} ({filename} -> {target_dir}) ...", flush=True)
    t0 = time.time()
    hf_tok = os.environ.get("HF_TOKEN", "").strip()
    aria_cmd = [
        "aria2c", "-x", "16", "-s", "12", "-k", "16M", "--file-allocation=none", "--disk-cache=64M",
        "--user-agent=Mozilla/5.0", "--console-log-level=error", "--summary-interval=0",
        "--allow-overwrite=true",
    ]
    if hf_tok and "huggingface.co" in url:
        aria_cmd.append(f"--header=Authorization: Bearer {hf_tok}")
    aria_cmd.extend(["-d", target_dir, "-o", filename, url])
    dl_p = subprocess.Popen(aria_cmd)
    while dl_p.poll() is None:
        time.sleep(6)
        cur_mb = round(os.path.getsize(dst) / 1e6, 0) if os.path.exists(dst) else 0
        print(f"[llm_server] 正在拉取 {filename} ... 已下载 {int(cur_mb)} MB ({int(time.time()-t0)}s)", flush=True)
    if dl_p.returncode != 0:
        raise RuntimeError(f"下载失败: {filename} (code={dl_p.returncode})")
    print(f"[llm_server] 权重已就绪: {filename} ({time.time()-t0:.1f}s)", flush=True)
    return dst

def _ensure_llama_server_bin() -> str:
    global LLAMA_BIN
    if LLAMA_BIN and os.path.isfile(LLAMA_BIN):
        return LLAMA_BIN
    bin_root = "/content/llama_cpp_cuda"
    found = glob.glob(os.path.join(bin_root, "**", "llama-server"), recursive=True)
    if found:
        LLAMA_BIN = found[0]
        return LLAMA_BIN

    _ensure_aria2()
    os.makedirs(bin_root, exist_ok=True)
    packages = [
        ("llama-b12282-bin-ubuntu-cuda-12.8-x64.tar.gz", "https://github.com/ggml-org/llama.cpp/releases/download/b12282/llama-b12282-bin-ubuntu-cuda-12.8-x64.tar.gz"),
        ("cudart-llama-b12282-bin-ubuntu-cuda-12.8-x64.tar.gz", "https://github.com/ggml-org/llama.cpp/releases/download/b12282/cudart-llama-b12282-bin-ubuntu-cuda-12.8-x64.tar.gz"),
    ]
    procs = []
    for tname, turl in packages:
        tpath = os.path.join(bin_root, tname)
        if not os.path.exists(tpath):
            print(f"[llm_server] 正在并发下载 CUDA 推理引擎组件 {tname} ...", flush=True)
            procs.append((tname, subprocess.Popen([
                "aria2c", "-x", "16", "-s", "8", "-k", "8M", "--file-allocation=none", "--disk-cache=32M",
                "--user-agent=Mozilla/5.0", "--console-log-level=error", "--summary-interval=0",
                "--allow-overwrite=true", "-d", bin_root, "-o", tname, turl,
            ])))
    for tname, p in procs:
        if p.wait() != 0:
            raise RuntimeError(f"下载推理组件失败: {tname}")
    for tname, _ in packages:
        with tarfile.open(os.path.join(bin_root, tname), "r:gz") as tf:
            tf.extractall(bin_root)

    found = glob.glob(os.path.join(bin_root, "**", "llama-server"), recursive=True)
    if not found:
        raise RuntimeError("解压后未找到 llama-server 可执行文件")
    LLAMA_BIN = found[0]
    os.chmod(LLAMA_BIN, 0o755)
    print(f"[llm_server] llama-server 已就绪 (b12282 CUDA 12.8): {LLAMA_BIN}", flush=True)
    return LLAMA_BIN

def _backend_healthy() -> bool:
    try:
        r = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{BACKEND_PORT}/health", timeout=2).read().decode())
        return r.get("status") == "ok"
    except Exception:
        return False

def load_model(model_key: str = "qwen27b"):
    global CURRENT_KEY, BACKEND_PROC
    model_key = MODEL_ALIASES.get(model_key, model_key)
    if model_key not in MODEL_SPECS:
        model_key = "qwen27b"
    if _backend_healthy() and CURRENT_KEY == model_key:
        return

    spec = MODEL_SPECS[model_key]
    _cleanup_legacy_models(spec["dir"])
    repo_val = spec["repo"]
    gguf_url = repo_val if repo_val.startswith(("http://", "https://")) else f"https://huggingface.co/{repo_val}/resolve/main/{spec['filename']}"
    res_box = {}
    err_box = []
    def _dl_gguf():
        try: res_box["gguf"] = _download_with_heartbeat(gguf_url, spec["dir"], spec["filename"], spec["label"])
        except Exception as e: err_box.append(e)
    def _dl_bin():
        try: res_box["bin"] = _ensure_llama_server_bin()
        except Exception as e: err_box.append(e)
    t1 = threading.Thread(target=_dl_gguf)
    t2 = threading.Thread(target=_dl_bin)
    t1.start(); t2.start()
    t1.join(); t2.join()
    if err_box:
        raise err_box[0]
    gguf_path = res_box["gguf"]
    server_bin = res_box["bin"]
    lib_dirs = sorted({os.path.dirname(p) for p in glob.glob("/content/llama_cpp_cuda/**/*.so*", recursive=True)})
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = ":".join(lib_dirs + ([env["LD_LIBRARY_PATH"]] if env.get("LD_LIBRARY_PATH") else []))

    subprocess.run(["pkill", "-9", "-f", f"llama-server.*{BACKEND_PORT}"], check=False)
    time.sleep(0.8)

    log_fh = open("/content/llama_backend.log", "a")
    cmd = [
        server_bin,
        "-m", gguf_path,
        "--host", "127.0.0.1",
        "--port", str(BACKEND_PORT),
        "-ngl", "999",
        "-c", "12288",
        "-np", "1",
        "-fa", "on",
        "--no-warmup",
    ]
    print(f"[llm_server] 正在将 {spec['filename']} 全层加载至 GPU 显存...", flush=True)
    t0 = time.time()
    BACKEND_PROC = subprocess.Popen(cmd, env=env, stdout=log_fh, stderr=log_fh, start_new_session=True)
    for _ in range(240):
        time.sleep(1.0)
        if BACKEND_PROC.poll() is not None:
            tail_e = open("/content/llama_backend.log", "r", encoding="utf-8", errors="ignore").read()[-800:]
            if "-fa" in cmd:
                cmd = [x for x in cmd if x not in ("-fa", "on")]
                BACKEND_PROC = subprocess.Popen(cmd, env=env, stdout=log_fh, stderr=log_fh, start_new_session=True)
                continue
            raise RuntimeError(f"llama-server 启动失败:\n{tail_e}")
        if _backend_healthy():
            CURRENT_KEY = model_key
            print(f"[llm_server] {spec['label']} 已全层常驻 GPU 显存 ({time.time()-t0:.1f}s)!", flush=True)
            return
    raise RuntimeError("llama-server 加载超时，请检查 /content/llama_backend.log")

def _build_qwen_chatml(messages: list, check_prefix: str) -> str:
    parts = []
    for m in messages:
        role = m.get("role", "user")
        content = (m.get("content") or "").strip()
        if role == "system":
            parts.append(f"<|im_start|>system\n{content}<|im_end|>\n")
        elif role == "user":
            parts.append(f"<|im_start|>user\n{content}<|im_end|>\n")
        elif role == "assistant":
            parts.append(f"<|im_start|>assistant\n{content}<|im_end|>\n")
    cp = (check_prefix or "").strip()
    if cp:
        suf = "\n" if cp.endswith("<提示词>") else ""
        parts.append(f"<|im_start|>assistant\n{cp}{suf}")
    else:
        parts.append("<|im_start|>assistant\n<think>\n\n</think>\n\n<提示词>\n")
    return "".join(parts)

def _clean_pure_zh_prompt_only(raw_text: str, strip_terms=None) -> str:
    """剥离 <核对>...</核对> 冲突自检草稿块与 <think> 块，精准提取 <提示词>...</提示词> 中的纯净中文提示词。"""
    if "</think>" in raw_text:
        t = raw_text.split("</think>", 1)[1].strip()
    elif "<think>" in raw_text and "<提示词>" not in raw_text:
        return ""
    else:
        t = re.sub(r"<think>.*?</think>\s*", "", raw_text, flags=re.DOTALL).strip()
    t = re.sub(r"<核对>.*?</核对>\s*", "", t, flags=re.DOTALL).strip()
    m = re.search(r"<提示词>\s*(.*?)\s*(?:</提示词>|$)", t, flags=re.DOTALL)
    if m:
        t = m.group(1).strip()
    else:
        t = t.replace("<提示词>", "").replace("</提示词>", "").strip()

    if "### Prompt in Chinese" in t:
        t = t.split("### Prompt in Chinese", 1)[1].strip()
    t = re.sub(r"^#{1,4}\s*.*?\n", "", t).strip()

    stop_markers = (
        "---", "=== ", "###", "## ", "🔍", "✨", "💡", "✅", "🎯", "🎬", "| ", "|---",
        "关键优化", "升级亮点", "使用建议", "推荐使用", "总结：", "一句话", "优化亮点", "为什么", "说明：", "注：", "<核对>"
    )
    out_lines = []
    for line in t.splitlines():
        s_line = line.strip()
        if not s_line:
            continue
        if any(s_line.startswith(mk) for mk in stop_markers):
            if out_lines:
                break
            continue
        if not re.search(r"[\u4e00-\u9fff]", s_line):
            continue
        clean_s = s_line.replace("**", "")
        clean_s = re.sub(r"(?:[，,]\s*超[\u4e00-\u9fa5]{1,4}){4,}.*$", "", clean_s)
        out_lines.append(clean_s)

    res = " ".join(out_lines).strip() if out_lines else t.replace("**", "").strip()
    active_strip_terms = DEFAULT_STRIP_TERMS if strip_terms is None else [x for x in strip_terms if isinstance(x, str) and x]
    for bad_term in active_strip_terms:
        res = res.replace(bad_term, "")
    res = re.sub(r"，+", "，", res)
    res = re.sub(r"；\s*，", "；", res)
    if res and res[-1] in "，；、":
        res = res[:-1] + "。"
    return res.strip()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            spec = MODEL_SPECS.get(CURRENT_KEY or "qwen27b", MODEL_SPECS["qwen27b"])
            ok_loaded = _backend_healthy()
            self._send({
                "ok": True,
                "loaded": ok_loaded,
                "model_key": CURRENT_KEY or "qwen27b",
                "model": f"{spec['repo']}/{spec['filename']}",
                "model_label": spec["label"],
                "multi_model": True,
                "zh_only": True,
                "qwen38": True,
                "qwen38_q8": True,
            })
            return
        self._send({"ok": False}, 404)

    def do_POST(self):
        if self.path == "/generate":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > 4 * 1024 * 1024:
                    raise ValueError("invalid Content-Length")
                data = json.loads(self.rfile.read(length).decode("utf-8", errors="ignore") or "{}")
                if not isinstance(data, dict):
                    raise ValueError("body must be JSON object")
            except (ValueError, json.JSONDecodeError) as exc:
                self._send({"ok": False, "error": f"bad request: {exc}"}, 400)
                return
            raw_msgs = data.get("messages") or []
            prompt = (data.get("prompt") or "").strip()
            raw_sys = data.get("system")
            system = raw_sys.strip() if isinstance(raw_sys, str) and raw_sys.strip() else DEFAULT_SYSTEM
            req_model = (data.get("model") or "qwen27b").strip()
            req_model = MODEL_ALIASES.get(req_model, req_model)
            if req_model not in MODEL_SPECS:
                req_model = "qwen27b"
            max_tokens = max(128, min(8192, int(data.get("max_tokens", 1600))))
            temperature = max(0.0, min(2.0, float(data.get("temperature", 0.6))))
            top_p = max(0.05, min(1.0, float(data.get("top_p", 0.9))))
            top_k = max(1, min(200, int(data.get("top_k", 20))))
            repeat_penalty = max(0.8, min(2.0, float(data.get("repeat_penalty", 1.05))))
            multi_turn_hint = data["multi_turn_hint"] if "multi_turn_hint" in data and isinstance(data["multi_turn_hint"], str) else DEFAULT_MULTI_TURN_HINT
            check_prefix = data["check_prefix"] if "check_prefix" in data and isinstance(data["check_prefix"], str) else DEFAULT_CHECK_PREFIX
            strip_terms = [x for x in data["strip_terms"] if isinstance(x, str) and x] if "strip_terms" in data and isinstance(data["strip_terms"], list) else list(DEFAULT_STRIP_TERMS)

            if raw_msgs:
                cleaned_hist = []
                for m in raw_msgs[-10:]:
                    if m.get("role") == "assistant":
                        c_txt = _clean_pure_zh_prompt_only(m.get("content", ""), strip_terms=strip_terms)
                        cleaned_hist.append({"role": "assistant", "content": f"<提示词>\n{c_txt}\n</提示词>"})
                    elif m.get("role") == "user":
                        cleaned_hist.append({"role": "user", "content": m.get("content", "")})
                if len(cleaned_hist) >= 3 and cleaned_hist[-1]["role"] == "user" and multi_turn_hint.strip():
                    cleaned_hist[-1] = {
                        "role": "user",
                        "content": cleaned_hist[-1]["content"] + "\n" + multi_turn_hint.strip(),
                    }
                messages = [{"role": "system", "content": system}] + cleaned_hist
            else:
                messages = [{"role": "system", "content": system}, {"role": "user", "content": prompt}]

            full_prompt = _build_qwen_chatml(messages, check_prefix)

            with LLM_LOCK:
                load_model(req_model)
                t0 = time.time()
                comp_payload = {
                    "prompt": full_prompt,
                    "n_predict": max_tokens,
                    "temperature": max(temperature, 0.01) if temperature > 0 else 0.0,
                    "top_p": top_p,
                    "top_k": top_k,
                    "repeat_penalty": repeat_penalty,
                    "stop": ["<|im_end|>", "<|endoftext|>", "</提示词>"],
                    "cache_prompt": True,
                }
                req = urllib.request.Request(
                    f"http://127.0.0.1:{BACKEND_PORT}/completion",
                    data=json.dumps(comp_payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                )
                c_resp = json.loads(urllib.request.urlopen(req, timeout=240).read().decode("utf-8"))
                gen_text = (c_resp.get("content") or "").strip()
                cp_strip = (check_prefix or "").strip()
                raw_reply = (cp_strip + ("\n" if cp_strip.endswith("<提示词>") else "") + gen_text) if cp_strip else ("<think>\n\n</think>\n\n<提示词>\n" + gen_text)
                if "<提示词>" in raw_reply and "</提示词>" not in raw_reply:
                    raw_reply = raw_reply + "</提示词>"
                reply = _clean_pure_zh_prompt_only(raw_reply, strip_terms=strip_terms)
                dt = time.time() - t0
                ntok = int(c_resp.get("tokens_predicted") or (c_resp.get("timings") or {}).get("predicted_n") or max(1, len(gen_text) // 2))
                tps = (c_resp.get("timings") or {}).get("predicted_per_second")
                tok_per_s = round(float(tps), 1) if tps else round(ntok / max(dt, 0.01), 1)
                spec = MODEL_SPECS[CURRENT_KEY]

            os.makedirs("/content/outputs/llm", exist_ok=True)
            out_path = f"/content/outputs/llm/llm_{int(time.time())}_{os.urandom(3).hex()}.md"
            with open(out_path, "w", encoding="utf-8") as fh:
                fh.write(reply + "\n")

            self._send({
                "ok": True,
                "reply": reply,
                "model_key": CURRENT_KEY,
                "model_label": spec["label"],
                "tokens": ntok,
                "elapsed": round(dt, 2),
                "tok_per_s": tok_per_s,
                "remote_file": out_path,
            })
            return
        self._send({"ok": False}, 404)

if __name__ == "__main__":
    load_model("qwen27b")
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"[llm_server] Listening on 127.0.0.1:{PORT}", flush=True)
    srv.serve_forever()
'''


def _initialization_guard(fn):
    @wraps(fn)
    def guarded(*args, **kwargs):
        with open("/content/.colab-ai-llm-init.lock", "a+") as fh:
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
def ensure_llm_server_and_tunnel(port=8189):
    def healthy():
        try:
            r = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2).read().decode())
            return bool(r.get("ok") and r.get("loaded"))
        except Exception:
            return False

    def prepare():
        with open("/content/llm_server.py", "w", encoding="utf-8") as fh:
            fh.write(LLM_SERVER_CODE)

    _ensure_http_service(
        port, healthy, [sys.executable, "/content/llm_server.py"],
        "/content/llm_server.log", 600, "/content/llm_server.py", prepare=prepare
    )
    lbl = os.environ.get("COLABBB_LLM_LABEL", "Qwen 27B GGUF (llama-server)").strip()
    print(f"[llm] {lbl} 提示词引擎已就绪！", flush=True)

    tunnel_key_b64 = os.environ["COLABBB_TUNNEL_KEY_B64"].strip()
    tunnel_host = os.environ["COLABBB_TUNNEL_HOST"].strip()
    tunnel_port = os.environ.get("COLABBB_TUNNEL_PORT", "22").strip() or "22"
    tunnel_user = os.environ.get("COLABBB_TUNNEL_USER", "root").strip() or "root"
    if not tunnel_key_b64 or not tunnel_host or tunnel_host.startswith("your-"):
        return

    key_path = "/content/.colabbb_tunnel_key_llm"
    tmp_kp = f"{key_path}.{os.getpid()}"
    with open(tmp_kp, "wb") as fh:
        fh.write(base64.b64decode(tunnel_key_b64))
    os.chmod(tmp_kp, 0o600)
    os.replace(tmp_kp, key_path)
    res = subprocess.run(["pgrep", "-f", f"{LLM_TUNNEL_PORT}:127.0.0.1:{port}"], capture_output=True, text=True, timeout=10)
    if res.returncode != 0:
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
            "-R", f"127.0.0.1:{LLM_TUNNEL_PORT}:127.0.0.1:{port}",
            f"{tunnel_user}@{tunnel_host}",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        time.sleep(1.5)


DEFAULT_LLM_SETTINGS = {
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
    "check_prefix": "<核对>\n1. 构图景别与环境光影：",
    "temperature": 0.6,
    "max_tokens": 1600,
    "top_p": 0.9,
    "top_k": 20,
    "repeat_penalty": 1.05,
    "strip_terms": [
        "画面真实无滤镜，", "，画面真实无滤镜", "画面真实无滤镜",
        "不过度磨皮，", "（不过度磨皮）",
    ],
}


def load_runtime_llm_settings() -> dict:
    cfg = dict(DEFAULT_LLM_SETTINGS)
    cfg["strip_terms"] = list(DEFAULT_LLM_SETTINGS["strip_terms"])
    raw = os.environ.get("COLAB_AI_SETTINGS_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and isinstance(parsed.get("llm"), dict):
                src = parsed["llm"]
                if isinstance(src.get("system_prompt"), str) and src["system_prompt"].strip():
                    cfg["system_prompt"] = src["system_prompt"].strip()
                if isinstance(src.get("multi_turn_hint"), str):
                    cfg["multi_turn_hint"] = src["multi_turn_hint"]
                if isinstance(src.get("check_prefix"), str):
                    cfg["check_prefix"] = src["check_prefix"]
                for fk, lo, hi in (("temperature", 0.0, 2.0), ("top_p", 0.05, 1.0), ("repeat_penalty", 0.8, 2.0)):
                    if fk in src and not isinstance(src[fk], bool) and isinstance(src[fk], (int, float)):
                        cfg[fk] = max(lo, min(hi, float(src[fk])))
                for ik, lo, hi in (("max_tokens", 128, 8192), ("top_k", 1, 200)):
                    if ik in src and not isinstance(src[ik], bool) and isinstance(src[ik], (int, float)):
                        cfg[ik] = max(lo, min(hi, int(src[ik])))
                if isinstance(src.get("strip_terms"), list):
                    cfg["strip_terms"] = [x for x in src["strip_terms"] if isinstance(x, str) and x]
        except Exception:
            pass
    return cfg


def main():
    p = argparse.ArgumentParser()
    p.add_argument("prompt_pos", nargs="*")
    p.add_argument("--prompt", "-p", default="")
    p.add_argument("--prompt-file", "-f", default="")
    p.add_argument("--messages-file", default="")
    p.add_argument("--system", "-s", default="")
    p.add_argument("--system-file", default="")
    p.add_argument("--out-file", default="")
    p.add_argument("--ephemeral-draft", action="store_true")
    p.add_argument("--model", "-m", default="qwen27b")
    p.add_argument("--max-tokens", type=int, default=None)
    p.add_argument("--temperature", type=float, default=None)
    p.add_argument("--warmup-only", action="store_true")
    args = p.parse_args()

    ensure_llm_server_and_tunnel()
    if args.warmup_only:
        print("COLAB_AI_WARMUP_OK=llm", flush=True)
        return

    sys_override = args.system.strip() if args.system else ""
    if args.system_file and os.path.exists(args.system_file):
        sys_override = open(args.system_file, "r", encoding="utf-8").read().strip()

    llm_cfg = load_runtime_llm_settings()
    payload = {
        "model": args.model,
        "system": sys_override if sys_override else llm_cfg["system_prompt"],
        "max_tokens": args.max_tokens if args.max_tokens is not None else llm_cfg["max_tokens"],
        "temperature": args.temperature if args.temperature is not None else llm_cfg["temperature"],
        "top_p": llm_cfg["top_p"],
        "top_k": llm_cfg["top_k"],
        "repeat_penalty": llm_cfg["repeat_penalty"],
        "multi_turn_hint": llm_cfg["multi_turn_hint"],
        "check_prefix": llm_cfg["check_prefix"],
        "strip_terms": llm_cfg["strip_terms"],
    }
    if args.messages_file and os.path.exists(args.messages_file):
        payload["messages"] = json.load(open(args.messages_file, "r", encoding="utf-8"))
    else:
        prompt = args.prompt or " ".join(args.prompt_pos).strip()
        if args.prompt_file and os.path.exists(args.prompt_file):
            prompt = open(args.prompt_file, "r", encoding="utf-8").read().strip()
        payload["prompt"] = prompt

    req = urllib.request.Request(
        "http://127.0.0.1:8189/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=300).read().decode("utf-8"))
    reply = resp["reply"]
    print(f"\n================ 提示词大师回复 [{resp.get('model_label', args.model)}] ================\n{reply}\n================================================ ({resp['tokens']} tokens, {resp['elapsed']}s, {resp['tok_per_s']} tok/s)", flush=True)
    print(f"COLAB_AI_OUTPUT_FILE={resp['remote_file']}", flush=True)


if __name__ == "__main__":
    main()
