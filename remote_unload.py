#!/usr/bin/env python3
"""释放 Colab G4 ComfyUI 常驻显存及旧 Kernel 缓存中的模型，无需重启 VM"""
import gc, json, os, sys, urllib.request

target = "all"
if os.environ.get("COLAB_AI_ARGS_JSON"):
    args = json.loads(os.environ["COLAB_AI_ARGS_JSON"])
    if args:
        target = args[0]

comfy_freed = False
if target in ("all", "image", "video", "comfy", "comfyui"):
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8188/free",
            data=json.dumps({"unload_models": True, "free_memory": True}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        comfy_freed = True
    except Exception as exc:
        print(f"[unload] ComfyUI /free 未响应或未运行: {exc}", flush=True)

g = globals()
cache = g.get("_COLAB_AI_CACHE", {})
removed = []
for k in list(cache.keys()):
    if target == "all" or k.startswith(f"{target}:"):
        del cache[k]
        removed.append(k)

gc.collect()
try:
    import torch
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        alloc = torch.cuda.memory_allocated() / (1024**3)
        print(f"[unload] ComfyUI释放={comfy_freed}，Kernel缓存清理={removed}，当前进程显存占用: {alloc:.2f} GiB", flush=True)
except Exception:
    print(f"[unload] ComfyUI释放={comfy_freed}，Kernel缓存清理={removed}", flush=True)
