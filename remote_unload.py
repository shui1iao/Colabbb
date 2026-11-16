#!/usr/bin/env python3
"""清空 Colab G4 Kernel 显存缓存中的模型，无需重启 VM"""
import gc, json, os, sys

target = "all"
if os.environ.get("COLAB_AI_ARGS_JSON"):
    args = json.loads(os.environ["COLAB_AI_ARGS_JSON"])
    if args:
        target = args[0]

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
        print(f"[unload] 已释放缓存 {removed}，当前显存占用: {alloc:.2f} GiB", flush=True)
except Exception:
    print(f"[unload] 已释放缓存 {removed}", flush=True)
