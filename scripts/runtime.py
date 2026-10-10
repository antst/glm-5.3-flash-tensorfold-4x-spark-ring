#!/usr/bin/env python3
"""Bind TensorFold and PyTorch to the reviewed library, then enter the CLI."""
import ctypes
import hashlib
import json
import os
from pathlib import Path
import sys


def main():
    library = Path(os.environ["TF_NCCL_LIB"]).resolve(strict=True)
    expected = os.environ["SPARK_RING_NCCL_SHA256"]
    actual = hashlib.sha256(library.read_bytes()).hexdigest()
    if actual != expected:
        raise RuntimeError(f"NCCL library changed: expected {expected}, got {actual}")
    ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)
    import torch
    from tensorfold.cuda import comm

    selected = Path(comm._library()._name).resolve(strict=True)
    if selected != library:
        raise RuntimeError(f"TensorFold selected another NCCL library: {selected}")
    mappings = sorted({line.split(maxsplit=5)[-1].strip()
                       for line in Path("/proc/self/maps").read_text().splitlines()
                       if "/libnccl" in line and len(line.split(maxsplit=5)) == 6})
    if not mappings or any(Path(path).resolve() != library for path in mappings):
        raise RuntimeError(f"Unexpected NCCL memory mappings: {mappings}")
    print("SPARK_RING_LIBRARY " + json.dumps({
        "rank": int(os.environ["SPARK_RING_RANK"]),
        "library": str(library), "sha256": actual, "maps": mappings,
        "torch_version": torch.__version__, "image_digest": os.environ["SPARK_RING_IMAGE_DIGEST"],
        "hcas": os.environ["NCCL_IB_HCA"], "comm": os.environ["TF_GLM_COMM"],
        "hc_exchange": os.environ["TF_GLM_HC_EXCHANGE"]}), flush=True)
    from tensorfold.cli import main as tensorfold_main
    sys.argv[0] = "tensorfold"
    tensorfold_main()


if __name__ == "__main__":
    main()
