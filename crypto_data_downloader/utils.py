import gzip
import json
import os
import pickle
import time
from datetime import datetime, timezone
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np

TO_MS = {
    "s": 1e3,
    "m": 60e3,
    "h": 60 * 60e3,
    "d": 24 * 60 * 60e3,
    "w": 7 * 24 * 60 * 60e3,
}


def parse_date(x="2024-01-01", fmt="%Y-%m-%d"):
    dt = datetime.strptime(x, fmt).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1e3)


def format_date(ts: int, fmt="%Y-%m-%d %H:%M:%S"):
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return dt.strftime(fmt)


def timestamp():
    return int(time.time() * 1e3)


def split_intervals(start, end, dt):
    return [(t, min(t + dt, end)) for t in range(start, end, dt)]


def save_json(x, path):
    def default(y):
        if isinstance(y, np.ndarray):
            return json.dumps(y.tolist())
        return str(y)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w+", encoding="utf-8") as f:
        json.dump(x, f, indent=2, ensure_ascii=False, default=default)


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_pkl(x, path, gz=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open_ = gzip.open if gz else open
    with open_(path, "wb+") as f:
        pickle.dump(x, f)


def load_pkl(path, gz=False):
    open_ = gzip.open if gz else open
    with open_(path, "rb") as f:
        return pickle.load(f)


def encode_query(x: Dict):
    return "&".join([f"{k}={v}" for k, v in x.items()])


def plot_crypto_data(x: Dict[str, np.ndarray], id):
    C = min(5, len(x))
    R = min(10, int(len(x) / C))
    plt.figure(figsize=(4 * C, 3 * R))
    plt.suptitle(f"{R*C}/{len(x)} symbols", fontsize=25)
    x = list(x.items())
    for i in range(R * C):
        plt.subplot(R, C, i + 1)
        sym, arr = x[i]
        plt.title(sym)
        time = arr[:, 0].astype("datetime64[ms]")
        plt.plot(time, arr[:, 1])
        plt.xticks(rotation=45)
    plt.tight_layout(rect=[0, 0, 1, 0.98])
    plt.savefig(f"{id}.png")
    plt.close()
