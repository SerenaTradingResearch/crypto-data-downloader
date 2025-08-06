import asyncio
import signal
import sys
from functools import wraps
from typing import Dict, List

import numpy as np
from PIL import Image

from crypto_data_downloader.binance import CryptoDataDownloader
from crypto_data_downloader.utils import format_date, plot_crypto_data, timestamp


def retry(sleep=60):
    def decorator(func):
        @wraps(func)
        async def func2(*args, **kwargs):
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    print(f"Retrying after {e}")
                    await asyncio.sleep(sleep)

        return func2

    return decorator


async def gather_n_cancel(*tasks):
    tasks = [asyncio.create_task(x) for x in tasks]
    try:
        return await asyncio.gather(*tasks)
    except Exception as e:
        [x.cancel() for x in tasks]
        raise e


def chunk(lst, n):
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def save_gif(frames: List[Image.Image], id, fps=5):
    frames[0].save(
        f"{id}.gif",
        format="GIF",
        append_images=frames[1:],
        save_all=True,
        duration=int(1000 / fps),
        loop=0,
        optimize=True,
    )


class SafeExit:
    signal = 0

    def __init__(s):
        def handler(sig, frame):
            s.signal = 1

        for SIG in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(SIG, handler)

    def check(s):
        if s.signal:
            sys.exit(0)


safe_exit = SafeExit()

MAP1 = {
    "t": "open_time",
    "T": "close_time",
    "s": "symbol",
    "o": "open",
    "c": "close",
    "h": "high",
    "l": "low",
    "v": "volume",
    "n": "n_trades",
    "x": "is_closed",
    "q": "quote_volume",
    "V": "taker_buy_base_volume",
    "Q": "taker_buy_quote_volume",
    "B": "unused",
}
MAP2 = {v: k for k, v in MAP1.items()}


class CryptoWatcher(CryptoDataDownloader):
    ws_base = "wss://stream.binance.com:9443"
    chunk_size = 50

    market = "MARGIN"
    max_num = 1000

    async def get_info_filtered(s):
        await s.get_info()

        def ok(x):
            return s.market in x["permissions"] and x["status"] == "TRADING"

        s.symbols = [x for x in s.symbols if ok(x)][: s.max_num]

    @retry(sleep=60)
    async def watch(s):
        assert s.columns[0] == "open_time"
        s.data: Dict[str, np.ndarray] = {}
        s.update_time: Dict[str, int] = {}

        async def get_one(sym: str):
            s.data[sym] = await s.get_kline(dict(symbol=sym))

        async def watch_some(syms: List[str]):
            streams = [f"{sym.lower()}@kline_{s.interval}" for sym in syms]
            url = f"{s.ws_base}/stream?streams={'/'.join(streams)}"
            async with s.ses.ws_connect(url) as ws:
                async for msg in ws:
                    r = msg.json()
                    e_time = r["data"]["E"]
                    k = r["data"]["k"]
                    sym, t = k["s"], k["t"]
                    s.update_time[sym] = e_time
                    arr = s.data[sym]
                    if t != arr[-1, 0]:
                        arr[:] = np.roll(arr, -1, axis=0)
                    for i, col in enumerate(s.columns):
                        arr[-1, i] = float(k[MAP2[col]])
                    await s.on_change(sym, arr, e_time)

        await s.get_info_filtered()
        await gather_n_cancel(*[get_one(x["symbol"]) for x in s.symbols])
        tasks = [watch_some(syms) for syms in chunk(list(s.data), s.chunk_size)]
        await gather_n_cancel(*tasks)

    async def on_change(s, sym: str, arr: np.ndarray, e_time: int):
        pass


if __name__ == "__main__":

    async def main():
        async def on_change(sym, arr, e_time):
            if sym == "BTCUSDT":
                my_time = timestamp()
                print(f"{format_date(my_time)} BTC {format_date(e_time)} {arr[-1, 1]}")

        x = CryptoWatcher()
        x.kline_lim = 10
        x.market = ["SPOT", "MARGIN"][1]
        x.max_num = 20
        x.on_change = on_change

        async def plot_task():
            frames: List[Image.Image] = []
            while True:
                await asyncio.sleep(2)
                if len(x.data):
                    id = "data/CryptoWatcher"
                    plot_crypto_data(x.data, id)
                    frames = frames[-149:] + [Image.open(f"{id}.png")]
                    # save_gif(frames, id, fps=5)
                safe_exit.check()

        await asyncio.gather(x.watch(), plot_task())

    asyncio.run(main())
