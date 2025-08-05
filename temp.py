import asyncio
import signal
import sys
from typing import Dict, List

import numpy as np
from PIL import Image

from crypto_data_downloader.binance import CryptoDataDownloader
from crypto_data_downloader.utils import plot_crypto_data


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

WS_COLUMNS = {
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


class CryptoWatcher(CryptoDataDownloader):
    ws_base = "wss://stream.binance.com:9443"
    chunk_size = 50

    async def watch(s, type, syms=None, num=None):
        assert s.columns[0] == "open_time"
        await s.get_info()
        s.symbols = [x for x in s.symbols if type in x["permissions"]]
        if syms is not None:
            s.symbols = [x for x in s.symbols if x["symbol"] in syms]
        if num is not None:
            s.symbols = s.symbols[:num]

        s.data: Dict[str, np.ndarray] = {}

        async def get_one(sym):
            s.data[sym] = await s.get_kline(dict(symbol=sym))

        await asyncio.gather(*[get_one(x["symbol"]) for x in s.symbols])

        streams = [f"{sym.lower()}@kline_{s.interval}" for sym in s.data]
        map = {v: k for k, v in WS_COLUMNS.items()}

        async def watch_some(streams):
            url = f"{s.ws_base}/stream?streams={'/'.join(streams)}"
            async with s.ses.ws_connect(url) as ws:
                async for msg in ws:
                    r = msg.json()
                    r = r["data"]["k"]
                    sym, t = r["s"], r["t"]
                    arr = s.data[sym]
                    if t != arr[-1, 0]:
                        arr[:] = np.roll(arr, -1, axis=0)
                    for i, col in enumerate(s.columns):
                        arr[-1, i] = float(r[map[col]])
                    await s.on_change(sym, arr)

        tasks = [watch_some(part) for part in chunk(streams, s.chunk_size)]
        await asyncio.gather(*tasks)

    async def on_change(s, sym: str, arr: np.ndarray):
        pass


if __name__ == "__main__":
    x = CryptoWatcher()
    x.kline_lim = 10

    async def main():
        async def plot_task():
            frames: List[Image.Image] = []
            while True:
                await asyncio.sleep(2)
                id = "data/CryptoWatcher"
                plot_crypto_data(x.data, id)
                frames = frames[-149:] + [Image.open(f"{id}.png")]
                save_gif(frames, id, fps=5)

                safe_exit.check()

        await asyncio.gather(x.watch("MARGIN", num=20), plot_task())

    asyncio.run(main())
