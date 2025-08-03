import asyncio
import json
import os
import time
from datetime import datetime, timezone

from aiohttp import ClientSession


def parse_date(x="2024-01-01", fmt="%Y-%m-%d"):
    dt = datetime.strptime(x, fmt).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1e3)


def timestamp():
    return int(time.time() * 1e3)


def split_intervals(start, end, dt):
    return [(t, min(t + dt, end)) for t in range(start, end, dt)]


def save_json(x, path):
    with open(path, "w+") as f:
        json.dump(x, f, indent=2)


def load_json(path):
    with open(path) as f:
        return json.load(f)


WEIGHTS = {
    "/api/v3/time": 1,
    "/api/v3/exchangeInfo": 20,
    "/api/v3/klines": 2,
}

TO_MS = {"m": 60e3}


class CryptoDataDownloader:
    base = "https://api.binance.com"
    info_path = "info.json"
    weight_lim = 3000
    weight_key = "x-mbx-used-weight-1m"
    kline_lim = 1000
    quote = "USDT"
    interval = "5m"

    async def get(s, url: str):
        assert url.replace(s.base, "") in WEIGHTS, url
        if not hasattr(s, "ses"):
            s.ses = ClientSession()
        async with s.ses.get(url) as r:
            if s.weight_key in r.headers:
                s.weight_used = int(r.headers[s.weight_key])
            return await r.json()

    async def get_time_n_weight(s):
        url = f"{s.base}/api/v3/time"
        r = await s.get(url)
        t_server = r["serverTime"]
        t_my = timestamp()
        s.t_diff = t_server - t_my
        print(
            f"server time: {t_server}, my time: {t_my}, diff: {s.t_diff} ms, weight used: {s.weight_used}"
        )

    async def get_info(s):
        if os.path.exists(s.info_path):
            s.info = load_json(s.info_path)
        else:
            url = f"{s.base}/api/v3/exchangeInfo"
            s.info = await s.get(url)
            save_json(s.info, s.info_path)

        r = next(
            x for x in s.info["rateLimits"] if x["rateLimitType"] == "REQUEST_WEIGHT"
        )
        s.weight_lim = min(s.weight_lim, r["limit"])

        symbols = [x for x in s.info["symbols"] if x["quoteAsset"] == s.quote]
        for x in symbols:
            x["permissions"] = sum(x["permissionSets"], start=[])
        spot = [x for x in symbols if "SPOT" in x["permissions"]]
        margin = [x for x in symbols if "MARGIN" in x["permissions"]]
        print(
            f"weight lim: {s.weight_lim}/{r['limit']}, {s.quote} symbols: {len(symbols)}, spot: {len(spot)}, margin: {len(margin)}"
        )
        s.symbols = symbols

    async def download(s, start, end):
        await s.get_time_n_weight()
        await s.get_info()

        start, end = parse_date(start), parse_date(end)
        a, b = int(s.interval[:-1]), s.interval[-1]
        dt = int(s.kline_lim * a * TO_MS[b])
        intervals = split_intervals(start, end, dt)
        n_req = len(intervals) * len(s.symbols)
        tot_weight = n_req * WEIGHTS["/api/v3/klines"]
        n_mins = tot_weight / s.weight_lim
        print(
            f"{len(intervals)} intervals * {len(s.symbols)} symbols = {n_req} requests -> {n_mins} minutes"
        )

        if hasattr(s, "ses"):
            await s.ses.close()


if __name__ == "__main__":
    dl = CryptoDataDownloader()
    asyncio.run(dl.download("2024-01-01", "2025-01-01"))
