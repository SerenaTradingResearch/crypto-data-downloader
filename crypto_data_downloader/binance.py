import asyncio
import os
from typing import Dict, List, Union

import numpy as np
from aiohttp import ClientSession

from .utils import (
    TO_MS,
    encode_query,
    load_pkl,
    parse_date,
    save_json,
    save_pkl,
    split_intervals,
    timestamp,
)

WEIGHTS = {
    "/api/v3/time": 1,
    "/api/v3/exchangeInfo": 20,
    "/api/v3/klines": 2,
    # ==========================================
    "/fapi/v1/time": 1,
    "/fapi/v1/exchangeInfo": 1,
    "/fapi/v1/klines": 5,  # https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data#request-weight
}


ALL_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "n_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "unused",
]


class CryptoDataDownloader:
    name = "crypto_data"
    base = "https://api.binance.com"
    INFO_PATH = "/api/v3/exchangeInfo"
    TIME_PATH = "/api/v3/time"
    KLINE_PATH = "/api/v3/klines"
    weight_lim = 5000  # max 6000

    weight_key = "x-mbx-used-weight-1m"

    quote = "USDT"
    interval = "5m"
    kline_lim = 1000
    columns = ["open_time", "close"]

    @property
    def is_spot(s):
        return s.name == "crypto_data"

    async def get(s, url: str):
        # assert urlparse(url).path in WEIGHTS, url
        if not hasattr(s, "ses"):
            s.ses = ClientSession()
        async with s.ses.get(url) as r:
            if s.weight_key in r.headers:
                s.weight_used = int(r.headers[s.weight_key])
            return await r.json()

    async def get_time_n_weight(s):
        url = f"{s.base}{s.TIME_PATH}"
        r = await s.get(url)
        t_server = r["serverTime"]
        t_my = timestamp()
        s.t_diff = t_server - t_my
        print(
            f"server time: {t_server}, my time: {t_my}, diff: {s.t_diff} ms, weight used: {s.weight_used}"
        )

    async def get_info(s):
        url = f"{s.base}{s.INFO_PATH}"
        s.info = await s.get(url)
        save_json(s.info, f"data/{s.name}_info.json")

        r = next(
            x for x in s.info["rateLimits"] if x["rateLimitType"] == "REQUEST_WEIGHT"
        )
        s.weight_lim = min(s.weight_lim, r["limit"])

        symbols = [x for x in s.info["symbols"] if x["quoteAsset"] == s.quote]
        for x in symbols:
            x["permissions"] = sum(x["permissionSets"], start=[]) if s.is_spot else []
        spot = [x for x in symbols if "SPOT" in x["permissions"]]
        margin = [x for x in symbols if "MARGIN" in x["permissions"]]
        print(
            f"weight lim: {s.weight_lim}/{r['limit']}, {s.quote} symbols: {len(symbols)}, spot: {len(spot)}, margin: {len(margin)}"
        )
        s.symbols = symbols

    async def get_kline(s, query: Dict):
        query.update(dict(interval=s.interval, limit=s.kline_lim))
        url = f"{s.base}{s.KLINE_PATH}?{encode_query(query)}"
        r = await s.get(url)
        if len(r) == 0:
            return r
        indices = [ALL_COLUMNS.index(x) for x in s.columns]
        if "msg" in r:
            return r["msg"]
        return np.array(r, float)[:, indices]

    async def get_kline_many(s, queries=[], works=None, callback=lambda: None):
        if works is None:
            works = [dict(query=q, res=None) for q in queries]
        while True:
            left = [x for x in works if x["res"] is None]
            print(f"left: {len(left)}/{len(works)}")
            if len(left) == 0:
                return [x["res"] for x in works]
            await s.get_time_n_weight()
            num = (s.weight_lim - s.weight_used) // WEIGHTS[s.KLINE_PATH]

            async def get_one(x):
                try:
                    x["res"] = await s.get_kline(x["query"])
                except Exception as e:
                    s.errors.append(f"{x['query']} -> {e}")

            s.errors = []
            res = await asyncio.gather(*map(get_one, left[:num]))
            callback()
            if len(s.errors):
                save_json(s.errors, "data/errors.json")
            await asyncio.sleep(60) if len(res) > 10 else None

    async def download(s, start, end):
        data_path = f"data/{s.name}_{start}_{end}.pkl"
        raw_path = f"data/raw_{s.name}_{start}_{end}.pkl"
        await s.get_info()

        start, end = parse_date(start), parse_date(end)
        a, b = int(s.interval[:-1]), s.interval[-1]
        dt = int(s.kline_lim * a * TO_MS[b])
        intervals = split_intervals(start, end, dt)
        n_req = len(intervals) * len(s.symbols)
        weight = WEIGHTS[s.KLINE_PATH]
        n_mins = n_req * weight / s.weight_lim
        print(
            f"{len(intervals)} intervals * {len(s.symbols)} symbols = {n_req} requests -> {n_mins} minutes"
        )

        if os.path.exists(raw_path):
            works = load_pkl(raw_path)
        else:
            works = []
            for x in s.symbols:
                sym = x["symbol"]
                works += [
                    dict(query=dict(symbol=sym, startTime=a, endTime=b), res=None)
                    for a, b in intervals
                ]

        await s.get_kline_many([], works, lambda: save_pkl(works, raw_path))

        data2: Dict[str, Union[np.ndarray, List[np.ndarray]]] = {}
        for x in works:
            sym = x["query"]["symbol"]
            if sym not in data2:
                data2[sym] = []
            if len(x["res"]):
                data2[sym].append(x["res"])
        for sym, arrays in list(data2.items()):
            # print(sym, [f"{format_date(x[0, 0])} {format_date(x[-1, 0])}" for x in arrays])
            if len(arrays) and all([isinstance(x, np.ndarray) for x in arrays]):
                data2[sym] = np.concatenate(arrays)
                # print(sym, data2[sym].shape)
            else:
                del data2[sym]
        save_pkl(data2, data_path, gz=True)

        if hasattr(s, "ses"):
            await s.ses.close()


class FuturesDataDownloader(CryptoDataDownloader):
    name = "futures_data"
    base = "https://fapi.binance.com"
    INFO_PATH = "/fapi/v1/exchangeInfo"
    TIME_PATH = "/fapi/v1/time"
    KLINE_PATH = "/fapi/v1/klines"
    weight_lim = 2000  # max 2400
