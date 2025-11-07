
## Intro

- Downloads crypto kline (candlestick) data **fast** by making as many concurrent API requests as possible
    - Takes `3 minutes` to download `1 month` of entire market data (`597 symbols`), with `5 minutes` time interval

![](https://raw.githubusercontent.com/SerenaTradingResearch/crypto-data-downloader/main/test/data/crypto_data_2025-07-01_2025-08-01.pkl.png)

## Usage

```bash
pip install crypto-data-downloader
```

```py
import asyncio

from crypto_data_downloader.binance import ALL_COLUMNS, CryptoDataDownloader
from crypto_data_downloader.utils import load_pkl, plot_crypto_data

# Refer to https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data


x = CryptoDataDownloader()
x.weight_lim = 5000  # Binance request weight limit per minute, max: 6000
x.quote = "USDT"  # Quote asset
x.interval = "5m"  # Kline time interval
x.kline_lim = 1000  # Kline number of data points per request
x.columns = ["open_time", "close"]  # Data columns to include
print(f"All data columns: {ALL_COLUMNS}")
asyncio.run(x.download("2025-07-01", "2025-08-01"))  # Time in UTC

path = f"data/{x.name}_2025-07-01_2025-08-01.pkl"
data = load_pkl(path, gz=True)
plot_crypto_data(data, path)
```

- Output

```bash
All data columns: ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'n_trades', 'taker_buy_base_volume', 'taker_buy_quote_volume', 'unused']
weight lim: 5000/6000, USDT symbols: 597, spot: 557, margin: 401
9 intervals * 597 symbols = 5373 requests -> 2.1492 minutes
left: 5373/5373
server time: 1754371957632, my time: 1754371957664, diff: -32 ms, weight used: 21
left: 2884/5373
server time: 1754372028132, my time: 1754372028165, diff: -33 ms, weight used: 1
left: 385/5373
server time: 1754372095736, my time: 1754372095764, diff: -28 ms, weight used: 1
left: 0/5373
```
