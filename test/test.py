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
