import asyncio
import gzip

from crypto_data_downloader.binance import CryptoDataDownloader
from crypto_data_downloader.utils import load_pkl, plot_crypto_data

if 0:
    dl = CryptoDataDownloader()
    asyncio.run(dl.download("2025-07-01", "2025-08-01"))
else:
    path = "data/crypto_data_2025-07-01_2025-08-01.pkl"
    data = load_pkl(path, gzip.open)
    plot_crypto_data(data, path)
