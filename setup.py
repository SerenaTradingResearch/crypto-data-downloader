from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="crypto-data-downloader",
    version="0.1.6",
    author="Ricky Ding",
    author_email="e0134117@u.nus.edu",
    description="High-speed cryptocurrency OHLCV data downloader via concurrent API requests",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SerenaTradingResearch/crypto-data-downloader",
    packages=find_packages(),
    install_requires=open("requirements.txt").read().splitlines(),
    python_requires=">=3.8",
    license="MIT",
    keywords="crypto,binance,ohlcv,market-data,downloader",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
    ],
)
