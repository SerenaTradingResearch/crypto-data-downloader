from setuptools import setup, find_packages

setup(
    name="crypto-data-downloader",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiohttp",
    ],
    python_requires='>=3.6',
)