from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requires = f.read().splitlines()

setup(
    name="crypto-data-downloader",
    version="0.1.0",
    packages=find_packages(),
    install_requires=requires,
    python_requires=">=3.6",
)
