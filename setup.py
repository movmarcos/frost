"""frost — Declarative Snowflake DDL manager with automatic dependency resolution."""

from setuptools import setup, find_packages
from frost import __version__

setup(
    name="frost",
    version=__version__,
    description="Declarative Snowflake DDL manager with automatic dependency resolution",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "snowflake-connector-python[secure-local-storage]>=3.6",
        "cryptography>=41.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "frost=frost.cli:main",
        ],
    },
)
