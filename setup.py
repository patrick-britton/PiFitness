"""
PiFitness Package Setup
========================

Minimal setup.py for development installation.
This makes the backend package importable in tests.
"""

from setuptools import setup, find_packages

setup(
    name="pifitness",
    version="0.1.0",
    description="PiFitness API Backend",
    author="Patrick Britton",
    author_email="your@email.com",
    packages=find_packages(),
    install_requires=[
        # Core dependencies
        "fastapi>=0.100.0",
        "uvicorn>=0.20.0",
        "psycopg2-binary>=2.9.6",
        "python-dotenv>=1.0.0",
        "sqlalchemy>=2.0.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "requests>=2.31.0",
        # Testing
        "pytest>=7.0.0",
        "httpx>=0.24.0",
        "pytest-asyncio>=0.21.0",
    ],
    python_requires=">=3.11",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.13",
    ],
)