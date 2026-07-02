"""
Setup configuration for SQL Server & API Ingestion Framework.
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="jdbc-ingestion-engine",
    version="1.0.0",
    author="Data Engineering Team",
    author_email="data-engineering@yourcompany.com",
    description="Production-ready ingestion engine for JDBC sources (SQL Server and other databases) to Databricks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourcompany/jdbc-ingestion-engine",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "pyyaml>=6.0",
        "requests>=2.31.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "pytest-xdist>=3.5.0",
            "responses>=0.24.1",
            "freezegun>=1.4.0",
            "black>=23.12.1",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
            "isort>=5.13.2",
        ],
        "spark": [
            "pyspark>=3.5.0",
            "delta-spark>=3.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            # Add CLI commands if needed
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yml", "*.yaml", "*.md"],
    },
)
