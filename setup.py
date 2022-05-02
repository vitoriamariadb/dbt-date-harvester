from setuptools import setup, find_packages

setup(
    name="dbt-date-harvester",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",
        "networkx>=2.8",
    ],
    entry_points={
        "console_scripts": [
            "dbt-parser=dbt_parser.cli:main",
        ],
    },
    python_requires=">=3.8",
)
