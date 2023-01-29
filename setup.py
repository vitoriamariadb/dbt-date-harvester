from setuptools import setup, find_packages

setup(
    name="dbt-date-harvester",
    version="1.0.0",
    description="Ferramenta de parsing e analise de projetos dbt",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",
        "networkx>=2.8",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "pytest-mock>=3.10",
            "mypy>=0.990",
            "flake8>=6.0",
            "black>=22.0",
            "isort>=5.10",
            "pre-commit>=2.20",
        ],
    },
    entry_points={
        "console_scripts": [
            "dbt-parser=dbt_parser.cli:main",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
