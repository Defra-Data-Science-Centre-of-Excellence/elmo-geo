from setuptools import find_packages, setup

__title__ = "elmo-geo"
__description__ = "Environmental Land Management Geospatial Analysis"
__version__ = "0.1"

REQUIRED_PACKAGES = [
    "pandas",
    "seaborn",
    "pydantic",
    "pyarrow",
    "rich",
    "geopandas",
    "pyspark",
    "sedona",
    "rioxarray",
    "sentinelsat",
    "bs4",
]

TEST_PACKAGES = [
    "black",
    "flake8",
    "Flake8-pyproject",
    "isort",
    "pytest",
]

DEV_PACKAGES = [
    *TEST_PACKAGES,
    "pylint",
    "ipython",
    "ipykernel",
    "ipywidgets",
    "dbx",
]


setup(
    name=__title__,
    description=__description__,
    version=__version__,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    package_data={"elmo": ["_conf/**/*.yaml", "_cache/.gitkeep"]},
    install_requires=REQUIRED_PACKAGES,
    extras_require={"test": TEST_PACKAGES, "dev": DEV_PACKAGES},
)