from setuptools import find_packages, setup

__title__ = "elmo-geo"
__description__ = "Environmental Land Management Geospatial Analysis"
__version__ = "0.1"

REQUIRED_PACKAGES = [
    "pandas>=1.5.3",
    "seaborn>=0.12.2",
    "pydantic>=1.10.5",
    "pyarrow>=10.0.1",
    "rich>=12.5.1",
    "geopandas>=0.12.2",
    "pyspark>=3.3.2",
    "sedona>=1.0.4",
    "rioxarray>=0.13.3",
    "sentinelsat>=1.1.1",
    "bs4",
    "pyogrio",
    "contextily",
    "databricks-sdk",
    "esridump",
    "osdatahub",
    "osmnx",
]

TEST_PACKAGES = [
    "ruff<=0.1.15",
    "pytest>=7.2.1",
]

DEV_PACKAGES = [
    *TEST_PACKAGES,
    "ipython>=8.10.0",
    "ipykernel>=6.21.2",
    "ipywidgets>=8.0.4",
    "dbx>=0.7.0",
]


setup(
    name=__title__,
    description=__description__,
    version=__version__,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=REQUIRED_PACKAGES,
    extras_require={"test": TEST_PACKAGES, "dev": DEV_PACKAGES},
)
