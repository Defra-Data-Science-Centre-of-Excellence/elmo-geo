from setuptools import find_packages, setup

__title__ = "elmo-geo"
__description__ = "Environmental Land Management Geospatial Analysis"
__version__ = "0.1"

REQUIRED_PACKAGES = [
    "apache-sedona==1.4.1",
    "beautifulsoup4",
    "contextily",
    "databricks-cli",
    "databricks-sdk",
    "esridump",
    "geopandas",
    "mapclassify",
    "numpy",
    "osdatahub",
    "osmnx",
    "pandas<=1.5.3",
    "pyarrow",
    "pydantic",
    "pyogrio",
    "pyspark==3.3.2",
    "rich",
    "rasterio",
    "rioxarray",
    "seaborn",
    "sentinelsat",
    "shapely",
    "xarray",
]

TEST_PACKAGES = [
    "ruff<=0.1.15",
    "pip-tools",
    "pytest",
]

DEV_PACKAGES = [
    *TEST_PACKAGES,
    "dbx",
    "ipykernel",
    "ipython",
    "ipywidgets",
]


setup(
    name=__title__,
    description=__description__,
    version=__version__,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=REQUIRED_PACKAGES,
    extras_require={"test": TEST_PACKAGES, "dev": DEV_PACKAGES},
)
