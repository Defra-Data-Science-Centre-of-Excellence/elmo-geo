"""Functions for processing geospatial data using GeoVector cluster for ELMO"""

import subprocess

requires = [
    # "numpy",
    "rich",
    # "pandas",
    # "shapely",
    # "geopandas",
    # "rasterio",
    # "xarray",
    # "dbruntime",
    # "folium",
    # "matplotlib",
    # "mapclassify",
    # "seaborn",
]

subprocess.run(["pip", "install"] + requires)


from elmo_geo.utils.log import LOG
from elmo_geo.utils.register import register
