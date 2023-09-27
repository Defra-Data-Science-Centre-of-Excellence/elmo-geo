# Databricks notebook source
# MAGIC %md
# MAGIC # Urban
# MAGIC Assign urbanity to each parcel.
# MAGIC
# MAGIC ### OS, ZoomStack Urban Areas
# MAGIC OS data of Region and National geometries that define Urban.
# MAGIC
# MAGIC ### Defra, 2011 Rural Urban Classification lookup tables for all geographies
# MAGIC [Rural Urban Classification][#ruc] by [Wards][#wards] are grouped to form larger geometries.
# MAGIC
# MAGIC
# MAGIC [#ruc_guide]:  https://www.gov.uk/government/statistics/practical-guides-for-using-the-2011-rural-urban-classification
# MAGIC [#ruc]:  https://www.gov.uk/government/statistics/2011-rural-urban-classification-lookup-tables-for-all-geographies
# MAGIC [#wards]:  https://geoportal.statistics.gov.uk/datasets/ons::wards-december-2011-full-clipped-boundaries-ew/explore

# COMMAND ----------

# MAGIC
# MAGIC %pip install -q xyzservices osdatahub

# COMMAND ----------

import geopandas as gpd
import osdatahub

key = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"

# COMMAND ----------

from datetime import datetime

from xyzservices import TileProvider

# MapAPI
AvailableLayers = [
    "Road_3857",
    "Road_27700",
    "Outdoor_3857",
    "Outdoor_27700",
    "Light_3857",
    "Light_27700",
    "Leisure_27700",
]


class OSTileProvider(TileProvider):
    """
    Main class for using OS MapsAPI
    (https://osdatahub.os.uk/docs/wmts/)
    Args:
        key (str): A valid OS MapsAPI key.
        layer (str): A valid Layer Name in the format <Style>_<projection>, Default "Light_3857", Options `osdatahub.MapsAPI.AvailableLayers`.
    Returns:
        OSTileProvider (TileProvider)
    Examples
    --------
    AvailableLayers
    >>> from osdatahub.MapsAPI import AvailableLayers
    >>> print(AvailableLayers)
    OSTileProvider
    >>> from osdatahub.MapsAPI import OSTileProvider
    >>> provider = OSTileProvider(key, 'Light_3857')
    Contextily
    >>> ctx.add_basemap(ax=ax, provider=provider)
    Folium
    >>> m = folium.Map(tile=provider)
    """

    def __init__(self, key: str, layer: str = "Light_3857", **kwargs):
        assert (
            layer in AvailableLayers
        ), f'{layer} not in AvailableLayers: {", ".join(AvailableLayers)}'
        if layer.endswith("_27700"):
            warn(f"{layer}, CRS=EPSG:27700 is not recognised by contextily or folium.")
        super().__init__(
            {
                "name": f"OS Maps {layer}",
                "url": f"https://api.os.uk/maps/raster/v1/zxy/{layer}/{{z}}/{{x}}/{{y}}.png?key={key}",
                "max_zoom": 16,
                "attribution": f"Contains OS data © Crown copyright and database right {datetime.now().year}",
            },
            **kwargs,
        )


provider = OSTileProvider(key, "Light_3857")

# COMMAND ----------

key = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"

product = "Zoomstack_UrbanAreas"
crs = "EPSG:27700"
f = f"/dbfs/mnt/lab/unrestricted/elm_data/os/{product}.parquet"

extent = osdatahub.Extent.from_bbox([0, 0, 0.7e6, 1.3e6], crs)
df = gpd.GeoDataFrame.from_features(
    osdatahub.FeaturesAPI(key, product, extent).query(100_000)
).set_crs(crs)
df.to_parquet(f)

df

# COMMAND ----------

df = gpd.read_parquet(f)
df.assign(geometry=df.geometry.simplify(10)).explore()

# COMMAND ----------

# MAGIC %pip install -q git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

from cdap_geo.sedona import st_join, st_load, st_register
from pyspark.sql import functions as F
from pyspark.sql import types as T

st_register()

# COMMAND ----------

sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/"
sf_urban = "dbfs:/mnt/lab/unrestricted/elm_data/os/Zoomstack_UrbanAreas.parquet"

sf_geom_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/urban_geometry.parquet/"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/urban.parquet/"

# COMMAND ----------

sdf_parcel = spark.read.parquet(sf_parcel).repartition(2000)

sdf_urban = (
    spark.read.parquet(sf_urban)
    .select(
        F.col("Type").alias("urban_type"),
        st_load("geometry").alias("geometry"),
    )
    .repartition(100)
)

# COMMAND ----------

aggr_geometry_urban_of = (
    lambda urban_type: f"""
  ST_Union_Aggr(CASE
    WHEN (urban_type=="{urban_type}")
    THEN geometry_urban
    ELSE ST_GeomFromText("Point EMPTY")
  END) AS geometry_urban_{urban_type.lower()}
"""
)


sdf_geom = (
    st_join(sdf_parcel, sdf_urban, lsuffix="_parcel", rsuffix="_urban")
    .groupBy("id_parcel")
    .agg(
        F.expr("ST_Union_Aggr(geometry_urban) AS geometry_urban"),
        F.expr(aggr_geometry_urban_of("Regional")),
        F.expr(aggr_geometry_urban_of("National")),
    )
    .join(sdf_parcel.withColumnRenamed("geometry", "geometry_parcel"), on="id_parcel", how="right")
)

sdf_geom.write.parquet(sf_geom_out, mode="overwrite")
display(sdf_geom)

# COMMAND ----------

df = spark.read.parquet(sf_geom_out).select(
    "id_business",
    "id_parcel",
    F.expr("ST_Area(geometry_parcel) AS sqm_parcel"),
    F.expr("ST_Area(ST_Intersection(geometry_parcel, geometry_urban)) AS sqm_urban"),
    F.expr(
        "ST_Area(ST_Intersection(geometry_parcel, geometry_urban_regional)) AS sqm_urban_regional"
    ),
    F.expr(
        "ST_Area(ST_Intersection(geometry_parcel, geometry_urban_national)) AS sqm_urban_national"
    ),
)

df.write.parquet(sf_out)
display(df)

# COMMAND ----------

import numpy as np
import seaborn as sns

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

ax = sns.kdeplot(
    (pdf["sqm_urban"] / pdf["sqm_parcel"]),
    clip=[0, 1],
    color="blue",
    shade=True,
)
ax.set(
    title="Rural-Urban proportion",
    xlabel="ρ~Urbananity",
    ylabel="ρ~Parcels",
)
ax.grid("on")

# COMMAND ----------

import pandas as pd

f = "/dbfs/mnt/lab/unrestricted/elm/buffer_strips/urban.parquet/"
pdf = pd.read_parquet(f)

(pdf["sqm_urban"] / pdf["sqm_parcel"]).hist()
