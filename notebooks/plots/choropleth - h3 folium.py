# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Choropleth Plot - H3 Folium
# MAGIC
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 08/04/2024
# MAGIC
# MAGIC This notebook produces an interactive, html based choropleth map of a spatial dataset aggregated to H3 tiles. This allows granular spatial data to be
# MAGIC mapped at national scale.
# MAGIC
# MAGIC How to use this notebook:
# MAGIC
# MAGIC 1. Using the widgets choose the dataset to plot
# MAGIC 2. Enter the variable name and source into the free text widgets. These are used in the plot.
# MAGIC 3. Run the notebook
# MAGIC
# MAGIC Key processing steps:
# MAGIC 1. The plot variable is aggregated to give a single value per H3 hexagon. It is aggregated by summing values.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install jenkspy

# COMMAND ----------


import json

import folium
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from folium import Map
from geojson.feature import Feature, FeatureCollection
from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets.datasets import datasets
from elmo_geo.io import download_link

register()

# COMMAND ----------

names = sorted([f"{d.source}/{d.name}/{v.name}" for d in datasets for v in d.versions])
default_name = [n for n in names if "shine" in n][0]
dbutils.widgets.dropdown("dataset", default_name, names)
_, name, version = dbutils.widgets.get("dataset").split("/")
dataset = next(d for d in datasets if d.name == name)

[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

dbutils.widgets.text("variable name", "Healthland area")
variable_name = dbutils.widgets.get("variable name")
print(f"Variable name:\t{variable_name}")

resolutions = [5, 6, 15]
dbutils.widgets.dropdown("H3 resolution", str(resolutions[0]), list(map(str, resolutions)))
res = int(dbutils.widgets.get("H3 resolution"))
print(f"\nH3 resolution:\t{res}")

# COMMAND ----------

sdf_geo_data = (
    spark.read.parquet(dataset.versions[0].path_read).withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)")).withColumn("area", F.expr("ST_Area(geometry)"))
)
sdf_geo_data.display()

# COMMAND ----------


def geo_to_h3_udf(col, res=15):
    @F.pandas_udf("binary")
    def udf(s: pd.Series) -> pd.Series:
        return gpd.GeoSeries.from_wkb(s, crs=27700).to_crs(4326).map(lambda p: h3.geo_to_h3(lat=p.centroid.y, lng=p.centroid.x, resolution=res))

    return udf(col)


sdf_h3_data = sdf_geo_data.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).select(
    "*", *[geo_to_h3_udf("geometry", res=r).alias(f"h3_{r}") for r in resolutions]
)
sdf_h3_data.display()

# COMMAND ----------

value_field = "area"
df_agged = (
    sdf_h3_data.groupby(f"h3_{res}").agg(
        F.expr(f"sum(coalesce({value_field}, 0)) as {value_field}"),
    )
).toPandas()

df_agged[f"h3_{res}"] = df_agged[f"h3_{res}"].map(lambda x: x.decode("utf-8"))
df_agged["h3_geo"] = df_agged[f"h3_{res}"].apply(lambda x: {"type": "Polygon", "coordinates": [h3.h3_to_geo_boundary(h=x, geo_json=True)]})

# COMMAND ----------

df_agged.head()

# COMMAND ----------


def base_empty_map(centre=(52.133236898161755, -1.2970901724513781)):
    """Prepares a folium map centred in a central GPS point of Toulouse"""
    m = Map(
        location=centre,
        zoom_start=7,
        tiles="cartodbpositron",
        attr="""© <a href="http://www.openstreetmap.org/copyright">
                      OpenStreetMap</a>contributors ©
                      <a href="http://cartodb.com/attributions#basemaps">
                      CartoDB</a>""",
    )
    return m


def hexagons_dataframe_to_geojson(df_hex, hex_id_field, geometry_field, value_fields, file_output=None):
    """Produce the GeoJSON representation containing all geometries in a dataframe
    based on a column in geojson format (geometry_field)"""

    list_features = []

    for i, row in df_hex.iterrows():
        feature = Feature(geometry=row[geometry_field], id=row[hex_id_field], properties={k: str(np.round(row[k], 2)) for k in value_fields})
        list_features.append(feature)

    feat_collection = FeatureCollection(list_features)

    geojson_result = json.dumps(feat_collection)

    # optionally write to file
    if file_output is not None:
        with open(file_output, "w") as f:
            json.dump(feat_collection, f)

    return geojson_result


def h3_foilium_map(df_agged, join_col, geo_col, variable, label, variable_name, title="", nan_value=None, **kwargs):
    m = base_empty_map()

    geo_data = hexagons_dataframe_to_geojson(df_agged, join_col, geo_col, [variable])
    df_data = df_agged.rename(columns={join_col: "id"})

    df_data[variable] = df_data[variable].replace({nan_value: np.nan})

    cp = folium.Choropleth(
        geo_data=geo_data,  # json
        name="choropleth",
        data=df_data,
        columns=["id", variable],  # columns to work on
        key_on="feature.id",
        bins=5,
        fill_opacity=0.7,
        nan_fill_opacity=0.0,
        line_opacity=0.1,
        line_weight=0.2,
        control=True,
        **kwargs,
        legend_name=label,
    ).add_to(m)

    # Add title
    title_html = f"""<h3 align="center" style="font-size:22px; color: #333333;"><b>{title}</b></h3>"""
    m.get_root().html.add_child(folium.Element(title_html))

    # Add tooltip
    """
    # creating a state indexed version of the dataframe so we can lookup values
    df_data_indexed = df_data.set_index('id')
    
    # looping through the geojson object and adding a new property(unemployment)
    # and assigning a value from our dataframe
    for s in cp.geojson.data['features']:
        s['properties']['profit_count'] = df_data_indexed.loc[s['id'], 'profit_count']
    """
    folium.GeoJsonTooltip(fields=[variable], aliases=[variable_name]).add_to(cp.geojson)

    return m, cp


# COMMAND ----------

variable = "area"
m, cp = h3_foilium_map(
    df_agged, f"h3_{res}", "h3_geo", variable, variable_name, variable_name, variable_name, nan_value=None, use_jenks=True, fill_color="YlOrRd"
)
m

# COMMAND ----------

path = f"/dbfs/tmp/{variable_name.replace(' ', '_') }_map_h3_{res}.html"
m.save(path)
download_link(path)
