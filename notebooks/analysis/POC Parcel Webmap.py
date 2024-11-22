# Databricks notebook source
# MAGIC %sh
# MAGIC pip install geojson jenkspy

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Proof of concept parcel web map
# MAGIC
# MAGIC This notebook explores the feasibility of mapping parcel geometries.
# MAGIC
# MAGIC The options explored should be sufficiently performant to potentially work on a SCE machine so that the mapping can be integrated with elmo.
# MAGIC
# MAGIC Currently this is achieved by defaulting to not displaying all parcels geometries and toggling with 50km area of the country to show geometries for.
# MAGIC
# MAGIC ## Summarising things I have checks/explored
# MAGIC - Parcel simplification: tollerance=100 is too high, parcels becomes messy and overlapping. tollerance=50 is roughly the upper limit.
# MAGIC - Trying to display all parcels in a folium map resulted in this error in Databricks 'Command result size exceeds limit: Exceeded 20971520 bytes (current = 20977394)'. Almost certain SCE machines also wouldn't be able to handle this.
# MAGIC - Means that we need a way to displays sections of the country at a time as people scroll/move around the map.
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC It will be challenging to flexibly visualise parcel level data for the whole country. To view parcels we would likely need to restrict the map to a certain area of `<`100km2
# MAGIC
# MAGIC ## Useful links for mapping with folium and leaflet
# MAGIC
# MAGIC - how to add custom functionality (eg load different layer on zoom): https://stackoverflow.com/questions/58774249/python-how-to-extend-folium-functionality-such-as-measuring-distance-by-using
# MAGIC - how to change layer on zoom https://gis.stackexchange.com/questions/182628/leaflet-layers-on-different-zoom-levels-how
# MAGIC - smoothing layer geometries: https://python-visualization.github.io/folium/latest/user_guide/geojson/smoothing.html

# COMMAND ----------

import os, shutil
import re
from datetime import datetime as dt
import pyspark.sql.functions as F
import pandas as pd
import time
import geojson
import json
import folium
import numpy as np
from functools import partial
import geopandas as gpd

from folium import Map
from geojson.feature import Feature, FeatureCollection
from folium.plugins import FeatureGroupSubGroup
from branca.element import MacroElement, Element

from elmo_geo.utils.register import register
from elmo_geo.io.convert import to_gdf
from elmo_geo.st.udf import st_clean
from elmo_geo.io.download import download_link

from elmo_geo.utils.types import SparkDataFrame

from elmo_geo.datasets import (
    reference_parcels,
    os_bng_parcels,
)

register(dir="elmo-geo2")

# COMMAND ----------

def base_empty_map(centre=None, zoom_start=None):
    """Prepares a folium map centered in a central GPS point of Toulouse"""
    if centre is None:
        centre = (52.133236898161755, -1.2970901724513781)
    if zoom_start is None:
        zoom_start=7
    m = Map(
        location=centre,
        zoom_start=zoom_start,
        tiles="cartodbpositron",
        attr="""© <a href="http://www.openstreetmap.org/copyright">
                      OpenStreetMap</a>contributors ©
                      <a href="http://cartodb.com/attributions#basemaps">
                      CartoDB</a>""",
    )
    return m

def foilium_map(geo_data, df_data, variable, variable_name, title="", nan_value=None, centre=None, zoom_start=None, **kwargs):
    m = base_empty_map(centre, zoom_start)

    df_data[variable] = df_data[variable].replace({nan_value: np.nan})

    cp = folium.Choropleth(
        geo_data=geo_data,  # json
        name="choropleth",
        data=df_data,
        columns=["id_parcel", variable],  # columns to work on
        key_on="feature.id",
        bins=5,
        fill_opacity=0.7,
        nan_fill_opacity=0.0,
        line_opacity=0.1,
        line_weight=0.2,
        control=True,
        **kwargs,
        legend_name=variable_name,
    ).add_to(m)

    # Add title
    title_html = f"""<h3 align="center" style="font-size:22px; color: #333333;"><b>{title}</b></h3>"""
    m.get_root().html.add_child(folium.Element(title_html))

    # Add tooltip
    """
    # creating a state indexed version of the dataframe so we can lookup values
    df_data_indexed = df_data.set_index('id')
    
    # looping thru the geojson object and adding a new property(unemployment)
    # and assigning a value from our dataframe
    for s in cp.geojson.data['features']:
        s['properties']['profit_count'] = df_data_indexed.loc[s['id'], 'profit_count']
    """
    folium.GeoJsonTooltip(fields=[variable], aliases=[variable_name]).add_to(cp.geojson)

    return m, cp

# COMMAND ----------

from_wkb = partial(gpd.GeoSeries.from_wkb, crs=27700)

def st_to_json(sdf: SparkDataFrame, key: str = "tile_name", col: str = "geometry") -> SparkDataFrame:
    """Convert a geographic spark dataframe to geojson, grouped by keys.
    """
    def _fn(pdf):
        gdf = gpd.GeoDataFrame(pdf, geometry=from_wkb(pdf[col]), crs="epsg:27700").to_crs("epsg:4326")
        return pd.DataFrame({key:[gdf[key].values[0]], "geojson":[json.dumps(gdf.to_json())]})

    _sdf = sdf.withColumn(col, F.expr(f"ST_AsBinary({col})"))
    return _sdf.groupby(key).applyInPandas(_fn, f"{key} String, geojson String")


# create geo-data
def _transform_to_simplified_json(reference_parcels, tollerance=50, os_grid = "10km_grid"):
    return (reference_parcels.sdf()
       .transform(st_clean, tollerance=tollerance)
       .join(os_bng_parcels.sdf().filter(f"layer='{os_grid}'"), on="id_parcel", how="left")
       .orderBy("proportion", ascending=False)
       .groupby("id_parcel").agg(
           F.expr("FIRST(geometry) as geometry"),
           F.expr("RAND() as profit"),
           F.expr("FIRST(tile_name) as tile_name"),
       )
       .transform(st_to_json, key="tile_name")
       .toPandas()
       )

pdf_json = _transform_to_simplified_json(reference_parcels)
geo_data = json.loads(pdf_json.set_index("tile_name").iloc[0]["geojson"])

# COMMAND ----------

m = base_empty_map(None, None)

# Add a main feature group for the base layers
main_feature_group = folium.FeatureGroup(name="Main Layers", show=False).add_to(m)

n=50
for tile_name, geojson in pdf_json.head(n).values:
    layer_group = FeatureGroupSubGroup(main_feature_group, tile_name)
    folium.GeoJson(
        json.loads(geojson),
        name=tile_name,
        style_function=lambda feature: {
            "fillColor": "#ffff00",
            "color": "red",
            "weight": 2,
            "dashArray": "5, 5",
        },
        show=True
    ).add_to(layer_group)

    m.add_child(layer_group)

# Add a layer control
folium.LayerControl(collapsed=False).add_to(m)

# Unused method for adding custom javascript to the map
# # JavaScript to toggle layers based on zoom level
# js_code = """
# <script>
#     map.on('zoomend', function () {
#         var zoom_level = map.getZoom();
#         if (zoom_level < 8) {
#             if (map.hasLayer(layer1_group)) {
#                 map.removeLayer(layer1_group);
#             }
#             if (!map.hasLayer(layer2_group)) {
#                 map.addLayer(layer2_group);
#             }
#         } else {
#             if (map.hasLayer(layer2_group)) {
#                 map.removeLayer(layer2_group);
#             }
#             if (!map.hasLayer(layer1_group)) {
#                 map.addLayer(layer1_group);
#             }
#         }
#     });
# </script>
# """

# # Create a MacroElement to inject the JavaScript
# class CustomJavaScript(MacroElement):
#     def __init__(self, code):
#         super().__init__()
#         self._code = code

#     def render(self, **kwargs):
#         return self._code

# Inject the JavaScript into the map
#m.get_root().html.add_child(CustomJavaScript(js_code))

# Save the map
f_out = "/dbfs/FileStore/elmo-geo-downloads/poc_parcel_map.html" 
m.save(f_out)
download_link(f_out) # 34Mb htlm file

# COMMAND ----------

df_eligibility = (reference_parcels.sdf()
                  .selectExpr("id_parcel", "RAND() as eligible_proportion")
                  .toPandas())

# Takes very long time - don't think this si the right way to do a choropleth for parcels. Need to pre calculate the colours.
# variable = "eligible_proportion"
# variable_name = "Eligible Proportion"
# m, cp = foilium_map(
#     geo_data,
#     pd.DataFrame(df_eligibility[["id_parcel", variable]]),
#     variable,
#     variable_name,
#     nan_value=None,
#     use_jenks=True,
#     centre=None,
#     zoom_start = 14,
#     fill_color="YlOrRd"
# )
# m

# COMMAND ----------

# Fails to maps all parcels in a single layer

# sdf = (reference_parcels.sdf()
#        .transform(st_clean, tollerance=50))

# gdf = to_gdf(sdf)

# all_parcel_json = gdf.to_json()

# m = base_empty_map(None, None)
# folium.GeoJson(
#     all_parcel_json,
#     style_function=lambda feature: {
#         "fillColor": "#ffff00",
#         "color": "red",
#         "weight": 2,
#         "dashArray": "5, 5",
#     },
# ).add_to(m)
# m
