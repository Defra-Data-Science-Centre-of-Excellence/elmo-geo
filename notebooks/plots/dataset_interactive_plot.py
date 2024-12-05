# Databricks notebook source
# MAGIC %md
# MAGIC # Dataset Interactive Plot
# MAGIC Edit the filter "query" to select a small enough amount of data to plot.  
# MAGIC I use https://britishnationalgrid.uk/ to find the tile I want.  
# MAGIC
# MAGIC TODO: add more layers

# COMMAND ----------

from elmo_geo import register
from elmo_geo.datasets import reference_parcels, wfm_parcels
from elmo_geo.io import to_gdf, download_link

register()

# COMMAND ----------

query = "id_parcel LIKE 'SE8_1_%' OR id_parcel LIKE 'SE8_2_%'"
# query = "id_business = 28141"

sdf = reference_parcels.sdf().join(wfm_parcels.sdf(), on="id_parcel", how="outer")
gdf = to_gdf(sdf.filter(query))
gdf

# COMMAND ----------

m = gdf.explore(
    tooltip=False,
    popup=True,
    popup_kwds={
        "style": """
            max-height: 300px;
            overflow-y: scroll;
        """,
    }
)

m

# COMMAND ----------

f_out = "/dbfs/FileStore/elmo-geo-downloads/poc_gdf_explore_map.html"
m.save(f_out)
download_link(f_out)  # 34Mb html file
