# Databricks notebook source
# MAGIC %md
# MAGIC # Boundary Use
# MAGIC How much available boundary is there?
# MAGIC How much hedgerow is lost by EVAST woodland uptake?
# MAGIC How many ditches are in peat parcels?
# MAGIC How many ditches on priority habitat?
# MAGIC
# MAGIC What's the flowrate of ditches?
# MAGIC How many ethermeral ponds?

# COMMAND ----------

# MAGIC %pip install -q contextily

# COMMAND ----------


from time import perf_counter

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shapely
from pyspark.sql import functions as F

from elmo_geo import register

register()
register()

# COMMAND ----------

cols = [
    "geometry_boundary",
    "geometry_adj",
    "geometry_water",
    "geometry_ditch",
    "geometry_wall",
    "geometry_hedge",
]

sf_neighbour = "dbfs:/mnt/lab/unrestricted/elm/elm_se/neighbouring_land_use_geometries.parquet/"
df = (
    spark.read.parquet(sf_neighbour)
    .filter('id_parcel REGEXP "NY9(2|3)7(0|1)[0-9]{4}"')
    .select(
        "id_parcel",
        *[F.expr(f"ST_AsBinary({col}) AS {col}") for col in cols],
    )
    .toPandas()
)

gdf = df.assign(**{col: gpd.GeoSeries.from_wkb(df[col]).set_crs(27700) for col in cols}).pipe(gpd.GeoDataFrame, geometry="geometry_boundary", crs=27700)

C = ["darkgoldenrod", "goldenrod", "royalblue", "slateblue", "forestgreen", "sienna"]


gdf.to_parquet("/dbfs/tmp/boundaries.parquet")
gdf

# COMMAND ----------


def to_polygon(x):
    if isinstance(x, shapely.MultiLineString):
        return shapely.MultiPolygon(shapely.Polygon(y) for y in x.geoms)
    elif isinstance(x, shapely.LineString):
        return shapely.Polygon(x)
    else:
        return shapely.Polygon([], [])


def union(gs):
    g = gs[0]
    for other in gs[1:]:
        g = g.union(other)
    return gpd.GeoSeries(g).set_crs(27700)


gdf = gpd.read_parquet("/dbfs/tmp/boundaries.parquet")
gdf["geometry_adj"] = union(gdf["geometry_adj"])
gdf["geometry_parcel"] = gdf["geometry_boundary"].apply(to_polygon)
colours = ["#b8860b", "#daa520", "#1f77b4", "#17becf", "#7f7f7f", "#2ca02c", "#daa520"]

m = None
for column, colour in zip(gdf.columns[1:], colours):
    m = gdf[column].explore(m=m, style_kwds={"color": colour + "a0", "fillColor": colour + "60"})

m

# COMMAND ----------


gdf = gpd.read_parquet("/dbfs/tmp/awest/boundaries.parquet").melt("id_parcel", var_name="layer", value_name="geometry").set_geometry("geometry").set_crs(27700)
gdf.to_parquet("/dbfs/tmp/awest/tmp.parquet")


def timeit(fn, n=1000):
    def timed():
        t = perf_counter()
        fn()
        return perf_counter() - t

    return [timed() for _ in range(n)]


gdf = gpd.read_parquet("/dbfs/tmp/awest/tmp.parquet")
df = pd.DataFrame(
    {
        "GeoJSON": timeit(lambda: gdf.to_file("/databricks/driver/tmp.geojson")),
        "GeoPackage": timeit(lambda: gdf.to_file("/databricks/driver/tmp.gpkg")),
        "Shapefile": timeit(lambda: gdf.to_file("/databricks/driver/tmp.shp")),
        "Zipped Shapefile": timeit(lambda: gdf.to_file("/databricks/driver/tmp.shp.zip")),
        "GeoParquet": timeit(lambda: gdf.to_parquet("/databricks/driver/tmp.parquet")),
    },
).melt()
df.to_feather("/dbfs/tmp/awest/write.feather")


df = pd.DataFrame(
    {
        "GeoJSON": timeit(lambda: gpd.read_file("/databricks/driver/tmp.geojson")),
        "GeoPackage": timeit(lambda: gpd.read_file("/databricks/driver/tmp.gpkg")),
        "Shapefile": timeit(lambda: gpd.read_file("/databricks/driver/tmp.shp")),
        "Zipped Shapefile": timeit(lambda: gpd.read_file("/databricks/driver/tmp.shp.zip")),
        "GeoParquet": timeit(lambda: gpd.read_parquet("/databricks/driver/tmp.parquet")),
    },
).melt()
df.to_feather("/dbfs/tmp/awest/read.feather")


df_read = pd.read_feather("/dbfs/tmp/awest/read.feather")
df_write = pd.read_feather("/dbfs/tmp/awest/write.feather")
df_read["io"] = "read"
df_write["io"] = "write"
df = pd.concat([df_read, df_write])


sns.set_theme(style="whitegrid", palette="pastel")
sns.barplot(data=df, x="variable", y="value", hue="io", errwidth=False)
plt.gca().set(
    title="Read/Write speed of Formats",
    xlabel="data: ~1000 features of many geometry types",
    ylabel="Time (s)",
)
plt.xticks(rotation=45)
None

# COMMAND ----------

sf_wall = "dbfs:/mnt/lab/unrestricted/elm_data/osm/wall.parquet"

sdf = spark.read.parquet(sf_wall)
gdf = elm_se.io.to_gdf(sdf, crs=27700)

ax = gdf.plot(figsize=(9, 16), color="k")
ctx.add_basemap(ax=ax, crs=gdf.crs.to_string(), attribution=None)
ax.axis("off")
None

# COMMAND ----------


def setup_plot():
    fig, ax = plt.subplots(figsize=(16, 9))
    ax.axis("off")
    ax.set(xlim=[446450, 446650], ylim=[349550, 349850])
    ctx.add_basemap(
        ax=ax,
        source=ctx.providers.Thunderforest.Landscape(apikey="25a2eb26caa6466ebc5c2ddd50c5dde8", attribution=None),
        crs="EPSG:27700",
    )
    return ax


# COMMAND ----------

sf_neighbour = "dbfs:/mnt/lab/unrestricted/elm/elm_se/neighbouring_land_use_geometries.parquet"
sf_boundary = "dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_use_geometries.parquet"

gdf_neighbour = elm_se.io.SparkDataFrame_to_PandasDataFrame(spark.read.parquet(sf_neighbour).filter('id_parcel == "SK46495469"'))
for col in gdf_neighbour.columns[1:]:
    gdf_neighbour[col] = gpd.GeoSeries.from_wkb(gdf_neighbour[col], crs=27700)
gdf_neighbour = gpd.GeoDataFrame(gdf_neighbour, geometry="geometry_boundary", crs=27700)

gdf_boundary = elm_se.io.to_gdf(spark.read.parquet(sf_boundary).filter('id_parcel == "SK46495469"'), column="geometry_boundary")

display(gdf_neighbour)
display(gdf_boundary)

# COMMAND ----------

ax = setup_plot()

gdf_neighbour["geometry_boundary"].plot(ax=ax, color="#000", linewidth=2)

None

# COMMAND ----------

ax = setup_plot()

gdf_neighbour["geometry_adj"].boundary.plot(ax=ax, color="darkgoldenrod", alpha=1)
gdf_neighbour["geometry_adj"].buffer(8).plot(ax=ax, color="goldenrod", alpha=0.4, linewidth=4)
gdf_boundary.query("elg_adj==True").plot(ax=ax, color="#000", linewidth=2, linestyle="--")
gdf_boundary.query("elg_adj==False").plot(ax=ax, color="#000", linewidth=2, linestyle="-")

None

# COMMAND ----------

ax = setup_plot()

gdf_neighbour["geometry_water"].plot(ax=ax, color="tab:blue", alpha=0.4, linewidth=4)
gdf_boundary.query("elg_adj==True & elg_water==True").plot(ax=ax, color="#00F", linewidth=2, linestyle="--")
gdf_boundary.query("elg_adj==False & elg_water==True").plot(ax=ax, color="#00F", linewidth=2, linestyle="-")
gdf_boundary.query("elg_adj==True & elg_water==False").plot(ax=ax, color="#000", linewidth=2, linestyle="--")
gdf_boundary.query("elg_adj==False & elg_water==False").plot(ax=ax, color="#000", linewidth=2, linestyle="-")

None

# COMMAND ----------

ax = setup_plot()

gdf_neighbour["geometry_hedge"].plot(ax=ax, color="tab:green", alpha=0.4, linewidth=4)
gpd.GeoSeries(shapely.LineString([[446521, 349767], [446550, 349740]])).plot(ax=ax, color="tab:green", alpha=0.4, linewidth=4)
gdf_boundary.query("elg_adj==True & elg_water==True & elg_hedge==True").plot(ax=ax, color="#F00", linewidth=2, linestyle="--")
gdf_boundary.query("elg_adj==False & elg_water==True & elg_hedge==True").plot(ax=ax, color="#F00", linewidth=2, linestyle="-")
gdf_boundary.query("elg_adj==True & elg_water==False & elg_hedge==True").plot(ax=ax, color="#0F0", linewidth=2, linestyle="--")
gdf_boundary.query("elg_adj==False & elg_water==False & elg_hedge==True").plot(ax=ax, color="#0F0", linewidth=2, linestyle="-")
gdf_boundary.query("elg_adj==True & elg_water==True & elg_hedge==False").plot(ax=ax, color="#00F", linewidth=2, linestyle="--")
gdf_boundary.query("elg_adj==False & elg_water==True & elg_hedge==False").plot(ax=ax, color="#00F", linewidth=2, linestyle="-")
gdf_boundary.query("elg_adj==True & elg_water==False & elg_hedge==False").plot(ax=ax, color="#000", linewidth=2, linestyle="--")
gdf_boundary.query("elg_adj==False & elg_water==False & elg_hedge==False").plot(ax=ax, color="#000", linewidth=2, linestyle="-")

None

# COMMAND ----------

ax = setup_plot()

df = gdf_boundary.copy()
cols = ["adj", "water", "ditch", "wall", "hedge"]
for col in cols:
    df[col] = np.where(df["elg_" + col], col, "")
df["boundary_use"] = df[cols].agg(lambda x: ", ".join(y for y in x if y != ""), axis=1)

df.plot(ax=ax, column="boundary_use", linewidth=2, legend=True, legend_kwds={"loc": "lower right"})
None

# COMMAND ----------

sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/"
sf_peatland = "dbfs:/mnt/lab/unrestricted/elm/elmo/peatland/polygons.parquet"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/elm_se/peatland.parquet"

null = 'ST_GeomFromText("Point EMPTY")'
sdf_parcel = spark.read.parquet(sf_parcel).withColumn("geometry", F.expr(f"COALESCE(geometry, {null})"))
sdf_peatland = spark.read.parquet(sf_peatland).withColumn("geometry", F.expr(f"COALESCE(ST_GeomFromWKB(geometry), {null})"))

sdf = elm_se.st.sjoin(sdf_parcel, sdf_peatland, lsuffix="_parcel", rsuffix="_peatland", distance=12)

display(sdf)
sdf.count()
sdf.write.format("geoparquet").save(sf_out)

# COMMAND ----------


f_out = "/dbfs/mnt/lab/unrestricted/elm/elm_se/peatland.parquet"

gdf = gpd.read_parquet(f_out)

gdf

# COMMAND ----------

sf_boundary = "dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_use_geometries.parquet"
sf_uptake = "dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_use_uptake.parquet"
sf_peatland = "dbfs:/mnt/lab/unrestricted/elm/elmo/peatland/polygons.parquet"

sdf_boundary = spark.read.parquet(sf_boundary)
sdf_uptake = spark.read.parquet(sf_uptake)
sdf_peatland = spark.read.parquet(sf_peatland)


# COMMAND ----------

sdf = (
    sdf_uptake.filter("elg_ditch AND 0.1<peatland")
    .select("id_parcel", "peatland")
    .distinct()
    .join(
        sdf_boundary.select("id_parcel", "geometry_boundary"),
        on="id_parcel",
        how="left",
    )
)

sdf.count()

# COMMAND ----------

# plot ditches and peatland

gdf_neighbour = elm_se.io.SparkDataFrame_to_PandasDataFrame(spark.read.parquet(sf_neighbour).filter("elg_ditch AND peatland"))


# COMMAND ----------

# # wetland
# living england
# ramsar
# soil moisture
# selection of ph
