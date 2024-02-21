# Databricks notebook source
# MAGIC %md
# MAGIC # Hedgerows
# MAGIC This notebooks defines the hedgerow lengths and buffer areas to be added to the ELM population simulation.
# MAGIC Here I join hedgerows to parcels and businesses, clip to bounds, and calculate the length, and buffers.
# MAGIC
# MAGIC EFA Hedge attaches the original OS data to parcels, but splits boundary hedges to each parcel, recording the as **adjacent**.
# MAGIC This means adjacent parcels are double counted.
# MAGIC I adjust for this as effectively half the hedgerow length.
# MAGIC
# MAGIC Buffers are calculated for each hedgerow at [4, 6, 8, 10, 12] meters and clipped to the parcel, and unclipped buffer is also provided for the largest buffer.
# MAGIC
# MAGIC
# MAGIC ### Data
# MAGIC - ELM Parcels `dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet`
# MAGIC - EFA Hedge `dbfs:/mnt/lab/unrestricted/elm_data/rpa/efa_control/2023_02_07.parquet`
# MAGIC - OSM Hedgerows `dbfs:/mnt/lab/unrestricted/elm_data/osm/hedgerows.parquet`
# MAGIC - **Output:** ELM Hedgerows `dbfs:/mnt/lab/unrestricted/elm/buffer_strips/hedgerows.parquet`
# MAGIC
# MAGIC
# MAGIC ### Table
# MAGIC > |      | Total Length (m) | Explanation
# MAGIC > | :--- | ---------------: | :---
# MAGIC > | 1984 |      572,670,000 | Managed hedgerow in 1984, [EIP 2023](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1133967/environmental-improvement-plan-2023.pdf)
# MAGIC > | 2007 |      477,000,000 | Managed hedgerows in 2007, [CS 2007](https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part2.pdf) *in Great Britain*
# MAGIC > | EFA  |      640,341,286 | EFA hedgerows
# MAGIC > | Adj  |  **419,882,012** | EFA hedgerows adjusted for adjacent hedges
# MAGIC > | Elg  |      220,459,274 | EFA hedgerows adjusted for both adjacency and EFA eligibility
# MAGIC > | OSM  |      102,408,028 | OSM hedgerows
# MAGIC > | OSMp |       73,510,661 | OSM hedgerows within parcels
# MAGIC > | 110% |      630,000,000 | A target of 110% of managed hedgerow in 1984 by 2050
# MAGIC > |  +75 |      494,000,000 | A target of +75e6m of hedgerow, defined using Adj, by 2050
# MAGIC > |  +45 |      464,000,000 | A intermediate target of +45e6m of hedgerow by 2037
# MAGIC >
# MAGIC > Table of Hedgerow datasets and their total lengths.  (for England except "2007")
# MAGIC
# MAGIC ##### EFA Adjacency
# MAGIC "Adj" is the most appropriate measurement for total amount of hedgerow in England.
# MAGIC This data is sourced from RPA's EFA Hedge, which is derived from OS data, along with some additions from ES.
# MAGIC The alternative source OSM has much less data, and doesn't require further consideration.
# MAGIC
# MAGIC ##### EFA Eligibility
# MAGIC The sources do not come with a managed status for the hedgerows, and such we cannot tell whether or not they are relict.
# MAGIC EFA eligibility is not a good analogous, as it was determined from length and adjacent arable land cover.
# MAGIC However EFA calculations for BPS stopped in 2018.
# MAGIC
# MAGIC ##### Targets
# MAGIC > |      | Target      | Year | Increase | Annual Increase |
# MAGIC > | ---: | ----------: | ---: | -------: | --------------: |
# MAGIC > |  Adj | 419,882,012 | 2022 |          |                 |
# MAGIC > | 110% | 630,000,000 | 2050 |    50.0% |            1.8% |
# MAGIC > |  +75 | 494,000,000 | 2050 |    17.9% |            0.6% |
# MAGIC > |  +45 | 464,000,000 | 2037 |    10.7% |            0.7% |
# MAGIC >
# MAGIC > Table of Targets for Hedgerow increases
# MAGIC
# MAGIC
# MAGIC ##### Managed Hedgerow
# MAGIC The managed status of all datasets' hedgerows is uncertain.
# MAGIC
# MAGIC > ![Relict Hedgerow](https://www.teagasc.ie/media/website/news/daily/environment-photos/Relict-Hedgerows---too-valuable-for-rejuvenation.jpg)
# MAGIC >
# MAGIC > [Teagasc on hedgerows](https://www.teagasc.ie/news--events/daily/environment/hedges-for-rejuvenation.php)

# COMMAND ----------

# MAGIC %pip install -qU contextily git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)

import shapely
from cdap_geo.sedona import st_fromwkb, st_join, st_valid
from sedona.sql.types import GeometryType


def st_buffer_udf(g, res, **kwargs):
    """Created for cap_style='flat'"""

    @F.udf(returnType=GeometryType())
    def _st_buffer_udf(g):
        return shapely.buffer(g, res, **kwargs)

    return _st_buffer_udf(g)


# COMMAND ----------

sf_parcels = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"
sf_rpa_hedge = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/efa_control/2023_02_07.parquet"
sf_osm_hedge = "dbfs:/mnt/lab/unrestricted/elm_data/osm/hedgerows.parquet"

sf_geoms_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/hedgerows_geometries.parquet"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/hedgerows.parquet"

# COMMAND ----------

# MAGIC %md ## RPA

# COMMAND ----------

sdf_parcels = spark.read.parquet(sf_parcels).withColumnRenamed("geometry", "geometry_parcel")

display(sdf_parcels)

# COMMAND ----------

sdf_geoms = (
    spark.read.parquet(sf_rpa_hedge)
    .withColumn("geometry", st_fromwkb("wkb_geometry", 27700))
    .select(
        "id",
        F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID").alias("id_parcel"),
        F.expr("ADJACENT_PARCEL_PARCEL_ID IS NOT NULL").alias("adjacent"),
        F.expr('ELIGIBLE_FOR_EFA == "Y"').alias("eligible"),
        F.expr("ST_Length(geometry)").alias("m_efa"),
        "geometry",
    )
    .withColumn("adj", F.expr("CASE WHEN adjacent THEN 0.5 ELSE 1.0 END"))
    .withColumn("elg", F.expr("CASE WHEN eligible THEN 1.0 ELSE 0.0 END"))
    .withColumn("geometry", F.explode(F.expr("ST_Dump(geometry)")))
    .groupby("id_parcel")
    .agg(
        F.sum("m_efa").alias("m_efa"),
        F.sum(F.expr("m_efa * adj")).alias("m_adj"),
        F.sum(F.expr("m_efa * adj * elg")).alias("m_adj_elg"),
        F.expr("ST_SimplifyPreserveTopology(ST_Union_Aggr(geometry), 0.001)").alias("geometry"),
    )
    .join(sdf_parcels, on="id_parcel", how="right")
    .withColumn("geometry_parcel", st_valid("geometry_parcel"))
    .withColumn("geometry", st_valid("geometry"))
    .select(
        "id_business",
        "id_parcel",
        "m_efa",
        "m_adj",
        "m_adj_elg",
        "geometry_parcel",
        F.col("geometry").alias("geometry_hedge"),
    )
    .repartition(200)
)


sdf_geoms.write.parquet(sf_geoms_out)
display(sdf_geoms)

# COMMAND ----------

sdf_rpa_hedge = (
    spark.read.parquet(sf_geoms_out)
    .withColumn("buf", F.expr("ST_MakeValid(ST_Buffer(geometry_hedge, 0.001))"))
    .withColumn("uncapped_buf2", st_buffer_udf("buf", 2, cap_style="flat"))
    .select(
        "id_business",
        "id_parcel",
        F.expr("ST_Length(geometry_parcel)").alias("m_parcel_boundary"),
        "m_efa",
        "m_adj",
        #'m_adj_elg',
        F.expr("ST_Length(ST_Difference(ST_Boundary(geometry_parcel), uncapped_buf2))").alias("m_none_hedge_boundary"),
        F.expr("ST_Area(ST_MakeValid(ST_Buffer(buf, 12)))").alias("sqm_buf12_unclipped"),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 12)), geometry_parcel))").alias("sqm_buf12"),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 10)), geometry_parcel))").alias("sqm_buf10"),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 8)), geometry_parcel))").alias("sqm_buf8"),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 6)), geometry_parcel))").alias("sqm_buf6"),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 4)), geometry_parcel))").alias("sqm_buf4"),
    )
)


sdf_rpa_hedge.write.parquet(sf_out, mode="overwrite")
display(sdf_rpa_hedge)

# COMMAND ----------

df = spark.read.parquet(sf_out).drop("m_adj_elg").withColumnRenamed("m_efa", "m_hedgerow").withColumnRenamed("m_adj", "m_hedgerow_adjusted_for_adjacency")
display(df)
df.count()

# COMMAND ----------

# MAGIC %md ## OSM

# COMMAND ----------

sdf_osm_hedge = st_join(
    spark.read.parquet(sf_parcels),
    spark.read.parquet(sf_osm_hedge),
    lsuffix="_parcel",
    rsuffix="",
).withColumn("geometry", F.expr("ST_Intersection(geometry, geometry_parcel)"))


display(sdf_osm_hedge)

# COMMAND ----------

display(
    spark.read.parquet(sf_rpa_hedge)
    .withColumn("geometry", st_fromwkb("wkb_geometry", 27700))
    .select(
        F.sum(F.expr("ST_Length(geometry)")).alias("m_efa_unjoined"),
    ),
)

display(
    spark.read.parquet(sf_out).select(
        F.sum("m_length").alias("m_efa"),
        F.sum("m_adj"),
        F.sum("m_adj_elg"),
    ),
)

display(
    spark.read.parquet(sf_osm_hedge).select(
        F.sum(F.expr("ST_Length(geometry)")).alias("m_osm"),
    ),
)


display(
    sdf_osm_hedge.select(
        F.sum(F.expr("ST_Length(geometry)")).alias("m_osmp"),
    ),
)

# COMMAND ----------

# MAGIC %md ## Plot

# COMMAND ----------

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt


def add_basemap(ax=None, basemap="Light", crs=27700, key="WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX", **kwargs):
    if ax is None:
        ax = plt.gca()
    basemaps = ["Road", "Outdoor", "Light"]
    if basemap not in basemaps:
        raise f"BasemapError: {basemap} not in {basemaps}."
    basemap_url = f'https://api.os.uk/maps/raster/v1/zxy/{basemap}_3857/{"{z}/{x}/{y}"}.png?key={key}'
    ctx.add_basemap(ax=ax, crs=crs, source=basemap_url, **kwargs)
    ax.axis("off")
    return ax


# COMMAND ----------

crs = 27700

# loc = 'id_parcel LIKE "NY9170%" OR id_parcel LIKE "NY9171%" OR id_parcel LIKE "NY9270%" OR id_parcel LIKE "NY9271%"'
# loc = 'SUBSTR(id_parcel, 1, 6) IN ("NY9170", "NY9171", "NY9270", "NY9271")'
loc = 'id_parcel REGEXP "NY9(1|2)7(0|1).*"'  # Humshaugh
loc = 'id_parcel REGEXP "NY9271.*"'

# COMMAND ----------

df_parcel = spark.read.parquet(sf_parcels).filter(loc).toPandas().pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=crs))

df_hedge = (
    spark.read.parquet(sf_rpa_hedge)
    .select(
        "id",
        F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID").alias("id_parcel"),
        F.expr("CASE WHEN (ADJACENT_PARCEL_PARCEL_ID IS NULL) THEN 1.0 ELSE 0.5 END").alias("adjacent"),
        st_fromwkb("wkb_geometry", 27700).alias("geometry"),
    )
    .filter(loc)
    .toPandas()
    .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=crs))
)

# COMMAND ----------

fig, ax = plt.subplots(figsize=[32, 18])
df_parcel.plot(ax=ax, alpha=0.2, color="darkgoldenrod")
df_parcel.boundary.plot(ax=ax, color="darkgoldenrod", linewidth=2)
df_hedge.plot(ax=ax, color="green", alpha=df_hedge["adjacent"], linewidth=4)
add_basemap()
