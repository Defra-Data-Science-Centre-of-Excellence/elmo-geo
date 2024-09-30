# Databricks notebook source
# MAGIC %md # Buffer Woodland

# COMMAND ----------

# MAGIC %pip install -q git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

from cdap_geo.sedona import F, st_fromwkb, st_join, st_register

st_register()


sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/"
sf_water = "dbfs:/mnt/lab/unrestricted/elm_data/os/mmtopo/lnd_fts_land.parquet/"

sf_geom = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/woodland_geometries.parquet/"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/woodland.parquet/"


sdf_geom = st_join(
    spark.read.parquet(sf_parcel).filter("geometry IS NOT null"),
    spark.read.parquet(sf_water)
    .select(
        "theme",
        "description",
        "landform",
        "capturespecification",
        st_fromwkb("geometry"),
    )
    .filter("geometry IS NOT null")
    .filter(F.col("description").contains("Trees")),
    lsuffix="_parcel",
    rsuffix="_water",
    distance=12,
)

sdf_geom.write.parquet(sf_geom)
display(sdf_geom)


def buf(x):
    return f"ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(geometry_water, {x})), geometry_parcel))/10000 AS ha_buf{x}"


sdf = (
    spark.read.parquet(sf_geom)
    .groupby("id_business", "id_parcel")
    .agg(
        F.expr("FIRST(geometry_parcel) AS geometry_parcel"),
        F.expr("ST_Union_Aggr(geometry_water) AS geometry_water"),
    )
    .select(
        "id_business",
        "id_parcel",
        F.expr(buf(4)),
        F.expr(buf(6)),
        F.expr(buf(8)),
        F.expr(buf(10)),
        F.expr(buf(12)),
    )
)

sdf.write.parquet(sf_out)
display(sdf)
