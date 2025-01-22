# Databricks notebook source
# MAGIC %md
# MAGIC # Hedgerows on Protected Landscapes

# COMMAND ----------

from pandera import DataFrameModel, Field

from elmo_geo import register
from elmo_geo.datasets import rpa_hedges_raw, protected_landscapes_tidy
from elmo_geo.st import sjoin
from elmo_geo.st.udf import st_clean
from elmo_geo.etl.transformations import _st_union_right
from elmo_geo.etl import Dataset, DerivedDataset
from elmo_geo.utils.types import SparkDataFrame

register()

# COMMAND ----------

class Model(DataFrameModel):
    source: str = Field()
    name: str = Field()
    m: float = Field()


def _transform(rpa_hedges_raw: Dataset, protected_landscapes_tidy: Dataset) -> SparkDataFrame:
    return (
        sjoin(
            rpa_hedges_raw.sdf().select("geometry"),
            protected_landscapes_tidy.sdf().select("source", "name", "geometry").limit(10),
            lsuffix="_right",
            rsuffix="_left",
        )
        .transform(lambda sdf: sdf.groupby("source", "name").applyInPandas(_st_union_right, sdf.schema))
        .transform(st_clean, "geometry_right")
        .selectExpr(
            "source",
            "name",
            "ST_Length(ST_Intersection(geometry_left, geometry_right)) AS m",
        )
    )


dataset = DerivedDataset(
    is_geo=False,
    name="hedges_on_pl",
    medallion="gold",
    source="elmo_geo",
    restricted=False,
    func=_transform,
    dependencies=[rpa_hedges_raw, protected_landscapes_tidy],
    model=Model,
)


dataset.sdf()
#_transform(rpa_hedges_raw, protected_landscapes_tidy).display()

# COMMAND ----------

# MAGIC %sh du -sh '/dbfs/mnt/lab-res-a1001004/restricted/elm_project/bronze/cec/cec_soilscapes_raw-2023_09_28-0ad04b4b.parquet'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Old

# COMMAND ----------

# MAGIC %md
# MAGIC # Hedges and Parcels on Protected Landscapes
# MAGIC **Protected Landscapes,** (PL) are both National Parks (NP) and National Landscapes (NL) (previously known as Areas of Outstanding National Beauty,
# MAGIC AONB). An additional unified PL geometry is appended (in v2023_12_15).
# MAGIC **Hedges,** are spatially joined to PL using ST_Intersects, but not clipped to the PL.  After joining, they are groupby PL Type + PL Name, I then
# MAGIC calculate the area for PL and the length for all hedgerows in that PL.
# MAGIC **Parcels,** are spatially joined to PL using the same method.  No groupby is done, but I drop the geometries only keeping unique id_parcels on each
# MAGIC PL and the proportion of that parcel's overlap with that PL, so near zero overlaps can be filtered later.  Latest supply from RPA is used
# MAGIC (in v2023_12_15).
# MAGIC
# MAGIC ### Assumptions
# MAGIC - **Generalisations** are made to geometries, PL at 1m, parcels at 0.1m, hedges are not simplified.
# MAGIC - The length of hedges is calculated individually and summed, assuming hedges are not **duplicated**.
# MAGIC - Calculating the length prior also means that hedgerow the starts within the PL and ends outside is entirely included and **not clipped**.
# MAGIC - All PL geometries are checked to be in England (BFC), but not clipped, small inconsistencies exist due to England geometry **CRS transform**.
# MAGIC - Parcels are spatially joined to PLs and a **proportion** is calculated, where they share a border that proportion will be very small, and judgement
# MAGIC on whether they are considered in or not is required.
# MAGIC - Hedgerow data is sourced from RPA, utilising; OS data, agreements, and site visits.  It only contains hedgerows **attached to parcels**.
# MAGIC
# MAGIC ### Future
# MAGIC RPA are developing a **new dataset** of hedgerows as polygons using aerial photography.  This dataset would provide a more accurate representation of
# MAGIC hedges, and provide better coverage outside of farms.  Assignment to a specific parcel and linearisation of these geometries would not be necessary.
# MAGIC
# MAGIC ### Data
# MAGIC - ons-countries_bfc-2022_12
# MAGIC - ne-national_parks-2023_11_17
# MAGIC - ne-aonb-2020_08_25
# MAGIC - rpa-parcel-2023_12_13
# MAGIC - rpa-hedge-2023_12_13
# MAGIC - elmo_geo-protected-landscapes-2023_12_15
# MAGIC
# MAGIC ### Output
# MAGIC - [awest-hedges_on_pl-2023_12_15.feather](https://adb-7422054397937474.14.azuredatabricks.net/files/awest-hedges_on_pl-2023_12_15.feather?o=7422054397937474)
# MAGIC - [awest-parcels_on_pl-2023_12_15.feather](https://adb-7422054397937474.14.azuredatabricks.net/files/awest-parcels_on_pl-2023_12_15.feather?o=7422054397937474)

# COMMAND ----------

from glob import glob

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

import elmo_geo
from elmo_geo.io import download_link
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import FOLDER_ODS, FOLDER_STG

elmo_geo.register()


def load_sdf(name):
    name = "elmo_geo-protected_landscapes"
    f = [
        *glob(f"{FOLDER_STG}/{name}*"),
        *glob(f"{FOLDER_ODS}/{name}*"),
    ][-1]
    elmo_geo.LOG.info(f"Reading: {f}")
    return spark.read.format("geoparquet").load(dbfs(f, True))


# COMMAND ----------

# Protected Landscapes (PL)
gdf_np = gpd.read_parquet("/dbfs/mnt/lab-res-a1001004/restricted/elm_project/stg/ne-national_parks-2023_11_17.parquet")
gdf_np["PL Type"] = "National Park"
gdf_np["PL Name"] = gdf_np["NAME"]
gdf_np = gdf_np[["PL Type", "PL Name", "geometry"]]
gdf_np.geometry = gdf_np.make_valid()

gdf_nl = gpd.read_parquet("/dbfs/mnt/lab-res-a1001004/restricted/elm_project/stg/ne-aonb-2020_08_25.parquet").to_crs(epsg=27700)
gdf_nl["PL Type"] = "National Landscape"
gdf_nl["PL Name"] = gdf_nl["NAME"]
gdf_nl = gdf_nl[["PL Type", "PL Name", "geometry"]]
gdf_nl.geometry = gdf_nl.make_valid()

gdf_pl = pd.concat(
    [
        gdf_np,
        gdf_nl,
        gpd.GeoDataFrame(
            {
                "PL Name": ["All Protected Landscapes", "Overlapping NP and NL"],
                "geometry": [
                    gpd.GeoSeries([gdf_np.unary_union]).union(gdf_nl.unary_union).unary_union,
                    gpd.GeoSeries([gdf_np.unary_union]).intersection(gdf_nl.unary_union).unary_union,
                ],
            },
            crs="EPSG:27700",
        ),
    ]
).reset_index()
gdf_pl.geometry = gdf_pl.simplify(0.001)

gdf_pl.to_parquet("/dbfs/mnt/lab-res-a1001004/restricted/elm_project/ods/elmo_geo-protected_landscapes-2024_03_14.parquet")
gdf_pl

# COMMAND ----------

# Hedges on PL
f = "/dbfs/mnt/lab-res-a1001004/restricted/elm_project/out/awest-hedges_on_pl-2023_12_15.feather"
sdf_pl = load_sdf("elmo_geo-protected_landscapes-2023_12_15")
sdf_hedge = load_sdf("rpa-hedge-2023_12_13")


sdf_pl.repartition(sdf_pl.count()).createOrReplaceTempView("pl")
sdf_hedge.createOrReplaceTempView("hedge")
sdf = spark.sql(
    """
    SELECT
        l.`PL Type`,
        l.`PL Name`,
        FIRST(l.ha) AS ha_pl,
        SUM(r.m) AS m_hedge
    FROM (
        SELECT
            ST_Length(geometry) AS m,
            geometry
        FROM hedge
    ) r
    JOIN (
        SELECT
            `PL Type`,
            `PL Name`,
            ST_Area(geometry)/10000 AS ha,
            ST_SubDivideExplode(ST_MakeValid(ST_SimplifyPreserveTopology(geometry, 1)), 256) AS geometry
        FROM pl
    ) l
    ON l.geometry IS NULL OR ST_Intersects(l.geometry, r.geometry)
    GROUP BY
        l.`PL Type`,
        l.`PL Name`
"""
)


sdf.toPandas().to_feather(f)
download_link(f)
display(sdf)

# COMMAND ----------

# Parcels on PL
f = "/dbfs/mnt/lab-res-a1001004/restricted/elm_project/out/awest-parcels_on_pl-2023_12_15.feather"
sdf_pl = load_sdf("elmo_geo-protected_landscapes-2023_12_15")
sdf_parcel = load_sdf("rpa-parcel-2023_12_13")


sdf_pl.repartition(sdf_pl.count()).createOrReplaceTempView("pl")
sdf_parcel.createOrReplaceTempView("parcel")
sdf = spark.sql(
    """
    SELECT
        `PL Type`,
        `PL Name`,
        id_parcel,
        ST_Area(ST_MakeValid(ST_Intersection(geometry_parcel, geometry_pl))) / ST_Area(geometry_parcel) AS proportion
    FROM (
        SELECT
            l.`PL Type`,
            l.`PL Name`,
            r.id_parcel,
            ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_Union_Aggr(l.geometry)), 1)) AS geometry_pl,
            ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_Union_Aggr(r.geometry)), 0.1)) AS geometry_parcel
        FROM (
            SELECT
                id_parcel,
                geometry
            FROM parcel
        ) AS r
        JOIN (
            SELECT
                `PL Type`,
                `PL Name`,
                ST_SubDivideExplode(ST_MakeValid(ST_SimplifyPreserveTopology(geometry, 1)), 256) AS geometry
            FROM pl
        ) AS l
        ON ST_Intersects(l.geometry, r.geometry)
        GROUP BY
            l.`PL Type`,
            l.`PL Name`,
            r.id_parcel
    )
"""
)


sdf.toPandas().to_feather(f)
download_link(f)
display(sdf)

# COMMAND ----------

gdf_pl = gpd.read_parquet("/dbfs/mnt/lab-res-a1001004/restricted/elm_project/ods/elmo_geo-protected_landscapes-2024_03_14.parquet")
gdf_hedge = gpd.read_parquet("/dbfs/mnt/lab-res-a1001004/restricted/elm_project/ods/rpa-hedge-2023_12_13.parquet/sindex=TG42").set_crs("EPSG:27700")

gdf_pl

# COMMAND ----------

fig, ax = plt.subplots(figsize=[16, 9])
ax = gdf_pl.plot(
    ax=ax,
    column="PL Type",
    legend=True,
    edgecolor="k",
    linewidth=0.5,
    alpha=0.5,
    cmap="summer_r",
)
ctx.add_basemap(ax=ax, crs="epsg:27700")
ax.axis("off")

# COMMAND ----------

fig, ax = plt.subplots(figsize=[16, 9])
ax = gdf_pl.iloc[-1:].plot(ax=ax, column="PL Name", legend=True, edgecolor="C0", linewidth=2)
ctx.add_basemap(ax=ax, crs="epsg:27700")
ax.axis("off")

# COMMAND ----------


gdf_pl_sample = gdf_pl.clip(gdf_hedge.total_bounds).dissolve("PL Type").reset_index()
gdf_hedge_duplicate = gdf_hedge[gdf_hedge.intersects(gdf_pl_sample.iloc[0:1].reset_index().intersection(gdf_pl_sample.iloc[1:2].reset_index()).unary_union)]

fig, ax = plt.subplots(figsize=[16, 9])
ax = gdf_pl_sample.plot(
    ax=ax,
    column="PL Type",
    legend=True,
    edgecolor="k",
    linewidth=0.5,
    alpha=0.5,
    cmap="summer_r",
)
ax = gdf_hedge.plot(ax=ax, alpha=0.5, color="g", label="hedge")
ax = gdf_hedge_duplicate.plot(ax=ax, alpha=1, color="r", label="hedge")
ctx.add_basemap(ax=ax, crs="epsg:27700")
ax.axis("off")

f"{gdf_hedge_duplicate.length.sum():,.0f}m hedgerow in overlap"
