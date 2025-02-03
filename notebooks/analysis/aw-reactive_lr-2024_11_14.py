# Databricks notebook source
# MAGIC %md
# MAGIC # Reactive data collection for Landscape Recovery
# MAGIC Request from George O'Reilly to produce a subset of geospatial parcels with WFM production for LR projects.

# COMMAND ----------

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets import reference_parcels, wfm_parcels
from elmo_geo.etl import SRID, Dataset, DerivedDataset
from elmo_geo.io import to_gdf
from elmo_geo.utils.misc import dbfs

register()

# COMMAND ----------


class LrModel(DataFrameModel):
    """Model for geo+WFM subset."""

    id_business: str = Field(nullable=True)
    id_parcel: str = Field()
    geometry: Geometry(crs=SRID) = Field(nullable=True)
    area_ha: float = Field(nullable=True)
    pass  # WFM data


def _transform_lr(reference_parcels: Dataset, wfm_parcels: Dataset, filepath: str) -> SparkDataFrame:
    return to_gdf(
        spark.read.csv(dbfs(filepath, True), header=True)
        .join(
            reference_parcels.sdf().drop("sbi"),
            on="id_parcel",
            how="left",
        )
        .join(
            wfm_parcels.sdf(),
            on="id_parcel",
            how="left",
        )
        .withColumn("id_business", F.expr("CAST(ROUND(id_business) AS STRING)"))
        .withColumn("geometry", F.expr('COALESCE(geometry, ST_GeomFromText("POINT EMPTY"))'))
    )


# COMMAND ----------

# Requested: 2024-12-18
fcp_elr = DerivedDataset(
    name="fcp_elr",
    medallion="silver",
    source="fcp",
    model=LrModel,
    restricted=True,
    is_geo=True,
    func=partial(_transform_lr, filepath="/dbfs/FileStore/elmo_geo-uploads/fcp_lr_elr_parcels_2024_12_18.csv"),
    dependencies=[reference_parcels, wfm_parcels],
)
# fcp_elr.refresh()  # if changing the filepath

fcp_elr.sdf().display()
fcp_elr.export("geojson")
fcp_elr.gdf().explore()

# COMMAND ----------

# Requested: 2024-11-13
fcp_ud = DerivedDataset(
    name="fcp_ud",
    medallion="silver",
    source="fcp",
    model=LrModel,
    restricted=True,
    is_geo=True,
    func=partial(_transform_lr, filepath="/dbfs/FileStore/elmo_geo-uploads/fcp_lr_ud_parcels_2024_11_14.csv"),
    dependencies=[reference_parcels, wfm_parcels],
)
# fcp_ud.refresh()  # if changing the filepath

fcp_ud.sdf().display()
fcp_ud.export("geojson")
fcp_ud.gdf().explore()
