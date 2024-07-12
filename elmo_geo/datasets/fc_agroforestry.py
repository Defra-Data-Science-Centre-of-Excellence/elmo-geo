"""SFI Agroforestry Sensitivity Low from Forestry Commission.

This dataset has only one sensitivity classification: *Low*.
The low sensitivity areas have fewest identified constraints to address,
and it should be easier to agree creating new woodland here than in other areas.

Source:
    - https://data-forestry.opendata.arcgis.com/datasets/94c16efc5c9d47ed8d6634a8d538d166_0/explore?location=53.557956%2C-1.754704%2C14.00
"""
import geopandas as gpd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.st.join import sjoin

from .rpa_reference_parcels import reference_parcels

SIMPLIFY_TOLERENCE: float = 20  # metres
MAX_VERTICES: int = 256  # per polygon (row) for subdivide exploding the feature polygons


class SfiAgroforestryClean(DataFrameModel):
    """Model for Forestry Commission's SFI Agroforestry dataset.

    Parameters:
        geometry: The sensitivity classification's geospatial extent (polygons).
        sensitivity: The sensitivity classification (only Low in this data).
    """

    geometry: Geometry(crs=SRID) = Field(coerce=True)
    sensitivity: Category = Field(coerce=True)


def _transform_dataset(ds: Dataset) -> gpd.GeoDataFrame:
    """Only keep the geometry and the sensitivity col, fixing typo in colname."""
    return ds.gdf(columns=["geometry", "sensitivit"]).rename(columns={"sensitivit": "sensitivity"}).assign(fid=lambda df: range(len(df)))


sfi_agroforestry_raw = SourceDataset(
    name="sfi_agroforestry_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/sfi_agroforestry/2024_04_15/SFI_Agroforestry.shp",
)
"""Definition for the raw sourced version of Forestry Commission's SFI Agroforestry dataset."""

sfi_agroforestry = DerivedDataset(
    name="sfi_agroforestry",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=SfiAgroforestryClean,
    func=_transform_dataset,
    dependencies=[sfi_agroforestry_raw],
)
"""Definition for the cleaned version of Forestry Commission's SFI Agroforestry dataset.

Columns have been renamed and dropped from the daw-version but the data/rows remain consistant.
"""


class FcSfiAgroforestryParcels(DataFrameModel):
    """Model describing the `sfi_agroforestry_parcels` dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        sensitivity: The sensitivity classification (only Low in this data).
        proportion: The proportion of the parcel that intersects with the sensitivity classification.
    """

    id_parcel: str
    sensitivity: Category = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


def _transform_dataset(parcels: Dataset, agroforestry: Dataset) -> gpd.GeoDataFrame:
    """Only keep the geometry and the sensitivity col, fixing typo in colname."""
    df_parcels = (
        parcels.sdf()
        .select("id_parcel", "geometry")
        .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
        .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry, {SIMPLIFY_TOLERENCE})"))
        .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
        .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    )
    df_feature = (
        agroforestry.sdf()
        .select("sensitivity", "geometry")
        .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
        .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry, {SIMPLIFY_TOLERENCE})"))
        .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
        .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
        .withColumn("geometry", F.expr(f"ST_SubdivideExplode(geometry, {MAX_VERTICES})"))
    )
    df = (
        sjoin(df_parcels, df_feature)
        .withColumn("geometry_intersection", F.expr("ST_Intersection(geometry_left, geometry_right)"))
        .withColumn("area_left", F.expr("ST_Area(geometry_left)"))
        .withColumn("area_intersection", F.expr("ST_Area(geometry_intersection)"))
        .withColumn("proportion", F.col("area_intersection") / F.col("area_left"))
        .drop("area_left", "area_intersection", "geometry_left", "geometry_right", "geometry_intersection")
    )
    # group up the result and sum the proportions in case multiple polygons with
    # the same attributes intersect with the parcel
    df = (
        df.groupBy(*[col for col in df.columns if col != "proportion"])
        .sum("proportion")
        .withColumn("proportion", F.round("sum(proportion)", 6))
        .where("proportion > 0")
        .drop("sum(proportion)")
    )
    return df.toPandas()


sfi_agroforestry_parcels = DerivedDataset(
    name="sfi_agroforestry_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=_transform_dataset,
    dependencies=[reference_parcels, sfi_agroforestry],
    model=FcSfiAgroforestryParcels,
)
"""Definition for Forestry Commission's SFI Agroforestry dataset joined to RPA Parcels."""
