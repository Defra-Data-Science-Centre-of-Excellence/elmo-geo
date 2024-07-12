"""Reference Parcels from Rural Payments Agency."""
from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, SourceDataset


class ReferenceParcels(DataFrameModel):
    """Model for Forestry Commission's SFI Agroforestry dataset.

    More columns may be present in the data and would be persisted but we have defined here the ones we care about.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        geometry: The parcel geospatial polygons in EPSG:27700.
    """

    id_parcel: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


reference_parcels = SourceDataset(
    name="reference_parcels",
    level0="silver",
    level1="rural_payments_agency",
    model=ReferenceParcels,
    restricted=False,
    source_path="/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet",
    partition_cols=["sindex"],
)
"""Definition for the raw sourced version of the RPA's Reference Parcels dataset.

Note this is labelled as silver as it is pre-processed outside of this ETL.
When we process the new 2023 parcels we will add in a `reference_parcels_raw` `SourceDataset`
and derive this `reference_parcels` dataset from it.
"""
