"""Agricultural Land Classification (ALC) from Defra, provided by DASH.

Data is sourced from DASH.  See GeoPortal description for references:
https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/Provisional%20Agricultural%20Land%20Classification%20(ALC)%20(England)/FeatureServer
"""
from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class ALCRaw(DataFrameModel):
    """Model for Defra Provisional Agricultural Land Classification (ALC) dataset.

    Attributes:
        alc_grade: One of these values: "Grade 1" (excellent quality agricultural land), "Grade 2", "Grade 3", "Grade 4", "Grade 5" (very poor quality argicultural land), "Urban", "Non-agricultural", "Woodland", "Agricultural buildings", "Open water", "Land not surveyed".
        geometry: ALC geometries in EPSG:27700.
    """
    alc_grade: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class ALCParcels(DataFrameModel):
    """Model for Defra ALC with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        alc_grade: One of these values: "Grade 1" (excellent quality agricultural land), "Grade 2", "Grade 3", "Grade 4", "Grade 5" (very poor quality argicultural land), "Urban", "Non-agricultural", "Woodland", "Agricultural buildings", "Open water", "Land not surveyed".
        geometry: ALC geometries in EPSG:27700.
    """
    id_parcel: str
    alc_grade: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


alc_raw = SourceDataset(
    name="alc_raw",
    level0="bronze",
    level1="defra",
    model=ALCRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_provisional_agricultural_land_classification_alc/format_GEOPARQUET_provisional_agricultural_land_classification_alc/SNAPSHOT_2021_03_16_provisional_agricultural_land_classification_alc/layer=Agricultural_Land_Classification_Provisional_England.snappy.parquet",
)

alc_parcels = DerivedDataset(
    name="alc_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["alc_grade"]),
    dependencies=[reference_parcels, alc_raw],
    model=ALCParcels,
)
