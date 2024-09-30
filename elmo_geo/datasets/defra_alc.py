"""Agricultural Land Classification (ALC) from Defra, provided by DASH.

[^DASH: ALC]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=627cd3a2-c9d0-4fb6-824d-5cee8dc5f867
[^GeoPortal API]: https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/Provisional%20Agricultural%20Land%20Classification%20(ALC)%20(England)/FeatureServer
[^ALC Guidelines 1988]: https://webarchive.nationalarchives.gov.uk/ukgwa/20130402200910/http://archive.defra.gov.uk/foodfarm/landmanage/land-use/documents/alc-guidelines-1988.pdf
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels


class ALCRaw(DataFrameModel):
    """Model for Defra Provisional Agricultural Land Classification (ALC) dataset.

    Attributes:
        alc_grade: One of these values:
            "Grade 1" (excellent quality agricultural land),
            "Grade 2",
            "Grade 3",
            "Grade 4",
            "Grade 5" (very poor quality agricultural land),
            "Urban",
            "Non-agricultural",
            "Woodland",
            "Agricultural buildings",
            "Open water",
            "Land not surveyed".
        geometry: ALC geometries in EPSG:27700.
    """

    alc_grade: str = Field()
    geometry: Geometry(crs=SRID) = Field()


alc_raw = SourceDataset(
    name="alc_raw",
    level0="bronze",
    level1="defra",
    model=ALCRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_provisional_agricultural_land_classification_alc/format_GEOPARQUET_provisional_agricultural_land_classification_alc/LATEST_provisional_agricultural_land_classification_alc/",
)


class ALCParcels(DataFrameModel):
    """Model for Defra ALC with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        alc_grade: One of these values:
            "Grade 1" (excellent quality agricultural land),
            "Grade 2",
            "Grade 3",
            "Grade 4",
            "Grade 5" (very poor quality agricultural land),
            "Urban",
            "Non-agricultural",
            "Woodland",
            "Agricultural buildings",
            "Open water",
            "Land not surveyed".
        proportion: proportion of parcel geometry overlapping with ALC geometry, separated by alc_grade.
    """

    id_parcel: str = Field()
    alc_grade: str = Field()
    proportion: float = Field(ge=0, le=1)


alc_parcels = DerivedDataset(
    is_geo=False,
    name="alc_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(sjoin_parcel_proportion, columns=["alc_grade"]),
    dependencies=[reference_parcels, alc_raw],
    model=ALCParcels,
)
