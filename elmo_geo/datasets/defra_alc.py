"""Agricultural Land Classification (ALC) from Defra, provided by DASH.

[^DASH Data Catalogue]: https://app.powerbi.com/Redirect?action=OpenReport&appId=5762de14-3aa8-4a83-92b3-045cc953e30c&reportObjectId=c8802134-4f3b-484e-bf14-1ed9f8881450&ctid=770a2450-0227-4c62-90c7-4e38537f1102&reportPage=ReportSectionf8b0041ad0335117bacb&pbi_source=appShareLink&portalSessionId=b29b27bf-6765-4900-91e4-e5f94f3fd339
[^GeoPortal API]: https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/Provisional%20Agricultural%20Land%20Classification%20(ALC)%20(England)/FeatureServer
[^ALC Guidelines 1988]: https://webarchive.nationalarchives.gov.uk/ukgwa/20130402200910/http://archive.defra.gov.uk/foodfarm/landmanage/land-use/documents/alc-guidelines-1988.pdf
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class ALCRaw(DataFrameModel):
    """Model for Defra Provisional Agricultural Land Classification (ALC) dataset.

    Attributes:
        alc_grade: One of these values:
            "Grade 1" (excellent quality agricultural land),
            "Grade 2",
            "Grade 3",
            "Grade 4",
            "Grade 5" (very poor quality argicultural land),
            "Urban",
            "Non-agricultural",
            "Woodland",
            "Agricultural buildings",
            "Open water",
            "Land not surveyed".
        geometry: ALC geometries in EPSG:27700.
    """

    alc_grade: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


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
            "Grade 5" (very poor quality argicultural land),
            "Urban",
            "Non-agricultural",
            "Woodland",
            "Agricultural buildings",
            "Open water",
            "Land not surveyed".
        proportion: proportion of Parcel geometry overlapping with ALC geometry, separated by alc_grade.
    """

    id_parcel: str = Field()
    alc_grade: str = Field()
    proportion: float = Field(ge=0, le=1)


alc_parcels = DerivedDataset(
    name="alc_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["alc_grade"]),
    dependencies=[reference_parcels, alc_raw],
    model=ALCParcels,
)
