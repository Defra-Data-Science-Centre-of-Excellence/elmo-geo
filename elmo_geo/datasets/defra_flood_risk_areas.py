"""Flood Risk Areas from Defra, provided by DASH.

[DASH Description][^1]:
This metadata record is for Approval for Access product AfA256.

Flood Risk Areas identify locations where there is believed to be significant flood risk. The EU Floods Directive refers to Flood Risk Areas as 'Areas of Potentially Significant Flood Risk' (APSFR). Flood Risk Areas have been defined by the Environment Agency (main rivers and the sea) and Lead Local Flood Authorities (surface water). Other sources of flooding are not covered. This dataset includes Flood Risk Areas defined for both Cycle 1 (December 2011) and Cycle 2 (December 2018). The criteria used to determine significance are explained in supporting guidance document supplied with this data. Flood Risk Areas determine where Flood Hazard and Risk Maps and Flood Risk Management Plans must subsequently be produced to meet obligations under the EU Floods Directive. 

This was the nearest match to 'Flood Catchment Risk Management Areas' data requirement for ELM.

*** 20/02/2023 GeoJSON format not updated ***

*** 02/09/2022 The ‘GeoJSON’ format data is downloaded from the DSP. It is known that the geometry column uses the esriGeometryPolygon geometry type and is therefore not a correct ‘GeoJSON’ format. Some packages will adjust and read the data, others such as the spark based MOSAIC library on the geospatial cluster will return an error. The data is additionally available in SHP format. The DSP team have confirmed "The DSP is built on ESRI software therefore the geojson export is in the ESRI format" ***


[^1]: https://app.powerbi.com/groups/me/apps/5762de14-3aa8-4a83-92b3-045cc953e30c/reports/c8802134-4f3b-484e-bf14-1ed9f8881450/ReportSectionff2a0c223272005d9b10?experience=power-bi
"""
from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class FloodRiskAreasRaw(DataFrameModel):
    """Model for Defra Flood Risk Areas dataset.

    Attributes:
        fra_id: Feature ID e.g. "UK04A0001ENG".
        fra_name: Name of fra_id, e.g. "Kingston upon Hull and Haltemprice, Humber".
        frr_cycle: 1, 2.
        flood_sour: "Surface Water", "Rivers and Sea".
        geometry: Flood Risk Area geometries in EPSG:27700.
    """
    fra_id: str = Field(coerce=True, unique=False)
    fra_name: str = Field(coerce=True)
    frr_cycle: int = Field(coerce=True)
    flood_sour: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class FloodRiskAreasParcels(DataFrameModel):
    """Model for Defra Flood Risk Areas with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        geometry: Flood Risk Area geometries in EPSG:27700.
    """
    id_parcel: str = Field(coerce=True)
    flood_sour: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


flood_risk_areas_raw = SourceDataset(
    name="fra_raw",
    level0="bronze",
    level1="defra",
    model=FloodRiskAreasRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_flood_risk_areas/format_GEOPARQUET_flood_risk_areas/LATEST_flood_risk_areas/layer=Flood_Risk_Areas.snappy.parquet",
)

flood_risk_areas_parcels = DerivedDataset(
    name="fra_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, flood_risk_areas_raw],
    model=FloodRiskAreasParcels,
)
