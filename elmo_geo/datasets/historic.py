"""Historic and archaeological feature dataset. It combines multiple sources of historic and archaeological
 features from Historic England into a single parquet file, and calculates the proportion of each parcel 
 intersected by each historic or archaeological feature. The source datasets include:

- SHINE: the Selected Heritage Inventory for Natural England
- Listed buildings
- protected_wreck_sites
- registered_battlefields 
- registered_parks_and_gardens
- scheduled_monuments 
- world_heritage_sites
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import combine_long, sjoin_parcel_proportion
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels


# Selected Heritage Inventory for Natural England (SHINE)
class HESHINERaw(DataFrameModel):
    """Model for the Selected Heritage Inventory for Natural England. It contains non-designated historic and archaeological features from across England.

    Attributes:
       shine_uid: Reference id for the SHINE feature
       shine_name: Name of the SHINE
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: str = Field(alias="shine_uid")
    name: str = Field(alias="shine_name")
    geometry: Geometry(crs=SRID) = Field()


he_shine_raw = SourceDataset(
    name="he_shine_raw",
    medallion="bronze",
    source="he",
    model=HESHINERaw,
    restricted=True,
    source_path="/dbfs/mnt/lab-res-a1001004/restricted/elm_project/raw/he/he-shine-2022_12_30.parquet",
)


# Historic England Historic Features (HEHF)
class HEHFRaw(DataFrameModel):
    """Model for Historic England Historic Features dataset.

    Attributes:
       list_entry: Reference id for each feature
       name: Name of the feature
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: int = Field(alias="ListEntry")
    name: str = Field(alias="Name")
    geometry: Geometry(crs=SRID) = Field()


he_lb_raw = SourceDataset(
    name="he_lb_raw",
    medallion="bronze",
    source="he",
    model=HEHFRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_listed_buildings_polys/format_GEOPARQUET_listed_buildings_polys/LATEST_listed_buildings_polys",
)


he_pws_raw = SourceDataset(
    name="he_pws_raw",
    medallion="bronze",
    source="he",
    model=HEHFRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_protected_wreck_sites/format_GEOPARQUET_protected_wreck_sites/SNAPSHOT_2024_04_29_protected_wreck_sites/",
)

he_rb_raw = SourceDataset(
    name="he_rb_raw",
    medallion="bronze",
    source="he",
    model=HEHFRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_battlefields/format_GEOPARQUET_registered_battlefields/SNAPSHOT_2024_04_29_registered_battlefields/",
)

he_rpg_raw = SourceDataset(
    name="he_rpg_raw",
    medallion="bronze",
    source="he",
    model=HEHFRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_parks_and_gardens/format_GEOPARQUET_registered_parks_and_gardens/SNAPSHOT_2024_04_29_registered_parks_and_gardens/",
)

he_sm_raw = SourceDataset(
    name="he_sm_raw",
    medallion="bronze",
    source="he",
    model=HEHFRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_scheduled_monuments/format_GEOPARQUET_scheduled_monuments/SNAPSHOT_2024_04_29_scheduled_monuments/",
)


he_whs_raw = SourceDataset(
    name="he_whs_raw",
    medallion="bronze",
    source="he",
    model=HEHFRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_world_heritage_sites/format_GEOPARQUET_world_heritage_sites/SNAPSHOT_2024_04_29_world_heritage_sites/",
)


class HECombinedSites(DataFrameModel):
    """Model that combines the individual historic features.

    Attributes:
       dataset: Source dataset of this feature, e.g. protected_wreck_sites
       list_entry: Reference id for each feature
       name: Name of the feature
       geometry: Geospatial polygons in EPSG:27700
    """

    source: str = Field()
    list_entry: str = Field()
    name: str = Field()
    geometry: Geometry(crs=SRID) = Field()


he_combined_sites = DerivedDataset(
    is_geo=True,
    name="he_combined_sites",
    medallion="silver",
    source="he",
    restricted=False,
    func=partial(
        combine_long,
        sources=[
            "shine",
            "listed_building",
            "protected_wreck_sites",
            "registered_battlefields",
            "registered_parks_and_gardens",
            "scheduled_monuments",
            "world_heritage_sites",
        ],
    ),
    dependencies=[
        he_shine_raw,
        he_lb_raw,
        he_pws_raw,
        he_rb_raw,
        he_rpg_raw,
        he_sm_raw,
        he_whs_raw,
    ],
    model=HECombinedSites,
)
"""A combined dataset of historic features.
"""


class HeCombinedSitesParcels(DataFrameModel):
    """Model for the combined historic feature dataset joined with Rural Payment Agency parcel dataset.
    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion_hist_arch_0m: the proportion of the parcel that intersects with the historic sites (buffer 0m).
        proportion_sched_monuments_0m: the proportion of the parcel that intersects with scheduled monuments (buffer 0m).
        proportion_hist_arch_ex_sched_monuments_0m: the proportion of the parcel that intersects with historic sites other than scheduled monuments (buffer 0m).

        proportion_hist_arch_6m: the proportion of the parcel that intersects with the historic sites (buffer 6m).
        proportion_sched_monuments_6m: the proportion of the parcel that intersects with scheduled monuments (buffer 6m).
        proportion_hist_arch_ex_sched_monuments_6m: the proportion of the parcel that intersects with historic sites other than scheduled monuments (buffer 6m).
    """

    id_parcel: str = Field()

    proportion_hist_arch_0m: float = Field(ge=0, nullable=True)
    proportion_sched_monuments_0m: float = Field(ge=0, nullable=True)
    proportion_hist_arch_ex_sched_monuments_0m: float = Field(ge=0, nullable=True)

    proportion_hist_arch_6m: float = Field(ge=0, nullable=True)
    proportion_sched_monuments_6m: float = Field(ge=0, nullable=True)
    proportion_hist_arch_ex_sched_monuments_6m: float = Field(ge=0, nullable=True)


def _calculate_historic_proportions(reference_parcels: DerivedDataset, he_combined_sites: DerivedDataset) -> SparkDataFrame:
    """Calculate the proportion of each parcel intersected by historic or archaeological features."""
    sdf = None

    for buf in [0, 6]:
        sdf_combined_sites = he_combined_sites.sdf()
        if buf > 0:
            sdf_combined_sites = sdf_combined_sites.withColumn("geometry", F.expr(f"ST_MakeValid(ST_Buffer(geometry, {buf}))"))

        _sdf = (
            sjoin_parcel_proportion(reference_parcels, he_combined_sites_buffered)
            .rename(columns={"proportion": f"proportion_hist_arch_{buf}m"})
            .merge(
                sjoin_parcel_proportion(reference_parcels, he_combined_sites_buffered.filter("source == 'scheduled_monuments'")).rename(
                    columns={"proportion": f"proportion_sched_monuments_{buf}m"}
                ),
                on="id_parcel",
                how="outer",
            )
            .merge(
                sjoin_parcel_proportion(reference_parcels, he_combined_sites_buffered.filter("source != 'scheduled_monuments'")).rename(
                    columns={"proportion": f"proportion_hist_arch_ex_sched_monuments_{buf}m"}
                ),
                on="id_parcel",
                how="outer",
            )
            .fillna(0)
        )
        sdf = sdf.merge(_sdf, on="id_parcel", how="outer") if sdf is not None else _sdf
    return sdf


he_combined_sites_parcels = DerivedDataset(
    name="he_combined_sites_parcels",
    medallion="silver",
    source="he",
    restricted=False,
    is_geo=True,
    func=_calculate_historic_proportions,
    dependencies=[reference_parcels, he_combined_sites],
    model=HeCombinedSitesParcels,
)
