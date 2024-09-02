"""Ordnance Survey Themed Layers, provided by DASH.

TODO: Waiting for OS data to be converted to GeoParquet by DASH, for easier layers.

os-wtr_fts-2023_12_16
os-wtr_ntwk-2023_12_16
/dbfs/mnt/base/unrestricted/source_ordnance_survey_data_hub/dataset_open_greenspace/format_GEOPARQUET_open_greenspace/LATEST_open_greenspace/
/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_water_network/format_GPKG_ngd_water_network/LATEST_ngd_water_network/
/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_water_features/format_GPKG_ngd_water_features/LATEST_ngd_water_features/
/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_transport_network/format_GPKG_ngd_transport_network/LATEST_ngd_transport_network/
/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_land_use_features/format_GPKG_ngd_land_use_features/LATEST_ngd_land_use_features/
/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_land_features/format_GPKG_ngd_land_features/LATEST_ngd_land_features/
/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_mastermap_highways_network_roads_prem/format_GML_mastermap_highways_network_roads_prem/
"""

from glob import iglob

import geopandas as gpd
from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, SourceDataset
from elmo_geo.utils.log import LOG


class OSNGDRaw(DataFrameModel):
    """Model for merged OS NGD datasets.

    Attributes:
        ...
        geometry: Any type geometries in EPSG:27700.
    """

    ...
    geometry: Geometry(crs=SRID) = Field(coerce=True)


def os_refresh(self: SourceDataset) -> None:
    LOG.info(f"Creating '{self.name}' dataset.")
    for f in iglob(self.source_path):
        for layer in gpd.list_layers(f)["name"]:
            LOG.info(f"{layer=}")
            f_out = f"{self._new_path}/{layer=}/part-0.snappy.parquet"
            gdf = gpd.read_file(f, layer=layer)
            gdf = gdf.pipe(self._validate).pipe(self.rename)
            gdf.to_parquet(f_out)
    LOG.info(f"Saved to '{self.path}'.")


os_ngd_raw = SourceDataset(
    name="os_ngd_raw",
    level0="bronze",
    level1="os",
    model=OSNGDRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd*/format_GPKG*/LATEST_*/*.gpkg",
)
os_ngd_raw.refresh = os_refresh
