"""SFI Agroforestry Sensitivity Low from Forestry Commission.

Source:
    - https://data-forestry.opendata.arcgis.com/datasets/94c16efc5c9d47ed8d6634a8d538d166_0/explore?location=53.557956%2C-1.754704%2C14.00
"""
import geopandas as gpd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset


class FcSfiAgroforestryClean(DataFrameModel):
    geometry: Geometry(crs=SRID) = Field(coerce=True)
    sensitivity: Category = Field(coerce=True)


def _func(ds: Dataset) -> gpd.GeoDataFrame:
    """Only keep the geometry and the sensitivity col, fixing typo in colname."""
    return ds.gdf(columns=["geometry", "sensitivit"]).rename(columns={"sensitivit": "sensitivity"}).assign(fid=lambda df: range(len(df)))


fc_sfi_agroforestry_raw = SourceDataset(
    name="sfi_agroforestry_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/sfi_agroforestry/2024_04_15/SFI_Agroforestry.shp",
)

fc_sfi_agroforestry = DerivedDataset(
    name="sfi_agroforestry",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=FcSfiAgroforestryClean,
    func=_func,
    dependencies=[fc_sfi_agroforestry_raw],
)
