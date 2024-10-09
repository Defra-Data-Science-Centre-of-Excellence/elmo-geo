"""Functions for transforming datasets.
"""
import geopandas as gpd

from elmo_geo.utils.types import PandasDataFrame, SparkDataFrame


def fn_pass(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf


def _st_union_right(pdf: PandasDataFrame) -> PandasDataFrame:
    "Select first row with the union of geometry_right."
    return pdf.iloc[:1].assign(geometry_right=gpd.GeoSeries.from_wkb(pdf["geometry_right"]).union_all().wkb)


def sjoin_parcels(
    sdf_parcels: SparkDataFrame,
    sdf_feature: SparkDataFrame,
    columns: list[str] = [],
    fn_pre: callable = fn_pass,
    fn_post: callable = fn_pass,
    **kwargs,
):
    """Spatially join features dataset to parcels and groups.
    Returns a geospatial dataframe; id_parcel, *columns, geometry_left, geometry_right.

    Parameters:
        parcels: dataset containing rpa reference parcels.
        feature: dataset is assumed to be a source dataset in EPSG:27700 without any geometry tidying.
        columns: from the feature dataset to keep, and group by.
        fn_pre: transforms sdf_feature after it is loaded, and before joined with parcels, used for filtering and renaming.
        fn_post: transforms sdf output after grouping by, used for filtering and renaming columns.
        **kwargs: passed to elmo_geo.st.sjoin, used for distance joins.
    """