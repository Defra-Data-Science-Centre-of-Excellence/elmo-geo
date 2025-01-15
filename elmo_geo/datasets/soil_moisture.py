"""Soil moisture datasets sourced from National Snow & Ice Data Service.

The source product is the L2_SM_SP SMAP/Sentinel 1 1km soil moisture product [^1].


# References

[^1]: [NASA SMAP Data Products](https://smap.jpl.nasa.gov/data/)
"""
from pathlib import Path

import earthaccess
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from pandera import DataFrameModel, Field, check
from pandera.dtypes import Category, Float32, Timestamp
from pandera.engines.geopandas_engine import Geometry
from pandera.typing import Series
from shapely import Polygon, box

from elmo_geo.etl import DATE_FMT, SRID, DerivedDatasetDeleteMatching, SourceTemporalAPIDataset, TabularDataset
from elmo_geo.etl.transformations import sjoin_parcels
from elmo_geo.utils.log import LOG
from elmo_geo.utils.ssl import no_ssl_verification
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels

GRANULES = ( # TODO: Rename
    "003W55N",
    "003W54N",
    "003W53N",
    "004W52N",
    "004W51N",
    "004W50N",
    "004W49N",
    "003W49N",
    "001W55N",
    "001W54N",
    "001W53N",
    "001W52N",
    # "000E55N", no valid numbers
    "000E54N",
    "000E53N",
    "000E52N",
    "000E51N",
    "000E50N",
    "001E51N",
    "001E52N",
)
GRANULE_PAT = "^(SMAP_L2_SM_SP_1[AB]IWDV_\d{8}T\d{6}_\d{8}T\d{6}_\d{3}[WE]\d{2}[N]_R\d{5}_\d{3}\.h5)$"
PATH_GRANULES = Path("/dbfs/mnt/lab/unrestricted/nasa/smap")


def _hash_geom(geom: Polygon) -> str:
    x, y = [i[0] for i in geom.centroid.xy]
    return f"{x:.0f}_{y:.0f}"


def _to_box(x: dict) -> box:
    polygon = x["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"][0]["Boundary"]["Points"]
    lats = [p["Latitude"] for p in polygon]
    longs = [p["Longitude"] for p in polygon]
    return box(min(longs), min(lats), max(longs), max(lats))


class SMAPS1GranulesDataset(SourceTemporalAPIDataset):
    def _func(self) -> gpd.GeoDataFrame:
        """Make API call to gather information on requested granules."""
        auth = earthaccess.Auth()
        load_dotenv()  # get EARTHDATA_USERNAME and EARTHDATA_PASSWORD from .env file
        auth.login(strategy="environment")
        if not auth.authenticated:
            raise AssertionError("Authentication failed")
        df = pd.DataFrame()
        for granule in GRANULES:
            msg = f"Running {granule}"
            LOG.debug(msg)
            # looping through granules as using polygon picked up too many unwanted ones and GET calls are limited to 2000 granules
            with no_ssl_verification():
                granules = earthaccess.granule_query().short_name("SPL2SMAP_S").readable_granule_name(f"*{granule}*").get()
                store = earthaccess.Store(auth)
                store.get(granules, PATH_GRANULES)
            if len(granules) == 0:
                raise AssertionError(f"No granules found for EASE Grid centre {granule}")

            df = pd.concat(
                [
                    df,
                    (
                        pd.DataFrame.from_dict(granules)
                        .loc[lambda df: df.umm.map(lambda x: "RelatedUrls" in x)]
                        .assign(
                            granule=lambda df: df.umm.map(lambda x: x["DataGranule"]["Identifiers"][0]["Identifier"]),
                            geometry=lambda df: gpd.GeoSeries(df.umm.map(_to_box), crs=4326),
                            ease_grid_centre=lambda df: df.granule.str.split("_").map(lambda x: x[7]),
                            smap_time=lambda df: df.granule.str.split("_").map(lambda x: pd.Timestamp(x[5])),
                            s1_time=lambda df: df.granule.str.split("_").map(lambda x: pd.Timestamp(x[6])),
                            month=lambda df: df.smap_time.dt.strftime("%Y_%m"),
                            version=lambda df: df.granule.str.split("_").map(lambda x: x[-2]),
                            satmodepol=lambda df: df.granule.str.split("_").map(lambda x: x[4]),
                            url_download=lambda df: df.umm.map(lambda x: next(url for url in x["RelatedUrls"] if url["Type"] == "GET DATA")["URL"]),
                        )
                        .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry"))
                        .to_crs(SRID)
                        .assign(geom_hash=lambda df: df.geometry.map(_hash_geom))
                        .loc[lambda df: df.url_download.str.contains("earthdatacloud")]
                        .drop(columns=["umm", "meta", "url_download", "size"])
                        .drop_duplicates(subset="granule")
                    ),
                ]
            )
        return df


class GranulesModel(DataFrameModel):
    """Model for SMAP/S1 granules dataframe.
    
    Attributes:
        granule: Full granule name, used as an unique identifier.
        geometry: Geometry converted to ESPG:27700 from 4324 at source.
        ease_grid_centre: The rough coordinates of the centre of the EASE grid.
        smap_time: The time the SMAP image was taken in `%Y_%m_%d` format as Timestamps don't seem to play well with parquet/spark.
        s1_time: The time the S1 image was taken in `%Y_%m_%d` format as Timestamps don't seem to play well with parquet/spark.
        version: The version of the granule.
        satmodepol: Information on the satellite, it's mode and polarisation.
        geom_hash: A simple hash of the geometry's geometry to be used as a unique identifier for the
            geometry as ease_grid_centre is not unique to a geometry.
    """

    granule: str = Field(regex=GRANULE_PAT, unique=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)
    ease_grid_centre: Category = Field(coerce=True)
    smap_time: Timestamp = Field()
    s1_time: Timestamp = Field()
    month: str = Field(regex=r"^(\d{4}_\d{2})$")
    version: str
    satmodepol: Category = Field(coerce=True)
    geom_hash: str

    @check("granule", name="check_granules_downloaded")
    def check_granules_downloaded(cls, granules: Series[str]) -> Series[bool]:
        """Checks each of the granules is actually downloaded, error if not."""
        return granules.map(lambda granule: (PATH_GRANULES / granule).exists())



soil_moisture_smap_s1_granules = SMAPS1GranulesDataset(
    name="soil_moisture_smap_s1_granules",
    level0="bronze",
    level1="nsidc",
    restricted=False,
    model=GranulesModel,
    partition_cols=["month"],
)
"""Table of SMAP/S1 granules available for selected EASE Grid centres from the National Snow & Ice Data Centre."""


class GeomsParcelsMappingModel(DataFrameModel):
    """Model for SMAP/S1 granules-parcels mapping dataframe.

    Attributes:
        id_parcel: The RPA land parcel ID.
        geom_hash: A simple hash of the geometry's geometry to be used as a unique identifier for the
            geometry as ease_grid_centre is not unique to a geometry.
    """

    id_parcel: str = Field(str_matches=r"(^[A-Z]{2}[\d]{8}$)")
    geom_hash: Category = Field(coerce=True)


def _mapping_func(reference_parcels: TabularDataset, soil_moisture_smap_s1_granules: TabularDataset) -> SparkDataFrame:
    """Joins parcels to the granules in preparation for further processing.

    Using two joins here, the first spatial as there are many duplicate geometries and it would otherwise take 24 hours...
    """
    return (
        sjoin_parcels(
            reference_parcels.sdf(),
            soil_moisture_smap_s1_granules.sdf().select("geom_hash", "geometry").dropDuplicates(subset=["geom_hash", "geometry"]),
            columns=["geom_hash"],
            ).drop("geometry_left", "geometry_right")
    )


soil_moisture_geoms_parcels_mapping = DerivedDatasetDeleteMatching(
    name="soil_moisture_geoms_parcels_mapping",
    level0="silver",
    level1="nsidc",
    restricted=False,
    is_geo=False,
    dependencies=[reference_parcels, soil_moisture_smap_s1_granules],
    func=_mapping_func,
    model=GeomsParcelsMappingModel,
    partition_cols=["geom_hash"],
)
"""Dataset with SMAP/S1 granules and their corresponding RPA land parcels.

This lets us know which parcels intersect with which granules so we can process them with zonal stats.

It is partitioned by geometry meaning if a new one is added we can just run that and not have to reprocess every partition.

This has a lot of duplicate geometries. We could not save them here and join in every time, or we could try and dictionary encode...
"""
