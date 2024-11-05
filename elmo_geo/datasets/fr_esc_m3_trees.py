"""Ecological Site Classification M3 tree suitability and carbon sequestration outputs.

These datasets are the outputs of Forest Research's Ecological Site Classification (ESC)[^1]
tree suitability and yield class models. They provide suitability suitability scores for different
tree species in 16 different scenarios (defined by four Representative Concentration Pathway (RCP)
climate scenarios and four time periods). The outputs are reported at 1km resolution corresponding to
the Ordnance Survey National Grid reference system.

This module also defines derived datasets that aggregate the 1km grid ESC trees outputs to RPA
parcels.

# Source dataset details

Full ESC scenarios documentation is available on SharePoint: [EVAST M3 Woodland Scenarios and Methods](https://defra.sharepoint.com/:w:/r/teams/Team1645/Evidence%20and%20Analysis%20WS/4.9_Workstream_Areas_Modelling_Strategy/4.9.7%20Modelling%20Strategy%20Documentation/Incoming/20240712%20-%20Amy%20Thomas%20-%20ESC%20tree%20data/ESC%20Trees%20Documentation%20Update%202024-07-31/EVAST%20M3%20Woodland%20Scenarios%20%26%20Method.docx?d=w48df3b0d3233438d8252a568155bf08a&csf=1&web=1&e=c5n9XP).
The following paraphrases this document.

There are two components to the outputs:
- tree suitability and yield scores (produced by ESC)
- carbon storage (produced by the CARBINE model)

Tree suitability and yield class scores are modelled based on CHESS-SCAPE UKCP18 forecasts under different
climate scenarios (RCP scenarios 2.6, 4.5, 6.0 and 8.5). A single score for each is given for each 20 year
time period from 1980-2000 to 2060-2080.

ESC suitability and yield class values for each species are used to simulate forest management scenarios, and model
tree and stand growth with CARBINE. There are four time periods over which change in carbon stored is modelled,
from 2021 to 2028, 2036, 2050, and 2100. The average species suitability and yield scores over the time period is used
for selecting the three most suitable/highest yielding species are at 1km grid for each forecast.

The woodland type scenario defines the long list of potential tree species (from which the three most suitable species
are chosen) and the woodland management prescription. This results in each woodland type, RCP scenario, time period and
1km grid being assigned three tree species, from which carbon sequestration is modelled. Woodland type and RCP categories
are split between source data files, with each file containing all potential time periods and 1km grids.

Carbon storage values are provided for two time periods. The whole time period from 2021, and the section
of the time period corresponding to a single suitability and yield class score (either 2021-2028, 2029-2036,
2037-2051, or 2051-2100). Values are maximum potential carbon sequestration, negative values represent net
sequestration and positive values net emissions.

# Aggregation to parcels methodology

The raw ESC model outputs for carbon and tree species are aggregated to parcels following the methodology used by EVAST:
[Carbon values](https://defra.sharepoint.com/:w:/r/teams/Team1645/Evidence%20and%20Analysis%20WS/4.9_Workstream_Areas_Modelling_Strategy/4.9.7%20Modelling%20Strategy%20Documentation/Incoming/20240712%20-%20Amy%20Thomas%20-%20ESC%20tree%20data/ESC%20Trees%20Documentation%20Update%202024-07-31/MethodNote_AggregationOfWoodlandCarbonToParcelLevel%20-%20Copy.docx?d=w5fb03e4998fa4dc2aef9b83850c339e3&csf=1&web=1&e=hUdTef)
[Species values](https://defra.sharepoint.com/:w:/r/teams/Team1645/Evidence%20and%20Analysis%20WS/4.9_Workstream_Areas_Modelling_Strategy/4.9.7%20Modelling%20Strategy%20Documentation/Incoming/20240712%20-%20Amy%20Thomas%20-%20ESC%20tree%20data/ESC%20Trees%20Documentation%20Update%202024-07-31/MethodNote_AggregationOfSpeciesDataToParcelLevel.docx?d=w0f27d6ebe52e450697d1aa0d661f0fcb&csf=1&web=1&e=scqSBn)

There are three main steps to the methodology:
1. Remove peat areas from parcel geometries.
2. Intersect parcel geoemtries with the 1km BNG grid
3. Assign parcels the weighted average of ESC outputs, based on the proportion of each 1km grid overlapping the parcel.

Steps 1. and 2. are implemented by producing the `os_bng_no_peat_parcels` dataset. Step 3. is performed separately for
the carbon and species varables, producing the `esc_species_parcels` and `esc_carbon_parcels` datasets.

[^1] [Forest Research - Ecological Site Classification](https://www.forestresearch.gov.uk/tools-and-resources/fthr/ecological-site-classification)
"""


import geopandas as gpd
import pandas as pd
import pyspark.sql.functions as F
from pandera import DataFrameModel, Field
from pandera.dtypes import Category, Int8, Int16, Int32
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import Dataset, DerivedDataset, SourceGlobDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion, sjoin_parcels
from elmo_geo.st.join import sjoin
from elmo_geo.st.udf import st_clean
from elmo_geo.utils.types import PandasDataFrame, SparkDataFrame

from .os import os_bng_raw
from .peat import peaty_soils_raw
from .rpa_reference_parcels import reference_parcels


class ESCM3WoodlandScenariosRaw(DataFrameModel):
    """Variables in the raw ESC M3 woodland scenario datasets.

    Attributes:
    X_BNG: OSGB Easting x-coordinate. (EPSG: 27700) lower left corner of grid cell
    Y_BNG: OSGB Northing y-coordinate. (EPSG: 27700) lower left corner of grid cell
    species_1: Tree species acronym - 1st most suitable or highest yielding, depending on selection criteria
    species_2: Tree species acronym - 2nd most suitable or highest yielding, depending on selection criteria
    species_3: Tree species acronym - 3rd most suitable or highest yielding, depending on selection criteria
    area_1: Percentage of 1km grid square allocated to planting of species 1
    area_2: Percentage of 1km grid square allocated to planting of species 2
    area_3: Percentage of 1km grid square allocated to planting of species 3
    open_space: Percentage of 1km grid square allocated to open space (no tree planting)
    yield_class_1: ESC yield class for species 1
    yield_class_2: ESC yield class for species 2
    yield_class_3: ESC yield class for species 3
    suitability_1: ESC suitability score for species 1
    suitability_2: ESC suitability score for species 2
    suitability_3: ESC suitability score for species 3
    period_AA_T1: Time periods for annual average (AA) and T1 carbon values
    period_AA_T1_duration: Number of years in each time period (AA_T1)
    AA_grass: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use grassland in tCO2/ha.
        This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon`.
    AA_crop: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use cropland in tCO2/ha.
        This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon`.
    AA_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, annual average over time period (period_AA_T1), previous
        land use grassland in tCO2/ha. This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon` +
        `wood_product_carbon_ipcc`.
    AA_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, annual average over time period (period_AA_T1), previous
        land use cropland in tCO2/ha. This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon` +
        `wood_product_carbon_ipcc`.
    T1_grass: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use grassland in tCO2/ha.
        This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon`) * `period_AA_T1_duration`.
    T1_crop: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use cropland in tCO2/ha.
        This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon`) * `period_AA_T1_duration`.
    T1_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use grassland
        in tCO2/ha. This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon` + `wood_product_carbon_ipcc`) *
        `period_AA_T1_duration`.
    T1_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use cropland
        in tCO2/ha. This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon` + `wood_product_carbon_ipcc`) *
        `period_AA_T1_duration`.
    period_T2: Time periods for T2 carbon values: 2021_2028, 2021_2036, 2021_2050, 2021_2100
    period_T2_duration: Number of years in each time period (T2): 8, 16, 30, 80
    T2_grass: Carbon stored in trees, litter, deadwood & soil, cumulative sum of T1 carbon values
        over time period (period_T2), previous land use grassland
    T2_crop: Carbon stored in trees, litter, deadwood & soil, cumulative sum of T1 carbon values
        over time period (period_T2), previous land use cropland
    T2_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
        cumulative sum of T1 carbon values over time period (period_T2), previous land use grassland
    T2_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
        cumulative sum of T1 carbon values over time period (period_T2), previous land use cropland
    tree_carbon: carbon sequestered into standing trees tCO2/ha average over period AA T1
    litter_carbon: carbon sequestered into litter tCO2/ha average over period AA T1
    deadwood_carbon: carbon sequestered into deadwood tCO2/ha average over period AA T1
    grass_soil_carbon: carbon sequestered into soil (previously land use grassland) tCO2/ha average over period AA T1
    crop_soil_carbon: carbon sequestered into soil (previous land use cropland) tCO2/ha average over period AA T1
    wood_product_carbon_ipcc: carbon sequestered into wood products tCO2/ha average over period AA T1
    """

    X_BNG: int = Field()
    Y_BNG: int = Field()
    species_1: str = Field(nullable=True)
    species_2: str = Field(nullable=True)
    species_3: str = Field(nullable=True)
    area_1: float = Field(ge=0, le=1)
    area_2: float = Field(ge=0, le=1)
    area_3: float = Field(ge=0, le=1)
    open_space: float = Field(ge=0, le=1)
    yield_class_1: float = Field(nullable=True)
    yield_class_2: float = Field(nullable=True)
    yield_class_3: float = Field(nullable=True)
    suitability_1: float = Field(ge=0, le=1, nullable=True)
    suitability_2: float = Field(ge=0, le=1, nullable=True)
    suitability_3: float = Field(ge=0, le=1, nullable=True)
    period_AA_T1: str = Field()
    period_AA_T1_duration: int = Field()
    AA_grass: float = Field()
    AA_crop: float = Field()
    AA_grass_wood: float = Field()
    AA_crop_wood: float = Field()
    T1_grass: float = Field()
    T1_crop: float = Field()
    T1_grass_wood: float = Field()
    T1_crop_wood: float = Field()
    period_T2: str = Field()
    period_T2_duration: int = Field()
    T2_grass: float = Field()
    T2_crop: float = Field()
    T2_grass_wood: float = Field()
    T2_crop_wood: float = Field()
    tree_carbon: float = Field()
    litter_carbon: float = Field()
    deadwood_carbon: float = Field()
    grass_soil_carbon: float = Field()
    crop_soil_carbon: float = Field()
    wood_product_carbon_ipcc: float = Field()


esc_m3_raw = SourceGlobDataset(
    level0="bronze",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    model=ESCM3WoodlandScenariosRaw,
    name="esc_m3_raw",
    glob_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_1km/EVAST_M3_*_rcp*.csv",
)
"""ESC M3 Trees raw dataset. Uses the SourceGlobDataset class to load and union multiple
csv files, each containing model outputs for a different woodland type and representative
concentration pathway (RCP).
"""


def _transform(os_bng_raw, esc_m3_raw):
    """Joins source ESC data to the BNG 1km grid to provide a geometry for the ESC outputs.

    Also extracts woodland type and rcp values from the _path field.
    """
    sdf_esc = (
        esc_m3_raw.sdf()
        .withColumn("name", F.expr("split(_path, '/')[8]"))
        .withColumn("woodland_type", F.expr("SUBSTRING(name, 10, LENGTH(name)-19)"))
        .withColumn("rcp", F.expr("LEFT(RIGHT(name, 6),2)"))
        .drop("name", "_path")
    )

    # Get lookup from esc coordinate to grid cell geometry
    # displace ESC coordinates by +1 in xy position to move coordinate off bottom left corner
    sdf_lu = (
        sdf_esc.selectExpr("XY_BNG", "ST_Point(X_BNG+1, Y_BNG+1) as geometry")
        .dropDuplicates()
        .transform(sjoin, os_bng_raw.sdf().filter(F.expr("layer='1km_grid'")))
        .drop("geometry_left")
        .withColumnRenamed("geometry_right", "geometry")
    )

    # Check lookup
    assert not sdf_lu.select("tile_name").toPandas()["tile_name"].duplicated().any()

    return sdf_esc.join(sdf_lu, on="XY_BNG", how="left")


class ESCGeoModel(DataFrameModel):
    """ESC M3 Trees model outputs combined data model.

    Data model only validates the additional columns produced by combining
    the source datasets and joining grid tile geometries.

    Attributes:
        woodland_type: The type of woodland being modelled.
        rcp: The Representative Concentration Pathway being modelled.
        tile_name: Name of the grid reference tile.
        geometry: Geometry of the tile.
    """

    woodland_type: str = Field(
        isin=[
            "native_broadleaved",
            "productive_conifer",
            "wood_pasture",
            "riparian",
            "silvoarable",
        ],
    )
    rcp: str = Field(isin=["26", "45", "60", "85"])
    tile_name: str = Field()
    geometry: Geometry = Field()


esc_m3_geo = DerivedDataset(
    name="esc_m3_geo",
    level0="silver",
    level1="forest_research",
    restricted=False,
    dependencies=[os_bng_raw, esc_m3_raw],
    func=_transform,
    model=ESCGeoModel,
    partition_cols=["woodland_type", "rcp", "period_T2"],
)
"""ESC M3 Trees model outputs joined to 1km OS BNG tile geometries.
"""


def _sjoin_bng_to_no_peat_parcel(
    reference_parcels: Dataset,
    peaty_soils_raw: Dataset,
    os_bng_raw: Dataset,
) -> PandasDataFrame:
    """Produces dataframe linking parcels to 1km BNG tiles.

    Combines parcels, peat and OS BNG geometries to identify the proportion of non-peat parcel area
    intersected by different 1km BNG grid tiles.

    These proportions are used to aggregate ESC M3 outputs to parcel level, since the ESC-CARBINE model
    outputs results for 1km grid tiles.
    """

    def _udf_difference(pdfs):
        """Get the parcel geometry excluding peaty soils"""
        for pdf in pdfs:
            yield (
                pdf.assign(geometry=gpd.GeoSeries.from_wkb(pdf["geometry_left"]).difference(gpd.GeoSeries.from_wkb(pdf["geometry_right"])).to_wkb()).reindex(
                    columns=["id_parcel", "geometry"]
                )
            )

    # parcels intersecting peat
    sdf_peat = (
        reference_parcels.sdf()
        .select("id_parcel", "geometry")
        .transform(sjoin_parcels, peaty_soils_raw.sdf().transform(st_clean))
        .selectExpr(
            "id_parcel",
            "ST_AsBinary(geometry_left) as geometry_left",
            "ST_AsBinary(geometry_right) as geometry_right",
        )
        .mapInPandas(_udf_difference, schema="id_parcel:string,geometry:binary")
        .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
        .transform(st_clean)
        .withColumn("nopeat_area", F.expr("ST_Area(geometry)/10000"))
    )

    # all parcels
    sdf_other = (
        reference_parcels.sdf()
        .join(sdf_peat.select("id_parcel", "nopeat_area"), on="id_parcel", how="left")
        .filter("nopeat_area IS NULL")
        .selectExpr("id_parcel", "geometry", "area_ha as nopeat_area")
    )

    return sjoin_parcel_proportion(sdf_other.unionByName(sdf_peat), os_bng_raw.sdf().filter("layer='1km_grid'"), columns=["tile_name", "nopeat_area"])


class NoPeatParcelBNGModel(DataFrameModel):
    """Proportion of no-peat parcel geometries intersected by BNG 1km grids.

    Attributes:
        id_parcel: Parcel ID
        nopeat_area: Geographic area of parcel excluding intersecting peaty soils geometries, in hectares.
        tile_name: Name of 1km OS BNG tile intersected the parcel.
        proportion: Proportion of parcel no peat area intersected by 1km BNG tile.
    """

    id_parcel: str = Field()
    nopeat_area: float = Field()
    tile_name: str = Field()
    proportion: float = Field(ge=0, le=1)


os_bng_no_peat_parcels = DerivedDataset(
    name="os_bng_no_peat_parcels",
    level0="silver",
    level1="os",
    restricted=False,
    is_geo=False,
    dependencies=[
        reference_parcels,
        peaty_soils_raw,
        os_bng_raw,
    ],
    func=_sjoin_bng_to_no_peat_parcel,
    model=NoPeatParcelBNGModel,
)
"""Proportion of non-peat parcels intersected by BNG 1km tiles.

Used as an input to the ESC M3 data aggregation to parcels process.
"""


def _join_esc_outputs(
    sdf_parcel_tiles: SparkDataFrame,
    sdf_esc: SparkDataFrame,
) -> SparkDataFrame:
    """Joins parcels to ESC outputs using 1km BNG tiles.

    Exludes ESC data for tiles with have missing carbon values, as per EVAST methodology.
    """
    return (
        sdf_parcel_tiles.join(sdf_esc.drop("geometry", "layer"), on="tile_name")
        .join(
            sdf_esc.filter("(AA_grass=0) OR (AA_crop=0) OR (AA_grass_wood=0) OR (AA_crop_wood=0)")
            .selectExpr("tile_name", "rcp", "woodland_type", "TRUE AS missing_data")
            .dropDuplicates(),
            on=["tile_name", "rcp", "woodland_type"],
            how="left",
        )
        .filter("missing_data IS NULL")
    )


def _aggregate_carbon_values(sdf_parcel_esc: SparkDataFrame) -> SparkDataFrame:
    """Aggregate ESC carbon values to parcels by calcualting the weighted sum across 1km tiles."""
    groupby_cols = [
        "id_parcel",
        "nopeat_area",
        "woodland_type",
        "rcp",
        "period_AA_T1",
        "period_T2",
        "period_AA_T1_duration",
        "period_T2_duration",
    ]
    value_cols = [
        "tree_carbon",
        "litter_carbon",
        "deadwood_carbon",
        "grass_soil_carbon",
        "crop_soil_carbon",
        "wood_product_carbon_ipcc",
        "AA_grass",
        "AA_crop",
        "AA_grass_wood",
        "AA_crop_wood",
        "T1_grass",
        "T1_crop",
        "T1_grass_wood",
        "T1_crop_wood",
        "T2_grass",
        "T2_crop",
        "T2_grass_wood",
        "T2_crop_wood",
    ]
    return (
        sdf_parcel_esc.repartition(*groupby_cols)
        .groupby(*groupby_cols)
        .agg(
            *[F.expr(f"ROUND(SUM(proportion * {c}), 5) as {c}") for c in value_cols],
            F.array_join(F.collect_list("tile_name"), "-").alias("tiles"),
        )
    )


def _aggregate_species_values(sdf_parcel_esc: SparkDataFrame) -> SparkDataFrame:
    """Aggregate ESC spcies area, yield class and suitability scores to parcels by calculating the weighted sum across 1km tiles."""
    groupby_cols = [
        "id_parcel",
        "nopeat_area",
        "woodland_type",
        "rcp",
        "period_AA_T1",
        "period_T2",
        "period_AA_T1_duration",
        "period_T2_duration",
    ]
    species_cols = [
        "area",
        "yield_class",
        "suitability",
    ]
    return (
        sdf_parcel_esc.repartition(*groupby_cols)
        .selectExpr(
            *groupby_cols,
            "tile_name",
            "proportion",
            """
            stack(4, 
                species_1, area_1, yield_class_1, suitability_1, 
                species_2, area_2, yield_class_2, suitability_2, 
                species_3, area_3, yield_class_3, suitability_3,
                'OPENSPACE', open_space, NULL, NULL) as (species, area, yield_class, suitability)
            """,
        )
        .filter("species IS NOT NULL")
        .groupby(*groupby_cols, "species")
        .agg(
            *[F.expr(f"ROUND(SUM(proportion * {c}),5) as {c}") for c in species_cols],
            F.array_join(F.collect_list("tile_name"), "-").alias("tiles"),
        )
    )


def _transform_esc_carbon(
    os_bng_no_peat_parcels: Dataset,
    esc_m3: Dataset,
) -> SparkDataFrame:
    """Aggregate ESC carbon values to parcels."""
    return _join_esc_outputs(os_bng_no_peat_parcels.sdf(), esc_m3.sdf()).transform(_aggregate_carbon_values)


def _transform_esc_species(
    os_bng_no_peat_parcels: Dataset,
    esc_m3: Dataset,
) -> SparkDataFrame:
    """Aggregate ESC carbon values to parcels."""
    return _join_esc_outputs(os_bng_no_peat_parcels.sdf(), esc_m3.sdf()).transform(_aggregate_species_values)


class ESCCarbonParcels(DataFrameModel):
    """ESC M3 carbon metrics for parcels data model.

    Attributes:
        id_parcel: Parcel ID
        nopeat_area: Geographic area of parcel excluding intersecting peaty soils geometries.
        woodland_type: Type of woodland modelled.
        rcp: Representating concetration pathway scenario (i.e cliamte change scenario)
        period_AA_T1: Time periods for annual average (AA) and T1 carbon values
        period_T2: period_T2: Time periods for T2 carbon values: 2021_2028, 2021_2036, 2021_2050, 2021_2100
        period_AA_T1_duration: Number of years in each time period (AA_T1)
        period_T2_duration: period_T2_duration: Number of years in each time period (T2): 8, 16, 30, 80
        tiles: Concatenated names of 1km tiles aggregated to this parcel
        AA_grass: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use grassland in tCO2/ha.
            This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon`.
        AA_crop: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use cropland in tCO2/ha.
            This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon`.
        AA_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, annual average over time period (period_AA_T1), previous
            land use grassland in tCO2/ha. This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon` +
            `wood_product_carbon_ipcc`.
        AA_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, annual average over time period (period_AA_T1), previous
            land use cropland in tCO2/ha. This is given by `tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon` +
            `wood_product_carbon_ipcc`.
        T1_grass: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use grassland in tCO2/ha.
            This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon`) * `period_AA_T1_duration`.
        T1_crop: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use cropland in tCO2/ha.
            This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon`) * `period_AA_T1_duration`.
        T1_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use
            grassland in tCO2/ha. This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon` + `wood_product_carbon_ipcc`) *
            `period_AA_T1_duration`.
        T1_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use cropland
            in tCO2/ha. This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon` + `wood_product_carbon_ipcc`) *
            `period_AA_T1_duration`.
        T2_grass: Carbon stored in trees, litter, deadwood & soil, cumulative sum of T1 carbon values
            over time period (period_T2), previous land use grassland
        T2_crop: Carbon stored in trees, litter, deadwood & soil, cumulative sum of T1 carbon values
            over time period (period_T2), previous land use cropland
        T2_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
            cumulative sum of T1 carbon values over time period (period_T2), previous land use grassland
        T2_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
            cumulative sum of T1 carbon values over time period (period_T2), previous land use cropland
        tree_carbon: carbon sequestered into standing trees tCO2/ha average over period AA T1
        litter_carbon: carbon sequestered into litter tCO2/ha average over period AA T1
        deadwood_carbon: carbon sequestered into deadwood tCO2/ha average over period AA T1
        grass_soil_carbon: carbon sequestered into soil (previously land use grassland) tCO2/ha average over period AA T1
        crop_soil_carbon: carbon sequestered into soil (previous land use cropland) tCO2/ha average over period AA T1
        wood_product_carbon_ipcc: carbon sequestered into wood products tCO2/ha average over period AA T1
    """

    id_parcel: str = Field()
    nopeat_area: float = Field()
    woodland_type: str = Field(
        isin=[
            "productive_conifer",
            "native_broadleaved",
            "riparian",
            "silvoarable",
            "wood_pasture",
        ]
    )
    rcp: Int32 = Field(isin=[26, 45, 60, 85])
    period_AA_T1: str = Field(
        isin=[
            "2021_2028",
            "2029_2036",
            "2037_2050",
            "2051_2100",
        ]
    )
    period_T2: str = Field(
        isin=[
            "2021_2050",
            "2021_2100",
            "2021_2036",
            "2021_2028",
        ]
    )
    period_AA_T1_duration: int = Field()
    period_T2_duration: int = Field()
    tree_carbon: float = Field()
    litter_carbon: float = Field()
    deadwood_carbon: float = Field()
    grass_soil_carbon: float = Field()
    crop_soil_carbon: float = Field()
    wood_product_carbon_ipcc: float = Field()
    AA_grass: float = Field()
    AA_crop: float = Field()
    AA_grass_wood: float = Field()
    AA_crop_wood: float = Field()
    T1_grass: float = Field()
    T1_crop: float = Field()
    T1_grass_wood: float = Field()
    T1_crop_wood: float = Field()
    T2_grass: float = Field()
    T2_crop: float = Field()
    T2_grass_wood: float = Field()
    T2_crop_wood: float = Field()


esc_carbon_parcels = DerivedDataset(
    name="esc_carbon_parcels",
    level0="silver",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    dependencies=[
        os_bng_no_peat_parcels,
        esc_m3_geo,
    ],
    func=_transform_esc_carbon,
    model=ESCCarbonParcels,
)
"""ESC M3 carbon metrics for parcels.
"""


class ESCCarbonParcels50YrTotals(DataFrameModel):
    """ESC M3 carbon metrics for parcels data model with an additional 2021-2071 T1 time period
    and filtered to only RCP 4.5 and 'native_broadleaved' and 'productive_conifer' woodland types.

    Attributes:
        id_parcel: Parcel ID
        nopeat_area: Geographic area of parcel excluding intersecting peaty soils geometries.
        woodland_type: Type of woodland modelled.
        rcp: Representative Concentration Pathway scenario (i.e climate change scenario). Fixed to 4.5 (45).
        period_AA_T1: Time periods for annual average (AA) and T1 carbon values
        period_AA_T1_duration: Number of years in each time period (AA_T1)
        tiles: Concatenated names of 1km tiles aggregated to this parcel
        AA_grass: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use grassland
        AA_crop: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use cropland
        AA_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
            annual average over time period (period_AA_T1), previous land use grassland
        AA_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
            annual average over time period (period_AA_T1), previous land use cropland
        AA_wood_only: Carbon stored in harvested wood products, annual average over time period (period_AA_T1). This is equivalent to `wood_product_carbon_ipcc`
            and does not vary between grassland and cropland.
        T1_grass: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use grassland in tCO2/ha.
            This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon`) * `period_AA_T1_duration`.
        T1_crop: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use cropland in tCO2/ha.
            This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon`) * `period_AA_T1_duration`.
        T1_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use
            grassland in tCO2/ha. This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `grass_soil_carbon` + `wood_product_carbon_ipcc`) *
            `period_AA_T1_duration`.
        T1_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use cropland
            in tCO2/ha. This is given by (`tree_carbon` + `litter_carbon` + `deadwood_carbon` + `crop_soil_carbon` + `wood_product_carbon_ipcc`) *
            `period_AA_T1_duration`.
        T1_wood_only: Carbon stored in harvested wood products, sum over time period (period_AA_T1), previous land use cropland in tCO2/ha.
            This is given by `wood_product_carbon_ipcc` * `period_AA_T1_duration`. This does not vary between grassland and cropland.
    """

    id_parcel: str = Field()
    nopeat_area: float = Field()
    woodland_type: Category = Field(
        isin=[
            "productive_conifer",
            "native_broadleaved",
        ],
        coerce=True,
    )
    rcp: Int8 = Field(isin=[45], coerce=True)
    period_AA_T1: str = Field(
        isin=[
            "2021_2028",
            "2029_2036",
            "2037_2050",
            "2051_2100",
            "2021_2071",
        ],
        coerce=True,
    )
    period_AA_T1_duration: Int16 = Field(coerce=True)
    AA_grass: float = Field()
    AA_crop: float = Field()
    AA_grass_wood: float = Field()
    AA_crop_wood: float = Field()
    AA_wood_only: float = Field()
    T1_grass: float = Field()
    T1_crop: float = Field()
    T1_grass_wood: float = Field()
    T1_crop_wood: float = Field()
    T1_wood_only: float = Field()


def _add_50_year_carbon_totals_and_filter(esc_carbon_parcels: DerivedDataset) -> pd.DataFrame:
    sdf = esc_carbon_parcels.sdf().selectExpr(
        "id_parcel",
        "nopeat_area"
        "rcp",
        "woodland_type",
        "period_AA_T1",
        "period_AA_T1_duration",
        "AA_grass",
        "AA_crop",
        "AA_grass_wood",
        "AA_crop_wood",
        "T1_grass",
        "T1_crop",
        "T1_grass_wood",
        "T1_crop_wood",
        "wood_product_carbon_ipcc AS AA_wood_only",
        "wood_product_carbon_ipcc * period_AA_T1_duration AS T1_wood_only",
    )

    aa_cols = ["AA_grass", "AA_crop", "AA_grass_wood", "AA_crop_wood", "AA_wood_only"]
    return (
        sdf.unionByName(
            sdf.withColumn("period_T1_duration_weight", F.expr("CASE period_AA_T1 WHEN '2051_2100' THEN 20/50 ELSE 1 END"))
            .groupby("id_parcel", "rcp", "woodland_type")
            .agg(
                F.first("nopeat_area")
                F.expr("'2021_2071' AS period_AA_T1"),
                F.expr("50 AS period_AA_T1_duration"),
                *[F.expr(f"SUM(period_AA_T1_duration * period_T1_duration_weight * {c} / 50) AS {c}") for c in aa_cols],
                *[F.expr(f"SUM(period_AA_T1_duration * period_T1_duration_weight * {c}) AS {c.replace('AA', 'T1')}") for c in aa_cols],
            ),
            allowMissingColumns=False,
        )
        .filter("(rcp=45) AND (woodland_type IN ('productive_conifer', 'native_broadleaved'))")
        .toPandas()
    )


esc_carbon_parcels_w_50yr_total = DerivedDataset(
    name="esc_carbon_parcels_w_50yr_total",
    level0="gold",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    dependencies=[esc_carbon_parcels],
    func=_add_50_year_carbon_totals_and_filter,
    model=ESCCarbonParcels50YrTotals,
)
"""ESC M3 carbon metrics used in elmo, including totals for the 50 year perios 2021-2071, filtere to the RCP 4.5
climate scenario and 'native_broadleaved' and 'productive_conifer' woodland types.
"""


class ESCSpeciesParcels(DataFrameModel):
    """ESC M3 tree species metrics for parcels data model.

    Attributes:
        id_parcel: Parcel ID
        nopeat_area: Geographic area of parcel excluding intersecting peaty soils geometries.
        woodland_type: Type of woodland modelled.
        rcp: Representating concetration pathway scenario (i.e cliamte change scenario)
        period_AA_T1: Time periods for annual average (AA) and T1 carbon values
        period_T2: period_T2: Time periods for T2 carbon values: 2021_2028, 2021_2036, 2021_2050, 2021_2100
        period_AA_T1_duration: Number of years in each time period (AA_T1)
        period_T2_duration: period_T2_duration: Number of years in each time period (T2): 8, 16, 30, 80
        tiles: Concatenated names of 1km tiles aggregated to this parcel
        species: Tree species. OPENSPACE refers to open space where trees are not growing.
        area: Proportion of no peat parcel area covered by this tree species.
        yield_class: Tree species yield score.
        suitability: Tree speices suitability score.
    """

    id_parcel: str = Field()
    nopeat_area: float = Field()
    woodland_type: str = Field(
        isin=[
            "productive_conifer",
            "native_broadleaved",
            "riparian",
            "silvoarable",
            "wood_pasture",
        ]
    )
    rcp: Int32 = Field(isin=[26, 45, 60, 85])
    period_AA_T1: str = Field(
        isin=[
            "2021_2028",
            "2029_2036",
            "2037_2050",
            "2051_2100",
        ]
    )
    period_T2: str = Field(
        isin=[
            "2021_2050",
            "2021_2100",
            "2021_2036",
            "2021_2028",
        ]
    )
    period_AA_T1_duration: int = Field()
    period_T2_duration: int = Field()
    species: str = Field()
    area: float = Field(ge=0, le=1)
    yield_class: float = Field(nullable=True)
    suitability: float = Field(ge=0, le=1, nullable=True)


esc_species_parcels = DerivedDataset(
    name="esc_species_parcels",
    level0="silver",
    level1="forest_research",
    restricted=False,
    is_geo=False,
    dependencies=[
        os_bng_no_peat_parcels,
        esc_m3_geo,
    ],
    func=_transform_esc_species,
    model=ESCSpeciesParcels,
)
"""ESC M3 species metrics for parcels.
"""
