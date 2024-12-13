"""UKCEH Land Cover Plus Fertilisers dataset.

The dataset consists of maps of the predicted average annual application rates (2010-2015) of three different inorganic chemical
fertilisers – nitrogen (N), phosphorus (P) and potassium (K) – in England across a six-year period, along with their respective
estimates of uncertainty, at a 1 km x 1 km resolution.
This data was modelled from Defra British Survey of Fertiliser Practice (BSFP) data using a spatial interpolation procedure.

These maps were created under the ASSIST project to enable exploration of the impacts of agrochemical usage on the environment,
enabling farmers and policymakers to implement better, more sustainable agricultural practices.

Rates as given in kg/km2.
"""
from functools import reduce

import pyspark.sql.functions as F
from pandera import DataFrameModel, Field
from pandera.typing import Float32

from elmo_geo.etl import DerivedDataset, SourceSingleFileRasterDataset
from elmo_geo.etl.transformations import get_centroid_value_from_raster
from elmo_geo.rs.raster import interp_nearest

from .rpa_reference_parcels import reference_parcels

KM2_TO_HA = 100.0  # 100 ha = 1 km2

ukceh_nitrogen_raw = SourceSingleFileRasterDataset(
    name="ukceh_nitrogen_raw",
    medallion="bronze",
    source="ukceh",
    restricted=True,
    source_path="/dbfs/mnt/lab/restricted/ukceh/land_cover_plus_fertilisers_2010_2015/data/fertiliser_n_prediction_uncertainty.tif",
)
"""Dataset definition for UKCEH nitrogen fertiliser application data.

This dataset has 2 bands, the first is the nitrogen prediction in kg/km2, the second is the uncertainty in the same units."""

ukceh_phosphorus_raw = SourceSingleFileRasterDataset(
    name="ukceh_phosphorus_raw",
    medallion="bronze",
    source="ukceh",
    restricted=True,
    source_path="/dbfs/mnt/lab/restricted/ukceh/land_cover_plus_fertilisers_2010_2015/data/fertiliser_p_prediction_uncertainty.tif",
)
"""Dataset definition for UKCEH phosphorus fertiliser application data.

This dataset has 2 bands, the first is the phosphorus prediction in kg/km2, the second is the uncertainty in the same units."""

ukceh_potassium_raw = SourceSingleFileRasterDataset(
    name="ukceh_potassium_raw",
    medallion="bronze",
    source="ukceh",
    restricted=True,
    source_path="/dbfs/mnt/lab/restricted/ukceh/land_cover_plus_fertilisers_2010_2015/data/fertiliser_k_prediction_uncertainty.tif",
)
"""Dataset definition for UKCEH potassium fertiliser application data.

This dataset has 2 bands, the first is the potassium prediction in kg/km2, the second is the uncertainty in the same units."""


class FertilierParcelsModel(DataFrameModel):
    """Model for UKCEH fertiliser data intersected with RPA parcels.

    Attributes:
        id_parcel: The RPA parcel ID including sheet reference.
        kg_km2: The estimated mean application rate for the fertilsier in question from 2010 to 2015, in kg/km2 per annum.
    """

    id_parcel: str = Field(str_matches=r"(^[A-Z]{2}[\d]{8}$)", unique=True)
    kg_km2: Float32 = Field(ge=0.0, nullable=True)


def _calculate_fertiliser(ukceh_fertiliser_raw: SourceSingleFileRasterDataset, reference_parcels: DerivedDataset) -> FertilierParcelsModel:
    """Lookup the UKCEH fertiliser value for each parcel in kg/km2, interpolating missing values from the nearest ones."""
    sdf = reference_parcels.sdf().select("id_parcel", "geometry").withColumn("geom_wkb", F.expr("ST_AsEWKB(geometry)"))
    return (
        sdf.withColumn(
            "kg_km2",
            get_centroid_value_from_raster(
                raster_dataset=ukceh_fertiliser_raw,
                raster_processing=lambda ra: interp_nearest(ra.sel(band=1)),
            )(sdf.geom_wkb),
        )
        .drop("geom_wkb", "geometry")
        .toPandas()
    )


ukceh_nitrogen_parcels = DerivedDataset(
    name="ukceh_nitrogen_parcels",
    medallion="silver",
    source="ukceh",
    restricted=True,
    is_geo=False,
    func=_calculate_fertiliser,
    dependencies=[ukceh_nitrogen_raw, reference_parcels],
    model=FertilierParcelsModel,
)
"""A dataset of parcel ids and their mean 2010-2015 nitrogen application rates in kg/km2.

Where UKCEH has NA values the value is interpolated from the nearest available value.
"""

ukceh_phosphorus_parcels = DerivedDataset(
    name="ukceh_phosphorus_parcels",
    medallion="silver",
    source="ukceh",
    restricted=True,
    is_geo=False,
    func=_calculate_fertiliser,
    dependencies=[ukceh_phosphorus_raw, reference_parcels],
    model=FertilierParcelsModel,
)
"""A dataset of parcel ids and their mean 2010-2015 phosphorus application rates in kg/km2.

Where UKCEH has NA values the value is interpolated from the nearest available value.
"""

ukceh_potassium_parcels = DerivedDataset(
    name="ukceh_potassium_parcels",
    medallion="silver",
    source="ukceh",
    restricted=True,
    is_geo=False,
    func=_calculate_fertiliser,
    dependencies=[ukceh_potassium_raw, reference_parcels],
    model=FertilierParcelsModel,
)
"""A dataset of parcel ids and their mean 2010-2015 potassium application rates in kg/km2.

Where UKCEH has NA values the value is interpolated from the nearest available value.
"""


def _combine_fertilisers(
    ukceh_nitrogen_parcels: DerivedDataset, ukceh_phosphorus_parcels: DerivedDataset, ukceh_potassium_parcels: DerivedDataset
) -> DerivedDataset:
    """Combine Nitrogen, Phosphorus and Potassium fertiliser application rates into a single parcel-level dataset in kg/ha."""
    dfs = [
        ds.pdf().assign(**{f"{ds.name.split('_')[1]}_kg_ha": lambda d: d.kg_km2 / KM2_TO_HA}).drop(columns=["kg_km2"])
        for ds in [ukceh_nitrogen_parcels, ukceh_phosphorus_parcels, ukceh_potassium_parcels]
    ]
    return reduce(lambda df1, df2: df1.merge(df2, on="id_parcel", validate="one_to_one"), dfs)


ukceh_fertilisers_parcels = DerivedDataset(
    name="ukceh_fertilisers_parcels",
    medallion="silver",
    source="ukceh",
    restricted=True,
    is_geo=False,
    func=_combine_fertilisers,
    dependencies=[ukceh_nitrogen_parcels, ukceh_phosphorus_parcels, ukceh_potassium_parcels],
)
"""A dataset of parcel ids and their mean 2010-2015 nitrogen, phosphorus and potassium application rates in kg/ha.

Where UKCEH has NA values the value has been interpolated from the nearest available value.
"""
