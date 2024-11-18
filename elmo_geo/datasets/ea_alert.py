"""Environment Agency's ALERT[^alert] Datasets.

[^alert]: https://www.farmingadviceservice.org.uk/csf/tools
[^alert_map]: https://experience.arcgis.com/experience/fd8e7ede2548409f965275da8e8d35f7/page/ALERT/
"""
from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset

from .rpa_reference_parcels import reference_parcels


# OLF Source
class OlfRaw(DataFrameModel):
    """Model for EA ALERT probable OverLand Flow pathways (OLF) dataset.
    This dataset is useful for eroasion risk assessment.

    Currently using a pre-released version from Crispin Hambridge directly.

    Attributes:
        geometry: BNG LineStrings
    """

    PermID: int = Field()
    OPERATIONAL_CATCHMENT: str = Field()
    WATERBODY_NAME: str = Field()
    CatchmentRiskDesc: str = Field()
    LandUseRisk: str = Field()
    SlopeRisk: str = Field()
    SoilErosion: str = Field()
    SoilRunoff: str = Field()
    CombinedSoilRisk: str = Field()
    ReceptorDistanceRisk: str = Field()
    MeanRainfalRisk: str = Field()
    # Unused; MajLandUse, MeanSlope, Slope1haWatershed, MinFlowWater, MinFlowRoad, SSSI_Intersect, FlowAccClass.
    MaxFlowAcc: float = Field()
    Shape_Length: float = Field()
    geometry: Geometry(crs=SRID) = Field(nullable=True)


ea_olf_raw = SourceDataset(
    name="ne_olf_raw",
    level0="silver",
    level1="ea",
    model=OlfRaw,
    restricted=True,
    source_path="/dbfs/mnt/lab/restricted/ELM-Project/bronze/ea-overland_flow-2024_06_19_direct.parquet",
)


# OLF Parcel
class OlfParcel(DataFrameModel):
    """OLF erosion model
    OverLand Flow pathway are joined to parcel boundaries, and for distinct pathways the scores are summed.
    """

    id_parcel: str = Field()
    score: int = Field()


def _transform(parcels: Dataset, olf: Dataset):
    parcels.sdf().createOrReplaceTempView("parcel")

    olf.sdf().selectExpr(
        "CAST(SUBSTRING(CatchmentRiskDesc, 1, 2) AS INT) AS CatchmentScore",
        "CAST(SUBSTRING(LandUseRisk, 1, 2) AS INT) AS LandUseScore",
        "CAST(SUBSTRING(SlopeRisk, 1, 2) AS INT) AS SlopeScore",
        "CAST(SUBSTRING(CombinedSoilRisk, 1, 2) AS INT) AS CombinedSoilScore",
        "CAST(SUBSTRING(MeanRainfalRisk, 1, 2) AS INT) AS MeanRainfallScore",
        "NTILE(5) OVER (ORDER BY MaxFlowAcc) AS MaxFlowAccQuintile",
        "(CatchmentScore + LandUseScore + SlopeScore + CombinedSoilScore + MeanRainfallScore + MaxFlowAccQuintile) AS score",
        "geometry",
    ).select(
        "score",
        "geometry",
    ).createOrReplaceTempView("olf")

    return spark.sql(
        """
        SELECT id_parcel, SUM(score) AS score
        FROM parcel JOIN olf
        ON ST_Intersects(parcel.geometry, olf.geometry)
        GROUP BY id_parcel
    """
    )


ea_olf_parcel = DerivedDataset(
    name="ea_olf_parcel",
    level0="gold",
    level1="ea",
    model=OlfParcel,
    restricted=True,
    func=_transform,
    dependencies=[reference_parcels, ea_olf_raw],
    is_geo=False,
)
