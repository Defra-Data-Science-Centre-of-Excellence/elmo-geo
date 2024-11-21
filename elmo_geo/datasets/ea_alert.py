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
        CatchmentRiskDesc: A risk score 1-5, which defines how suseptible the river is to erosion.
        LandUseRisk: A risk score 1-5, which defines the risk due to potential runoff common on this land use.
        SlopeRisk: A risk score 1-4, using both the immediate and local land slope.
        CombinedSoilRisk: A risk score 1-5, for the risk of erosion for this type of soil.
        ReceptorDistanceRisk: A risk score 1-5, for the closeness to a waterbody.
        MeanRainfalRisk: A risk score 1-5, for the amount of rainfall expected in the area.
        MaxFlowAcc: The area in which this particular pathway collects water from.
        geometry: BNG LineStrings
    """

    PermID: int = Field(unique=True)
    OPERATIONAL_CATCHMENT: str = Field()
    WATERBODY_NAME: str = Field()
    CatchmentRiskDesc: str = Field(nullable=True, str_matches=r"^[1-5].")
    LandUseRisk: str = Field(nullable=True, str_matches=r"^[1-5].")
    SlopeRisk: str = Field(nullable=True, str_matches=r"^[1-4].")
    SoilErosion: str = Field(nullable=True, str_matches=r"^[1-6].")
    SoilRunoff: str = Field(nullable=True, str_matches=r"^[1-5].")
    CombinedSoilRisk: str = Field(nullable=True, str_matches=r"^[1-5].")
    ReceptorDistanceRisk: str = Field(nullable=True, str_matches=r"^[1-5].")
    MeanRainfalRisk: float = Field(ge=0, le=5)
    MajLandUse: str = Field()
    MeanSlope: float = Field()
    Slope1haWatershed: float = Field(nullable=True)
    MinFlowWater: float = Field()
    MinFlowRoad: float = Field()
    SSSI_Intersect: float = Field(nullable=True)
    FlowAccClass: str = Field(isin=["1Ha", "1Km", "10Km"])
    MaxFlowAcc: int = Field()
    Shape_Length: float = Field()
    geometry: Geometry(crs=SRID) = Field()


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
    """OLF erosion model"""

    id_parcel: str = Field()
    score: int = Field()


def _transform(parcels: Dataset, olf: Dataset):
    """OLF Scores are"""

    parcels.sdf().createOrReplaceTempView("parcel")

    olf.sdf().selectExpr(
        "CAST(SUBSTRING(CatchmentRiskDesc, 1, 2) AS INT) AS CatchmentQuintile",
        "CAST(SUBSTRING(LandUseRisk, 1, 2) AS INT) AS LandUseQuintile",
        "CAST(SUBSTRING(SlopeRisk, 1, 2) AS INT) AS SlopeQuintile",
        "CAST(SUBSTRING(CombinedSoilRisk, 1, 2) AS INT) AS CombinedSoilQuintile",
        "NTILE(5) OVER (ORDER BY MeanRainfalRisk) AS MeanRainfallQuintile",
        "NTILE(5) OVER (ORDER BY MaxFlowAcc) AS MaxFlowAccQuintile",
        "(CatchmentQuintile + LandUseQuintile + SlopeQuintile + CombinedSoilQuintile + MeanRainfallQuintile + MaxFlowAccQuintile) AS score",
        "geometry",
    ).createOrReplaceTempView("olf")

    return spark.sql(
        """
        SELECT
            id_parcel,
            NTILE(5) OVER (ORDER BY MAX(score)) AS score,
            MAX(CatchmentQuintile) AS CatchmentQuintile,
            MAX(LandUseQuintile) AS LandUseQuintile,
            MAX(SlopeQuintile) AS SlopeQuintile,
            MAX(CombinedSoilQuintile) AS CombinedSoilQuintile,
            MAX(MeanRainfallQuintile) AS MeanRainfallQuintile,
            MAX(MaxFlowAccQuintile) AS MaxFlowAccQuintile
        FROM parcel JOIN olf
        ON ST_Intersects(parcel.geometry, olf.geometry)
        GROUP BY id_parcel
    """
    )


ea_olf_parcels = DerivedDataset(
    name="ea_olf_parcels",
    level0="gold",
    level1="ea",
    model=OlfParcel,
    restricted=True,
    func=_transform,
    dependencies=[reference_parcels, ea_olf_raw],
    is_geo=False,
)
