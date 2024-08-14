"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
import pandas as pd
from pyspark.sql import functions as F

from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import sjoin
from elmo_geo.utils.types import SparkDataFrame

from .etl import Dataset
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry
from pyspark.sql import functions as F

def _calc_proportion(geometry_left:str, geometry_right:str, denominator:str|None=None) -> callable:
    """Calculate the proportion overlap between two geometries.

    Parameters:
        geometry_left: Name of one of the left hand geometry field. 
        geometry_right: Name of one of the right hand geometry field.
        demoninator: Name of the denominator field to use in proportion calculation.
            If none defaults to the area of the geometry_left geometry. 
    """
    if not denominator:
        denominator = f"ST_Area({geometry_left})"
    string = f"ST_Intersection({geometry_left}, {geometry_right})"
    string = f"ST_Area({string}) / {denominator}"
    string = f"LEAST(GREATEST({string}, 0), 1)"
    return F.expr(f"{string} AS proportion")

def sjoin_and_proportion(sdf_parcels:SparkDataFrame, 
                         sdf_features: SparkDataFrame, 
                         columns:list[str],
                         ):
    """Join a parcels data frame to a features dataframe and calculate the 
    proportion of each parcel that is overlapped by features.

    Parameters:
        sdf_parcels: The parcels dataframe.
        sdf_features: The features dataframe.
        columns: Columns in the features dataframe to include in the group by when calculating 
            the proportion value.
    """
    return (
        sjoin(
            (
                sdf_parcels
                .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
            ),
            (
                sdf_features
                .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
            ),
        )
        # join in area, required to handle multi polygons
        .join(
            sdf_parcels.groupby("id_parcel").agg(F.expr("SUM(ST_Area(geometry)) as area_left")),
            on="id_parcel",
        )
        .groupby("id_parcel", "area_left", *columns)
        .agg(
            F.expr("ST_Union_Aggr(geometry_left) AS geometry_left"),
            F.expr("ST_Union_Aggr(geometry_right) AS geometry_right"),
        )
        .withColumn("proportion", _calc_proportion("geometry_left", "geometry_right", "area_left"))
        .drop("geometry_left", "geometry_right")
    )

def join_parcels(
    parcels: Dataset,
    features: Dataset,
    columns: list[str] | None = None,
    simplify_tolerence: float = 1.0,
    max_vertices: int = 256,
) -> pd.DataFrame:
    """Spatial join the two datasets and calculate the proportion of the parcel that intersects.

    Parameters:
        - parcels: The RPA `reference_parcels` `Dataset`
        - features: The dataset to join in, assumed to be comprised of polygons.
        - columns: The columns in `features` to be included (on top of `geometry`).
        - simplify_tolerence: The tolerance to simplify geometries to (in both datasets).
            Defaults to 20m (assuming SRID 27700).
        - max_vertices: The features polygons will be subdivided and exploded to reduce them
            to this number of vertices to improve performance and memory use. Defaults to 256.
        - geometry_dim: Only select geometries of this dimension after intersection.
            0 means any, 1 = Points, 2 = LineStrings, 3 = Polygons, Multi is included.

    Returns:
        - A Pandas dataframe with `id_parcel`, `proportion` and columns included in the `columns` list.
    """
    if columns is None:
        columns = []
    
    sdf_parcels = parcels.sdf()
    sdf_features = (features.sdf()
                    .withColumn("geometry", load_geometry(encoding_fn="", simplify_tolerance=simplify_tolerence))
                    .withColumn("geometry", F.expr(f"ST_SubDivideExplode(geometry, {max_vertices})"))
                    )

    return (sjoin_and_proportion(sdf_parcels, 
                                sdf_features, 
                                columns=columns,
                                )
            .toPandas())
