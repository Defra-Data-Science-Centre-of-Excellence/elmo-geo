from pyspark.sql import functions as F


def load_missing(column: str) -> callable:
    """Returns a string that replace NULL with GeometryNULL
    Useful for non-inner joins.
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    return F.expr(f"COALESCE({column}, {null})")


def load_geometry(
    column: str = "geometry",
    encoding_fn: str = "ST_GeomFromWKB",
    geometry_dim: int | None = None,
    subdivide: bool = False,
) -> callable:
    """Load Geometry

    Loads and cleans geometries.

    Parameters:
        column: The name of the geometry column to load.
        encoding_fn = Function to load geometries with. Either "ST_GeomFromWKB" or "ST_GeomFromWKB" or "" to apply cleaning to pre-loaded geometries.
        geometry_dim: Geometry type to extract from collection. 1 for Point, 2 for LineString, 3 for Polygon.
        subdivide: Creates multiple optimised geometries with new rows.
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f"ST_MakeValid({encoding_fn}({column}))"
    expr = f"COALESCE({expr}, {null})"
    expr = f"ST_MakeValid(ST_Force_2D({expr}))"
    expr = f"ST_MakeValid(ST_SimplifyPreserveTopology({expr}, 1))"
    expr = f"ST_MakeValid(ST_ReducePrecision({expr}, 0))"  # DONE: Required for new Sedona (1.4.1 > 1.6.1)
    expr = f"ST_MakeValid(ST_CollectionExtract({expr}, {geometry_dim}))" if geometry_dim else expr
    expr = f"ST_SubDivideExplode({expr}, 256)" if subdivide else expr
    expr = expr + " AS " + column
    return F.expr(expr)


def get_boundary(column: str) -> callable:
    """Get geometry boundaries.

    Returns a function that operates on the input column to produce geometry boundaries.

    Parameters:
        column: The geometry column to convert to geometry boundaries.

    Returns:
        Pyspark sql function
    """
    return load_geometry(column, encoding_fn="ST_Boundary")
