from pyspark.sql import functions as F


def load_missing(column: str) -> callable:
    """Returns a string that replace NULL with GeometryNULL
    Useful for non-inner joins.
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    return F.expr(f"COALESCE({column}, {null})")


def load_geometry(column: str = "geometry", encoding_fn: str = "ST_GeomFromWKB", geometry_dim: int | None = None) -> callable:
    """Load Geometry

    Loads and cleans geometries.

    Parameters:
      column: The name of the geometry column to load.
      encoding_fn = Function to load geometries with. Either "ST_GeomFromWKB" or "ST_GeomFromWKB" or "" to
      apply claening to pre-loaded geometries.
      geometry_dim: Geometry type to extract from collection. 1 for Point, 2 for LineString, 3 for Polygon.

    """
    null = 'ST_GeomFromText("Point EMPTY")'
    string = f"ST_MakeValid({encoding_fn}({column}))"
    string = f"COALESCE({string}, {null})"
    string = f"ST_MakeValid(ST_Force_2D({string}))"
    string = f"ST_MakeValid(ST_SimplifyPreserveTopology({string}, 1))"
    string = f"ST_MakeValid(ST_PrecisionReduce({string}, 0))"
    string = f"ST_MakeValid(ST_CollectionExtract({string}, {geometry_dim}))" if geometry_dim else string
    string = string + " AS " + column
    return F.expr(string)


def get_boundary(column: str) -> callable:
    """Get geometry boundaries.

    Returns a function that operates on the input column to produce geometry boundaries.

    Parameters:
      column: The geometry column to convert to geometry boundaries.

    Returns:
      Pyspark sql function
    """
    return load_geometry(column, encoding_fn="ST_Boundary")
