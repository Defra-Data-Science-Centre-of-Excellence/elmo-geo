from pyspark.sql import functions as F


def load_missing(column: str) -> callable:
    """Returns a string that replace NULL with GeometryNULL
    Useful for non-inner joins.
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    return F.expr(f"COALESCE({column}, {null})")


def load_geometry(column: str = "geometry", precision: int = 0, encoding_fn: str = "ST_GeomFromWKB", simplify_tolerance: int = 1) -> callable:
    """Load Geometry
    Useful for ingesting data.
    Missing
      Check for NULL values and replace with an empty point
    Encoding
      encoding_fn; '', 'ST_GeomFromWKB', 'ST_GeomFromWKT'
    Standardise
      Force 2D
      Normalise ordering
    Optimise
      0 = 1m precision
      1 = 1m simplify
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    string = f"ST_MakeValid({encoding_fn}({column}))"
    string = f"COALESCE({string}, {null})"
    string = f"ST_MakeValid(ST_Force_2D({string}))"
    string = f"ST_MakeValid(ST_SimplifyPreserveTopology({string}, {simplify_tolerance}))"
    string = f"ST_MakeValid(ST_PrecisionReduce({string}, {precision}))"
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
