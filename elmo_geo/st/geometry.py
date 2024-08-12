from pyspark.sql import functions as F


def load_missing(column: str) -> callable:
    """Returns a string that replace NULL with GeometryNULL
    Useful for non-inner joins.
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    return F.expr(f"COALESCE({column}, {null})")
    # return F.expr(f'NVL({column}, {null})')
    # return F.expr(f'CASE WHEN {column} IS NULL THEN {null} ELSE {column} END')


def load_geometry(column: str = "geometry", precision: int = 3, encoding_fn: str = "ST_GeomFromWKB", simplify_tolerence: float = 1.0) -> callable:
    """Load Geometry
    Useful for ingesting data.
    Missing
      Check for NULL values and replace with an empty point
    Encoding
      encoding_fn '', 'ST_GeomFromWKB', 'ST_GeomFromWKT'
    Standardise
      Force 2D
      Normalise ordering
    Optimise
      3 = 0.001m = 1mm precision
      0 simplify to precision
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    string = f"ST_MakeValid({encoding_fn}({column}))"
    string = f"COALESCE({string}, {null})"
    string = f"ST_MakeValid(ST_Force_2D({string}))"
    string = f"ST_MakeValid(ST_PrecisionReduce({string}, {precision}))"
    string = f"ST_MakeValid(ST_SimplifyPreserveTopology({string}, {simplify_tolerence}))"
    string = string + " AS " + column
    return F.expr(string)


def get_boundary(column: str) -> callable:
    """Get geometry boundaries.

    Returns a function that operates on the input column to produce geometry boundaries.

    Parameters:
      coluns: The geometry column to convert to geometry boundaries.

    Returns:
      Pyspark sql function
    """
    return F.expr(
        f"""
                ST_MakeValid(ST_Force_2D(ST_PrecisionReduce(
                    ST_SimplifyPreserveTopology(ST_Boundary({column}), 1),
                     3))) AS geometry_boundary
                """
    )
