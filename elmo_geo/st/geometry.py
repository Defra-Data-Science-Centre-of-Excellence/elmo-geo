from pyspark.sql import functions as F


def load_missing(column: str) -> callable:
    """Returns a string that replace NULL with GeometryNULL
    Useful for non-inner joins.
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    return F.expr(f"COALESCE({column}, {null})")
    # return F.expr(f'NVL({column}, {null})')
    # return F.expr(f'CASE WHEN {column} IS NULL THEN {null} ELSE {column} END')


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
