from pyspark.sql import functions as F


def load_missing(column:str) -> str:
  '''Returns a string that replace NULL with GeometryNULL
  Useful for non-inner joins.
  '''
  null = 'ST_GeomFromText("Point EMPTY")'
  return F.expr(f'COALESCE({column}, {null})')
  # return F.expr(f'NVL({column}, {null})')
  # return F.expr(f'CASE WHEN {column} IS NULL THEN {null} ELSE {column} END')

def load_geometry(column:str='geometry', from_crs:str='EPSG:27700', encoding_fn:str='ST_GeomFromWKB') -> Callable:
  '''Load Geometry
  Useful for ingesting data.
  Missing
    Check for NULL values and replace with an empty point
  Encoding
    encoding_fn '', 'ST_GeomFromWKB', 'ST_GeomFromWKT'
    Validate
  Standardise
    Force 2D
    Normalise ordering
    CRS = EPSG:27700 = British National Grid, which uses 1m coordinates
  Optimise
    3 = 0.001m = 1mm precision
    0 simplify to precision
  AS column
    simplify column name
  '''
  null = 'ST_GeomFromText("Point EMPTY")'
  to_crs = 'EPSG:27700'
  precision = 3
  string = f'ST_MakeValid({encoding_fn}({column}))'
  string = f'COALESCE({string}, {null})'
  string = f'ST_Transform(ST_Normalize(ST_Force_2D({string})), "{from_crs}", "{to_crs}")'
  string = f'ST_SimplifyPreserveTopology(ST_PrecisionReduce({string}, {precision}), 0)'
  string = string + ' AS ' + column
  return F.expr(string)
