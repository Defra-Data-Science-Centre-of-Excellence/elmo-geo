from pyspark.sql import functions as F
from databricks.sdk.runtime import display


def gtypes(sdf:SparkDataFrame, col:str='geometry'):
  display(sdf
    .select(F.expr('ST_GeometryType({col}) AS gtype'))
    .groupby('gtype')
    .count()
  )
