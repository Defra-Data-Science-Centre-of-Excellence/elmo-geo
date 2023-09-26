from elmo_geo.utils.dbr import display
from elmo_geo.utils.types import SparkDataFrame
from pyspark.sql import functions as F



def gtypes(sdf:SparkDataFrame, col:str='geometry'):
  display(sdf
    .select(F.expr('ST_GeometryType({col}) AS gtype'))
    .groupby('gtype')
    .count()
  )
