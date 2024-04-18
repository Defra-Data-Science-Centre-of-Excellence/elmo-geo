# Databricks notebook source
import geopandas as gpd
from pyspark.sql import functions as F
import elmo_geo

elmo_geo.register()


def summary_gpq(sf, tile='NY96'):
    sdf = spark.read.format('geoparquet').load(sf)

    pdf = sdf.groupby().agg(
        F.expr('COLLECT_SET(ST_GeometryType(geometry)) AS gtypes'),
        F.expr('FIRST(ST_NDims(geometry)) AS dims'),
        F.expr('MEAN(ST_NPoints(geometry)) AS npoints'),
        F.expr('MEAN(CAST(ST_IsEmpty(geometry) AS INT)) AS empty'),
        F.expr('MEAN(CAST(ST_IsSimple(geometry) AS INT)) AS simple'),
        F.expr('MEAN(CAST(ST_IsValid(geometry) AS INT)) AS valid'),
        F.expr('ARRAY(ST_XMin(ST_Envelope_Aggr(geometry)), ST_YMin(ST_Envelope_Aggr(geometry)), ST_XMax(ST_Envelope_Aggr(geometry)), ST_YMax(ST_Envelope_Aggr(geometry))) AS bbox'),
        F.expr('SUM(ST_Area(geometry))/10000 AS ha'),
        F.expr('SUM(ST_Length(geometry)) AS m'),
    ).toPandas()
    displayHTML(pdf.T._repr_html_())

    gdf = sdf.filter(f'sindex LIKE "{tile}%"').select('geometry').toPandas().pipe(gpd.GeoDataFrame)
    gdf.plot(figsize=[16,16], alpha=.3, edgecolor='k', linewidth=2, markersize=5, marker='*').axis('off');


# COMMAND ----------

summary_gpq('dbfs:/mnt/lab/restricted/ELM-Project/ods/os-water-2022.parquet')

# COMMAND ----------

summary_gpq('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet')

# COMMAND ----------

summary_gpq('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-2023_12_13.parquet')

# COMMAND ----------

summary_gpq('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-2023_12_13.parquet')
