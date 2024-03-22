# Databricks notebook source
# MAGIC %%mosaic_kepler

# COMMAND ----------

import os
import pathlib
import requests
import warnings
from pyspark.sql import functions as F, types as T
import mosaic as mos

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.shuffle.partitions", 1_024)

warnings.simplefilter("ignore")

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

data_dir = f"/tmp/mosaic/{user_name}"
print(f"Initial data stored in '{data_dir}'")


zone_dir = f"{data_dir}/taxi_zones"
zone_dir_fuse = f"/dbfs{zone_dir}"
dbutils.fs.mkdirs(zone_dir)

os.environ['ZONE_DIR_FUSE'] = zone_dir_fuse
print(f"ZONE_DIR_FUSE? '{zone_dir_fuse}'")



zone_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON'

zone_fuse_path = pathlib.Path(zone_dir_fuse) / 'nyc_taxi_zones.geojson'
if not zone_fuse_path.exists():
  req = requests.get(zone_url)
  with open(zone_fuse_path, 'wb') as f:
    f.write(req.content)
else:
  print(f"...skipping '{zone_fuse_path}', already exits.")

display(dbutils.fs.ls(zone_dir))#



neighbourhoods = (
  spark.read
    .option("multiline", "true")
    .format("json")
    .load(zone_dir)
    .select("type", explode(col("features")).alias("feature"))
    .select("type", col("feature.properties").alias("properties"), to_json(col("feature.geometry")).alias("json_geometry"))
    .withColumn("geometry", mos.st_aswkt(mos.st_geomfromgeojson("json_geometry")))
)

print(f"count? {neighbourhoods.count():,}")
neighbourhoods.limit(3).show() # <- limiting + show for ipynb only



display(
  neighbourhoods
    .withColumn("calculatedArea", mos.st_area(col("geometry")))
    .withColumn("calculatedLength", mos.st_length(col("geometry")))
    # Note: The unit of measure of the area and length depends on the CRS used.
    # For GPS locations it will be square radians and radians
    .select("geometry", "calculatedArea", "calculatedLength")
    .limit(3)
    .show() # <- limiting + show for ipynb only
)




trips = (
  spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")
  # - .1% sample
  .sample(.001)
    .drop("vendorId", "rateCodeId", "store_and_fwd_flag", "payment_type")
    .withColumn("pickup_geom", mos.st_astext(mos.st_point(col("pickup_longitude"), col("pickup_latitude"))))
    .withColumn("dropoff_geom", mos.st_astext(mos.st_point(col("dropoff_longitude"), col("dropoff_latitude"))))
  # - adding a row id
  .selectExpr(
    "xxhash64(pickup_datetime, dropoff_datetime, pickup_geom, dropoff_geom) as row_id", "*"
  )
)
print(f"count? {trips.count():,}")
trips.limit(10).display() # <- limiting for ipynb only
     


     
from mosaic import MosaicFrame

neighbourhoods_mosaic_frame = MosaicFrame(neighbourhoods, "geometry")
optimal_resolution = neighbourhoods_mosaic_frame.get_optimal_resolution(sample_fraction=0.75)

print(f"Optimal resolution is {optimal_resolution}")



display(
  neighbourhoods_mosaic_frame.get_resolution_metrics(sample_rows=150)
)




tripsWithIndex = (
  trips
    .withColumn("pickup_h3", mos.grid_pointascellid(col("pickup_geom"), lit(optimal_resolution)))
    .withColumn("dropoff_h3", mos.grid_pointascellid(col("dropoff_geom"), lit(optimal_resolution)))
  # - beneficial to have index in first 32 table cols
  .selectExpr(
    "row_id", "pickup_h3", "dropoff_h3", "* except(row_id, pickup_h3, dropoff_h3)"
  )
)
display(tripsWithIndex.limit(10))





neighbourhoodsWithIndex = (
  neighbourhoods
    # We break down the original geometry in multiple smaller mosaic chips, each with its
    # own index
    .withColumn(
      "mosaic_index", 
      mos.grid_tessellateexplode(col("geometry"), lit(optimal_resolution))
    )

    # We don't need the original geometry any more, since we have broken it down into
    # Smaller mosaic chips.
    .drop("json_geometry", "geometry")
)

print(f"count? {neighbourhoodsWithIndex.count():,}") # <- notice the explode results in more rows
neighbourhoodsWithIndex.limit(5).show()              # <- limiting + show for ipynb only







pickupNeighbourhoods = neighbourhoodsWithIndex.select(col("properties.zone").alias("pickup_zone"), col("mosaic_index"))

withPickupZone = (
  tripsWithIndex.join(
    pickupNeighbourhoods,
    tripsWithIndex["pickup_h3"] == pickupNeighbourhoods["mosaic_index.index_id"]
  ).where(
    # If the borough is a core chip (the chip is fully contained within the geometry), then we do not need
    # to perform any intersection, because any point matching the same index will certainly be contained in
    # the borough. Otherwise we need to perform an st_contains operation on the chip geometry.
    col("mosaic_index.is_core") | mos.st_contains(col("mosaic_index.wkb"), col("pickup_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "pickup_h3", "dropoff_h3"
  )
)

display(withPickupZone.limit(10)) # <- limiting for ipynb only






dropoffNeighbourhoods = neighbourhoodsWithIndex.select(col("properties.zone").alias("dropoff_zone"), col("mosaic_index"))

withDropoffZone = (
  withPickupZone.join(
    dropoffNeighbourhoods,
    withPickupZone["dropoff_h3"] == dropoffNeighbourhoods["mosaic_index.index_id"]
  ).where(
    col("mosaic_index.is_core") | mos.st_contains(col("mosaic_index.wkb"), col("dropoff_geom"))
  ).select(
    "trip_distance", "pickup_geom", "pickup_zone", "dropoff_geom", "dropoff_zone", "pickup_h3", "dropoff_h3"
  )
  .withColumn("trip_line", mos.st_astext(mos.st_makeline(array(mos.st_geomfromwkt(col("pickup_geom")), mos.st_geomfromwkt(col("dropoff_geom"))))))
)

display(withDropoffZone.limit(10)) # <- limiting for ipynb only


displayHTML("""""")


displayHTML("""""")




# %%mosaic_kepler
# withDropoffZone "pickup_h3" "h3" 5000
