# Databricks notebook source
import os
import geopandas as gpd
import rasterio as rio
from rasterio.features import rasterize
from rasterio.transform import from_bounds
import elmo_geo

elmo_geo.register()

# COMMAND ----------

sdf_parcel = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet')
sdf_wfm = spark.read.parquet('dbfs:/mnt/lab/restricted/ELM-Project/stg/wfm-field-2024_01_26.parquet')

gdf = (sdf_wfm
    .select(
        'id_parcel',
        *(col for col in sdf_wfm.columns if col.startswith(('t_', 'n_'))),
    )
    .join(
        sdf_parcel.selectExpr('id_parcel', 'ST_SimplifyPreserveTopology(geometry, 25) AS geometry'),
        on = 'id_parcel',
    )
    .toPandas()
    .pipe(gpd.GeoDataFrame, crs='epsg:27700')
    .set_index('id_parcel')
)


gdf

# COMMAND ----------

path = '/tmp/wfm-25m-2024_01_26'
resolution = 25
bounds = 0, 0, 700_000, 700_000  # gdf.total_bounds


width, height = int((bounds[2] - bounds[0]) / resolution), int((bounds[3] - bounds[1]) / resolution)
band_names = list(gdf.columns[:-1])
dtype = 'float32'

# os.makedirs(path)
for idx, col in enumerate(band_names, start=1):
    f = f'{path}/{col}.geotiff'
    with rio.open(
        f,
        'w',
        driver = 'GTiff',
        height = height,
        width = width,
        count = 1,
        dtype = dtype,
        crs = gdf.crs,
        transform = from_bounds(*bounds, width, height)
    ) as dst:
        df = gdf[[col, 'geometry']].query(f'0<{col}')
        df[col] *= resolution**2 / df.area
        band = rasterize(
            zip(df['geometry'], df[col].values),
            out_shape = (height, width),
            transform = dst.transform,
            all_touched = False,
            merge_alg = rio.enums.MergeAlg('ADD'),
            dtype = dtype,
        )
        dst.write(band, 1)
        print(f'{col}, {df.shape[0]}, {os.path.getsize(f)/2**20:,.1f}MB')


# COMMAND ----------

# MAGIC %sh
# MAGIC cp -r '/tmp/wfm-25m-2024_01_26.tif' '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26.tif'
# MAGIC cp -r '/tmp/wfm-25m-2024_01_26' '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26'
# MAGIC
# MAGIC du -sh '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26.tif'
# MAGIC du -sh '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26'

# COMMAND ----------

rs = rio.open('/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26/t_peas.geotiff')
rs
