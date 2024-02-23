# Databricks notebook source
import os
import numpy as np
import matplotlib.pyplot as plt
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
# path = '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26'
resolution = 25
bounds = 0, 0, 700_000, 700_000  # gdf.total_bounds


width, height = int((bounds[2] - bounds[0]) / resolution), int((bounds[3] - bounds[1]) / resolution)    
save_kwargs = dict(
    crs = gdf.crs,
    width = width,
    height = height,
    transform = from_bounds(*bounds, width, height),
    count = 1,
    dtype = 'float32',
    driver = 'GTiff',
    compress = 'lzw',
)
band_kwargs = dict(
    merge_alg = rio.enums.MergeAlg.add,
    all_touched = False,
    out_shape = (save_kwargs['height'], save_kwargs['width']),
    transform = save_kwargs['transform'],
    dtype = save_kwargs['dtype'],
)


os.makedirs(path, exist_ok=True)
for idx, col in enumerate(gdf.columns[:-1], start=1):
    f = f'{path}/{col}.geotiff'
    df = gdf[[col, 'geometry']].query(f'0<{col}')
    df[col] *= resolution**2 / df.area
    band = rasterize(zip(df['geometry'].values, df[col].values), **band_kwargs)
    with rio.open(f, 'w', **save_kwargs) as dst:
        dst.write(band, 1)
    print(f'{col}, {df.shape[0]}, {os.path.getsize(f)/2**20:,.1f}MB, {f}')
    # break


rs = rio.open(f).read(1)
plt.imshow(rs)
np.nanmin(rs), np.nanmax(rs)

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26'
# MAGIC cp -r '/tmp/wfm-25m-2024_01_26' '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26'
# MAGIC du -sh '/dbfs/mnt/lab/unrestricted/elm/wfm/wfm-25m-2024_01_26'
