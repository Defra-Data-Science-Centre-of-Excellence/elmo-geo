# Databricks notebook source
# MAGIC %sh
# MAGIC make fmt
# MAGIC make verify

# COMMAND ----------

from elmo_geo.utils.settings import FOLDER_STG
from elmo_geo.io.file import convert_file
from elmo_geo.utils.misc import sh_run


def ingest_esri(url, name=None):
    """Download and Convert data stored on an ESRI server"""
    f_in, f_out = FOLDER_STG + f"/{name}.geojson", FOLDER_STG + f"/{name}.parquet"
    exc = f"esri2geojson '{url}' '{f_in}'"
    print(exc)
    # sh_run(exc)
    # convert_file(f_in, f_out)
    # append_to_catalogue(
    #     {
    #         name: {
    #             "url": url,
    #             "filepath_tmp": f_in,
    #             "filepath": f_out,
    #             "function": "ingest_esri",
    #         }
    #     }
    # )


# COMMAND ----------

# from elmo_geo.io.esri import ingest_esri

ingest_esri(
    url = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services/Countries_December_2022_UK_BFC/FeatureServer/0',
    name = 'ons-countries_bfc-2022_12',
)

# COMMAND ----------

# MAGIC %sh
# MAGIC rm '/dbfs/mnt/lab/restricted/ELM-Project/stg/ons-countries_bfc-2022_12.geojson'
# MAGIC esri2geojson 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services/Countries_December_2022_UK_BFC/FeatureServer/0' '/dbfs/mnt/lab/restricted/ELM-Project/stg/ons-countries_bfc-2022_12.geojson'

# COMMAND ----------

# MAGIC %sh
# MAGIC ogr2gpq() {
# MAGIC     # Inputs
# MAGIC     fin=$1
# MAGIC     fout=$2
# MAGIC     # Extra environment variables
# MAGIC     PATH=$PATH:/databricks/miniconda/bin
# MAGIC     TMPDIR=/tmp
# MAGIC     PROJ_LIB=/databricks/miniconda/share/proj
# MAGIC     OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
# MAGIC     # ogr2ogr for all layers
# MAGIC     mkdir -p $fout
# MAGIC     for layer in $(ogrinfo -so $fin | grep -oP '^\\d+: \\K[^ ]*'); do
# MAGIC         ogr2ogr -t_srs EPSG:27700 $fout/$layer $fin $layer
# MAGIC     done
# MAGIC }
# MAGIC
# MAGIC ogr2gpq fin fout

# COMMAND ----------

import contextily as ctx


ctx.add_basemap(ax, source=provider)

# COMMAND ----------

import os

passwords = 'words'.split()
filepath = '/dbfs/FileStore/Parcels4ADAS_gdb.7z'

for password in passwords:
	if os.system(f'7z t {filepath} -p{password}') == 0:
		print(password)
        break


# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/
# MAGIC rm Parcels4ADAS.gdb
# MAGIC ls *

# COMMAND ----------

# MAGIC %sh
# MAGIC PATH=$PATH:/databricks/miniconda/bin
# MAGIC
# MAGIC export TMPDIR=/tmp
# MAGIC export PROJ_LIB=/databricks/miniconda/share/proj
# MAGIC export OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
# MAGIC
# MAGIC fin="/dbfs/FileStore/Parcels4ADAS_gdb.7z"
# MAGIC dout="/dbfs/mnt/lab/restricted/ELM-Project/stg/"
# MAGIC
# MAGIC dtmp="/databricks/driver/tmp.d"
# MAGIC fgdb=$dtmp"/Parcels4ADAS.gdb"
# MAGIC fpq="/databricks/driver/rpa-parcels-adas.parquet"
# MAGIC
# MAGIC 7z x $fin -p"dsaasd" -o$dtmp
# MAGIC ogr2ogr $fpq $fgdb
# MAGIC cp $fpq $dout

# COMMAND ----------

import numpy as np
from scipy.stats import linregress
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

import elmo_geo
elmo_geo.register()


sdf = elmo_geo.io.datasets.load_sdf('rpa-parcels-adas')
df = sdf.select(
    F.expr('Shape_Area/10000 AS `area (ha)`'),
    F.expr('Shape_Lengt AS `perimeter (m)`'),
).toPandas()
df_central = df[abs(df['area']-df['area'].mean()) < df['area'].std()]


display(sdf)

# COMMAND ----------

model = linregress(df['area'], df['length'])
model0 = linregress(df['area']-df['area'].mean(), df['length']-df['length'].mean())

# COMMAND ----------

import numpy as np
help(np.linalg.lstsq)

# COMMAND ----------

g = sns.jointplot(
    data = df_central.iloc[:1_000],
    x = 'area',
    y = 'length',
    kind = 'reg',
    scatter_kws = {'s': 1},
    line_kws = {'color': 'k'},
)

plt.suptitle(f'''Parcel Area vs Perimeter
    OLR:  $length (m) = {model.slope:.1f} \\times area (ha) + {model.intercept:.1f}$
    P: {model.pvalue:.3f},  R²: {model.rvalue:.3f}
''');None

# COMMAND ----------

g = sns.jointplot(
    data = df_central.iloc[:1_000],
    x = 'area',
    y = 'length',
    kind = 'hist',
)
xlim = np.asarray([.0, .9]) * g.ax_joint.get_xlim()[1]
g.ax_joint.plot(xlim, model0.slope*xlim, 'k')
g.ax_joint.grid('on')

plt.suptitle(f'Parcel Area vs Perimeter\nOLR:  $length (m) = {model0.slope:.1f} \\times area (ha)$\nP: {model0.pvalue:.3f},  R²: {model0.rvalue:.3f}')
None

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/lab/unrestricted/elm_data/os/ngd

# COMMAND ----------

FOLDER_CONDA='/ogr/'
f_out='/bye/'
f_in='/hi/'
s = f"""
    PATH=$PATH:{FOLDER_CONDA}
    export TMPDIR=/tmp
    export PROJ_LIB=/databricks/miniconda/share/proj
    export OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO

    mkdir -p {f_out}
    layers=$(ogrinfo -so {f_in} | grep -oP '^\\d+: \\K[^ ]*')
    if [ ${"{#layers[@]}"} < 2 ]; then
        ogr2ogr -t_srs EPSG:27700 {f_out} {f_in}
    else
        for layer in $(ogrinfo -so {f_in} | grep -oP '^\\d+: \\K[^ ]*'); do
            ogr2ogr -t_srs EPSG:27700 {f_out}/$layer {f_in} $layer
        done
    fi
"""
print(s)

# COMMAND ----------

import geopandas as gpd
import contextily as ctx
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.io import load_sdf, ingest_esri
from elmo_geo.utils.types import SparkDataFrame
from elmo_geo.st import sjoin


elmo_geo.register()

# COMMAND ----------

ingest_esri('https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/National_Parks_England/FeatureServer/0')
ingest_esri('https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/Areas_of_Outstanding_Natural_Beauty_England/FeatureServer/0')

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_02_07.parquet

# COMMAND ----------

# MAGIC %sh
# MAGIC fin='/dbfs/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet'
# MAGIC fout='/dbfs/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_02_07.parquet'
# MAGIC
# MAGIC cp -r $fin $fout
# MAGIC
# MAGIC ls /dbfs/mnt/lab/restricted/ELM-Project/stg/

# COMMAND ----------

# sdf_hedge
(
    spark.read.format('geoparquet')
    .load('dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet')
    .write.format('geoparquet')
    .save('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_02_07.parquet')
)

# COMMAND ----------

sdf_hedge = load_sdf('hedge')
sdf_np = load_sdf('National_Parks')
sdf_aonb = load_sdf('Areas_of_Outstanding_Natural_Beauty')


sdf_pl = SparkDataFrame.union(
    sdf_np.select(
        F.lit('National Park').alias('group'),
        F.col('NAME').alias('name'),
        F.expr('ST_Transform(ST_FlipCoordinates(geometry), "epsg:4326", "epsg:27700") AS geometry'),
    ),
    sdf_aonb.select(
        F.lit('Area of Outstanding Natural Beauty').alias('group'),
        F.col('NAME').alias('name'),
        F.expr('ST_Transform(ST_FlipCoordinates(geometry), "epsg:4326", "epsg:27700") AS geometry'),
    ),
)




sdf = sjoin(
    sdf_pl,
    sdf_hedge.select('geometry'),
    how = 'left',
    lsuffix = '',
    rsuffix = '_hedge',
)


sdf.show()
sdf.count()

# COMMAND ----------

import requests
url = 'https://gandalf.lakera.ai/api/guess-password?defender=gandalf-the-white&password='

displayHTML(requests.get(url+'cocoloco', verify=False).text)
