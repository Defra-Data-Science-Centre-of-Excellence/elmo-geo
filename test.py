# Databricks notebook source
# MAGIC %ls -lh /dbfs/FileStore

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/lab/restricted/ELM-Project/out

# COMMAND ----------

# MAGIC %pip install -q ruff

# COMMAND ----------

# MAGIC %sh
# MAGIC ruff check .
# MAGIC ruff format . --check

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

ingest_esri('https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/National_Parks_England/FeatureServer/0')
ingest_esri('https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/Areas_of_Outstanding_Natural_Beauty_England/FeatureServer/0')

ingest_esri(
    url = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services/Countries_December_2022_UK_BFC/FeatureServer/0',
    name = 'ons-countries_bfc-2022_12',
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
