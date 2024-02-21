# Databricks notebook source
# MAGIC %pip install -qU xyzservices osdatahub git+https://github.com/aw-west-defra/cdap_geo.git pyarrow tqdm

# COMMAND ----------

import os

import geopandas as gpd

key = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"

# COMMAND ----------

f = f"/dbfs/mnt/lab/unrestricted/elm_data/os/{product}.parquet"
f_parcel = None

f_geom_out = None
f_out = None

# COMMAND ----------

# ABCDE
# FGHJK - HJ
# LMNOP - NO
# QRSTU - ST
# VWXYZ   ^^ UK

LETTERS = "ABCDEFGHJKLMNOPQRSTUVWXYZ"
BNG_LIMITS = [-1_000_000, -500_000, 1_500_000, 2_000_000]
BNG_STEP = 500_000
N = 5

letters_split = list(map("".join, zip(*[iter(LETTERS)] * N)))


# x, y = 100_000, 1_299_000  # HM09
x, y = -1_000_000, 2_000_000  # AA
x, y = 0, 0  # SV
precision = 2


result = ""
step = BNG_STEP
for i in range(-1, precision):
    if precision == -1:
        pass
    dx, dy = x // step, y // step
    i, j = (dx - 2) % N, (2 - dy) % N
    x, y = x - dx, y - dy
    step //= N
    print(x, y, i, j)
    result += letters_split5[i][j]
result

# COMMAND ----------

BNG_STEP = 100_000
BNG_LIMITS = [0, 0, 700_000, 1_300_000]
BNG_LETTERS = """HL,HM,HN,HO,HP,JL,JM
HQ,HR,HS,HT,HU,JQ,JR
HV,HW,HX,HY,HZ,JV,JW
NA,NB,NC,ND,NE,OA,OB
NF,NG,NH,NJ,NK,OF,OG
NL,NM,NN,NO,NP,OL,OM
NQ,NR,NS,NT,NU,OQ,OR
NV,NW,NX,NY,NZ,OV,OW
SA,SB,SC,SD,SE,TA,TB
SF,SG,SH,SJ,SK,TF,TG
SL,SM,SN,SO,SP,TL,TM
SQ,SR,SS,ST,SU,TQ,TR
SV,SW,SX,SY,SZ,TV,TW"""
BNG_LOOKUP = {(i * BNG_STEP, j * BNG_STEP): pair for j, row in enumerate(reversed(BNG_LETTERS.split("\n"))) for i, pair in enumerate(row.split(","))}


def bng(x: int, y: int, *, precision: int = 1) -> str:
    assert 0 <= precision <= 5, f"{precision:d} not within precision range[0, 5]"
    assert BNG_LIMITS[0] <= x < BNG_LIMITS[2], f"{x:_} not within BNG range[{BNG_LIMITS[0]:_}, {BNG_LIMITS[2]:_})"
    assert BNG_LIMITS[1] <= y < BNG_LIMITS[3], f"{y:_} not within BNG range[{BNG_LIMITS[1]:_}, {BNG_LIMITS[3]:_})"
    L = lambda a: (a // 100_000) * 100_000
    D = lambda b: f"{int(b)%100_000:05d}"[:precision]
    return BNG_LOOKUP[L(x), L(y)] + D(x) + D(y)


print(bng(int(651_130.5), 1_299_000, precision=3))
print(bng(100_000, 1_299_000))
# print(bng(100_000, 1_300_000))

# COMMAND ----------

from geopandas import GeoDataFrame
from geopandas.io.arrow import _geopandas_to_arrow
from osdatahub import Extent, FeaturesAPI
from pandas import DataFrame as PandasDataFrame
from pyarrow.dataset import write_dataset
from tqdm import tqdm


def log(x: float, b: float) -> float:
    if x < b:
        return 0
    return 1 + log(x / b, b)


def os_features(key: str, product: str, bbox: list, crs: str = "EPSG:27700", max_query: int = 100_000):
    return FeaturesAPI(
        key,
        product,
        Extent.from_bbox(bbox, crs),
    ).query(max_query)


def df_to_geoparquet(df, path_out, name):
    return write_dataset(
        _geopandas_to_arrow(df),
        path_out,
        basename_template=name + "-{i}",
        format="parquet",
    )


def writer_os(key: str, product: str, bbox: list, path_out: str, name: str) -> int:
    result = os_features(key, product, bbox)
    count = len(result["features"])
    if count:
        df = GeoDataFrame.from_features(df)
        df_to_geoparquet(result, path_out, name)
    return count


def os_initalise_df(precision: int, bbox: list = None) -> PandasDataFrame:
    if bbox is None:
        bbox = [0, 0, 700_000, 1_300_000]
    assert -1 <= precision <= 5, f"Precision: {precision} not in [-1, 5], -1 is a letter, 0 is both letters, 1+ adds two numbers"
    step = 10 ** (5 - precision)
    return PandasDataFrame.merge(
        PandasDataFrame({"x": range(bbox[0], bbox[2], step)}),
        PandasDataFrame({"y": range(bbox[1], bbox[3], step)}),
        how="cross",
    ).assign(
        bbox=lambda df: df.apply(lambda df: [df.x, df.y, df.x + step, df.y + step], axis=1),
        name=lambda df: df.apply(lambda df: bng(df.x, df.y, precision=precision), axis=1),
        count=None,
    )


def os_loop_writer(key: str, product: str, path_out: str, df: PandasDataFrame) -> PandasDataFrame:
    for i in (pbar := tqdm(df.index)):
        bbox, name = df.at[i, "bbox"], df.at[i, "name"]
        pbar.set_description(name)
        if df.at[i, "count"] is None:
            df.at[i, "count"] = writer_os(key, product, bbox, path_out, name)
    return df


# COMMAND ----------

key = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"
product = "Zoomstack_Woodland"
path_out = f"/dbfs/tmp/os/{product}.parquet/"
f_status = path_out + "status.csv"

if os.path.exists(f_status):
    df = pd.read_csv(f_status)
else:
    df = os_initalise_df(1)

df = os_loop_writer(key, product, path_out, df)

df.to_csv()

df

# COMMAND ----------

help(sc.parallelize)

# COMMAND ----------

from os.path import join
from time import sleep
from urllib.error import URLError
from urllib.request import urlopen

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
sc = spark.sparkContext


def download_file(url, path, retry_count, retry_delay):
    try:
        filepath = join(path, url.split("/")[-1])
        with open(filepath, "wb") as f:
            f.write(urlopen(url))
    except URLError:
        if retry_count:
            sleep(retry_delay)
            download_file(url, retry_count=retry_count - 1)


def download_files(urls, path, num_retries=3, retry_delay=60):
    _dl = lambda url: download_file(url, path, num_retries, retry_delay)
    sc.parallelize(urls).map(_dl).collect()


from os import mkdir
from shutil import rmtree


def test_download_files():
    urls = ["https://example.com/file1.csv", "https://example.com/file2.csv"]
    expected_filenames = ["file1.csv", "file2.csv"]
    expected_data = [b"file1 data", b"file2 data"]

    temp_dir = "/dbfs/tmp/dltest/"
    mkdir(temp_dir)
    try:
        download_files(urls, temp_dir)
        for filename, data in zip(expected_filenames, expected_data):
            file_path = os.path.join(temp_dir, filename)
            assert os.path.exists(file_path)
            with open(file_path, "rb") as f:
                assert f.read() == data
    finally:
        rmtree(temp_dir)


test_download_files()

# COMMAND ----------

sdf = spark.read.parquet(path_out)

# COMMAND ----------

df = gpd.read_parquet(f)
df.assign(geometry=df.geometry.simplify(10)).explore()
