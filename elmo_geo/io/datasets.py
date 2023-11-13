import json
from glob import glob

from elmo_geo import LOG
# from elmo_geo.io import ingest
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import FILEPATH_CATALOGUE, FOLDER_ODS, FOLDER_STG
from elmo_geo.utils.types import SparkDataFrame


def append_to_catalogue(dataset: dict):
    """Append a new dictionary about a dataset to our data catalogue, datasets.json."""
    LOG.info(f"Catalogued:  {list(dataset.keys())}")
    with open(FILEPATH_CATALOGUE, "r") as f:
        datasets = json.load(f)
    datasets.update(dataset)
    with open(FILEPATH_CATALOGUE, "w") as f:
        json.dump(datasets, f, indent=4)


def load_sdf(name: str) -> SparkDataFrame:
    """Load a SparkDataFrame from our data catalogue."""
    files = glob(f"{FOLDER_STG}/*{name}*.parquet")
    files.extend(glob(f"{FOLDER_ODS}/*{name}*.parquet"))
    if len(files):
        f = files[-1]  # Selecting most recent
    else:
        raise Exception(f'Not found any: "{name}"')
    LOG.info(f"Loading:  {f}")
    return spark.read.option("mergeSchema", "true").format("geoparquet").load(dbfs(f, True))


# def verify_catalogue():
#     with open(FILEPATH_CATALOGUE, "r") as f:
#         datasets = json.load(f)
#     for name, kwargs in datasets.items():
#         if False:  # check if staged
#             ingest[kwargs["function"]](**kwargs)
