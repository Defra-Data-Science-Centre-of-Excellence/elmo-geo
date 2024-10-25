"""Classes for loading data, transforming them and validating them.

This module contains an abstract DatasetLoader class and concrete subclasses of it. These are
designed to manage data loading and any updates needed due to changes to source files or their
dependants.
"""
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from glob import glob, iglob
from hashlib import sha256
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pyspark.sql import functions as F, types as T, DataFrame as SparkDataFrame
from pandera import DataFrameModel

from elmo_geo.io import load_sdf, ogr_to_geoparquet, read_file, write_parquet
from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import dbmtime
from elmo_geo.utils.types import DataFrame
from .io import convert_ogr


DATE_FMT: str = r"%Y_%m_%d"
SRC_HASH_FMT: str = r"%Y%m%d%H%M%S"
HASH_LENGTH = 8
PATH_FMT: str = "/dbfs/mnt/lab/{restricted}/ELM-Project/{level0}/{level1}/"
FILE_FMT: str = "{name}-{date}-{hsh}.parquet"
PAT_FMT: str = r"(^{name}-[\d_]+-{hsh}.parquet$)"
PAT_DATE: str = r"(?<=^{name}-)([\d_]+)(?=-{hsh}.parquet$)"
SRID: int = 27700


@dataclass
class BaseDataset:
    """Class for managing loading and validation of data.

    A Dataset manages the loading of datasets from the cache, and populates the cache when
    that dataset doesn't exist or needs to be refreshed.
    """
    restricted: str
    medallion: str
    source: str
    name: str

    model: DataFrameModel
    dependencies: list[object]


    @property
    def timestamp(self) -> int:
        raise NotImplementedError("Dataset.transform")

    @property
    def date(self) -> str:
        return datetime.fromtimestamp(self.timestamp).strftime(DATE_FMT)

    @property
    def path(self) -> str:
        return PATH_FMT.format(
            restricted=self.restricted,
            medallion=self.medallion,
            source=self.source,
            name=self.name,
            version=self.date,
        )

    @property
    def path_matches(self) -> list[str]:
        return glob(PATH_FMT.format(
            restricted=self.restricted,
            medallion=self.medallion,
            source=self.source,
            name=self.name,
            version="*",
        ))

    @property
    def latest_path(self) -> str:
        return self.path_matches[-1]

    @property
    def is_fresh(self) -> bool:
        return self.path in self.path_matches

    def refresh(self) -> SparkDataFrame:
        if self.is_fresh:
            return self.sdf()
        for dataset in self.dependencies:
            if not dataset.is_fresh:
                dataset.refresh()
        df = self.transform(self.dependencies)
        self.model.validate(df)  # BUG: Doesn't support alias
        return self.sdf()

    def transform(self, *args) -> SparkDataFrame:
        raise NotImplementedError("Dataset.transform")

    def sdf(self) -> SparkDataFrame:
        if not self.is_fresh:
            LOG.info(f"Loading Latest: {self.path_latest}")
        return spark.read.parquet(self.path_latest.replace("/dbfs/", "dbfs:/"))

    def destroy(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


class SourceDataset(BaseDataset):
    source_path: str
    dependencies: list[object] = []

    @property
    def source_paths(self) -> list[str]:
        return glob(self.source_path)

    def timestamp(self):
        return max(dbmtime(path) for path in self.source_paths)

    def transform(self):
        for path in self.source_paths:
            convert_ogr(path, path_out)
        raise NotImplemented
        



@dataclass
class SourceDataset(Dataset):
    """Dataset from outside of the managed environment.

    A SourceDataset is a Dataset for datasets that are not derived from other datasets
    defined in this repo.

    Attributes:
        model: The pandera dataframe model to use for validation
        source_path: The path to the data
    """

    source_path: str
    model: DataFrameModel | None = None
    partition_cols: list[str] | None = None
    is_geo: bool = True
    clean_geometry: bool = True

    @property
    def _new_date(self) -> str:
        """Return the last-modified date of the source file in ISO string format."""
        return time.strftime(DATE_FMT, time.gmtime(dbmtime(self.source_path)))

    @property
    def _hash(self) -> str:
        """Return the last-modified date of the source file."""
        date = time.strftime(SRC_HASH_FMT, time.gmtime(dbmtime(self.source_path)))
        return sha256(date.encode()).hexdigest()[:HASH_LENGTH]

    @property
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""
        return dict(
            name=self.name,
            level0=self.level0,
            level1=self.level1,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
            source_path=self.source_path,
            date=self.date,
        )

    def rename(self, df: DataFrame) -> DataFrame:
        if self.model is None:
            return df
        mapping = {field.alias: field.original_name for _, field in self.model.__fields__.values()}
        if isinstance(df, SparkDataFrame):
            return df.withColumnsRenamed(mapping)
        else:
            return df.rename(columns=mapping)

    def refresh(self):
        LOG.info(f"Creating '{self.name}' dataset.")
        df = read_file(self.source_path, is_geo=self.is_geo, clean_geometry=self.clean_geometry)
        df = self._validate(df).transform(self.rename)
        write_parquet(df, path=self._new_path, partition_cols=self.partition_cols)
        LOG.info(f"Saved to '{self.path}'.")


@dataclass
class SourceGlobDataset(SourceDataset):
    """SourceGlobDataset is to ingest multiple files using a glob path.
    - This is better suited for ingesting very large geographic file, as it uses ogr2ogr.
    - This does not support aliasing/renaming, as it writes before validating.

    For non-geographic data, each path in the glob path is loaded with pandas, then converted
    to a SparkDataFrame and unioned together. A '_path' column is added, so that data belonging
    to different paths can be identified.

    Defines new _new_date() and _hash() methods that are based on the glob path rather
    than source path.

    Attributes:
        glob_path: The glob path to the data.
    """

    glob_path: str = None
    source_path: str = None

    @property
    def _new_date(self) -> str:
        """Return the mean of last-modified dates of the files
        contain in the glob path ISO string format."""
        mtimes = [dbmtime(f) for f in iglob(self.glob_path)]
        mtime = sum(mtimes) / len(mtimes)
        return time.strftime(DATE_FMT, time.gmtime(mtime))

    @property
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""
        d = super().dict
        d["glob_path"] = self.glob_path
        return d

    @property
    def _hash(self) -> str:
        """Return a hash calculated from of last-modified dates of the files
        contain in the glob."""
        mtimes = [dbmtime(f) for f in iglob(self.glob_path)]
        mtime = sum(mtimes) / len(mtimes)
        date = time.strftime(SRC_HASH_FMT, time.gmtime(mtime))
        return sha256(date.encode()).hexdigest()[:HASH_LENGTH]

    def refresh(self):
        LOG.info(f"Creating '{self.name}' dataset.")
        new_path = self._new_path
        if self.is_geo:
            ogr_to_geoparquet(self.glob_path, new_path)
            df = load_sdf(new_path)
            df = self._validate(df)
        else:

            def union(x: SparkDataFrame, y: SparkDataFrame) -> SparkDataFrame:
                return x.unionByName(y, allowMissingColumns=True)

            gen_sdfs = (read_file(f, is_geo=self.is_geo).withColumn("_path", F.lit(f)) for f in iglob(self.glob_path))
            df = reduce(union, gen_sdfs)
            df = self._validate(df)
            write_parquet(df, path=self._new_path, partition_cols=self.partition_cols)
        LOG.info(f"Saved to '{self.path}'.")


@dataclass
class DerivedDataset(Dataset):
    """Dataset derived (transformed) from others in the ETL pipeline.

    A DerivedDataset is a DataSet for datasets that are derived other datasets.
    It is thus dependent on those datasets, and includes a function that transforms those
    dependencies when the dataset needs to be refreshed. A derived dataset needs refreshing
    if it is missing entirely, or if any of its dependencies need refreshing.
    """

    dependencies: list[Dataset]
    func: Callable[[list[SparkDataFrame]], SparkDataFrame]
    model: DataFrameModel | None = None
    partition_cols: list[str] | None = None
    is_geo: bool = True

    @property
    def _new_date(self) -> str:
        """Return the current date for use in file naming."""
        return datetime.today().strftime(DATE_FMT)

    @property
    def _hash(self) -> str:
        """A hash derived from this dataset's dependencies.

        If any dependency's hashes change, then this hash will also change
        """
        hshs = "-".join(dependency._hash for dependency in self.dependencies)
        return sha256(hshs.encode()).hexdigest()[:HASH_LENGTH]

    @property
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""
        return dict(
            name=self.name,
            level0=self.level0,
            level1=self.level1,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
            dependencies=[dep.name for dep in self.dependencies],
            date=self.date,
        )

    def refresh(self) -> None:
        """Populate the cache with a fresh version of this dataset."""
        LOG.info(f"Creating '{self.name}' dataset.")
        df = self.func(*self.dependencies)
        df = self._validate(df)
        write_parquet(df, path=self._new_path, partition_cols=self.partition_cols)
        LOG.info(f"Saved to '{self.path}'.")
