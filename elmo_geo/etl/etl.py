"""Classes for loading data, transforming them and validating them.

This module contains an abstract DatasetLoader class and concrete subclasses of it. These are
designed to manage data loading and any updates needed due to changes to source files or their
dependants.
"""
import os
import re
import shutil
import time
from abc import ABC, abstractmethod, abstractproperty
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pandera import DataFrameModel
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from elmo_geo.io import gpd_to_partitioned_parquet
from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import load_sdf

DATE_FMT: str = r"%Y_%m_%d"
SRC_HASH_FMT: str = r"%Y%m%d%H%M%S"
HASH_LENGTH = 8
PATH_FMT: str = "/dbfs/mnt/lab/{restricted}/ELM-Project/{level0}/{level1}/"
FILE_FMT: str = "{name}-{date}-{hsh}.parquet"
PAT_FMT: str = "(^{name}-[\d_]+-{hsh}.parquet$)"
PAT_DATE: str = "(?<=^{name}-)([\d_]+)(?=-{hsh}.parquet$)"
SRID: int = 27700


class NotGeoError(Exception):
    """The dataset is not geospatial."""


class UnknownFileExtension(Exception):
    """Don't know how to read file with extension."""


@dataclass
class Dataset(ABC):
    """Class for managing loading and validation of data.

    A Dataset manages the loading of datasets from the cache, and populates the cache when
    that dataset doesn't exist or needs to be refreshed.
    """

    name: str
    level0: str
    level1: str
    restricted: bool

    @abstractproperty
    def _new_date(self) -> str:
        """New date for parquet file being created."""

    @abstractproperty
    def _hash(self) -> str:
        """A semi-unique identifier of the last modified dates of the data file(s) from which a dataset is derived."""

    @property
    def date(self) -> str | None:
        """Return the last-modified date from the filename in ISO string format.

        If the dataset has not been generated yet, return `None`."""
        pat = re.compile(PAT_DATE.format(name=self.name, hsh=self._hash))
        try:
            return pat.findall(self.filename)[0]
        except IndexError:
            return None

    @abstractproperty
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""

    @abstractmethod
    def refresh(self) -> None:
        """Populate the cache with a fresh version of this dataset."""

    @property
    def path_dir(self) -> str:
        """Path to the directory where the data will be saved."""
        restricted = "restricted" if self.restricted else "unrestricted"
        return PATH_FMT.format(restricted=restricted, level0=self.level0, level1=self.level1)

    @property
    def is_fresh(self) -> bool:
        """Check whether this dataset needs to be refreshed in the cache."""
        return len(self.file_matches) > 0

    @property
    def file_matches(self) -> list[str]:
        """List of files that match the file path but may have different dates."""
        pat = re.compile(PAT_FMT.format(name=self.name, hsh=self._hash))
        return [y.group(0) for y in [pat.fullmatch(x) for x in os.listdir(self.path_dir)] if y is not None]

    @property
    def filename(self) -> str:
        """Name of the file if it has been saved, else OSError."""
        if not self.is_fresh:
            msg = "The dataset has not been built yet. Please run `Dataset.refresh()`"
            raise OSError(msg)
        return next(iter(self.file_matches))

    @property
    def path(self) -> str:
        """Path to the file if it has been saved, else OSError."""
        return self.path_dir + self.filename

    @property
    def _new_filename(self) -> str:
        """New filename for parquet file being created."""
        return FILE_FMT.format(name=self.name, date=self._new_date, hsh=self._hash)

    @property
    def _new_path(self) -> str:
        """New filepath for parquet file being created."""
        return self.path_dir + self._new_filename

    def gdf(self, **kwargs) -> gpd.GeoDataFrame:
        """Load the dataset as a `geopandas.GeoDataFrame`

        Columns and filters can be applied through `columns` and `filters` arguments, along with other options specified here:
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html#pyarrow.parquet.read_table
        """
        if not self.is_fresh:
            self.refresh()
        return gpd.read_parquet(self.path, **kwargs)

    def pdf(self, **kwargs) -> pd.DataFrame:
        """Load the dataset as a `pandas.DataFrame`

        Columns and filters can be applied through `columns` and `filters` arguments, along with other options specified here:
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html#pyarrow.parquet.read_table
        """
        if not self.is_fresh:
            self.refresh()
        return pd.read_parquet(self.path, **kwargs)

    def sdf(self, **kwargs) -> SparkDataFrame:
        """Load the dataset as a `pyspark.sql.dataframe.DataFrame`."""
        if not self.is_fresh:
            self.refresh()
        return load_sdf(self.path, **kwargs)

    def _validate(self, df: gpd.GeoDataFrame | SparkDataFrame | pd.DataFrame) -> gpd.GeoDataFrame | SparkDataFrame | pd.DataFrame:
        """Validate the data against a model specification if one is defined."""
        if self.model is not None:
            self.model.validate(df)
        return df

    def destroy(self) -> None:
        """Delete the cached dataset at `self.path`."""
        if Path(self.path).exists():
            shutil.rmtree(self.path)
            msg = f"Destroyed '{self.name}' dataset."
            LOG.warning(msg)
            return
        msg = f"'{self.name}' dataset cannot be destroyed as it doesn't exist yet."
        LOG.warning(msg)

    @classmethod
    def __type__(cls) -> str:
        return cls.__name__


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

    @property
    def _new_date(self) -> str:
        """Return the last-modified date of the source file in ISO string format."""
        return time.strftime(DATE_FMT, time.gmtime(os.path.getmtime(self.source_path)))

    @property
    def _hash(self) -> str:
        """Return the last-modified date of the source file."""
        date = time.strftime(SRC_HASH_FMT, time.gmtime(os.path.getmtime(self.source_path)))
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

    def refresh(self) -> None:
        LOG.info(f"Creating '{self.name}' dataset.")
        if self.is_geo:
            if Path(self.source_path).suffix == ".parquet" or Path(self.source_path).is_dir():
                gdf = gpd.read_parquet(self.source_path)
            else:
                gdf = gpd.read_file(self.source_path)
            gdf = self._validate(gdf)
            gpd_to_partitioned_parquet(gdf, path=self._new_path, partition_cols=self.partition_cols)
        else:
            if Path(self.source_path).suffix == ".parquet" or Path(self.source_path).is_dir():
                pdf = pd.read_parquet(self.source_path)
            elif Path(self.source_path).suffix == ".csv":
                pdf = pd.read_csv(self.source_path)
            else:
                raise UnknownFileExtension()
            pdf = self._validate(pdf)
            # TODO: to partitioned parquet
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
        if isinstance(df, SparkDataFrame):
            df.write.format("parquet").partitionBy(self.partition_cols).formatsave(self._new_path, mode="overwrite")
        elif isinstance(df, gpd.GeoDataFrame):
            gpd_to_partitioned_parquet(df, path=self._new_path, partition_cols=self.partition_cols)
        elif isinstance(df, pd.DataFrame):
            df.to_parquet(path=self._new_path, partition_cols=self.partition_cols)
        else:
            msg = f"Expected Spark, GeoPandas or Pandas dataframe, recieved {type(df)}."
            raise TypeError(msg)
        LOG.info(f"Saved to '{self.path}'.")
