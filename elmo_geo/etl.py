"""Classes for loading data, transforming them and validating them.

This module contains an abstract DatasetLoader class and concrete subclasses of it. These are
designed to manage data loading and any updates needed due to changes to source files or their
dependants.
"""
import os
import time
from abc import ABC, abstractmethod, abstractproperty
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256

import geopandas as gpd
import pandas as pd
from pandera import DataFrameModel
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from elmo_geo.io import gpd_to_partitioned_parquet, to_sdf
from elmo_geo.utils.log import LOG

HASH_FMT: str = r"%y%m%d%H%M%S"
PATH_FMT: str = "/dbfs/mnt/lab/{restricted}/ELM-Project/{layer}/{directory}/{name}-{hsh}.parquet"
SRID: int = 27700


@dataclass
class Dataset(ABC):
    """Class for managing loading and validation of data.

    A Dataset manages the loading of datasets from the cache, and populates the cache when
    that dataset doesn't exist or needs to be refreshed.
    """

    name: str
    layer: str
    directory: str
    restricted: bool

    @abstractproperty
    def metahash(self) -> str:
        """The last modified date of the data file(s) from which a dataset is derived."""

    @abstractproperty
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""

    @abstractmethod
    def refresh(self) -> None:
        """Populate the cache with a fresh version of this dataset."""

    @property
    def path(self) -> str:
        restricted = "restricted" if self.restricted else "unrestricted"
        # TODO: mkdir if not exists
        return PATH_FMT.format(
            restricted=restricted,
            layer=self.layer,
            directory=self.directory,
            name=self.name,
            hsh=self.metahash,
        )

    @property
    def is_fresh(self) -> bool:
        """Check whether this dataset needs to be refreshed in the cache."""
        return os.path.exists(self.path)

    @property
    def gdf(self) -> gpd.GeoDataFrame:
        """Load the dataset as a `geopandas.GeoDataFrame`"""
        if not self.is_fresh:
            self.refresh()
        return gpd.read_parquet(self.path)

    @property
    def df(self) -> pd.DataFrame:
        """Load the dataset as a `pandas.DataFrame`"""
        if not self.is_fresh:
            self.refresh()
        return gpd.read_parquet(self.path)

    @property
    def sdf(self) -> SparkDataFrame:
        """Load the dataset as a `pyspark.sql.dataframe.DataFrame`"""
        if not self.is_fresh:
            self.refresh()
        return to_sdf(self.gdf)

    def _validate(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if self.model is not None:
            self.model.validate(gdf)
        return gdf


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

    @property
    def metahash(self) -> str:
        """Return the last-modified date of the source file."""
        return time.strftime(HASH_FMT, time.gmtime(os.path.getmtime(self.source_path)))

    @property
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""
        return dict(
            name=self.name,
            layer=self.layer,
            directory=self.directory,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
            source_path=self.source_path,
        )

    def refresh(self) -> None:
        LOG.info(f"Creating '{self.name}' dataset.")
        gdf = gpd.read_file(self.source_path)
        gdf = self._validate(gdf)
        gpd_to_partitioned_parquet(gdf, path=self.path, partition_cols=self.partition_cols)
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

    @property
    def metahash(self) -> str:
        """A hash derived from this dataset's dependencies."""
        # if any dependency's metahash changes, then this metahash will also change
        metahashes = "-".join(dependency.metahash for dependency in self.dependencies)
        return sha256(metahashes.encode()).hexdigest()

    @property
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""
        return dict(
            name=self.name,
            layer=self.layer,
            directory=self.directory,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
            dependencies=[dep.name for dep in self.dependencies],
        )

    def refresh(self) -> None:
        """Populate the cache with a fresh version of this dataset."""
        LOG.info(f"Creating '{self.name}' dataset.")
        gdf = self.func(*self.dependencies)
        gdf = self._validate(gdf)
        gpd_to_partitioned_parquet(gdf, path=self.path, partition_cols=self.partition_cols)
        LOG.info(f"Saved to '{self.path}'.")
