"""Classes for loading data, transforming them and validating them.

This module contains an abstract DatasetLoader class and concrete subclasses of it. These are
designed to manage data loading and any updates needed due to changes to source files or their
dependants.
"""
import os
import re
import shutil
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from glob import iglob
from hashlib import sha256
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyspark.sql.functions as F
import rioxarray as rxr
from pandera import DataFrameModel
from rioxarray.raster_array import RasterArray

from elmo_geo.io import download_link, load_sdf, ogr_to_geoparquet, read_file, to_gdf, write_parquet
from elmo_geo.rs.raster import to_raster
from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import dbmtime
from elmo_geo.utils.types import DataFrame, GeoDataFrame, PandasDataFrame, SparkDataFrame

DATE_FMT: str = r"%Y_%m_%d"
SRC_HASH_FMT: str = r"%Y%m%d%H%M%S"
HASH_LENGTH = 8
PATH_FMT: str = "/dbfs/mnt/{restricted}/{medallion}/{source}/"
FILE_FMT: str = "{name}-{date}-{hsh}.parquet"
PAT_FMT: str = r"(^{name}-[\d_]+-{hsh}.parquet$)"
PAT_DATE: str = r"(?<=^{name}-)([\d_]+)(?=-{hsh}.parquet$)"
SRID: int = 27700

FILE_FMT_RASTER: str = "{name}.tif"
PAT_FMT_RASTER: str = r"(^{name}.tif$)"


@dataclass
class Dataset(ABC):
    """Class for managing loading and validation of data.

    A Dataset manages the loading of datasets from the cache, and populates the cache when
    that dataset doesn't exist or needs to be refreshed.
    """

    name: str
    medallion: str
    source: str
    restricted: bool

    @property
    @abstractmethod
    def _hash(self) -> str:
        """A semi-unique identifier of the last modified dates of the data file(s) from which a dataset is derived."""

    @property
    @abstractmethod
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""

    @abstractmethod
    def refresh(self) -> None:
        """Populate the cache with a fresh version of this dataset."""

    @property
    def path_dir(self) -> str:
        """Path to the directory where the data will be saved."""
        restricted = "lab-res-a1001004/restricted/elm_project" if self.restricted else "lab/unrestricted/ELM-Project"
        return PATH_FMT.format(restricted=restricted, medallion=self.medallion, source=self.source)

    @property
    @abstractmethod
    def file_matches(self) -> list[str]:
        """List of files that match the file path, used in is_fresh."""

    @property
    def is_fresh(self) -> bool:
        """Check whether this dataset needs to be refreshed in the cache."""
        return len(self.file_matches) > 0

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
    @abstractmethod
    def _new_filename(self) -> str:
        """New filename for file being created."""

    @property
    def _new_path(self) -> str:
        """New filepath for parquet file being created."""
        return self.path_dir + self._new_filename

    def destroy(self) -> None:
        """Delete the cached dataset at `self.path`."""
        if Path(self.path).exists():
            shutil.rmtree(self.path)
            msg = f"Destroyed '{self.name}' dataset."
            LOG.warning(msg)
            return
        msg = f"'{self.name}' dataset cannot be destroyed as it doesn't exist yet."
        LOG.warning(msg)


@dataclass
class TabularDataset(Dataset, ABC):
    """Class for managing loading and validation of tabular data."""

    # model:  DataFrameModel | None

    @property
    @abstractmethod
    def _new_date(self) -> str:
        """New date for parquet file being created."""

    @property
    def date(self) -> str | None:
        """Return the last-modified date from the filename in ISO string format.

        If the dataset has not been generated yet, return `None`."""
        pat = re.compile(PAT_DATE.format(name=self.name, hsh=self._hash))
        try:
            return pat.findall(self.filename)[0]
        except IndexError:
            return None

    @property
    @abstractmethod
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""

    @abstractmethod
    def refresh(self) -> None:
        """Populate the cache with a fresh version of this dataset."""

    @property
    def file_matches(self) -> list[str]:
        """List of files that match the file path but may have different dates.

        Return in order of newest to oldest by the modified date of the path. Does
        not take into account modified dates of the dataset dependencies.
        """
        if not os.path.exists(self.path_dir):
            return []

        pat = re.compile(PAT_FMT.format(name=self.name, hsh=self._hash))
        return sorted(
            [y.group(0) for y in [pat.fullmatch(x) for x in os.listdir(self.path_dir)] if y is not None],
            key=lambda x: dbmtime(self.path_dir + x),
            reverse=True,
        )

    @property
    def filename(self) -> str:
        """Name of the file if it has been saved, else OSError."""
        if not self.is_fresh:
            msg = "The dataset has not been built yet. Please run `Dataset.refresh()`"
            raise OSError(msg)
        return next(iter(self.file_matches))

    @property
    def _new_filename(self) -> str:
        """New filename for parquet file being created."""
        return FILE_FMT.format(name=self.name, date=self._new_date, hsh=self._hash)

    def gdf(self, **kwargs) -> GeoDataFrame:
        """Load the dataset as a `geopandas.GeoDataFrame`

        Columns and filters can be applied through `columns` and `filters` arguments, along with other options specified here:
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html#pyarrow.parquet.read_table
        """
        if not self.is_fresh:
            self.refresh()
        return gpd.read_parquet(self.path, **kwargs)

    def pdf(self, **kwargs) -> PandasDataFrame:
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

    def _validate(self, df: DataFrame) -> DataFrame:
        """Validate the data against a model specification if one is defined."""
        if self.model is not None:
            if isinstance(df, SparkDataFrame):
                LIMIT_SDF: int = 100_000
                _df = to_gdf(df.limit(LIMIT_SDF)) if self.is_geo else df.limit(LIMIT_SDF).toPandas()
                self.model.validate(_df)
            else:
                df = self.model.validate(df)
        return df

    def export(self, ext: str = "parquet") -> str:
        """Create a copy of the dataset as a monolithic file in the /FileStore/elmo-geo-exports/ folder
        and return a link to download the file from this location.

        Parameters:
            ext: File extension to use when saving the dataset.

        Returns:
            HTML download link for exported data.
        """
        fname, _ = os.path.splitext(self.filename)
        path_exp = f"/dbfs/FileStore/elmo-geo-downloads/{fname}.{ext}"

        if os.path.exists(path_exp):
            LOG.info("Export file already exists. Returning download link.")
            return download_link(path_exp)

        LOG.info("Exporting file to FileStore/elmo-geo-downloads/.")
        if ext == "parquet":
            path_exp = self.path
        elif self.is_geo:
            if ext == "gpkg":
                f_tmp = f"/tmp/{self.name}.{ext}"
                self.gdf().to_file(f_tmp)
                shutil.copy(f_tmp, path_exp)
            else:
                self.gdf().to_file(path_exp)
        elif ext == "csv":
            self.pdf().to_csv(path_exp)
        else:
            raise NotImplementedError(f"Requested export format '{ext}' for non-spatial data not currently supported.")
        return download_link(path_exp)

    @classmethod
    def __type__(cls) -> str:
        return cls.__name__


@dataclass
class SourceDataset(TabularDataset):
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
            medallion=self.medallion,
            source=self.source,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
            source_path=self.source_path,
            date=self.date,
        )

    def rename(self, df: DataFrame) -> DataFrame:
        if self.model is None:
            return df
        # Replace "name" with "_name", so we can replace "useful_name" with "name" and not have duplicate columns.
        mapping_old = {field.original_name: "_" + field.original_name for _, field in self.model.__fields__.values() if field.alias is not None}
        mapping = {field.alias: field.original_name for _, field in self.model.__fields__.values() if field.alias is not None}
        if isinstance(df, SparkDataFrame):
            return df.withColumnsRenamed(mapping_old).withColumnsRenamed(mapping)
        else:
            return df.rename(columns=mapping_old).rename(columns=mapping)

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
    model: DataFrameModel | None = None

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
class DerivedDataset(TabularDataset):
    """Dataset derived (transformed) from others in the ETL pipeline.

    A DerivedDataset is for tabular data that is derived other datasets.
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
            medallion=self.medallion,
            source=self.source,
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


@dataclass
class RasterDataset(Dataset):
    """Class for managing loading and validation of raster data."""

    @property
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""
        return dict(
            name=self.name,
            medallion=self.medallion,
            source=self.source,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
        )

    @property
    def file_matches(self) -> list[str]:
        """List of files that match the file path but may have different dates.

        Return in order of newest to oldest by the modified date of the path. Does
        not take into account modified dates of the dataset dependencies.
        """
        if not os.path.exists(self.path_dir):
            return []

        pat = re.compile(PAT_FMT_RASTER.format(name=self.name))
        return [y.group(0) for y in [pat.fullmatch(x) for x in os.listdir(self.path_dir)] if y is not None]

    @property
    def _new_filename(self) -> str:
        """New filename for parquet file being created."""
        return FILE_FMT_RASTER.format(name=self.name)

    @property
    def _hash(self) -> str:
        "Fixed hash as no updates required once built."
        return "0"

    def ra(self, **kwargs) -> RasterArray:
        """Load the dataset as a `rioxarray.raster_array.RasterArray`

        By default the dataset na values are masked out from their stored _FillValue.
        """
        if not self.is_fresh:
            self.refresh()
        default_params = dict(masked=True)
        [kwargs.setdefault(k, v) for k, v in default_params.items()]
        return rxr.open_rasterio(self.path, **kwargs)

    def _validate(self, ra: RasterArray) -> DataFrame:
        """Validate the data against a model specification if one is defined."""
        # TODO: Implement validation using https://github.com/xarray-contrib/xarray-schema
        return ra


@dataclass
class SourceSingleFileRasterDataset(RasterDataset):
    """Class for managing loading and validation of source raster data in a single file."""

    source_path: str = None

    @property
    def dict(self) -> dict:
        """A dictionary representation of the dataset."""
        return dict(
            name=self.name,
            medallion=self.medallion,
            source=self.source,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
            source_path=self.source_path,
        )

    def refresh(self):
        LOG.info(f"Creating '{self.name}' dataset.")
        ra = rxr.open_rasterio(self.source_path).squeeze()
        if (crs := ra.rio.crs) != SRID:
            msg = f"Converting CRS from {crs} to {SRID}."
            LOG.debug(msg)
            ra = ra.rio.to_crs(SRID)
        ra = self._validate(ra)
        to_raster(ra, self._new_path)
        LOG.info(f"Saved to '{self.path}'.")


@dataclass
class DerivedRasterDataset(RasterDataset):
    """Raster Dataset derived (transformed) from others in the ETL pipeline.

    A DerivedRasterDataset is for raster data that is derived other datasets.
    It is thus dependent on those datasets, and includes a function that transforms those
    dependencies when the dataset needs to be refreshed. A derived dataset needs refreshing
    if it is missing entirely, or if any of its dependencies need refreshing.
    """

    dependencies: list[Dataset]
    func: Callable[[list[Dataset]], RasterArray]

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
            medallion=self.medallion,
            source=self.source,
            restricted=self.restricted,
            path=self.path,
            type=str(type(self)),
            dependencies=[dep.name for dep in self.dependencies],
        )

    def refresh(self) -> None:
        """Populate the cache with a fresh version of this dataset."""
        LOG.info(f"Creating '{self.name}' dataset.")
        ra = self.func(*self.dependencies)
        ra = self._validate(ra)
        to_raster(ra, self._new_path)
        LOG.info(f"Saved to '{self.path}'.")
