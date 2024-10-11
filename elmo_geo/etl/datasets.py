"""Classes for loading data, transforming them and validating them.

This module contains an abstract Dataset class and concrete subclasses of it. These are designed
to manage data loading and any updates needed due to changes to source files or their dependants.
"""
import shutil
from dataclasses import dataclass
from glob import iglob

import geopandas as gpd
import pandas as pd
from pandera import DataFrameModel
from pyspark.sql import DataFrame as SparkDataFrame

from elmo_geo.io import S3Handler, clean_geometry, ogr_to_geoparquet, read_file, write_parquet
from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import dbmtime, is_snake_case

from .transforms import sjoin_boundary_proportion, sjoin_parcel_proportion
from .io import read_ogr, write_geoparquet
from .s3 import S3Handler


DATE_FMT: str = r"%Y_%m_%d"
PATH_FMT: str = "/dbfs/mnt/lab/{license}/ELM-Project/{medallion}/{source}-{dataset}-{date}.parquet/"
SRID: int = 27700


@dataclass
class BaseDataset:
    """Base Dataset
    TODO: docstring

    Parameters:
        source_path
        dependencies
        license
        medallion:
            bronze (aka raw, staging, landing, "as is") manually uploaded datasets, any format.
            silver (aka data, ods, enriched) ingested and intermediate parquet datasets.
            gold (aka curated) output datasets for export, typically non-geospatial.
        source
        dataset
        model
        transform
        write_kwargs
        timestamp
    """

    license: str  # restricted, unrestricted
    medallion: str  # test, bronze, silver, gold
    source: str  # elmo_geo, ...
    dataset: str
    model: DataFrameModel
    transform: callable
    dependencies: list[object] | None

    @property
    def timestamp(self) -> float:
        pass

    @property
    def date(self) -> str:
        return self.timestamp.strftime(DATE_FMT)

    @property
    def name(self) -> str:
        return f"{self.source}-{self.dataset}-{self.date}"

    @property
    def path_matches(self) -> list[str]:
        path_data = dict(
            license=self.license,
            medallion=self.medallion,
            source=self.source,
            dataset=self.dataset,
            date="*",
        )
        return sorted(iglob(PATH_FMT.format(**path_data)))

    @property
    def path_fresh(self) -> str:
        path_data = dict(
            license=self.license,
            medallion=self.medallion,
            source=self.source,
            dataset=self.dataset,
            date=self.date,
        )
        assert all(is_snake_case(v) for v in path_data.values())
        return PATH_FMT.format(**path_data)

    @property
    def path_latest(self) -> str:
        if len(self.path_matches) == 0:
            raise "No data available"
        if not self.is_fresh:
            path = self.path_matches[-1]
            LOG.warning(f"Not Fresh: {self.path_fresh}\nLatest: {path}")
        else:
            path = self.path
        return path

    @property
    def is_fresh(self) -> bool:
        return self.path_fresh in self.path_matches

    def refresh(self, accept_old: False) -> SparkDataFrame:
        if self.is_fresh:
            sdf = self.transform(*self.dependencies)
            write_parquet(sdf, self.path_fresh)
        else:
            sdf = self.sdf()
        return sdf

    def sdf(self) -> SparkDataFrame:
        LOG.info(f"Loading: {self.path_latest}")
        return spark.read.parquet(self.path_latest.replace("/dbfs/", "dbfs:/"))

    def pdf(self) -> pd.DataFrame | gpd.GeoDataFrame:
        LOG.info(f"Loading: {self.path_latest}")
        pdf = pd.read_parquet(self.path_latest)
        if "geometry" in pdf.columns:
            pdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkb(pdf["geometry"]), crs=SRID)
        return pdf

    def export(self, ext: str = "parquet") -> str:
        """Export dataset to SCE s3-ranch-013."""
        path = f"data/ELM-Project/{self.medallion}/{self.name}.parquet"
        s3 = S3Handler()
        s3.write_file(self.pdf(), path)

    def download(self, ext: str = "parquet") -> str:
        """Save to FileStore and return a link for downloading the dataset."""
        pdf = self.pdf()
        if "geometry" in pdf.columns:
            pdf = gpd.GeoDataFrame(pdf, crs=SRID)
        path = f"/dbfs/FileStore/elmo_geo-downloads/{self.name}.{ext}"
        if ext == "parquet":
            pdf.to_parquet(path)
        else:
            if ext == "gpkg":
                tmp = f"/tmp/{self.name}.{ext}"
                self.gdf().to_file(tmp)
                shutil.move(tmp, path)
            else:
                self.gdf().to_file(path)
        workspace = spark.conf.get("spark.databricks.workspaceUrl")
        org = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
        path = path.replace("/dbfs/FileStore/", "")
        url = f"https://{workspace}/files/{path}?o={org}"
        displayHTML(f"Download: <a href={url} target='_blank'>{self.name}</a>")
        return url


class SourceDataset(BaseDataset):
    """SourceDataset docstring.
    TODO
    """

    source_path: str
    dependencies: list[BaseDataset] | None = None
    read_mode: str = "ogr2ogr"
    subdivide: bool = True
    read_kwargs: dict = {}

    @property
    def source_paths(self) -> dict[str, float]:
        return {path: dbmtime(path) for path in iglob(self.source_path)}

    def timestamp(self) -> float:
        return max(self.source_paths.values())

    def transform(self):
        # Read Original
        if self.read_mode == "ogr2ogr":
            tmp_path = f"/tmp/{self.source}-{self.dataset}-{self.date}.parquet/"
            ogr_to_geoparquet(self.source_path, tmp_path)
            sdf = spark.read.parquet(tmp_path.replace("/dbfs/", "dbfs:/"))
        elif self.read_mode == "geopandas":
            sdf = gpd.read_file(self.source_path, **self.read_kwargs).to_wkb().pipe(spark.createDataFrame)
        else:
            sdf = spark.read.format(self.read_mode).load(self.source_path.replace("/dbfs/", "dbfs:/"), **self.read_kwargs)
        read_file  # TODO
        # Clean Geometries
        if "geometry" in sdf.columns:
            sdf = sdf.withColumn("geometry", clean_geometry(subdivide=self.subdivide))
        # Validate
        sdf = self.model.validate(sdf)
        # Renaming  _(alias is only available after validation)_
        rename = {field.alias: field.original_name for _, field in self.model.__fields__.values()}
        sdf = sdf.withColumnsRenamed(rename)
        return sdf


class DerivedDataset(BaseDataset):
    """DerivedDataset docstring.
    TODO
    """

    def timestamp(self) -> float:
        return max(d.timestamp for d in self.dependencies)


class ParcelDataset(DerivedDataset):
    """ParcelDataset docstring.
    TODO
    """

    transform_kwargs: dict = {}

    def transform(self, reference_parcels: BaseDataset, features: BaseDataset) -> SparkDataFrame:
        return sjoin_parcel_proportion(
            reference_parcels.sdf(),
            features.sdf(),
            **self.transform_kwargs,
        )


class BoundaryDataset(DerivedDataset):
    """BoundaryDataset docstring.
    TODO
    """

    transform_kwargs: dict = {}

    def transform(self, reference_parcels: BaseDataset, boundary_segments: BaseDataset, features: BaseDataset) -> SparkDataFrame:
        return sjoin_boundary_proportion(
            reference_parcels.sdf(),
            boundary_segments.sdf(),
            features.sdf(),
            **self.transform_kwargs,
        )
