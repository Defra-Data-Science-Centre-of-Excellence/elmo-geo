from pyspark.sql import functions as F

from elmo_geo.st.index import centroid_index, chipped_index, sindex
from elmo_geo.utils.misc import sh_run
from elmo_geo.utils.settings import FOLDER_CONDA
from elmo_geo.utils.types import SparkDataFrame


def convert_file(f_in: str, f_out: str):
    sh_run(
        f"""
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
    )


def to_gpq_partitioned(sdf: SparkDataFrame, sf_out: str):
    """SparkDataFrame to GeoParquet, partitioned by BNG index"""
    sdf.transform(centroid_index).write.format("geoparquet").save(sf_out, partitionBy="sindex")


def to_gpq_sorted(sdf: SparkDataFrame, sf_out: str):
    """SparkDataFrame to GeoParquet, sorted by BNG index"""
    (
        sdf.transform(centroid_index, resolution="1km")
        .sort("sindex")
        .write.format("geoparquet")
        .save(sf_out)
    )


def to_gpq_zsorted(sdf: SparkDataFrame, sf_out: str):
    """SparkDataFrame to GeoParquet, sorted by geohash index"""
    (
        sdf.transform(sindex, resolution="1km", index_join=chipped_index)
        .withColumn(
            "geohash",
            F.expr(
                'ST_GeoHash(ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326")))'
            ),
        )
        .sort("geohash")
        .write.format("geoparquet")
        .save(sf_out)
    )
