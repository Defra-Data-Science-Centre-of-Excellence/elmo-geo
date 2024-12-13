"""Functions for processing geospatial data using GeoVector cluster for ELMO"""

from elmo_geo.datasets import catalogue
from elmo_geo.utils.log import LOG
from elmo_geo.utils.register import register


def refresh_datasets():
    """Run the whole ETL to build any missing datasets and refresh any stale ones."""
    register()
    LOG.info("Refreshing all datasets...")
    for dataset in catalogue:
        if not dataset.is_fresh:
            dataset.refresh()
            # TODO: dataset.sync()  # sync to s3
            # TODO: dataset.export()  # display download link
    LOG.info("All datasets are up to date.")
