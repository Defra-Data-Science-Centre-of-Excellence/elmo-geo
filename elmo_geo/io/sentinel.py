from contextlib import contextmanager

import urllib3
from sentinelsat import SentinelAPI

from elmo_geo.log import LOG

urllib3.disable_warnings()


@contextmanager
def sentinel_api_session(*args, **kwargs):
    """Context manager for SentinelSat API session
    To be used in with statement as follows:

    ```with SentinelAPISession() as api:
          ...
    ```
    Yields:
        The SentinelAPI object
    """
    USER = "burrowsej-defra"
    PASSWORD = "elmo_e1m0"
    URL = "https://apihub.copernicus.eu/apihub"

    api = SentinelAPI(USER, PASSWORD, URL, *args, **kwargs)
    # SSL certification issue with Databricks - need to disable verification for now
    api.session.verify = False
    # don't show SSL warnings from turning of verification
    urllib3.disable_warnings()
    LOG.info("API session initiated")
    try:
        yield api
    finally:
        api.session.close()
        LOG.info("API session closed")


def download_sentinel():
    raise NotImplementedError('download_sentinel')
