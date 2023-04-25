from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from elmo_geo.log import LOG

# Define SparkSession and dbutils
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)


def run_with_retry(notebook: str, timeout_seconds: int = 3600, max_retries: int = 1):
    """
    This function runs a notebook that is in the elmo-geo git repo.
    Parameters:
    notebok: The notebook you would like to run
    timeout_seonds: the number of seconds it will take before it times out
    max_retries: number of retries the notebook will try

    Returns: The finished notebook
    """

    def _run_with_retry(args):
        num_retries = 0
        LOG.info(f"Starting {args}")
        while True:
            try:
                return dbutils.notebook.run(
                    path=notebook, timeout_seconds=timeout_seconds, arguments=args
                )
            except Exception as e:
                if num_retries > max_retries:
                    LOG.warning(f"Ran out of retries for {args}")
                    LOG.info(e)
                    return
                else:
                    LOG.warning(f"Retrying error for {args}")
                    num_retries += 1
        LOG.info(f"Finished {args}")

    return _run_with_retry
