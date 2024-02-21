from elmo_geo import LOG
from elmo_geo.utils.dbr import dbutils


def run_with_retry(notebook: str, timeout_seconds: int = 8000, max_retries: int = 3):
    """This function runs a notebook that is in the elmo-geo git repo.

    Parameters
    ----------
    notebok: The notebook you would like to run
    timeout_seonds: the number of seconds it will take before it times out
    max_retries: number of retries the notebook will try

    Returns: The finished notebook

    """

    def _run_with_retry(args):
        LOG.info(f"Starting {args}")
        for n in range(max_retries):
            try:
                return dbutils.notebook.run(path=notebook, timeout_seconds=timeout_seconds, arguments=args)
            except Exception:
                if n > max_retries:
                    LOG.warning(f"Ran out of retries for {args}")
                    return None
                else:
                    LOG.warning(f"Retrying error for {args}")

    return _run_with_retry
