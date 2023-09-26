from elmo_geo.utils.dbr import dbutils, sc
from elmo_geo.utils.log import LOG



def run_with_retry(notebook: str, timeout_seconds: int = 8000, max_retries: int = 3):
  """
  This function runs a notebook that is in the elmo-geo git repo.
  Parameters:
  notebok: The notebook you would like to run
  timeout_seonds: the number of seconds it will take before it times out
  max_retries: number of retries the notebook will try

  Returns: The finished notebook
  """
  def _run_with_retry(args):
    LOG.info(f"Starting {args}")
    for n in range(max_retries):
      try:
        return dbutils.notebook.run(
          path = notebook,
          timeout_seconds = timeout_seconds,
          arguments = args,
        )
      except Exception:
        if n > max_retries:
          LOG.warning(f"Ran out of retries for {args}")
          return
        else:
          LOG.warning(f"Retrying error for {args}")
  return _run_with_retry


def map_for(fn:callable, lst:iter) -> list:
  '''Breaks on first element of loop'''
  for i in lst:
    return fn(i)

def map_single(fn:callable, lst:iter) -> list:
  return list(map(fn, list))

def map_parallelised(fn:callable, lst:iter) -> list:
  return list(Pool().map(fn, lst))

def map_distributed(fn:callable, lst:iter) -> list:
  return listsc.parallelize(lst).map(fn).collect()