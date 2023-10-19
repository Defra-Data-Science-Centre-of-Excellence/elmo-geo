import pkgutil

from elmo_geo import LOG
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.types import SparkSession

try:
    from sedona.spark import SedonaContext

    register_sedona = SedonaContext.create
except Exception:
    from sedona.register import SedonaRegistrator

    register_sedona = SedonaRegistrator.registerAll


def register(spark: SparkSession = spark):
    register_sedona(spark)
    LOG.info("Registered: Sedona")


def register_submodules(path):
    """Import all modules/submodule <file>.py to __init__.py
    https://stackoverflow.com/a/3365846/10450752
    """
    for loader, module_name, _ in pkgutil.walk_packages(path):
        globals()[module_name] = loader.find_module(module_name).load_module(module_name)
