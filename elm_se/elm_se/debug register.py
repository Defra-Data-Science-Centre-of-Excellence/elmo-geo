# Databricks notebook source
import register

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

register.register(spark)

# COMMAND ----------

from elm_se.log import LOG
from elm_se.types import SparkSession
import sedona
import pkgutil
import inspect
from functools import partial


# COMMAND ----------

module = inspect.getmodule(inspect.stack()[1][0])

# COMMAND ----------

from register import register

# COMMAND ----------

module = inspect.getmodule(register)

# COMMAND ----------

g = pkgutil.walk_packages([module.__file__])

# COMMAND ----------

next(g)
