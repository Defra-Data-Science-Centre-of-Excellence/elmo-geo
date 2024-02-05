# Databricks notebook source
from glob import glob
from importlib import import_module
import os

# COMMAND ----------

def test_imports():
    fails = {}
    for f in glob("elmo_geo/**/*.py", recursive=True):
        m = f.replace('/', '.')[:-3]
        if f.endswith('__init__.py'):
            m = m.replace('.__init__', '')
        try:
            import_module(m)
        except Exception as e:
            fails[m] = e
    assert not fails, fails

# COMMAND ----------

fs = glob("../elmo_geo/**/*.py", recursive=True)
f = fs[3]
f

# COMMAND ----------

m = f.replace('/', '.')[:-3]
m

# COMMAND ----------

import_module(m)

# COMMAND ----------

test_imports()
