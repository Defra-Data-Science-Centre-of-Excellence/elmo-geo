# Databricks notebook source
# MAGIC %md
# MAGIC # Spatial Join lookup tables 

# COMMAND ----------

from glob import iglob
import json
import os
import re

from elmo_geo.st import sjoin
from elmo_geo.utils.misc import dbfs


def string_to_dict(string: str, pattern: str) -> dict:
    """Reverse f-string
    https://stackoverflow.com/a/36838374/10450752
    """
    regex = re.sub(r'{(.+?)}', r'(?P<_\1>.+)', pattern)
    return dict(zip(
        re.findall(r'{(.+?)}', pattern),
        list(re.search(regex, string).groups()),
    ))

def get_f_lookup_parcel(filepath):
    pattern = "{path}/{source}-{dataset}-{version}.{format}"
    path, source, dataset, version, format = string_to_dict(filepath, pattern).values()
    return f"{path}/elmo_geo-lookup_{dataset}-{version}.{format}"

def to_pq(sdf, f):
    sdf.write.format("parquet").save(dbfs(f, True))
    return sdf

def load_sdf():
    for f in iglob():
        pass
    return sdf


os.chdir(os.getcwd().replace('/notebooks/ingestion', ''))

# COMMAND ----------

datasets = json.load('data/catalogue.json')
sdf_parcel = load_sdf('rpa-parcel-adas')

for dataset in datasets:
    if dataset['parcel_lookup'] == "todo":
        dataset["f_parcel_lookup"] = dataset["f_parcel_lookup"] or get_f_lookup_parcel(dataset["filepath"])
        sdf = (
            load_sdf(dataset["filepath"])
            .transform(sjoin, sdf_parcel, lsuffix='_left', rsuffix='', distance=12)
            .selectExpr(
                'id_left AS id',
                'id_parcel',
                'sindex',
            )
            .transform(to_pq, dataset["f_parcel_lookup"])
        )
        dataset['parcel_lookup'] = "done"


datasets



# COMMAND ----------

# DASH
requirements = ['national_parks', 'ramsar', 'peaty_soils', 'national_character_areas', 'sssi', 'aonb', 'moorline', 'commons', 'flood_risk_areas']


# COMMAND ----------

proportion_sql = "ST_Area(ST_Intersection(geometry_left, geometry_parcel)) / ST_Area(geometry_parcel)"



dataset

