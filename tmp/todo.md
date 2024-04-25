- [x] 00_readme should be the title page, merging the readmes
- [x] 01_ingest is copied from aw-notebooks
- [ ] 02_splitting_method snapping to a segmented boundary
- [ ] 03_metrics should contain the output table
- [ ] move Business Info to another place
- [ ] load_sdf
- [ ] delete copied bits from aw-notebooks once merged
- QA
  - [x] check the overlaps of rpa-parcel-adas
  - [ ] ensure buffer totals are less than parcel total area
  - [ ] ensure meter total is reasonable
  - [ ] boundary segmentation method
  - [ ] review my OSM and OS-NGD filters


```py
from pyspark.sql import functions as F, types as T

from elmo_geo import register, LOG
from elmo_geo.utils.misc import dbfs, sh_run, snake_case


def dl_esri(dataset):
    '''ESRI
    Uses esridump to serially download batches.
    '''
    uri, name = dataset['uri'], dataset['name']
    f_raw = f'{RAW}/{name}.geojson'
    sh_run(f"esri2geojson '{uri}' '{f_raw}'")
    LOG.info(f'Task ESRI complete: {name}, {uri}, {f_raw}')
    return f_raw

def dl_spol(dataset):  # TODO
    '''SharePoint OnLine
    '''
    uri, name = dataset['uri'], dataset['name']
    f_raw = f'{RAW}/{name}'
    raise NotImplementedError
    LOG.info(f'Task SharePoint complete: {name}, {uri}, {f_raw}')
    return f_raw

def suffix(l, r, lsuffix, rsuffix):
    """Add a suffix for columns with the same name in both SparkDataFrames
    """
    for col in l.columns:
        if col in r.columns:
            l.withColumnRenamed(col, col+lsuffix)
            r.withColumnRenamed(col, col+rsuffix)
    return l, r

def find_geometry_column(sdf):
    for col in sdf.schema:
        if isinstance(col.dataType, T.BinaryType):
            return col.name


def string_to_dict(string: str, pattern: str) -> dict:
    """Reverse f-string
    https://stackoverflow.com/a/36838374/10450752
    """
    regex = re.sub(r'{(.+?)}', r'(?P<_\1>.+)', pattern)
    return dict(zip(
        re.findall(r'{(.+?)}', pattern),
        list(re.search(regex, string).groups()),
    ))

def create_lookup_parcel_path(filepath):
    pattern = "{path}/{source}-{dataset}-{version}.{format}"
    path, source, dataset, version, format = string_to_dict(filepath, pattern).values()
    return f"{path}/elmo_geo-lookup_{dataset}-{version}.{format}"

# COMMAND ----------

datasets = json.load("data/catalogue.json")
sdf_parcel = load_sdf("rpa-parcel-adas")

for dataset in datasets:
    if dataset["lookup_parcel"] == True:
        dataset["lookup_parcel"] = create_lookup_parcel_path(dataset["name"])
        sdf = (
            load_sdf(dataset["name"])
            .transform(sjoin, sdf_parcel, lsuffix="_left", rsuffix="", distance=12)
            .selectExpr(
                "id_left AS id",
                "id_parcel",
                "sindex",
            )
            .transform(to_pq, dataset["lookup_parcel"])
        )
        LOG.info("Task lookup_parcel: {}".format(dataset["name"]))

requirements = ['national_parks', 'ramsar', 'peaty_soils', 'national_character_areas', 'sssi', 'aonb', 'moorline', 'commons', 'flood_risk_areas']
proportion_sql = "ST_Area(ST_Intersection(geometry_left, geometry_parcel)) / ST_Area(geometry_parcel)"
```