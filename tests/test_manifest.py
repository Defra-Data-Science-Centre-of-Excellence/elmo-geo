from glob import glob
import json


def used_datapaths():
    return set(list(
        glob('/dbfs/mnt/lab/unrestricted/elm/data/*.parquet'),
        glob('/dbfs/mnt/lab/restricted/ELM-Project/data/*.parquet'),
    ))


def test_manifests():
    manifests = json.load(open('../manifests.json'))
    for path in used_datapaths():
        assert path in data.keys(), f'{path} missing from manifest, please provide atleast a source and creator'
