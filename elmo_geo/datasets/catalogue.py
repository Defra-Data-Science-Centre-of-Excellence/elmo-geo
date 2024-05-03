import json


def load_catalogue() -> dict | list:
    """Load the data catalogue"""
    f = "data/catalogue.json"
    with open(f, "r") as fp:
        obj = json.loads(fp.read())
    return obj


def save_catalogue(obj: dict | list):
    """Save the data catalogue"""
    f = "data/catalogue.json"
    with open(f, "w", encoding="utf-8") as fp:
        json.dump(obj, fp, ensure_ascii=False, indent=4)


def run_task_on_catalogue(task: str, fn: callable):
    """Run a task on all datasets with that task set to "todo".
    ```py
    def lookup_parcel(dataset):
        f = "{}/elmo_geo-lookup_{}.parquet".format(SILVER, dataset["name"].split("-")[1])
        sdf_parcel = spark.read.parquet(find_datasets("rpa-parcel-adas")["uri"])
        sdf_other = spark.read.parquet(dataset["uri"])
        sdf = sjoin(sdf_parcel, sdf_other).select("id_parcel", "fid")
        sdf.toPandas().to_parquet(f)
        LOG.info(f"Complete Task: lookup_parcel, {dataset['name']}. {f}")
        dataset["tasks"]["lookup_parcel"] = f
        return dataset

    run_task_on_catalogue("lookup_parcel", lookup_parcel)
    ```
    """
    catalogue = load_catalogue()
    for i, dataset in enumerate(catalogue):
        if getattr(dataset["tasks"], task, False) == "todo":
            try:
                catalogue[i] = fn(dataset)
            except Exception as err:
                LOG.warning(f"Failed {task}\n{dataset}\n{err}")
    save_catalogue(catalogue)


def find_datasets(string: str) -> list[dict]:
    """Find datasets with like names, returns a list
    Example:
        ```py
        parcel = find_dataset('rpa-parcel-adas')[0]
        sdf_parcel = spark.read.parquet(parcel['uri'])
        ```
    """
    return [dataset for dataset in load_catalogue() if string in dataset["name"]]


def add_to_catalogue(datasets: list[dict]):
    """Add a new dataset to the catalogue
    By replacing the same name or appending.
    """
    catalogue = load_catalogue()
    for dataset_new in datasets:
        for i, dataset_catalogue in enumerate(catalogue):
            if dataset_new["name"] == dataset_catalogue["name"]:
                catalogue[i] = dataset_new 
                break
        else:
            catalogue.append(dataset_new)
    save_catalogue(catalogue)
