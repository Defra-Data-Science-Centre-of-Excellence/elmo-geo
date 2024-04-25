# Ingest

## Method
1. `01_identify`, Identify datasets, manually adding the "dataset" to `data/catalogue.json`.
2. `02_convert`, Convert datasets to be easily access by spark, partitioning large datasets approriately.
3. `03_lookup`, Spatially join with parcels, using distance, KNN, and metrics if declared in dataset, saving just lookup tables.

## Data Sources
A list of places that have been used to identify datasets.

