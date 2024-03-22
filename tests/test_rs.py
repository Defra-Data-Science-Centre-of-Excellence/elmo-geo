import numpy as np

from elmo_geo.rs.raster import write_array_to_raster


def test_write_array_to_raster():
    data = np.ones((10, 10))
    outpath = "/dbfs/tmp/elmo_geo_test_write_array_to_raster.tif"
    meta = {"width": 10, "height": 10, "count": 1, "nodata": np.nan}
    write_array_to_raster(data, outpath, meta)
