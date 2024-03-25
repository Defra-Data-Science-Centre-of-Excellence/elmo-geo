import numpy as np
import pytest

from elmo_geo.rs.raster import write_array_to_raster


@pytest.mark.dbr
def test_write_array_to_raster():
    data = np.ones((1, 10, 10), dtype=np.uint8)
    outpath = "/dbfs/tmp/elmo_geo_test_write_array_to_raster.tif"

    write_array_to_raster(data, outpath, width=10, height=10, count=1, dtype=data.dtype)
