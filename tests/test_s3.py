import pytest

from elmo_geo.io.s3 import S3Handler
from tests.test_etl import test_source_dataset


@pytest.mark.dbr
def test_s3_write_read_pdf():
    s3 = S3Handler()
    df = test_source_dataset.pdf()
    path = "data/ELM-Project/" + test_source_dataset.path.split("ELM-Project")[1]

    s3.write_file(df, path)
    df_s3 = s3.read_file(path)

    assert df.equals(df_s3)
