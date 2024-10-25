import io

import botocore
import boto3
import dotenv
import pandas as pd

from elmo_geo import LOG
from elmo_geo.etl import Dataset, DerivedDataset


class S3Handler:
    """Simple wrapper around boto3.client which is specific for ELMO's S3 bucket and portal.
    Intended use is within a elmo_geo.etl.Dataset for exporting.
    But it could also be used for uploading larger than 2GB datasets as S3 has no 2GB limit.

    ```py
    >>> from elmo_geo.io.s3 import S3Handler
    >>> s3 = S3Handler()  # defaults to s3-ranch-013

    >>> s3.list_files("data/Elm-Project")
    ["s3://s3-ranch-013/data/Elm-Project/silver/rural_payments_agency/reference_parcels-2024_08_06-6cbd7056.parquet", ...]

    >>> gdf = s3.read_file("data/Elm-Project/silver/rural_payments_agency/reference_parcels-2024_08_06-6cbd7056.parquet", fn_read=gpd.read_parquet)
    >>> gdf
    | id_parcel | area | geometry |
    | --------- | ---- | -------- |
    |           |      |          |

    >>> s3.write_file(gdf, "test-elmo_geo-test.parquet", fn_write=gpd.GeoDataFrame.to_parquet)
    ```
    """

    def __init__(self, bucket: str = "s3-ranch-013"):
        """Create an s3 client connection, set the bucket, and test it works."""
        self.bucket = bucket
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=dotenv.get_key(".env", "aws_access_key_id"),
            aws_secret_access_key=dotenv.get_key(".env", "aws_secret_access_key"),
            aws_session_token=dotenv.get_key(".env", "aws_session_token"),
        )
        self.test()

    def test(self):
        """Test the connection works."""
        try:
            self.s3_client.list_objects_v2(Bucket=self.bucket)
        except (botocore.exception.BotoCoreError, botocore.exceptions.ClientError) as error:
            LOG.error(f"""S3HandlerError: update .env AWS credentials.
                Login and click 'Access keys' (next to 'ELMModelling').
                Copy option 2 into .env
                https://sso-int-sce-network.awsapps.com/start#/

                {error}
            """)

    def list_files(self, prefix=""):
        """List files in the S3 bucket, with pagination support to access more than 1000 responses."""
        files = []
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        while response.get("IsTruncated"):
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                ContinuationToken=response.get("NextContinuationToken"),
            )
        if "Contents" in response:
            files.extend([f"{obj['Key']}" for obj in response["Contents"] if not obj["Key"].endswith("/")])
        return files

    def read_file(self, path: str, fn_read: callable = pd.read_parquet) -> pd.DataFrame:
        """Read a file from S3 and return its content as a DataFrame."""
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=path)
        buf = io.BytesIO(obj["Body"].read())
        return fn_read(buf)

    def write_file(self, df: pd.DataFrame, path: str, fn_write: callable = pd.DataFrame.to_parquet):
        """Write a DataFrame to the S3 bucket as a Parquet file."""
        buf = io.BytesIO()
        fn_write(df, buf)
        buf.seek(0)
        self.s3_client.put_object(Bucket=self.bucket, Key=path, Body=buf.getvalue())

    def copy_obj(self, key_from: str, key_to: str):
        """Rather than writing twice, you can copy the object."""
        self.s3_client.copy_object(
            CopySource={"Bucket": self.bucket, "Key": key_from},
            Bucket=self.bucket,
            Key=key_to,
        )


def sync_datasets(catalogue: list[Dataset]):
    """Writes datasets to S3 ranch-013 bucket.

    Loops through all datasets in the catalogue and writes fresh non-geographic
    DerivedDatasets to S3, if they are not already present in the bucket.

    Parameters:
        catalogue: List of elmo-geo datasets.
    """
    s3 = S3Handler()
    root = "data/ELM-Project/"
    s3_files = s3.list_files(root)

    count = 0
    for dataset in catalogue:
        path = root + dataset.path.split("ELM-Project/")[1]
        path = path.replace("/silver/", "/gold/")  # TODO: Issue #320
        path_latest = "-".join(path.split("-")[:-2]) + "-latest.parquet"
        if not dataset.is_geo and isinstance(dataset, DerivedDataset) and dataset.is_fresh and path not in s3_files:
            LOG.info(f"Exporting {dataset.name} to {s3.bucket}. Path: {path}")
            df = dataset.pdf()
            s3.write_file(df, path)
            s3.copy_obj(path, path_latest)
            count += 1
    LOG.info(f"Exported {count} datasets to {s3.bucket}.")
