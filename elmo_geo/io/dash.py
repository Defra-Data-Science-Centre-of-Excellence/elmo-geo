import os
import shutil
import zipfile

from elmo_geo import LOG
from elmo_geo.utils.dbr import spark


def download_link(path: str) -> str:
    """Returns html for a download link
    Parameters:
        path: Path to the file to be downloaded, must be in the format `/dbfs/` not `dbfs:/`.
    Returns:
        html for a download link as a string to be shown using the displayHTML
            function: `displayHTML(download_link(path))`.
    Note:
        This copies the file to `FileStore` - you may want to clean up afterwards!
    """
    # filepath must be in the format `/dbfs/` not `dbfs:/`
    # Get filename
    filename = path.split("/")[-1]
    shutil.copyfile(path, f"/dbfs/FileStore/{filename}")
    # Construct download url
    url = (
        f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}"
        f"?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    )
    # Return html snippet
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"


def extract_file(from_path: str, to_path: str):
    """Extract a zipped file from FileStore to
    a chosen location
    Parameters:
        from_path: Path of the zip file
        to_path: Directory to move the unzipped file to
    """
    filename = from_path.split("/")[-1]
    from_parent = "/".join(from_path.split("/")[:-1])
    LOG.info(f"Moving {filename} from {from_parent} to {to_path}")
    shutil.move(from_path, to_path + filename)
    LOG.info(f"Extracting {filename}")
    with zipfile.ZipFile(to_path + filename, "r") as zip_ref:
        zip_ref.extractall(to_path)
    LOG.info("Deleting zip file")
    os.remove(to_path + filename)
    LOG.info("Done!")
