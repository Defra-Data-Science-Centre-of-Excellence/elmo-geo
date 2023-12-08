import os
import shutil
import tempfile
import zipfile


def download_link_file(
    filepath,
    filestore_path = '/dbfs/FileStore',
    copy = True,
) -> str:
    filename = filepath[filepath.rfind("/"):]
    if move:
        shutil.copy(filepath, f"{filestore_path}/{filename}")
    else:
        shutil.move(filepath, f"{filestore_path}/{filename}")
    url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"


def download_link_dir(
    folderpath,
    filestore_path = '/dbfs/FileStore'
) -> str:
    foldername = folderpath[folderpath.rfind("/"):]
    with tempfile.NamedTemporaryFile(suffix='.zip') as zf:
        with zipfile.ZipFile(zf.name, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
            for folder_name, subfolders, filenames in os.walk(folderpath):
                for filename in filenames:
                    file_path = os.path.join(folder_name, filename)
                    zip_ref.write(file_path, arcname=os.path.relpath(file_path, folderpath))
    return download_file(f"{filestore_path}/{foldername}.zip", filestore_path=filestore_path, copy=True)
