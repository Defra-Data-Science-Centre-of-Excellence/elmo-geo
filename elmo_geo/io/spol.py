"""Upload and Download data directly from SharePoint Online (SPOL)

Using the [Office365-REST-Python-Client](^py-spol) python package.
This handler attempts to read your details via .env, or getpass will wait for users to enter them.

[^py-spol]: https://github.com/vgrem/Office365-REST-Python-Client/
"""
import getpass
from io import BytesIO

import dotenv
from office365.sharepoint.client_context import ClientContext


class SpolHandler:
    def __init__(self, spol_url="https://defra.sharepoint.com/teams/Team1645/"):
        """A wrapper around office365.sharepoint.client_context.ClientContext specifically for Defra portal.
        Intended use is within a elmo_geo.etl.Dataset for exporting.

        ```py
        >>> from elmo_geo.io.spol import SpolHandler
        >>> spol = SpolHandler()

        >>> for f in spol.list_files("Evidence and Analysis WS/0.Archive"):
        >>> if f.serverRelativeUrl.endswith((".csv",)):
        >>>     print(f.serverRelativeUrl)

        >>> spol.download_file("/teams/Team1645/Evidence and Analysis WS/0.Archive/test.csv", "data/test.csv")
        test.csv

        >>> spol.upload_file("data/test.csv", "/teams/Team1645/Evidence and Analysis WS/0.Archive")
        ```
        """
        spol_username = dotenv.get_key(".env", "SPOL_EMAIL") or getpass.getpass("Warning: missing .env SPOL_EMAIL:")
        spol_password = dotenv.get_key(".env", "SPOL_PASS") or getpass.getpass("Info: missing .env SPOL_PASS:")
        self.ctx = ClientContext(spol_url).with_user_credentials(spol_username, spol_password)
        self.test()

    def test(self):
        """Test the connection is working by listing files in the root folder."""
        return self.list_files(spol_path="", recursive=False)

    def list_files(self, spol_path, recursive=True):
        """List files in a SharePoint folder.
        https://github.com/vgrem/Office365-REST-Python-Client/blob/master/examples/sharepoint/folders/list_files.py
        """
        return self.ctx.web.get_folder_by_server_relative_path(spol_path).get_files(recursive).execute_query()

    def upload_file(self, local_path: str | BytesIO, spol_path: str) -> str | BytesIO:
        """Upload a file to a SharePoint folder.
        https://github.com/vgrem/Office365-REST-Python-Client/blob/master/examples/sharepoint/files/upload_large.py
        """
        spol_obj = self.ctx.web.get_folder_by_server_relative_path(spol_path)
        with open(local_path, "rb") as f:
            spol_obj.files.create_upload_session(f, chunk_size=2**20).execute_query()

    def download_file(self, spol_path: str, local_path: str | BytesIO) -> str | BytesIO:
        """Download a file from a SharePoint folder.
        https://github.com/vgrem/Office365-REST-Python-Client/blob/master/examples/sharepoint/files/download_large.py
        """
        spol_obj = self.ctx.web.get_file_by_server_relative_path(spol_path)
        with open(local_path, "wb") as f:
            spol_obj.download_session(f).execute_query()
        return local_path
