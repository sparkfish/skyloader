import datetime
import io
import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

import pandas as pd

from simpleloader.config import GDRIVE_ROOT_FOLDER_ID, GDRIVE_SA_TOKEN, RUN_ID
from simpleloader.datafile import DataFile
from simpleloader.utils import authenticated

SCOPES = ["https://www.googleapis.com/auth/drive"]


class Drive:
    def __init__(self):
        self.root_id = GDRIVE_ROOT_FOLDER_ID
        self.root = DataFile(
            name="Root",
            identifier=self.root_id,
            mimetype="application/vnd.google-apps.folder",
        )
        self.authenticated = False
        self.service = None
        self.successful = None
        self.fail = None

    def authenticate(self):
        """Authenticate to Google Drive using a service account."""
        creds = Credentials.from_service_account_info(GDRIVE_SA_TOKEN, scopes=SCOPES)
        service = build("drive", "v3", credentials=creds)
        self.service = service
        self.authenticated = True


    @authenticated
    def _ls(self, folder_id=GDRIVE_ROOT_FOLDER_ID):
        """List all files in a Google Drive folder."""
        fields = "files(id, name, createdTime, kind, mimeType, modifiedTime, properties, parents)"
        results = (
            self.service.files()
            .list(
                q=f"'{folder_id}' in parents", 
                fields=fields,
                corpora="allDrives",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True
            )
            .execute()
        )
        return results.get("files", [])

    def ls(
        self, folder_id=GDRIVE_ROOT_FOLDER_ID, download=False, most_recent_only=False
    ):
        datafiles = [
            DataFile.from_gdrive(item) for item in self._ls(folder_id=folder_id)
        ]
        if most_recent_only and datafiles:
            datafiles = [max(datafiles, key=lambda d: d.modified_at and d.is_file)]
        datafiles.sort(key=lambda d: d.modified_at)
        if download:
            self.download_files(datafiles)
        return datafiles


    @authenticated
    def download_files(self, files: list):
        for file in files:
            if not file.is_folder:
                fh = self.download_file(file.identifier)
                file.fh = fh
                file.data = pd.read_excel(fh) ###  <<< wouldn't this blow up the memory usage if we are loading all files into memory??  should we move this function outside the "drive" to simply return file-like objects to be processed externally??


    @authenticated
    def download_file(self, file_id):
        """Download a file from Google Drive and return a file-like object."""
        request = self.service.files().get_media(fileId=file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        i = 0
        while not done:
            status, done = downloader.next_chunk()
            if not (i % 5):
                print(f"Downloaded {i+1} chunks")
        fh.seek(0)
        return fh


    @authenticated
    def move_file(self, file_id, new_parent_id):
        """Move a file to a new folder."""
        file = self.service.files().get(fileId=file_id, fields="parents", supportsAllDrives=True).execute()
        previous_parents = ",".join(file.get("parents"))
        file = (
            self.service.files()
            .update(
                fileId=file_id,
                addParents=new_parent_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )
        return file


    @authenticated
    def rename_file(self, file_id, new_name):
        """Rename a file."""
        file = (
            self.service.files()
            .update(fileId=file_id, body={"name": new_name}, fields="id, name", supportsAllDrives=True)
            .execute()
        )
        return file


    @authenticated
    def move_and_rename_file(self, file_id, new_parent_id, new_name):
        """Move and rename a file."""
        file = self.service.files().get(fileId=file_id, fields="parents", supportsAllDrives=True).execute()
        previous_parents = ",".join(file.get("parents"))
        file = (
            self.service.files()
            .update(
                fileId=file_id,
                body={"name": new_name},
                addParents=new_parent_id,
                removeParents=previous_parents,
                fields="id, parents, name",
                supportsAllDrives=True
            )
            .execute()
        )
        return file
    

    @authenticated
    def upload_file(self, fh, file_name, parent_folder_id):
        """Upload a file-like object to Google Drive."""
        fh.seek(0)
        media = MediaIoBaseUpload(fh, mimetype='application/octet-stream', resumable=True)
        request = self.service.files().create(
            media_body=media,
            body={
                'name': file_name,
                'parents': [parent_folder_id]
            }
        )
        request.execute()


    @authenticated
    def upload_log_to_drive(self, log_stream, file_name, parent_folder_id):
        """Upload a log stream to Google Drive."""
        log_stream.seek(0)  # Ensure the stream is at the beginning
        byte_stream = io.BytesIO(log_stream.getvalue().encode())  # Encode text to bytes
        media = MediaIoBaseUpload(byte_stream, mimetype='text/plain', resumable=True)
        request = self.service.files().create(
            media_body=media,
            body={
                'name': file_name,
                'parents': [parent_folder_id]
            },
            fields='id',
            supportsAllDrives=True
        )
        request.execute()
