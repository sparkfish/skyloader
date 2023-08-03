from datetime import datetime
from pathlib import Path

import numpy as np

class DataFile:
    def __init__(
        self,
        name=None,
        mimetype=None,
        identifier=None,
        kind=None,
        stat=None,
        parents=None,
        run_id=None
    ):
        self.name = name
        self.mimetype = mimetype
        self.identifier = identifier
        self.kind = kind
        self.stat = stat
        self.fh = None
        self.data = None
        self.success = False
        self.fail = False
        self.processed = 0
        self.parents = parents if parents is not None else []

    @classmethod
    def from_gdrive(cls, gdrive_object):
        c = cls()
        c.name = gdrive_object["name"]
        c.mimetype = gdrive_object["mimeType"]
        c.identifier = gdrive_object["id"]
        c.kind = gdrive_object["kind"]
        c.stat = {
            "created_at": gdrive_object["createdTime"],
            "modified_at": gdrive_object["modifiedTime"],
        }
        c.parents = gdrive_object["parents"]
        return c

    @property
    def id(self):
        return self.identifier

    @property
    def as_path(self):
        return Path(self.name)

    @property
    def stem(self):
        return self.as_path.stem

    @property
    def suffix(self):
        return self.as_path.suffix

    @property
    def run_name(self):
        return f"{self.stem}-{self.run_id}{self.suffix}"

    @property
    def _created_at(self):
        return self.stat["created_at"] if self.stat is not None else None

    @property
    def _modified_at(self):
        return self.stat["modified_at"] if self.stat is not None else None

    @property
    def created_at(self):
        return datetime.strptime(self._created_at, '%Y-%m-%dT%H:%M:%S.%fZ')

    @property
    def modified_at(self):
        return datetime.strptime(self._modified_at, '%Y-%m-%dT%H:%M:%S.%fZ')

    @property
    def is_folder(self):
        return self.mimetype == "application/vnd.google-apps.folder"

    @property
    def is_file(self):
        return not self.is_folder

    @property
    def is_inbox(self):
        return self.name.lower() == "Inbox" ## <<< NOTE: we are NOW using the root as the inbox for each registered folder

    @property
    def is_archive(self):
        return self.name.lower() == "archive" if self.name else False

    @property
    def is_error(self):
        return self.name.lower() == "error" if self.name else False

    @property
    def is_logs(self):
        return self.name.lower() == "log" if self.name else False

    @property
    def records(self):
        records = self.data.replace({np.nan: None}).to_dict(orient='records')
        for item in records:
            self.processed += 1
            yield tuple(item.values())

    @property
    def columns(self):
        return self.data.columns

    @property
    def tablename(self):
        return self.stem

    def __repr__(self):
        return f"<Datafile {self.name} id={self.identifier}>"
