import datetime
import traceback

from simpleloader.config import logger, RUN_ID, log_stream
from simpleloader.datafile import DataFile
from simpleloader.drive import Drive
from simpleloader.loader import Loader


class LoaderManager():

    def __init__(self):
        self.drive = Drive()
        self.loader = Loader()
        self.root = self.drive.root
        self.inbox = None
        self.archive = None
        self.logs = None
        self.error = None


    def __enter__(self):
        self.configure_folders()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        return


    def find_folders(self):
        for folder in self.drive.ls():
            if folder.is_inbox:
                self.inbox = folder
            elif folder.is_archive:
                self.archive = folder
            elif folder.is_logs:
                self.logs = folder
            elif folder.is_error:
                self.error = folder


    def configure_folders(self):
        logger.debug("Locating inbox, archive, logs, error folders")
        self.find_folders()
        for item in ("inbox", "archive", "logs", "error"):
            if getattr(self, item) is None:
                setattr(self, item, self.drive.root)
                logger.warn(
                    f"{item.title()} not found; setting {item} as root folder (gdrive_id={self.drive.root.id})"
                )


    def ls(self, *args, **kwargs):
        return self.drive.ls(*args, **kwargs)


    def process_datafile(self, datafile):
        if datafile.is_folder:
            logger.warning(f"Found folder {datafile} in Inbox, skipping")
            return
        logger.info(f"Now processing {datafile}")
        try:
            self.insert_metadata_fields(datafile)
            self.loader.load_datafile(datafile)
            self.mark_success(datafile)
        except Exception as e:
            logger.error(traceback.format_exc())
            self.mark_fail(datafile)
        else:
            logger.info(f"Successfully processed {datafile}")


    def process_files(self):
        datafiles = self.ls(
            self.inbox.identifier, most_recent_only=False, download=True
        )
        if datafiles:
            for datafile in datafiles:
                self.process_datafile(datafile)
        else:
            logger.info("No datafiles in Inbox, nothing to do! Exiting")


    def insert_metadata_fields(self, datafile):
        logger.debug(f"Adding metadata fields to {datafile}")
        if datafile.data is not None:
            datafile.data["run_id"] = RUN_ID
            datafile.data["loaded_at"] = datetime.datetime.now()
        else:
            logger.warning(f"No data present in datafile {datafile}")


    def datafile_name(self, datafile, kind="archive"):
        valid_kinds = ("archive", "error")
        if kind not in valid_kinds:
            raise Exception(f"Expected kind to be one of {valid_kinds}; got {kind} instead")
        if self.is_root(kind):
            return f"{kind}-{datafile.run_name}"
        return datafile.run_name
    

    def is_root(self, kind):
        return getattr(self, kind).id == self.root.id


    def move_file_to_destination(self, datafile, kind):
        logger.debug(f"Moving {datafile} to {kind} folder")
        name = self.datafile_name(datafile, kind)
        parent_folder = getattr(self, kind)
        self.drive.move_and_rename_file(datafile.id, parent_folder.id, name)
        parent_name = "/" if parent_folder.name == "Root" else f"{parent_folder.name}/"
        logger.info(f"Moved {datafile} to {parent_name}{name}")


    def mark_fail(self, datafile):
        self.move_file_to_destination(datafile, "error")


    def mark_success(self, datafile):
        self.move_file_to_destination(datafile, "archive")


    def upload_run_logs_to_drive(self):
        self.drive.upload_log_to_drive(log_stream, f"{RUN_ID}.logs", self.logs.identifier)
        log_stream.truncate()
