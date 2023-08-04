from skyloader.datafile import DataFile
from pytest import fixture

import pandas as pd

@fixture
def mock_datafile():
    mock_json = {
      "name": "my_data_file",
      "mimeType": "application/vnd.google-apps.spreadsheet",
      "id": "abcd1234",
      "kind": "drive#file",
      "createdTime": "2022-01-01T00:00:00Z",
      "modifiedTime": "2022-01-02T00:00:00Z",
      "parents": ["parent_folder_id"],
    }
    datafile = DataFile.from_gdrive(mock_json)
    datafile.run_id = "20230101_12345"
    return datafile


def test_datafile_initialize_from_gdrive(mock_datafile):
    assert mock_datafile.name == "my_data_file"
    assert mock_datafile.mimetype == "application/vnd.google-apps.spreadsheet"
    assert mock_datafile.id == "abcd1234"
    assert mock_datafile.kind == "drive#file"
    assert mock_datafile.stat == {
      "created_at": "2022-01-01T00:00:00Z",
      "modified_at": "2022-01-02T00:00:00Z"

    }
    assert mock_datafile.parents == ["parent_folder_id"]
    assert mock_datafile.run_id == "20230101_12345"
    assert not mock_datafile.is_inbox
    assert not mock_datafile.is_archive
    assert not mock_datafile.is_error
    assert not mock_datafile.is_logs


def test_processed_total(mock_datafile):
    process_data = lambda _: 1
    mock_datafile.data = pd.DataFrame(
      {
        "a": [1, 2, 3, 4],
        "b": [6, 7, 8, 9],
        "c": ["some", "string", "da", "ta"]
      }
    )
    for record in mock_datafile.records:
        process_data(record)
    assert mock_datafile.processed == 4


