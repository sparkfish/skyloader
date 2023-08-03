# skyloader
Loads Excel, CSV and other tabular data files from a cloud drive provider into a target database table


Skyloader loads Excel files into a destination Postgres or SQL Server database.  Loading is performed using a conventions over configuration approach, while offering several configuration options to manage variations that may naturally come up in the real world.

For each registered folder, the source folder name should match the target table's name.  If the table doesn't exist, one will create be created that matches the configured source folder name.  Filenames should match the base folder name, otherwise a filename matching pattern will be required and followed to identify files to be loaded from the source folder.

Each source folder must be configured by registering the source folder path.  Parameters for each registration include the following, each with applicable defaults to streamline the registration process:

| Parameter                 | Default                     | Description                                                 |
|---------------------------|-----------------------------|-------------------------------------------------------------|
| `name`                    | _(required)_                | Name of folder sync process (often the same as actual folder name) |
| `id`                      | _(required)_                | unique folder ID -- obtained from the end of the Google Drive folder URL  |
| `mode`                    | `append`                    | `append` retains records from prior loads; `overwrite` drops target table prior to load; `merge` deletes any rows in target previously associated with this filename or its associated date extracted from the filename |
| `match_pattern`           | `(folder-name)-(YYYY-MM-DD).*` | Uses custom regex expression where `YYYY`, `MM` and `DD` identify date parts extracted from the file |
| `allow_schema_evolution`  | `true`                      | Defaults to `true`; setting this to `false` will trigger an error when the source file does not match the target schema |
| `sheet_name`              |  -                          | Defaults to first sheet in workbook, if a specific sheet name not supplied |
| `date_mask`               |  `YYYYMMDD`                 | Pattern for extracting the date associated with a given file from the filename (e.g., `ABC-20200101.txt` => `01/01/2020`) |
| `target_table`            |  _{name}_                   | Defaults to `name` property but can be overwritten to target a different table name, may include a schema if necessary |

### Folder Structure

The root of each configured folder is scanned by the loader.  The following sub-folders will be created as needed.

- **archive/** - after files are processed, they go into its `archive` subfolder
- **error/** - if a file failed to process, it will be placed in the `error` folder
- **log/** - at the end of each run where files are present, a log of the run will reside in this folder

### Job-Level Parameters

The loader expects the following parameters to be provided:

- **Host or IP** - the IP or hostname of the destination database server that will receive the loaded data
- **Port** - the port number for the destination server
- **Target Schema** - the schema in the destination database
- **Additional Connection Parameters** - if supplied, must be delimited by a semicolon (";") character
