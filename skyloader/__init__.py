from __future__ import annotations

from skyloader.__version__ import (
    __title__,
    __description__,
    __url__,
    __version__,
    __author__,
    __author_email__,
    __license__,
    __copyright__,
)

_hard_dependencies = set(["numpy", "pandas", "requests"])
_optional_dependencies = {
    "gdrive": set(
        [
            "google-api-core",
            "google-api-python-client",
            "google-auth",
            "googleapis-common-protos",
        ]
    )
}
_dependencies = _optional_dependencies["gdrive"].union(_hard_dependencies)


def _available(dependency: str):
    try:
        __import__(dependency)
        return True
    except:
        return False


_missing_hard_dependencies = set()
for _dep in _hard_dependencies:
    if not _available(_dep):
        _missing_hard_dependencies.add(_dep)

if m := _missing_hard_dependencies:
    raise ImportError(
        f"""The dependencies {*_hard_dependencies,} are required; missing {(*m,)}
      """
    )

del _hard_dependencies, _optional_dependencies, _dependencies, m

from skyloader import (
    datafile,
    drive,
    folder_google,
    folder_sharepoint,
    loader_base,
    loader_manager,
    loader_postgres,
    loader_sql_server,
    mode,
    utils,
)

__all__ = [
    "datafile",
    "drive",
    "folder_google",
    "folder_sharepoint",
    "loader_base",
    "loader_manager",
    "loader_postgres",
    "loader_sql_server",
    "mode",
    "utils",
]
