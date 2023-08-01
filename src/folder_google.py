from .mode import Mode

class GoogleFolder:
    def __init__(self, folder_id, desc, mode=Mode.APPEND, match_pattern="", allow_schema_evolution=True, sheet_name=None, date_mask="YYYYMMDD"):
        self.id = folder_id
        self.desc = desc
        self.mode = mode
        self.match_pattern = match_pattern
        self.allow_schema_evolution = allow_schema_evolution
        self.sheet_name = sheet_name
        self.date_mask = date_mask