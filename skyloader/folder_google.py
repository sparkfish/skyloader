import re
from .mode import Mode

class GoogleFolder:
    def __init__(self, folder_id, desc, mode=Mode.APPEND, match_pattern="", allow_schema_evolution=True, sheet_name=None, date_mask="YYYYMMDD"):
        # Validate folder_id
        if not folder_id or not isinstance(folder_id, str):
            raise ValueError("Folder ID must be a non-empty string")
        
        folder_id = folder_id.strip()
        
        # Google Drive folder IDs are typically 33 characters long and contain alphanumeric characters, hyphens, and underscores
        if not re.match(r'^[a-zA-Z0-9_-]{10,50}$', folder_id):
            raise ValueError("Invalid Google Drive folder ID format")
        
        # Validate description
        if not desc or not isinstance(desc, str):
            raise ValueError("Description must be a non-empty string")
        
        # Validate mode
        if not isinstance(mode, Mode):
            raise ValueError("Mode must be a Mode enum value")
        
        # Validate match_pattern if provided
        if match_pattern and not isinstance(match_pattern, str):
            raise ValueError("Match pattern must be a string")
        
        # Validate allow_schema_evolution
        if not isinstance(allow_schema_evolution, bool):
            raise ValueError("allow_schema_evolution must be a boolean")
        
        # Validate sheet_name if provided
        if sheet_name is not None and (not isinstance(sheet_name, str) or not sheet_name.strip()):
            raise ValueError("Sheet name must be a non-empty string if provided")
        
        # Validate date_mask
        if not isinstance(date_mask, str) or not date_mask.strip():
            raise ValueError("Date mask must be a non-empty string")
        
        # Check for valid date mask format (should contain Y, M, D)
        if not all(char in date_mask for char in ['Y', 'M', 'D']):
            raise ValueError("Date mask must contain Y, M, and D characters")
        
        self.id = folder_id
        self.desc = desc.strip()
        self.mode = mode
        self.match_pattern = match_pattern
        self.allow_schema_evolution = allow_schema_evolution
        self.sheet_name = sheet_name.strip() if sheet_name else None
        self.date_mask = date_mask.strip()
