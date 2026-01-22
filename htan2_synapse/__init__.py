"""
HTAN2 Synapse utilities package.
Shared utilities for managing HTAN2 Synapse projects, folders, and permissions.
"""

__version__ = "1.0.0"

from .config import (
    HTAN_DCC_ADMINS_TEAM_ID,
    HTAN_DCC_TEAM_ID,
    ACT_TEAM_ID,
    RECORD_BASED_MODULES,
    FILE_BASED_MODULES,
    IMAGING_SUBFOLDERS,
)
from .projects import load_projects
from .teams import find_contributor_team
from .permissions import set_folder_permissions
from .folders import create_folder

__all__ = [
    "HTAN_DCC_ADMINS_TEAM_ID",
    "HTAN_DCC_TEAM_ID",
    "ACT_TEAM_ID",
    "RECORD_BASED_MODULES",
    "FILE_BASED_MODULES",
    "IMAGING_SUBFOLDERS",
    "load_projects",
    "find_contributor_team",
    "set_folder_permissions",
    "create_folder",
]


