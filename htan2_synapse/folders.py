"""
Utilities for creating and managing Synapse folders.
"""

import synapseclient
from typing import Optional
from synapseclient.core.exceptions import SynapseHTTPError


def create_folder(syn, parent_id: str, folder_name: str) -> Optional[str]:
    """
    Create a folder in Synapse if it doesn't exist.
    Checks for existing folders by listing children of the parent.

    Args:
        syn: Synapse client
        parent_id: Synapse ID of the parent folder/project
        folder_name: Name of the folder to create

    Returns:
        Synapse ID of the folder, or None if creation failed
    """
    try:
        # Check if folder already exists by listing children
        children = list(syn.getChildren(parent_id, includeTypes=['folder']))
        for child in children:
            if child['name'] == folder_name:
                print(f"  ✓ Folder '{folder_name}' already exists: {child['id']}")
                return child['id']

        # Create the folder
        folder = synapseclient.Folder(name=folder_name, parent=parent_id)
        folder = syn.store(folder)
        print(f"  ✓ Created folder '{folder_name}': {folder.id}")
        return folder.id

    except Exception as e:
        print(f"  ✗ Failed to create folder '{folder_name}': {e}")
        return None

