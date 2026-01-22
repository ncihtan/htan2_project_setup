"""
Utilities for setting folder permissions in Synapse.
"""

import json
from .config import HTAN_DCC_ADMINS_TEAM_ID, HTAN_DCC_TEAM_ID, ACT_TEAM_ID
from .teams import find_contributor_team


def set_folder_permissions(syn, folder_id: str, folder_type: str, version: str, project_name: str = None):
    """
    Set permissions for a folder based on its type.
    
    Args:
        syn: Synapse client
        folder_id: Synapse ID of the folder
        folder_type: Type of folder (e.g., "v8_ingest", "v8_staging", "v8_release")
        version: Version prefix (e.g., "v8")
        project_name: Name of the project (e.g., "HTAN2_Ovarian") - used to find contributor team
    """
    try:
        if folder_type == f"{version}_ingest":
            # Ingest: Centers can edit/delete
            # HTAN DCC Admins: Admin
            try:
                syn.setPermissions(
                    folder_id,
                    principalId=HTAN_DCC_ADMINS_TEAM_ID,
                    accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE", "MODERATE", "CHANGE_PERMISSIONS", "CHANGE_SETTINGS"]
                )
            except Exception as e:
                print(f"    ⚠ Warning: Could not set permissions for HTAN DCC Admins: {e}")
            
            # HTAN DCC: Edit/Delete
            try:
                syn.setPermissions(
                    folder_id,
                    principalId=HTAN_DCC_TEAM_ID,
                    accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
                )
            except Exception as e:
                print(f"    ⚠ Warning: Could not set permissions for HTAN DCC: {e}")
            
            # ACT: Edit/Delete
            try:
                syn.setPermissions(
                    folder_id,
                    principalId=ACT_TEAM_ID,
                    accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
                )
            except Exception as e:
                print(f"    ⚠ Warning: Could not set permissions for ACT: {e}")
            
            # Contributors: Edit/Delete
            if project_name:
                contributors_team_id = find_contributor_team(syn, project_name)
                if contributors_team_id:
                    try:
                        syn.setPermissions(
                            folder_id,
                            principalId=contributors_team_id,
                            accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
                        )
                        print(f"    ✓ Set permissions for {project_name}_contributors: Edit/Delete")
                    except Exception as e:
                        print(f"    ⚠ Warning: Could not set permissions for contributors: {e}")
                else:
                    print(f"    ⚠ Warning: Contributor team not found for {project_name}")
            
            # All other users: View Only (inherited from project)
            
        elif folder_type == f"{version}_staging":
            # Staging: Centers view-only, DCC can edit
            # HTAN DCC Admins: Admin
            try:
                syn.setPermissions(
                    folder_id,
                    principalId=HTAN_DCC_ADMINS_TEAM_ID,
                    accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE", "MODERATE", "CHANGE_PERMISSIONS", "CHANGE_SETTINGS"]
                )
            except Exception as e:
                print(f"    ⚠ Warning: Could not set permissions for HTAN DCC Admins: {e}")
            
            # HTAN DCC: Edit/Delete
            try:
                syn.setPermissions(
                    folder_id,
                    principalId=HTAN_DCC_TEAM_ID,
                    accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
                )
            except Exception as e:
                print(f"    ⚠ Warning: Could not set permissions for HTAN DCC: {e}")
            
            # ACT: Edit/Delete
            try:
                syn.setPermissions(
                    folder_id,
                    principalId=ACT_TEAM_ID,
                    accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
                )
            except Exception as e:
                print(f"    ⚠ Warning: Could not set permissions for ACT: {e}")
            
            # Contributors: Modify only (no Create, no Delete)
            if project_name:
                contributors_team_id = find_contributor_team(syn, project_name)
                if contributors_team_id:
                    try:
                        syn.setPermissions(
                            folder_id,
                            principalId=contributors_team_id,
                            accessType=["READ", "DOWNLOAD", "UPDATE"]
                        )
                        print(f"    ✓ Set permissions for {project_name}_contributors: Modify (no Create/Delete)")
                    except Exception as e:
                        print(f"    ⚠ Warning: Could not set permissions for contributors: {e}")
                else:
                    print(f"    ⚠ Warning: Contributor team not found for {project_name}")
            
            # All other users: View Only (inherited from project)
            
        elif folder_type == f"{version}_release":
            # Release: Everyone view-only, only DCC Admins can edit
            # Use REST API to set ACL explicitly and break inheritance
            try:
                # Build the ACL JSON structure
                resource_access_list = []
                
                # HTAN DCC Admins: Admin
                resource_access_list.append({
                    "principalId": int(HTAN_DCC_ADMINS_TEAM_ID),
                    "accessType": ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE", "MODERATE", "CHANGE_PERMISSIONS", "CHANGE_SETTINGS"]
                })
                
                # HTAN DCC: View Only
                resource_access_list.append({
                    "principalId": int(HTAN_DCC_TEAM_ID),
                    "accessType": ["READ", "DOWNLOAD"]
                })
                
                # ACT: View Only
                resource_access_list.append({
                    "principalId": int(ACT_TEAM_ID),
                    "accessType": ["READ", "DOWNLOAD"]
                })
                
                # Contributors: View Only
                if project_name:
                    contributors_team_id = find_contributor_team(syn, project_name)
                    if contributors_team_id:
                        resource_access_list.append({
                            "principalId": int(contributors_team_id),
                            "accessType": ["READ", "DOWNLOAD"]
                        })
                
                # Set ACL using REST API
                acl_body = {
                    "id": folder_id,
                    "resourceAccess": resource_access_list
                }
                
                syn.restPUT(f"/entity/{folder_id}/acl", body=json.dumps(acl_body))
                print(f"    ✓ Set ACL for {folder_type}/ with explicit permissions")
                print(f"      - HTAN DCC Admins: Admin")
                print(f"      - HTAN DCC: View Only")
                print(f"      - ACT: View Only")
                if project_name:
                    contributors_team_id = find_contributor_team(syn, project_name)
                    if contributors_team_id:
                        print(f"      - {project_name}_contributors: View Only")
                
            except Exception as e:
                print(f"    ⚠ Warning: Could not set ACL using REST API, trying setPermissions: {e}")
                # Fallback to setPermissions
                try:
                    # First set admin permissions to break inheritance
                    syn.setPermissions(
                        folder_id,
                        principalId=HTAN_DCC_ADMINS_TEAM_ID,
                        accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE", "MODERATE", "CHANGE_PERMISSIONS", "CHANGE_SETTINGS"],
                        overwrite=True
                    )
                    # Then add other permissions
                    syn.setPermissions(
                        folder_id,
                        principalId=HTAN_DCC_TEAM_ID,
                        accessType=["READ", "DOWNLOAD"],
                        overwrite=False
                    )
                    syn.setPermissions(
                        folder_id,
                        principalId=ACT_TEAM_ID,
                        accessType=["READ", "DOWNLOAD"],
                        overwrite=False
                    )
                    if project_name:
                        contributors_team_id = find_contributor_team(syn, project_name)
                        if contributors_team_id:
                            syn.setPermissions(
                                folder_id,
                                principalId=contributors_team_id,
                                accessType=["READ", "DOWNLOAD"],
                                overwrite=False
                            )
                    print(f"    ✓ Set permissions using setPermissions method")
                except Exception as e2:
                    print(f"    ✗ Failed to set permissions: {e2}")
    except Exception as e:
        print(f"    ⚠ Warning: Error setting permissions: {e}")


