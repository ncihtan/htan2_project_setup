#!/usr/bin/env python3
"""
Script to verify folder permissions are set correctly.
"""

import synapseclient
import yaml
import argparse
from typing import Dict

# Import shared utilities
from htan2_synapse import (
    load_projects,
    find_contributor_team,
    HTAN_DCC_ADMINS_TEAM_ID,
    HTAN_DCC_TEAM_ID,
    ACT_TEAM_ID,
)


def check_permissions(syn, folder_id: str, folder_type: str, project_name: str):
    """Check if permissions are set correctly."""
    print(f"\n  Checking {folder_type}/ ({folder_id})...")
    
    try:
        acl = syn.get_acl(folder_id)
        
        # Expected permissions based on folder type
        expected = {}
        contributors_team_id = find_contributor_team(syn, project_name)
        
        if folder_type.endswith("_ingest"):
            expected[HTAN_DCC_ADMINS_TEAM_ID] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE", "MODERATE", "CHANGE_PERMISSIONS", "CHANGE_SETTINGS"]
            expected[HTAN_DCC_TEAM_ID] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
            expected[ACT_TEAM_ID] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
            if contributors_team_id:
                expected[contributors_team_id] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
        
        elif folder_type.endswith("_staging"):
            expected[HTAN_DCC_ADMINS_TEAM_ID] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE", "MODERATE", "CHANGE_PERMISSIONS", "CHANGE_SETTINGS"]
            expected[HTAN_DCC_TEAM_ID] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
            expected[ACT_TEAM_ID] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
            if contributors_team_id:
                expected[contributors_team_id] = ["READ", "DOWNLOAD", "CREATE", "UPDATE"]
        
        elif folder_type.endswith("_release"):
            expected[HTAN_DCC_ADMINS_TEAM_ID] = ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE", "MODERATE", "CHANGE_PERMISSIONS", "CHANGE_SETTINGS"]
            expected[HTAN_DCC_TEAM_ID] = ["READ", "DOWNLOAD"]
            expected[ACT_TEAM_ID] = ["READ", "DOWNLOAD"]
            if contributors_team_id:
                expected[contributors_team_id] = ["READ", "DOWNLOAD"]
        
        # Check actual permissions
        actual_perms = {}
        if isinstance(acl, dict) and 'resourceAccess' in acl:
            for ra in acl['resourceAccess']:
                principal_id = str(ra.get('principalId', ''))
                access_type = ra.get('accessType', [])
                actual_perms[principal_id] = sorted(access_type)
        
        # Compare
        issues = []
        for principal_id, expected_perms in expected.items():
            expected_sorted = sorted(expected_perms)
            actual = actual_perms.get(principal_id, [])
            actual_sorted = sorted(actual) if actual else []
            
            if expected_sorted != actual_sorted:
                issues.append(f"    ✗ Principal {principal_id}: Expected {expected_sorted}, Got {actual_sorted}")
            else:
                team_name = "HTAN DCC Admins" if principal_id == HTAN_DCC_ADMINS_TEAM_ID else \
                           "HTAN DCC" if principal_id == HTAN_DCC_TEAM_ID else \
                           "ACT" if principal_id == ACT_TEAM_ID else \
                           f"{project_name}_contributors" if principal_id == contributors_team_id else \
                           f"Principal {principal_id}"
                print(f"    ✓ {team_name}: {expected_sorted}")
        
        if issues:
            print("    Issues found:")
            for issue in issues:
                print(issue)
            return False
        else:
            print("    ✓ All permissions correct")
            return True
            
    except Exception as e:
        print(f"    ✗ Error checking permissions: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Verify folder permissions")
    parser.add_argument("--version", type=str, default="v8", help="Version prefix")
    parser.add_argument("--folder-type", type=str, help="Folder type to check (e.g., v8_release)")
    parser.add_argument("--folder-structure-file", type=str, default="folder_structure_v8.yml")
    
    args = parser.parse_args()
    
    # Load folder structure
    with open(args.folder_structure_file, 'r') as f:
        data = yaml.safe_load(f)
    
    projects = data[args.version]["projects"]
    
    # Login
    print("Logging in to Synapse...")
    syn = synapseclient.Synapse()
    syn.login()
    print("✓ Logged in\n")
    
    # Check permissions
    folder_types = [args.folder_type] if args.folder_type else [
        f"{args.version}_ingest", f"{args.version}_staging", f"{args.version}_release"
    ]
    
    for project_name, project_data in sorted(projects.items()):
        print(f"{'='*80}")
        print(f"Project: {project_name}")
        print(f"{'='*80}")
        
        folders = project_data.get("folders", {})
        for folder_type in folder_types:
            if folder_type in folders:
                folder_id = folders[folder_type].get("synapse_id")
                if folder_id:
                    check_permissions(syn, folder_id, folder_type, project_name)


if __name__ == "__main__":
    main()


