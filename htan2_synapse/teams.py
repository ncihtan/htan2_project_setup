"""
Utilities for finding and managing Synapse teams.
"""

from typing import Optional


def find_contributor_team(syn, project_name: str) -> Optional[str]:
    """
    Find the contributor team ID for a project.
    
    Args:
        syn: Synapse client
        project_name: Name of the project (e.g., "HTAN2_Ovarian")
    
    Returns:
        Team ID if found, None otherwise
    """
    try:
        contributors_team_name = f"{project_name}_contributors"
        result = syn.restGET(f"/teams?fragment={contributors_team_name}")
        team_list = result.get("results", [])
        
        if team_list:
            return team_list[0]["id"]
        else:
            return None
    except Exception as e:
        print(f"    ⚠ Warning: Could not find contributor team for {project_name}: {e}")
        return None


