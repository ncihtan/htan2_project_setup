"""
Utilities for loading and managing project configurations.
"""

import yaml
from typing import Dict


def load_projects(projects_path: str = "projects.yml") -> Dict[str, str]:
    """
    Load projects from YAML file.
    
    Args:
        projects_path: Path to the projects YAML file
        
    Returns:
        Dictionary mapping project names to Synapse IDs
    """
    try:
        with open(projects_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: {projects_path} not found")
        return {}


