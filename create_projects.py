import synapseclient
from synapseclient import Project
from synapseclient.core.exceptions import SynapseHTTPError

# Login to Synapse
syn = synapseclient.Synapse()
syn.login()

# Define project names
project_names = [
    "HTAN2"
    "HTAN2_Ovarian",
    "HTAN2_Glioma",
    "HTAN2_Gastric",
    "HTAN2_Skin",
    "HTAN2_Pediatric",
    "HTAN2_Myeloma",
    "HTAN2_Pancreatic",
    "HTAN2_Prostate",
    "HTAN2_CRC",
    "HTAN2_Lymphoma"
]

# Define team IDs
htan_dcc_admins_team_id = '3497313'  # HTAN DCC Admins team
htan_dcc_team_id = '3391844'  # HTAN DCC team
act_team_id = '464532'  # ACT team

# Function to get a project by name
def get_project_by_name(project_name):
    try:
        project = syn.findEntityId(project_name)
        return syn.get(project)
    except SynapseHTTPError:
        return None

# Loop through project names and create projects or reset permissions if already exists
for project_name in project_names:
    project = get_project_by_name(project_name)
    
    if project:
        print(f"Project '{project_name}' already exists. Resetting permissions.")
    else:
        # Create the project
        project = Project(name=project_name)
        project = syn.store(project)
        print(f"Project '{project_name}' created.")
    
    # Reset the permissions
    # Add HTAN DCC Admins team with admin permissions
    syn.setPermissions(project, principalId=htan_dcc_admins_team_id, accessType=['READ', 'DOWNLOAD', 'CREATE', 'UPDATE', 'DELETE','MODERATE','CHANGE_PERMISSIONS','CHANGE_SETTINGS'])
    
    # Add HTAN DCC team with edit and delete permissions
    syn.setPermissions(project, principalId=htan_dcc_team_id, accessType=['READ', 'DOWNLOAD', 'CREATE', 'UPDATE', 'DELETE'])
    
    # Add ACT team with administrator permissions
    syn.setPermissions(project, principalId=act_team_id, accessType=['READ', 'DOWNLOAD', 'CREATE', 'UPDATE', 'DELETE','MODERATE','CHANGE_PERMISSIONS','CHANGE_SETTINGS'])

    print(f"Permissions set for project '{project_name}'.")
