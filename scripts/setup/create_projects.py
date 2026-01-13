import synapseclient
from synapseclient import Project
from synapseclient.core.exceptions import SynapseHTTPError
import yaml

# Login to Synapse
syn = synapseclient.Synapse()
syn.login()

# Define project names
project_names = [
    "HTAN2",
    "HTAN2_Ovarian",
    "HTAN2_Glioma",
    "HTAN2_Gastric",
    "HTAN2_Skin",
    "HTAN2_Pediatric",
    "HTAN2_Myeloma",
    "HTAN2_Pancreatic",
    "HTAN2_Prostate",
    "HTAN2_CRC",
    "HTAN2_Lymphoma",
]

# Define team IDs
htan_dcc_admins_team_id = "3497313"  # HTAN DCC Admins team
htan_dcc_team_id = "3391844"  # HTAN DCC team
act_team_id = "464532"  # ACT team

# Initialize a dictionary to store project names and Synapse IDs
project_info = {}


# Function to get a project by name
def get_project_by_name(project_name):
    try:
        project_id = syn.findEntityId(project_name)
        if project_id:
            return syn.get(project_id)
        else:
            return None
    except SynapseHTTPError:
        return None


# Function to create a new project
def create_project(project_name):
    project = Project(name=project_name)
    return syn.store(project)


# Function to set permissions for a project
def set_project_permissions(project):
    # Add HTAN DCC Admins team with admin permissions
    syn.setPermissions(
        project,
        principalId=htan_dcc_admins_team_id,
        accessType=[
            "READ",
            "DOWNLOAD",
            "CREATE",
            "UPDATE",
            "DELETE",
            "MODERATE",
            "CHANGE_PERMISSIONS",
            "CHANGE_SETTINGS",
        ],
    )

    # Add HTAN DCC team with edit and delete permissions
    syn.setPermissions(
        project,
        principalId=htan_dcc_team_id,
        accessType=["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"],
    )

    # Add ACT team with administrator permissions
    syn.setPermissions(
        project,
        principalId=act_team_id,
        accessType=[
            "READ",
            "DOWNLOAD",
            "CREATE",
            "UPDATE",
            "DELETE",
            "MODERATE",
            "CHANGE_PERMISSIONS",
            "CHANGE_SETTINGS",
        ],
    )


# Function to save project information to a YAML file
def save_projects_to_yaml(project_info, filename="projects.yml"):
    with open(filename, "w") as file:
        yaml.dump(project_info, file)


# Main loop to create projects or reset permissions if they already exist
for project_name in project_names:
    project = get_project_by_name(project_name)

    if project:
        print(f"Project '{project_name}' already exists. Resetting permissions.")
    else:
        # Create the project
        project = create_project(project_name)
        print(f"Project '{project_name}' created.")

    # Set or reset the permissions
    set_project_permissions(project)
    print(f"Permissions set for project '{project_name}'.")

    # Store project name and Synapse ID in the dictionary
    project_info[project_name] = project.id

# Save the project info to 'projects.yml'
save_projects_to_yaml(project_info)
print("Project information saved to 'projects.yml'.")
