import synapseclient
from synapseclient import Project, Team
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

# Define team IDs for existing teams
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


def get_team_by_name(team_name):
    try:
        # Search for the team by name
        result = syn.restGET(f"/teams?query={team_name}")
        team_list = result.get("results", [])

        # If a team with the provided name is found, return the first matching team
        for team in team_list:
            if team["name"] == team_name:
                return syn.getTeam(team["id"])
        return None
    except SynapseHTTPError as e:
        print(f"Error while searching for team '{team_name}': {e}")
        return None


# Function to create a new team
def create_team(team_name):
    try:
        team = Team(name=team_name)
        return syn.store(team)
    except SynapseHTTPError as e:
        print(f"Failed to create team '{team_name}': {e}")
        return None


# Function to set permissions for a project
def set_project_permissions(project, team_id, access_type):
    syn.setPermissions(
        project,
        principalId=team_id,
        accessType=access_type,
    )


# Function to create teams for each project
def create_project_teams(project_name):
    editors_team_name = f"{project_name}_contributors"
    downloaders_team_name = f"{project_name}_users"

    # Create editors team
    editors_team = get_team_by_name(editors_team_name)
    if not editors_team:
        editors_team = create_team(editors_team_name)
        print(f"Team '{editors_team_name}' created.")
    else:
        print(f"Team '{editors_team_name}' already exists.")

    # Create downloaders team
    downloaders_team = get_team_by_name(downloaders_team_name)
    if not downloaders_team:
        downloaders_team = create_team(downloaders_team_name)
        print(f"Team '{downloaders_team_name}' created.")
    else:
        print(f"Team '{downloaders_team_name}' already exists.")

    return editors_team, downloaders_team


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
    set_project_permissions(
        project,
        htan_dcc_admins_team_id,
        [
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
    set_project_permissions(
        project, htan_dcc_team_id, ["READ", "DOWNLOAD", "CREATE", "UPDATE", "DELETE"]
    )
    set_project_permissions(
        project,
        act_team_id,
        [
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

    # Create teams for editors and downloaders
    editors_team, downloaders_team = create_project_teams(project_name)

    # Set permissions for the editors and downloaders teams
    if editors_team:
        set_project_permissions(
            project, editors_team.id, ["READ", "DOWNLOAD", "CREATE", "UPDATE"]
        )
        print(
            f"Permissions set for team '{editors_team.name}' on project '{project_name}'."
        )

    if downloaders_team:
        set_project_permissions(project, downloaders_team.id, ["READ", "DOWNLOAD"])
        print(
            f"Permissions set for team '{downloaders_team.name}' on project '{project_name}'."
        )

    # Store project name and Synapse ID in the dictionary
    project_info[project_name] = project.id

# Save the project info to 'projects.yml'
save_projects_to_yaml(project_info)
print("Project information saved to 'projects.yml'.")
