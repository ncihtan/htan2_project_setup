import synapseclient
from synapseclient import EntityViewSchema, EntityViewType
from synapseclient.core.exceptions import SynapseHTTPError

# Login to Synapse
syn = synapseclient.Synapse()
syn.login()

# Define the Synapse table ID for the project metadata
project_table_id = "syn63300517"  # Replace this with the actual table ID if different

# Query the table for project info
query = f"SELECT id, name FROM {project_table_id}"
results = syn.tableQuery(query)
project_data = results.asDataFrame()

# Define team IDs for general access
htan_dcc_admins_team_id = "3497313"  # HTAN DCC Admins team
htan_dcc_team_id = "3391844"  # HTAN DCC team (general users)

# Create the file view schema
file_view = EntityViewSchema(
    name="HTAN2 File View",
    parent=syn.findEntityId("HTAN2"),  # HTAN2 Project ID where the view will be stored
    scopes=project_data["id"].tolist(),  # List of project IDs from the query
    includeEntityTypes=[EntityViewType.FILE],  # Scope only to file entities
    addDefaultViewColumns=True,  # Automatically add default file view columns
)

# Store the file view on Synapse
file_view = syn.store(file_view)
print(f"File view '{file_view.name}' created with Synapse ID: {file_view.id}")


# Function to set permissions for the file view
def set_file_view_permissions(file_view, team_id, access_type):
    try:
        syn.setPermissions(
            file_view,
            principalId=team_id,
            accessType=access_type,
        )
        print(f"Permissions set for team {team_id} on file view '{file_view.name}'.")
    except SynapseHTTPError as e:
        print(f"Error setting permissions for team {team_id}: {e}")


# Set permissions for HTAN DCC Admins as admins
set_file_view_permissions(
    file_view,
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

# Set permissions for HTAN DCC team as contributors
set_file_view_permissions(file_view, htan_dcc_team_id, ["READ", "DOWNLOAD"])

# Loop through each project and give download access to each project's downloaders team
for index, row in project_data.iterrows():
    project_name = row["name"]
    contributors_team_name = f"{project_name}_contributors"

    # Search for the team by name
    result = syn.restGET(f"/teams?fragment={contributors_team_name}")
    team_list = result.get("results", [])

    print(team_list)

    # Get the team ID if the team exists
    if team_list:
        contributors_team_id = team_list[0]["id"]
        set_file_view_permissions(file_view, contributors_team_id, ["READ", "DOWNLOAD"])
    else:
        print(f"Contributors team '{contributors_team_name}' not found, skipping.")

    users_team_name = f"{project_name}_users"

    # Search for the team by name
    result = syn.restGET(f"/teams?query={users_team_name}")
    team_list = result.get("results", [])

    # Get the team ID if the team exists
    if team_list:
        users_team_id = team_list[0]["id"]
        set_file_view_permissions(file_view, users_team_id, ["READ", "DOWNLOAD"])
    else:
        print(f"Users team '{users_team_name}' not found, skipping.")

print("Permissions setup complete for all teams.")
