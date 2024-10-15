# Creates a synapse table with HTAN teams

import synapseclient
import pandas as pd

# Login to Synapse
syn = synapseclient.Synapse()
syn.login()


# Pagination parameters
limit = 50
offset = 0
all_teams = []

# Loop through paginated results
while True:
    result = syn.restGET(f"/teams?fragment=HTAN2&limit={limit}&offset={offset}")
    team_list = result.get("results", [])

    if not team_list:  # Break if no more results
        break

    all_teams.extend(team_list)
    offset += limit  # Move to the next page

# Synapse table where teams will be stored
htan_teams_table_id = "syn63714328"

# Query existing table data
query = f"SELECT * FROM {htan_teams_table_id}"
existing_table = syn.tableQuery(query)
existing_df = existing_table.asDataFrame()

# Convert the existing table data to a set of IDs for faster comparison
existing_team_ids = set(existing_df["id"].astype(str))

# Prepare new rows to update the table
new_rows = []
for team in all_teams:
    team_id = team["id"]
    print(f"Checking team {team_id}...")
    if str(team_id) not in existing_team_ids:
        team_name = team["name"]
        team_creation_date = team["createdOn"]
        team_modified_date = team["modifiedOn"]
        team_owner_id = team["createdBy"]
        team_owner = syn.getUserProfile(team_owner_id)["userName"]

        row = [
            team_name,
            team_id,
            team_creation_date,
            team_modified_date,
            team_owner_id,
        ]
        new_rows.append(row)

# Only send new rows to the table if there are any
if new_rows:
    syn.store(synapseclient.Table(htan_teams_table_id, values=new_rows))
    print(f"Added {len(new_rows)} new rows to the Synapse table.")
else:
    print("No new teams to add.")
