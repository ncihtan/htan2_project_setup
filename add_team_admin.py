import synapseclient
import json
import logging

# Initialize Synapse client
syn = synapseclient.Synapse()
syn.login()


def promote_user_to_admin_with_invite(syn, team_id, user_id):
    """
    Invite a user to a team (if required) and promote them to admin.

    Args:
        syn (synapseclient.Synapse): Authenticated Synapse client.
        team_id (str): The ID of the team.
        user_id (str): The ID of the user to promote.

    Returns:
        None
    """
    try:
        # Check if the user is already a member
        membership_status = syn.restGET(
            f"/team/{team_id}/member/{user_id}/membershipStatus"
        )
        if not membership_status["isMember"]:
            print(
                "User %s is not a member of team %s. Sending invitation.",
                user_id,
                team_id,
            )

            # Send an invitation
            invitation_body = {
                "teamId": team_id,
                "inviteeId": user_id,
                "message": "Invitation to join this HTAN2 team. Let Adam know when accepted so he can promote you to admin.",
            }
            syn.restPOST("/membershipInvitation", body=json.dumps(invitation_body))
            print("Invitation sent to user %s for team %s.", user_id, team_id)
            return  # Wait for the user to accept before proceeding
    except synapseclient.core.exceptions.SynapseHTTPError as e:
        print(
            "Error sending invitation to user %s for team %s: %s",
            user_id,
            team_id,
            str(e),
        )
        return


# Get team ids from the team tables syn63714328

query = "SELECT * FROM syn63714328"
results = syn.tableQuery(query)
team_data = results.asDataFrame()


# Example usage
user_id = "3429359"  # Replace with the user ID you want to promote (Lisa)

for index, row in team_data.iterrows():
    team_id = row["id"]
    promote_user_to_admin_with_invite(syn, team_id, user_id)
