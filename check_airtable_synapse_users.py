import synapseclient
import pyairtable
from pyairtable import Api
import pandas as pd
from tqdm import tqdm
import snowflake.connector
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Load environment variables
load_dotenv()

syn = synapseclient.Synapse()
# Add error handling for Synapse login
try:
    syn.login()
except Exception as e:
    logger.error(f"Failed to login to Synapse: {e}")
    sys.exit(1)


# Login to snowflake with PAT
def login_to_snowflake():
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    pat = os.getenv("SNOWFLAKE_PAT")  # Retrieve PAT from .env file
    logging.info(f"Using user: {user}, account: {account}")
    if not user or not account or not pat:
        logging.error(
            "Missing SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, or SNOWFLAKE_PAT environment variables."
        )
        raise Exception("Missing Snowflake credentials")
    try:
        conn = snowflake.connector.connect(
            user=user, account=account, password=pat  # Use PAT for authentication
        )
        logging.info("Successfully connected to Snowflake using PAT.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        raise e


# Run a query in snowflake and return the results as DataFrame
def run_snowflake_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)

        # Get column names
        columns = [desc[0] for desc in cursor.description]

        # Fetch all results
        results = cursor.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=columns)

        logging.info(f"Query executed successfully. Retrieved {len(df)} rows.")
        return df
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


def check_synapse_usernames_snowflake(usernames_to_check):
    """Check usernames using Snowflake query instead of individual API calls"""
    print(f"\nChecking {len(usernames_to_check)} unique usernames with Snowflake...")

    if not usernames_to_check:
        return {}

    # Connect to Snowflake
    conn = login_to_snowflake()

    # Create the username list for the IN clause
    username_list = "', '".join(usernames_to_check)
    username_list = f"'{username_list}'"

    # Build the Snowflake query
    query = f"""
    WITH team_users AS (
    SELECT DISTINCT
        TRY_TO_NUMBER(member_id) AS user_id               -- use TRY_TO_NUMBER only if upl.id is numeric
    FROM synapse_data_warehouse.synapse.teammember_latest
    WHERE team_id = '3518522'
        AND member_id IS NOT NULL
    ),
    base AS (
    SELECT *
    FROM synapse_data_warehouse.synapse.userprofile_latest
    WHERE user_name IN ({username_list})
    ),
    upl_flat AS (
    SELECT
        b.*,
        t.value AS tos_item
    FROM base b,
        LATERAL FLATTEN(
            input => TRY_PARSE_JSON(b.tos_agreements),
            OUTER => TRUE
        ) t
    )
    SELECT
    uf.user_name,
    uf.id,
    uf.first_name,
    uf.last_name,
    uf.company,
    (tu.user_id IS NOT NULL)       AS IN_HTAN2_Community,
    uf.is_two_factor_auth_enabled,
    COALESCE(cql.certified, FALSE) AS certified,
    COALESCE(BOOLOR_AGG(uf.tos_item:version::string = '1.0.1'), FALSE) AS tos_101_agreed
    FROM upl_flat AS uf
    LEFT JOIN synapse_data_warehouse.synapse.certifiedquiz_latest AS cql
    ON cql.user_id = uf.id
    LEFT JOIN team_users AS tu
    ON tu.user_id = uf.id
    GROUP BY
    uf.user_name, uf.id, uf.first_name, uf.last_name,
    uf.company, uf.is_two_factor_auth_enabled,
    COALESCE(cql.certified, FALSE),
    (tu.user_id IS NOT NULL);
    """

    # Execute query
    df = run_snowflake_query(conn, query)

    # Convert results to the same format as the original API results
    username_results = {}

    # First, mark all usernames as not found
    for username in usernames_to_check:
        username_results[username] = {"exists": False, "certified": False}

    # Then update with found results
    for _, row in df.iterrows():
        username = row["USER_NAME"]
        is_certified = bool(row["CERTIFIED"]) if pd.notna(row["CERTIFIED"]) else False

        username_results[username] = {
            "exists": True,
            "certified": is_certified,
            "snowflake_data": {
                "id": row["ID"],
                "first_name": row["FIRST_NAME"],
                "last_name": row["LAST_NAME"],
                "company": row["COMPANY"],
                "in_htan2_community": bool(row["IN_HTAN2_COMMUNITY"]),
                "is_two_factor_auth_enabled": (
                    bool(row["IS_TWO_FACTOR_AUTH_ENABLED"])
                    if pd.notna(row["IS_TWO_FACTOR_AUTH_ENABLED"])
                    else False
                ),
                "tos_101_agreed": (
                    bool(row["TOS_101_AGREED"])
                    if pd.notna(row["TOS_101_AGREED"])
                    else False
                ),
            },
        }

    print(f"✅ Found {len(df)} users in Snowflake")
    print(f"❌ {len(usernames_to_check) - len(df)} users not found")

    return username_results


# Test with known good user first
print("Testing with known good user: dgibbs")
try:
    test_profile = syn.getUserProfile("dgibbs")
    test_certified = syn.is_certified("dgibbs")
    print(f"✅ Test successful - User exists: True, Certified: {test_certified}")
except Exception as e:
    print(f"❌ Test failed: {e}")
    print("This may indicate an issue with Synapse connection or API")

print("\nProceeding with Airtable data processing...\n")

# Setup outputs directory
import shutil

outputs_dir = "outputs"
emails_dir = os.path.join(outputs_dir, "emails")

# Create outputs directory if it doesn't exist
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
    print(f"✅ Created outputs directory: {outputs_dir}")
else:
    print(f"✅ Outputs directory exists: {outputs_dir}")

# Empty the outputs directory (remove all contents)
if os.path.exists(outputs_dir):
    for item in os.listdir(outputs_dir):
        item_path = os.path.join(outputs_dir, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
            print(f"🗑️  Removed file: {item}")
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
            print(f"🗑️  Removed directory: {item}")
    print(f"✅ Emptied outputs directory")

# Create emails subdirectory
os.makedirs(emails_dir, exist_ok=True)
print(f"✅ Created emails directory: {emails_dir}")

# Airtable settings
appExampleBaseId = "apptQ3yP7le2ZvLjH"
tblExampleTableId = "tblcZfdLxDUj9mlvi"

AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")

api = Api(AIRTABLE_PAT)
table = api.table(appExampleBaseId, tblExampleTableId)

records = table.all()
syn.logger.info(f"Total records fetched from Airtable: {len(records)}")

# Filter for only users who need Synapse access
records_needing_synapse = []
for record in records:
    fields = record.get("fields", {})
    synapse_needed = fields.get("Synapse Access Needed?", False)

    # Convert to boolean if it's a string
    if isinstance(synapse_needed, str):
        synapse_needed = synapse_needed.lower() == "yes"

    if synapse_needed:
        records_needing_synapse.append(record)

print(
    f"📊 Filtered to {len(records_needing_synapse)} records needing Synapse access (from {len(records)} total)"
)
records = records_needing_synapse  # Use filtered records for all subsequent processing

# Create a list to store the simplified report data
report_data = []

# Pre-define NA values set for faster lookup
NA_VALUES = {"na", "n/a", "none", "null", "undefined", ""}

# Extract and pre-process all records first
processed_records = []
usernames_to_check = []

print("Pre-processing records...")
for record in tqdm(records, desc="Processing records"):
    fields = record.get("fields", {})

    # Extract fields once
    airtable_synapse_username = fields.get("Synapse Username", "")
    airtable_synapse_needed = fields.get("Synapse Access Needed?", False)

    # Convert synapse needed to boolean (optimized)
    if isinstance(airtable_synapse_needed, str):
        airtable_synapse_needed = airtable_synapse_needed.lower() == "yes"

    # Pre-process username
    username_clean = (
        airtable_synapse_username.strip()
        if isinstance(airtable_synapse_username, str)
        else ""
    )
    username_is_na = username_clean.lower() in NA_VALUES

    # Check if they provided an email instead of a username
    username_is_email = (
        username_clean and "@" in username_clean and "." in username_clean
    )

    should_check_synapse = (
        username_clean and not username_is_na and not username_is_email
    )

    # Store processed record
    processed_record = {
        "fields": fields,
        "username": username_clean,
        "username_is_na": username_is_na,
        "username_is_email": username_is_email,
        "should_check_synapse": should_check_synapse,
        "synapse_needed": airtable_synapse_needed,
    }
    processed_records.append(processed_record)

    # Collect unique usernames to check
    if should_check_synapse and username_clean not in usernames_to_check:
        usernames_to_check.append(username_clean)

# Check all usernames with Snowflake (single query)
username_results = check_synapse_usernames_snowflake(usernames_to_check)

# Now build the final report data
print("Building final report...")
for processed_record in tqdm(processed_records, desc="Building report"):
    fields = processed_record["fields"]
    username = processed_record["username"]
    username_is_na = processed_record["username_is_na"]
    username_is_email = processed_record["username_is_email"]
    should_check_synapse = processed_record["should_check_synapse"]
    synapse_needed = processed_record["synapse_needed"]

    # Initialize report entry
    report_entry = {
        "Synapse Username": username,
        "Username Exists": None,
        "In HTAN2_Community team": None,
        "Is Certified": None,
        "Has 2FA": None,
        "TOS 1.0.1 Agreed": None,
        "Synapse Access Needed": synapse_needed,
        "First Name": fields.get("First Name", ""),
        "Last Name": fields.get("Last Name", ""),
        "Role": fields.get("Role", ""),
        "Institution": fields.get("Institution", ""),
        "Atlas": fields.get("Atlas", ""),
        "Contact Email": fields.get("Contact Email", ""),
    }

    # Set results based on pre-computed data
    if should_check_synapse:
        result = username_results[username]
        report_entry["Username Exists"] = result["exists"]
        report_entry["In HTAN2_Community team"] = result.get("snowflake_data", {}).get(
            "in_htan2_community", False
        )
        report_entry["Is Certified"] = result["certified"]
        if result["exists"] and "snowflake_data" in result:
            snowflake_data = result["snowflake_data"]
            report_entry["Has 2FA"] = snowflake_data.get(
                "is_two_factor_auth_enabled", False
            )
            report_entry["TOS 1.0.1 Agreed"] = snowflake_data.get(
                "tos_101_agreed", False
            )
        else:
            report_entry["Has 2FA"] = False
            report_entry["TOS 1.0.1 Agreed"] = False
    elif username_is_email:
        # User provided email instead of username - treat as missing username
        report_entry["Username Exists"] = "Email Provided"
        report_entry["Is Certified"] = "Email Provided"
        report_entry["Has 2FA"] = "Email Provided"
        report_entry["TOS 1.0.1 Agreed"] = "Email Provided"
    elif username_is_na:
        report_entry["Username Exists"] = False
        report_entry["Is Certified"] = False
        report_entry["Has 2FA"] = False
        report_entry["TOS 1.0.1 Agreed"] = False
    else:
        report_entry["Username Exists"] = "N/A"
        report_entry["Is Certified"] = "N/A"
        report_entry["Has 2FA"] = "N/A"
        report_entry["TOS 1.0.1 Agreed"] = "N/A"

    report_data.append(report_entry)

# Create DataFrame from report data
df = pd.DataFrame(report_data)

# Save the simplified report
df.to_csv("outputs/airtable_synapse_crosscheck.csv", index=False)

print(f"\nReport generated with {len(df)} records")
print(f"Saved to: outputs/airtable_synapse_crosscheck.csv")


# Helper function to check if username is effectively empty or NA-like
def is_username_empty_or_na(username):
    if pd.isna(username) or username == "":
        return True
    if isinstance(username, str):
        cleaned = username.strip().lower()
        return cleaned in ["", "na", "n/a", "none", "null", "undefined"]
    return False


def analyze_user_issues(user_row):
    """Analyze what specific issues a user has and return a list of problems"""
    issues = []

    username_exists = user_row["Username Exists"]
    is_certified = user_row["Is Certified"]
    has_2fa = user_row["Has 2FA"]
    tos_agreed = user_row["TOS 1.0.1 Agreed"]
    in_community_team = user_row["In HTAN2_Community team"]
    synapse_username = user_row["Synapse Username"]

    # Check for account existence issues
    if username_exists == "Email Provided":
        issues.append("email_instead_of_username")
    elif username_exists == False:
        if is_username_empty_or_na(synapse_username):
            issues.append("no_username_provided")
        else:
            issues.append("username_not_found")
    elif username_exists == "N/A":
        issues.append("no_username_provided")
    elif username_exists == True:
        # Account exists, check for compliance issues
        if is_certified == False:
            issues.append("not_certified")
        if has_2fa == False:
            issues.append("no_2fa")
        if tos_agreed == False:
            issues.append("tos_not_agreed")

    # Check community team membership for all users with valid usernames
    if username_exists == True and in_community_team == False:
        issues.append("not_in_community_team")

    return issues


# Calculate and display issue breakdown for all users
print(f"\n=== ISSUE BREAKDOWN (ALL USERS) ===")

# Count issues for all users
all_issue_counts = {}
issue_names = {
    "email_instead_of_username": "Email provided instead of username",
    "no_username_provided": "No username provided",
    "username_not_found": "Username not found in Synapse",
    "not_certified": "Not certified",
    "no_2fa": "No 2FA enabled",
    "tos_not_agreed": "TOS not agreed to",
    "not_in_community_team": "Not in HTAN2 Community team",
}

for _, user in df.iterrows():
    issues = analyze_user_issues(user)
    for issue in issues:
        all_issue_counts[issue] = all_issue_counts.get(issue, 0) + 1

for issue, count in all_issue_counts.items():
    print(f"{issue_names.get(issue, issue)}: {count} users")

# Calculate and display issue breakdown for community team members only
print(f"\n=== ISSUE BREAKDOWN (COMMUNITY TEAM MEMBERS ONLY) ===")

# Count issues for community team members only
community_issue_counts = {}
community_members = df[df["In HTAN2_Community team"] == True]

for _, user in community_members.iterrows():
    issues = analyze_user_issues(user)
    for issue in issues:
        community_issue_counts[issue] = community_issue_counts.get(issue, 0) + 1

for issue, count in community_issue_counts.items():
    print(f"{issue_names.get(issue, issue)}: {count} users")

print(f"\nTotal users: {len(df)}")
print(f"Community team members: {len(community_members)}")

# Generate email files for users who need Synapse account action
print(f"\n=== GENERATING EMAIL NOTIFICATIONS ===")


def generate_customized_email(user_row, issues):
    """Generate a customized email based on the specific issues a user has"""
    first_name = user_row["First Name"]
    last_name = user_row["Last Name"]
    email = user_row["Contact Email"]
    synapse_username = user_row["Synapse Username"]
    institution = user_row["Institution"]
    atlas = user_row["Atlas"]

    # Clean email address to remove any extra characters
    if isinstance(email, str):
        email = email.strip().strip("<>;").strip()

    # Create filename based on issues
    name_part = f"{first_name}_{last_name}".replace(" ", "_").replace(".", "")
    issues_str = "_".join(issues)
    filename = f"{name_part}_{issues_str}_email.txt"
    filepath = os.path.join(emails_dir, filename)

    # Determine email subject and opening
    if (
        "username_not_found" in issues
        or "no_username_provided" in issues
        or "email_instead_of_username" in issues
    ):
        subject = "HTAN Synapse Account - Username Required"
        problem_description = "We need to establish your Synapse account information"
    else:
        subject = "HTAN Synapse Account - Action Required"
        problem_description = (
            "Your Synapse account needs to be updated for HTAN Phase 2 participation"
        )

    # Build the email body
    body = f"""Dear {first_name} {last_name},

Thank you for registering as a member of the HTAN community! We're excited to have you join us for the HTAN Phase 2 project "{atlas}" at {institution}.

Since you indicated that you need access to the Synapse platform, we've reviewed your account information to help streamline your onboarding process. We want to ensure you have everything set up properly for seamless collaboration.

{problem_description}. Here's what we found and the simple steps to get you fully set up:

"""

    # Add specific next steps
    action_items = []

    if "email_instead_of_username" in issues:
        body += f'📋 **Username Setup**: We see you provided an email address ("{synapse_username}") - we just need your Synapse username to complete the setup.\n'
        action_items.append(
            "Provide your actual Synapse username (not your email address)"
        )

    if "no_username_provided" in issues:
        body += f"📋 **Username Setup**: We'll need your Synapse username to get you connected to the platform.\n"
        action_items.append("Provide your Synapse username")

    if "username_not_found" in issues:
        body += f"📋 **Username Setup**: We couldn't locate the username \"{synapse_username}\" in the Synapse system - let's get this sorted out together.\n"
        action_items.append(
            "Verify your Synapse username is correct, or create a new account if needed"
        )

    if "not_certified" in issues:
        body += f"🎓 **Certification Step**: To gain data upload permissions you must pass a quiz on the technical and ethical aspects of sharing data in our system.\n"
        action_items.append("Complete the Synapse user certification quiz")

    if "no_2fa" in issues:
        body += f"🔐 **Security Enhancement**: Let's add Multi-Factor Authentication to your account for enhanced security.\n"
        action_items.append("Enable Multi-Factor Authentication (2FA) on your account")

    if "tos_not_agreed" in issues:
        body += f'📄 **Terms Update**: Your account "{synapse_username}" needs to accept the latest Terms of Service (version 1.0.1) to stay current.\n'
        action_items.append("Accept the current Synapse Terms and Conditions of Use")

    # Add action steps
    body += f"\n**Next Steps to Get You Set Up:**\n"

    # First, account creation/verification steps
    if any(
        issue in issues
        for issue in [
            "email_instead_of_username",
            "no_username_provided",
            "username_not_found",
        ]
    ):
        body += f"""
1. **Getting Your Synapse Account Ready**:
   - If you don't have a Synapse account yet, you can create one at: https://www.synapse.org/#!RegisterAccount:0
   - If you already have an account, you can verify your username at: https://www.synapse.org/
   - Just reply to this email with your correct Synapse username when you're ready

"""

    # Then, account compliance steps
    if any(issue in issues for issue in ["not_certified", "no_2fa", "tos_not_agreed"]):
        body += f"""2. **Final Setup Steps** (visit https://accounts.synapse.org/authenticated/myaccount?appId=synapse.org):
"""
        if "tos_not_agreed" in issues:
            body += f"   - ✅ Accept the Synapse Terms and Conditions of Use (version 1.0.1)\n"
        if "no_2fa" in issues:
            body += f"   - ✅ Enable Multi-Factor Authentication (2FA) for account security\n"
        if "not_certified" in issues:
            body += f"   - ✅ Complete the Synapse user certification quiz (quick and informative!)\n"
        body += f"   - ✅ Make sure your profile is complete with your full name and institution\n\n"
    else:
        body += f"""2. **Complete Your Synapse Setup** (visit https://accounts.synapse.org/authenticated/myaccount?appId=synapse.org):
   - ✅ Accept the Synapse Terms and Conditions of Use (version 1.0.1) [REQUIRED] 
   - ✅ Enable Multi-Factor Authentication (2FA) for account security [REQUIRED] 
   - ✅ Complete the Synapse user certification quiz on ethics and data use [REQUIRED FOR UPLOAD PERMISSIONS] 
   - ✅ Make sure your profile is complete with your full name and institution [PREFERRED] 

"""

    body += f"""3. **Let Us Know When You're Ready**:
   - Just reply to this email once you've completed the steps above
   - You'll then be all set and we will onboard you into your Synapse projects and teams in the coming months

**We're Here to Help!**
- Synapse documentation: https://help.synapse.org/
- Feel free to reach out if you have any questions - we're happy to assist you through this process

Best regards,
HTAN Data Coordination Team"""

    # Write email file
    with open(filepath, "w") as f:
        f.write(f"{email}\n")
        f.write(f"{subject}\n")
        f.write(f"{body}\n")

    return filename, issues


# Find all users who need action (only those already in community team)
users_needing_action = df[
    (df["Synapse Access Needed"] == True)
    & (df["Contact Email"].notna())
    & (df["Contact Email"] != "")
    & (df["In HTAN2_Community team"] == True)  # Only users already in community team
    & (
        (df["Username Exists"] == False)  # Username not found
        | (df["Username Exists"] == "Email Provided")  # Email instead of username
        | (df["Username Exists"] == "N/A")  # No username provided
        | (df["Synapse Username"].apply(is_username_empty_or_na))  # Empty username
        | (df["Is Certified"] == False)  # Not certified
        | (df["Has 2FA"] == False)  # No 2FA
        | (df["TOS 1.0.1 Agreed"] == False)  # TOS not agreed
    )
]

print(f"Total users needing action (in community team): {len(users_needing_action)}")

# Generate customized emails
email_count_by_issue = {}
generated_emails = []

for _, user in users_needing_action.iterrows():
    issues = analyze_user_issues(user)

    if issues:  # Only generate email if there are actual issues
        filename, user_issues = generate_customized_email(user, issues)
        generated_emails.append(
            {
                "user": f"{user['First Name']} {user['Last Name']}",
                "email": user["Contact Email"],
                "username": user["Synapse Username"],
                "issues": user_issues,
                "filename": filename,
            }
        )

        # Count issues for reporting
        for issue in user_issues:
            email_count_by_issue[issue] = email_count_by_issue.get(issue, 0) + 1

print(f"\nGenerated {len(generated_emails)} customized email files:")
print(f"Email files saved in: {emails_dir}/")

# Report issue breakdown
print(f"\n=== ISSUE BREAKDOWN ===")
issue_names = {
    "email_instead_of_username": "Email provided instead of username",
    "no_username_provided": "No username provided",
    "username_not_found": "Username not found in Synapse",
    "not_certified": "Not certified",
    "no_2fa": "No 2FA enabled",
    "tos_not_agreed": "TOS not agreed to",
}

for issue, count in email_count_by_issue.items():
    print(f"{issue_names.get(issue, issue)}: {count} users")

# Create issue co-occurrence visualization
print(f"\n=== ISSUE CO-OCCURRENCE MATRIX ===")

# Collect all issue combinations
issue_combinations = {}
all_issues = list(issue_names.keys())

# Count co-occurrences
for email_info in generated_emails:
    user_issues = set(email_info["issues"])
    for issue1 in all_issues:
        for issue2 in all_issues:
            if issue1 in user_issues and issue2 in user_issues:
                key = (issue1, issue2)
                issue_combinations[key] = issue_combinations.get(key, 0) + 1

# Create ASCII grid
if issue_combinations:
    # Short names for display
    short_names = {
        "email_instead_of_username": "Email",
        "no_username_provided": "NoUser",
        "username_not_found": "NotFnd",
        "not_certified": "NoCert",
        "no_2fa": "No2FA",
        "tos_not_agreed": "NoTOS",
    }

    # Filter to issues that actually occurred
    present_issues = [
        issue
        for issue in all_issues
        if any(issue in email_info["issues"] for email_info in generated_emails)
    ]

    if present_issues:
        # Print header
        print("Co-occurrence counts (how many users have both issues):")
        print()

        # Print column headers
        header = "        "
        for issue in present_issues:
            header += f"{short_names[issue]:>7}"
        print(header)

        # Print separator
        print("        " + "─" * (7 * len(present_issues)))

        # Print each row
        for issue1 in present_issues:
            row = f"{short_names[issue1]:>7} │"
            for issue2 in present_issues:
                count = issue_combinations.get((issue1, issue2), 0)
                if issue1 == issue2:
                    # Diagonal - total count for this issue
                    row += f"{count:>6} "
                else:
                    # Off-diagonal - co-occurrence count
                    row += f"{count:>6} "
            print(row)

        print()
        print("Legend:")
        print("- Diagonal values: Total users with that issue")
        print("- Off-diagonal values: Users with BOTH issues")
        for issue, name in short_names.items():
            if issue in present_issues:
                print(f"- {name}: {issue_names[issue]}")

# Show some examples of generated emails
if generated_emails:
    print(f"\n=== SAMPLE GENERATED EMAILS ===")
    for i, email_info in enumerate(generated_emails[:3]):  # Show first 3
        print(f"{i+1}. {email_info['user']} ({email_info['email']})")
        print(f"   Issues: {', '.join(email_info['issues'])}")
        print(f"   File: {email_info['filename']}")

print(
    f"\nTotal {len(generated_emails)} email files are ready to send. Each file contains:"
)
print(f"  Line 1: Recipient email address")
print(f"  Line 2: Email subject")
print(f"  Line 3+: Email body")
