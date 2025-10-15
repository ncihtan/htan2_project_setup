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
syn.login()


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
    SELECT
      upl.user_name,
      upl.id,
      upl.first_name,
      upl.last_name,
      upl.company,
      upl.is_two_factor_auth_enabled,
      cql.certified,
      COALESCE(BOOLOR_AGG(t.value:version::string = '1.0.1'), FALSE) AS tos_101_agreed
    FROM synapse_data_warehouse.synapse.userprofile_latest AS upl
    LEFT JOIN synapse_data_warehouse.synapse.certifiedquiz_latest AS cql
      ON upl.id = cql.user_id
    , LATERAL FLATTEN(
        input => TRY_PARSE_JSON(upl.tos_agreements),
        OUTER => TRUE
      ) AS t
    WHERE upl.user_name IN ({username_list})
    GROUP BY
      upl.user_name, upl.id, upl.first_name, upl.last_name, upl.company,
      upl.is_two_factor_auth_enabled, upl.tos_agreements, cql.certified
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
        "Username Exists": None,
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
        "Synapse Username": username,
    }

    # Set results based on pre-computed data
    if should_check_synapse:
        result = username_results[username]
        report_entry["Username Exists"] = result["exists"]
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

# Calculate statistics
total_records = len(df)
records_with_usernames = len(
    df[df["Synapse Username"].notna() & (df["Synapse Username"] != "")]
)

# Count usernames that exist (True values, excluding N/A)
usernames_exist = len(df[df["Username Exists"] == True])

# Count certified users (True values among those that exist)
certified_users = len(df[df["Is Certified"] == True])

print(f"\n=== USERNAME STATISTICS ===")
print(f"Total records: {total_records}")
print(f"Records with usernames: {records_with_usernames}")

if records_with_usernames > 0:
    exist_proportion = usernames_exist / records_with_usernames
    print(
        f"Usernames that exist: {usernames_exist}/{records_with_usernames} ({exist_proportion:.1%})"
    )
else:
    print(f"Usernames that exist: 0/0 (N/A)")

if usernames_exist > 0:
    certified_proportion = certified_users / usernames_exist
    print(
        f"Existing users that are certified: {certified_users}/{usernames_exist} ({certified_proportion:.1%})"
    )

    # Count 2FA and TOS statistics for existing users
    users_with_2fa = len(df[df["Has 2FA"] == True])
    users_with_tos = len(df[df["TOS 1.0.1 Agreed"] == True])

    tfa_proportion = users_with_2fa / usernames_exist
    tos_proportion = users_with_tos / usernames_exist

    print(
        f"Existing users with 2FA enabled: {users_with_2fa}/{usernames_exist} ({tfa_proportion:.1%})"
    )
    print(
        f"Existing users with TOS 1.0.1 agreed: {users_with_tos}/{usernames_exist} ({tos_proportion:.1%})"
    )
else:
    print(f"Existing users that are certified: 0/0 (N/A)")
    print(f"Existing users with 2FA enabled: 0/0 (N/A)")
    print(f"Existing users with TOS 1.0.1 agreed: 0/0 (N/A)")

# Show breakdown by status
print(f"\n=== USERNAME STATUS BREAKDOWN ===")
status_counts = df["Username Exists"].value_counts()
for status, count in status_counts.items():
    print(f"{status}: {count}")

print(f"\n=== CERTIFICATION STATUS BREAKDOWN ===")
cert_counts = df["Is Certified"].value_counts()
for status, count in cert_counts.items():
    print(f"{status}: {count}")

print(f"\n=== 2FA STATUS BREAKDOWN ===")
tfa_counts = df["Has 2FA"].value_counts()
for status, count in tfa_counts.items():
    print(f"{status}: {count}")

print(f"\n=== TOS AGREEMENT STATUS BREAKDOWN ===")
tos_counts = df["TOS 1.0.1 Agreed"].value_counts()
for status, count in tos_counts.items():
    print(f"{status}: {count}")

# Generate email files for users who need Synapse account action
print(f"\n=== GENERATING EMAIL NOTIFICATIONS ===")


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

    return issues


def generate_customized_email(user_row, issues):
    """Generate a customized email based on the specific issues a user has"""
    first_name = user_row["First Name"]
    last_name = user_row["Last Name"]
    email = user_row["Contact Email"]
    synapse_username = user_row["Synapse Username"]
    institution = user_row["Institution"]
    atlas = user_row["Atlas"]

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

Thank you for registering as a HTAN community member through the Airtable form. As you indicated that you needed access to the Synapse platform we are now reviewing your account information in preparation for onboarding.

We are working to set up Synapse access for your participation in the HTAN Phase 2 project "{atlas}" at {institution}.

{problem_description}. Here's what we found and what needs to be addressed:

"""

    # Add specific issue descriptions
    action_items = []

    if "email_instead_of_username" in issues:
        body += f'❌ **Username Issue**: You provided an email address ("{synapse_username}") instead of a Synapse username.\n'
        action_items.append(
            "Provide your actual Synapse username (not your email address)"
        )

    if "no_username_provided" in issues:
        body += f"❌ **Username Issue**: No Synapse username was provided in your registration.\n"
        action_items.append("Provide your Synapse username")

    if "username_not_found" in issues:
        body += f'❌ **Username Issue**: The username "{synapse_username}" was not found in the Synapse system.\n'
        action_items.append(
            "Verify your Synapse username is correct, or create a new account if needed"
        )

    if "not_certified" in issues:
        body += f'❌ **Certification Issue**: Your account "{synapse_username}" is not certified.\n'
        action_items.append("Complete the Synapse user certification quiz")

    if "no_2fa" in issues:
        body += f'❌ **Security Issue**: Your account "{synapse_username}" does not have Multi-Factor Authentication (2FA) enabled.\n'
        action_items.append("Enable Multi-Factor Authentication (2FA) on your account")

    if "tos_not_agreed" in issues:
        body += f'❌ **Terms Issue**: Your account "{synapse_username}" has not agreed to the latest Terms of Service (version 1.0.1).\n'
        action_items.append("Accept the current Synapse Terms and Conditions of Use")

    # Add action steps
    body += f"\n**Required Actions:**\n"

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
1. **Synapse Account Setup**:
   - If you don't have a Synapse account, create one at: https://www.synapse.org/#!RegisterAccount:0
   - If you have an account, verify your username at: https://www.synapse.org/
   - Reply to this email with your correct Synapse username

"""

    # Then, account compliance steps
    if any(issue in issues for issue in ["not_certified", "no_2fa", "tos_not_agreed"]):
        body += f"""2. **Account Compliance** (visit https://accounts.synapse.org/authenticated/myaccount?appId=synapse.org):
"""
        if "tos_not_agreed" in issues:
            body += f"   - ✅ Accept the Synapse Terms and Conditions of Use (version 1.0.1)\n"
        if "no_2fa" in issues:
            body += f"   - ✅ Enable Multi-Factor Authentication (2FA) - required for all accounts\n"
        if "not_certified" in issues:
            body += f"   - ✅ Complete the Synapse user certification quiz on data sharing ethics\n"
        body += f"   - ✅ Ensure your profile is complete with full name and institution\n\n"
    else:
        body += f"""2. **Once you have a Synapse account** (visit https://accounts.synapse.org/authenticated/myaccount?appId=synapse.org):
   - ✅ Accept the Synapse Terms and Conditions of Use (version 1.0.1)
   - ✅ Enable Multi-Factor Authentication (2FA) - required for all accounts
   - ✅ Complete the Synapse user certification quiz on data sharing ethics
   - ✅ Ensure your profile is complete with full name and institution

"""

    body += f"""3. **Confirm Completion**:
   - Reply to this email once all requirements are met
   - We will then add you to the appropriate HTAN project teams

**Need Help?**
- Synapse documentation: https://help.synapse.org/
- Contact us if you have questions about any of these requirements

Best regards,
HTAN Data Coordination Team"""

    # Write email file
    with open(filepath, "w") as f:
        f.write(f"{email}\n")
        f.write(f"{subject}\n")
        f.write(f"{body}\n")

    return filename, issues


# Find all users who need action
users_needing_action = df[
    (df["Synapse Access Needed"] == True)
    & (df["Contact Email"].notna())
    & (df["Contact Email"] != "")
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

print(f"Total users needing action: {len(users_needing_action)}")

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
