# HTAN2 Synapse Project Setup

> [!NOTE]
> These scripts are intended for internal use by the HTAN DCC.  
> Resulting projects will not be publically accessible

## create_project.py


This script automates the creation of projects in Synapse and sets permissions for specific teams. If a project already exists, it resets the permissions for that project. After execution, the script saves the created or updated project names and their corresponding Synapse IDs in a `projects.yml` file.

### Features
- Automatically creates Synapse projects if they do not exist.
- Resets permissions for existing Synapse projects.
- Adds specified teams to the projects with customizable permissions.
- Saves the project names and Synapse IDs to `projects.yml`.

### Prerequisites
- Python 3.x
- Synapse Python Client (`synapseclient`)
- PyYAML (`yaml` for reading/writing YAML files)

### Install dependencies
You can install the necessary Python libraries with the following command:

```bash
pip install synapseclient pyyaml
```

### Usage
1. Clone the repository or copy the script to your local machine.
2. Open the script and make sure the following fields are correct:
   - **`project_names`**: A list of project names that you want to create or manage in Synapse.
   - **`htan_dcc_admins_team_id`**: The ID of the HTAN DCC Admins team.
   - **`htan_dcc_team_id`**: The ID of the HTAN DCC team.
   - **`act_team_id`**: The ID of the ACT team.
   
3. Run the script using Python:

```bash
python synapse_project_creator.py
```

4. The script will:
   - Log into Synapse.
   - Loop through the project names.
   - Create the project if it doesn't exist or reset its permissions if it already exists.
   - Set team permissions for the projects.
   - Save the project name and Synapse ID to `projects.yml`.

#### Output of `projects.yml`
After running the script, the `projects.yml` file will contain project names and their corresponding Synapse IDs. This is presented as a table below

| Project          | Synapse ID                                                   |
|------------------|--------------------------------------------------------------|
| HTAN2            | [syn63296487](https://www.synapse.org/#!Synapse:syn63296487) |
| HTAN2_Ovarian    | [syn63298044](https://www.synapse.org/#!Synapse:syn63298044) |
| HTAN2_Glioma     | [syn63298048](https://www.synapse.org/#!Synapse:syn63298048) |
| HTAN2_Gastric    | [syn63298051](https://www.synapse.org/#!Synapse:syn63298051) |
| HTAN2_Skin       | [syn63298054](https://www.synapse.org/#!Synapse:syn63298054) |
| HTAN2_Pediatric  | [syn63298059](https://www.synapse.org/#!Synapse:syn63298059) |
| HTAN2_Myeloma    | [syn63298063](https://www.synapse.org/#!Synapse:syn63298063) |
| HTAN2_Pancreatic | [syn63298065](https://www.synapse.org/#!Synapse:syn63298065) |
| HTAN2_Prostate   | [syn63298068](https://www.synapse.org/#!Synapse:syn63298068) |
| HTAN2_CRC        | [syn63298073](https://www.synapse.org/#!Synapse:syn63298073) |
| HTAN2_Lymphoma   | [syn63298076](https://www.synapse.org/#!Synapse:syn63298076) |


### Customization
- **Project Names**: You can modify the `project_names` list to include any project names you need.
- **Permissions**: The permissions for each team can be adjusted within the `set_project_permissions` function.
  - The current setup adds admin permissions to the **HTAN DCC Admins** and **ACT** teams, while the **HTAN DCC** team gets edit and delete permissions.

#### Troubleshooting
- If you encounter a `SynapseHTTPError`, ensure that your Synapse credentials are correct and you have the necessary permissions to create and manage the projects.
- Verify that the team IDs are accurate by checking the Synapse web interface.

## check_airtable_synapse_users.py

This script cross-references HTAN community member registration data from Airtable with Synapse account information to validate user accounts and generate customized onboarding emails. It performs comprehensive checks for account existence, certification status, Multi-Factor Authentication (2FA), and Terms of Service (TOS) compliance.

### Script Features

- **Airtable Integration**: Fetches user registration data from HTAN Airtable forms
- **Snowflake Data Warehouse**: Queries Synapse user data efficiently using Snowflake instead of individual API calls
- **Comprehensive Account Validation**: Checks for:
  - Username existence in Synapse
  - Certification status
  - 2FA enablement
  - TOS agreement (version 1.0.1)
- **Smart Email Detection**: Identifies when users provide email addresses instead of usernames
- **Customized Email Generation**: Creates personalized emails based on each user's specific issues
- **Clean Output Management**: Automatically cleans and recreates output directories for fresh runs
- **Detailed Reporting**: Provides comprehensive statistics and breakdowns

### Script Prerequisites

- Python 3.x
- Required packages: `synapseclient`, `pyairtable`, `pandas`, `tqdm`, `snowflake-connector-python`, `python-dotenv`
- Environment variables in `.env` file:
  - `AIRTABLE_PAT`: Airtable Personal Access Token
  - `SNOWFLAKE_USER`: Snowflake username
  - `SNOWFLAKE_ACCOUNT`: Snowflake account identifier
  - `SNOWFLAKE_PAT`: Snowflake Personal Access Token

### Install Script Dependencies

```bash
pip install synapseclient pyairtable pandas tqdm snowflake-connector-python python-dotenv
```

### Script Usage

1. Ensure your `.env` file contains the required environment variables
2. Run the script:

```bash
python3 check_airtable_synapse_users.py
```

### Script Output Files

The script generates:

- **`outputs/airtable_synapse_crosscheck.csv`**: Complete user data with validation results
- **`outputs/emails/`**: Directory containing customized email files for users needing action

### Generated Email Categories

The script generates targeted emails for various scenarios:

- **Username Issues**: Missing username, email provided instead, username not found
- **Compliance Issues**: Missing certification, no 2FA, TOS not agreed
- **Combined Issues**: Multiple problems requiring different actions

### CSV Report Data Columns

- `Username Exists`: Whether the Synapse username was found
- `Is Certified`: Certification status
- `Has 2FA`: Multi-Factor Authentication status
- `TOS 1.0.1 Agreed`: Terms of Service agreement status
- User details: Name, institution, atlas, contact email, etc.

### Email Customization Features

- **Personalized greetings** with user name and project details
- **Issue-specific descriptions** with clear problem identification
- **Numbered action steps** in logical order
- **Conditional instructions** showing only relevant tasks
- **Professional formatting** with checkboxes and links
- **Consistent messaging** across all HTAN Phase 2 communications

#### Script Troubleshooting

- Verify Snowflake credentials and database access permissions
- Ensure Airtable PAT has read access to the user registration table
- Check that Synapse login credentials are valid
- Confirm network connectivity to all external services
