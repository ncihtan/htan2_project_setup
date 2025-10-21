# HTAN2 Synapse Project Setup

> [!NOTE]
> These scripts are intended for internal use by the HTAN DCC.  
> Resulting projects will not be publically accessible

## Overview

This repository contains a collection of Python scripts for automating HTAN2 project setup in Synapse. The scripts handle project creation, team management, permissions, annotations, and integration with Jira for user onboarding.

## Prerequisites

- Python 3.x
- Synapse Python Client (`synapseclient`)
- PyYAML (`pyyaml`)
- Additional dependencies listed in `requirements.txt`

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Scripts

### create_projects.py

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

---

### create_teams.py

Creates contributor and user teams for each HTAN2 project and sets appropriate permissions.

#### Features

- Creates `{project_name}_contributors` teams with edit permissions
- Creates `{project_name}_users` teams with read/download permissions
- Checks for existing teams before creating new ones
- Sets up permissions for HTAN DCC Admins, HTAN DCC, and ACT teams
- Updates `projects.yml` with project information

#### Usage

```bash
python create_teams.py
```

The script will:
1. Create or verify projects exist
2. Create contributor and user teams for each project
3. Set appropriate permissions for all teams
4. Save project information to `projects.yml`

---

### add_project_annotations.py

Adds standardized annotations to HTAN2 projects based on metadata from `project_details.yml`.

#### Features

- Reads project metadata from `project_details.yml`
- Validates required fields (project_synid, grant_number, center, shortname, grant_name)
- Adds schema.org compliant annotations to projects
- Includes error handling and validation

#### Required YAML Structure

The `project_details.yml` file should follow this format:

```yaml
projects:
  HTAN2_Glioma:
    project_synid: syn63298048
    project_name: HTAN2_Glioma
    grant_number: CA294551
    center: CalTech
    shortname: HTAN2_Glioma
    grant_name: Understanding the role of tumor microenvironment in low grade glioma progression to malignancy.
```

#### Usage

```bash
python add_project_annotations.py
```

---

### create_fileview.py

Creates a unified file view across all HTAN2 projects for centralized file browsing and querying.

#### Features

- Queries project metadata from a Synapse table
- Creates an EntityViewSchema scoped to all project files
- Sets permissions for HTAN DCC teams
- Adds read/download permissions for project-specific contributor and user teams
- Includes default file view columns automatically

#### Configuration

Update the following variables in the script:
- `project_table_id`: Synapse ID of the table containing project metadata (default: syn63300517)
- Parent project for the file view (default: HTAN2 project)

#### Usage

```bash
python create_fileview.py
```

---

### create_team_table.py

Maintains a Synapse table of all HTAN2 teams with their metadata.

#### Features

- Searches for all teams with "HTAN2" in the name
- Handles paginated results from the Synapse API
- Checks for existing teams to avoid duplicates
- Captures team creation date, modification date, and owner information
- Updates an existing Synapse table with new teams only

#### Configuration

Update the `htan_teams_table_id` variable with your Synapse table ID (default: syn63714328)

#### Usage

```bash
python create_team_table.py
```

---

### add_team_admin.py

Promotes a specified user to admin status across all HTAN2 teams.

#### Features

- Fetches team IDs from a Synapse table
- Checks user membership status in each team
- Sends team invitations if user is not yet a member
- Includes error handling for HTTP errors
- Can be modified to promote different users

#### Configuration

Update the following variables:
- `user_id`: The Synapse user ID to promote (default: "3429359")
- Table query to fetch team information (default: syn63714328)

#### Usage

```bash
python add_team_admin.py
```

**Note**: Users must accept team invitations before they can be promoted to admin.

---

### raise_user_jira_tickets.py

Creates Jira Service Management requests for user onboarding from a formatted text file.

#### Features

- Parses email/subject/body blocks from an input file
- Creates Jira requests on behalf of users
- Adds public comments with customizable intro/footer
- Supports dry-run mode for testing
- Includes retry logic for rate limiting and errors
- Automatically assigns tickets to the creator
- Rich CLI with colorful help output

#### Configuration

Set the following environment variables (or use command-line options):
- `JIRA_URL`: Base URL for your Jira instance
- `JIRA_EMAIL`: Agent email address
- `JIRA_API_TOKEN`: API token for authentication
- `SERVICE_DESK_ID`: Service desk identifier
- `REQUEST_TYPE_ID`: Request type identifier
- `ADD_PARTICIPANTS`: Comma-separated list of participant emails
- `SUBJECT_PREFIX`: Optional prefix for subjects (e.g., "HTAN – ")
- `INTRO_LINE`: Optional introduction line for emails
- `FOOTER_LINE`: Optional footer/signature line

#### Input File Format

Create a text file with blocks formatted as:

```
user@example.com
Email Subject Line
Email body content goes here.
Can span multiple lines.

another.user@example.com
Another Subject
Another email body...
```

#### Usage

```bash
# Basic usage
python raise_user_jira_tickets.py input_file.txt

# Dry run to test without creating tickets
python raise_user_jira_tickets.py --dry-run input_file.txt

# With custom options
python raise_user_jira_tickets.py \
  --subject-prefix "HTAN – " \
  --delay 2 \
  --max-requests 5 \
  input_file.txt

# View all options
python raise_user_jira_tickets.py --help
```

#### Command-line Options

- `--dry-run`: Test mode - shows what would be created without making API calls
- `--delay SECONDS`: Sleep time between requests (default: 0)
- `--max-requests N`: Limit number of requests to process
- `--timeout SECONDS`: HTTP request timeout (default: 30)
- `--skip-customer`: Don't create customer accounts before raising requests
- `--assign-to-creator`: Assign tickets to authenticated agent (default: True)
- `--verbose`: Enable debug logging

---

### scratch.py

A scratch/testing file used for exploring Synapse REST API endpoints. Not intended for production use.

#### Contains Examples Of

- Fetching team information by ID
- Listing team members
- Retrieving team ACLs (Access Control Lists)
- Looking up user profiles from principal IDs

---

## Configuration Files

### projects.yml

Generated by `create_projects.py` and `create_teams.py`. Contains mappings of project names to Synapse IDs.

### project_details.yml

Used by `add_project_annotations.py`. Contains detailed metadata for each project including grant information, center affiliations, and project descriptions.

### requirements.txt

Lists all Python package dependencies required by the scripts.

### test_email.txt

Example input file for `raise_user_jira_tickets.py` demonstrating the email/subject/body format.

---

## Workflow

Typical setup workflow for new HTAN2 projects:

1. **Create Projects**: Run `create_projects.py` to create Synapse projects and set base permissions
2. **Create Teams**: Run `create_teams.py` to create project-specific teams with appropriate permissions
3. **Add Annotations**: Run `add_project_annotations.py` to add metadata annotations to projects
4. **Create File View**: Run `create_fileview.py` to create a unified view of all project files
5. **Update Team Table**: Run `create_team_table.py` to maintain the central team registry
6. **Onboard Users**: Use `raise_user_jira_tickets.py` to create onboarding tickets for new users
7. **Promote Admins**: Use `add_team_admin.py` to promote users to team admin roles as needed

---

## Security Notes

- Never commit credentials or API tokens to version control
- Use environment variables or `.env` files for sensitive configuration
- Ensure proper Synapse permissions before running scripts
- Test with `--dry-run` flags when available before production runs

---

## Troubleshooting

### SynapseHTTPError

- Verify your Synapse credentials are correct
- Ensure you have necessary permissions for the operations
- Check that team IDs and project IDs are accurate

### Jira API Errors

- Verify JIRA_URL, JIRA_EMAIL, and JIRA_API_TOKEN are set correctly
- Ensure the service desk ID and request type ID are valid
- Check that you have permissions to create requests on behalf of users

### YAML Parsing Errors

- Validate YAML syntax using an online validator
- Ensure consistent indentation (use spaces, not tabs)
- Check that all required fields are present in configuration files
