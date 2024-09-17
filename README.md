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
