# Pythons script to add annotations to a project in Synapse
# Follows required and recommended annotations for datasets schema.org

# Import necessary libraries
import synapseclient
from synapseclient import Project
from synapseclient.core.exceptions import SynapseHTTPError
import yaml

# Login to Synapse
syn = synapseclient.Synapse()
syn.login()

# Read project info from YAML file
try:
    with open("project_details.yml", "r") as file:  # Fixed extension
        project_data = yaml.safe_load(file)
except FileNotFoundError:
    logger.error("project_details.yml file not found")
    sys.exit(1)
except yaml.YAMLError as e:
    logger.error(f"Error parsing YAML file: {e}")
    sys.exit(1)

# The projects dir contains a list of project_names with their annotations
# The entityId of the project is stored as project_synid

## Example project_details.yaml
# projects:
#   HTAN2_Glioma:
#     project_synid: syn63298048
#     project_name: HTAN2_Glioma
#     grant_number: CA294551
#     center: CalTech
#     shortname: HTAN2_Glioma
#     grant_name: Understanding the role of tumor microenvironment in low grade glioma progression to malignancy.
#
#   HTAN2_Lymphoma:
#     project_synid: syn63298076
#     project_name: HTAN2_Lymphoma
#     grant_number: CA294514
#     center: Yale
#     shortname: HuLymSTA
# grant_name: Center for Human Lymphoma Spatiotemporal grant_name (HuLymSTA)


# Function to add annotations to a project
def add_project_annotations(project_synid, annotations):
    project = syn.get(project_synid)
    project.annotations.update(annotations)
    syn.store(project)


# Loop through each project and add annotations
for project_name, project_details in project_info["projects"].items():
    project_synid = project_details["project_synid"]
    annotations = {
        "grantNumber": project_details["grant_number"],
        "center": project_details["center"],
        "shortname": project_details["shortname"],
        "grantName": project_details["grant_name"],
    }
    add_project_annotations(project_synid, annotations)
