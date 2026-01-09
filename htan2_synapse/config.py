"""
Configuration constants for HTAN2 Synapse setup.
"""

# Team IDs
HTAN_DCC_ADMINS_TEAM_ID = "3497313"
HTAN_DCC_TEAM_ID = "3391844"
ACT_TEAM_ID = "464532"

# Record-based schemas (Clinical components and Biospecimen)
# Based on actual v1.0.0 schemas from ncihtan/htan2-data-model
RECORD_BASED_MODULES = {
    "Clinical": [
        "Demographics",
        "Diagnosis",
        "Therapy",
        "FollowUp",
        "MolecularTest",
        "Exposure",
        "FamilyHistory",
        "VitalStatus"
    ],
    "Biospecimen": [
        # BiospecimenData schema - no subfolders, just the main folder
    ]
}

# File-based schemas (Assay types with levels)
# Based on actual v1.0.0 schemas from ncihtan/htan2-data-model
FILE_BASED_MODULES = {
    "WES": [
        "Level_1",
        "Level_2",
        "Level_3"
    ],
    "scRNA_seq": [
        "Level_1",
        "Level_2",
        "Level_3_4"  # Note: scRNALevel3_4 is combined
    ],
    "Imaging": [
        "DigitalPathology",  # DigitalPathologyData - no levels, just the main folder
        "MultiplexMicroscopy"  # Has Level_2, Level_3, Level_4 subfolders
    ],
    "SpatialTranscriptomics": [
        "Level_1",
        "Level_3",
        "Level_4",
        "Panel"  # SpatialPanel
    ]
}

# Special handling for Imaging subfolders
IMAGING_SUBFOLDERS = {
    "DigitalPathology": [],  # No subfolders
    "MultiplexMicroscopy": [
        "Level_2",
        "Level_3",
        "Level_4"
    ]
}

