#!/usr/bin/env python3
"""
Create a Digital Pathology file-based curation task using the curator extension.
See: https://python-docs.synapse.org/en/stable/guides/extensions/curator/metadata_curation/
"""

import synapseclient

try:
    from synapseclient.extensions.curator import create_file_based_metadata_task
except ImportError:
    raise SystemExit("Install curator extension: pip install 'synapseclient[curator]'")


FOLDER_ID = "syn73742697"  # htan2-testing1 Imaging/DigitalPathology


def get_bound_schema_uri(syn, entity_id):
    try:
        binding = syn.restGET(f"/entity/{entity_id}/schema/binding")
        return binding.get("jsonSchemaVersionInfo", {}).get("$id") if binding else None
    except Exception:
        return None


def main():
    syn = synapseclient.Synapse()
    syn.login()

    schema_uri = get_bound_schema_uri(syn, FOLDER_ID)
    if not schema_uri:
        raise SystemExit(f"No schema bound to folder {FOLDER_ID}. Bind a schema first.")

    entity_view_id, task_id = create_file_based_metadata_task(
        synapse_client=syn,
        folder_id=FOLDER_ID,
        curation_task_name="DigitalPathology",
        instructions="Annotate each file with Digital Pathology metadata according to the schema.",
        attach_wiki=False,
        entity_view_name="Digital Pathology Files View",
        schema_uri=schema_uri,
    )
    print(f"Created EntityView: {entity_view_id}")
    print(f"Created CurationTask: {task_id}")


if __name__ == "__main__":
    main()
