#!/usr/bin/env python3
"""
Bind the latest HTAN2 schemas (as already bound on v8_ingest real projects)
to the v8_ingest folder tree in the htan2-testing1 project.

Strategy:
- Use `schema_binding_config.yml` to find one reference v8_ingest folder per schema.
- Read the bound schema URI from that reference folder.
- Traverse the v8_ingest tree in htan2-testing1 and bind the same schema URI
  to the corresponding folders (matching the relative path under v8_ingest).

This avoids guessing schema file names or versions and guarantees we use the
same registered schema URIs (e.g., HTAN2Organization-*-1.2.0) that are already
in production.
"""

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

import synapseclient
import yaml


def get_bound_schema_uri(syn: synapseclient.Synapse, entity_id: str) -> Optional[str]:
    """Get the schema URI currently bound to a Synapse entity."""
    try:
        binding = syn.restGET(f"/entity/{entity_id}/schema/binding")
        return binding.get("jsonSchemaVersionInfo", {}).get("$id") if binding else None
    except synapseclient.core.exceptions.SynapseHTTPError as e:
        # 404 = no binding
        if getattr(e.response, "status_code", None) == 404:
            return None
        raise
    except Exception:
        return None


def get_child_by_name(syn: synapseclient.Synapse, parent_id: str, name: str) -> Optional[str]:
    """Find a direct child folder by name."""
    for child in syn.getChildren(parent_id):
        if child.get("name") == name:
            return child.get("id")
    return None


def get_descendant_by_path(syn: synapseclient.Synapse, root_id: str, rel_path: str) -> Optional[str]:
    """Traverse a path like 'WES/Level_1' under a given root folder ID."""
    current = root_id
    for part in rel_path.split("/"):
        if not part:
            continue
        child_id = get_child_by_name(syn, current, part)
        if not child_id:
            return None
        current = child_id
    return current


def build_reference_bindings(config_path: Path) -> Dict[str, str]:
    """
    From schema_binding_config.yml, build a mapping of:
        relative_path_under_v8_ingest -> reference_folder_id
    using the first v8_ingest occurrence for each schema.
    """
    with config_path.open("r") as f:
        config = yaml.safe_load(f)

    refs: Dict[str, str] = {}

    for section in ("file_based", "record_based"):
        schemas = config.get("schema_bindings", {}).get(section, {})
        for _schema_name, schema_cfg in schemas.items():
            for proj in schema_cfg.get("projects", []):
                subfolder = proj.get("subfolder", "")
                folder_id = proj.get("synapse_id")
                if not folder_id or not subfolder.startswith("v8_ingest/"):
                    continue
                rel_path = subfolder[len("v8_ingest/") :]
                # Only need one reference per relative path
                refs.setdefault(rel_path, folder_id)

    return refs


def bind_schema_to_entity(syn: synapseclient.Synapse, entity_id: str, schema_uri: str) -> None:
    """Bind a registered schema URI directly via REST."""
    body = {
        "entityId": entity_id,
        "schema$id": schema_uri,
        # Do not enable derived annotations here; consistent with non-AR usage.
    }
    syn.restPUT(f"/entity/{entity_id}/schema/binding", body=json.dumps(body))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bind latest HTAN2 schemas to v8_ingest folders in htan2-testing1."
    )
    parser.add_argument(
        "--project-id",
        default="syn63834783",
        help="Testing project Synapse ID (default: syn63834783 for htan2-testing1).",
    )
    parser.add_argument(
        "--config",
        default="schema_binding_config.yml",
        help="Path to schema_binding_config.yml (default: schema_binding_config.yml).",
    )
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    syn.login()

    project_id = args.project_id
    config_path = Path(args.config)

    print("=" * 80)
    print(f"Binding latest schemas to v8_ingest in testing project {project_id}")
    print("=" * 80)

    # Locate v8_ingest root in the testing project
    v8_ingest_id = get_child_by_name(syn, project_id, "v8_ingest")
    if not v8_ingest_id:
        print(f"❌ Could not find v8_ingest under project {project_id}")
        return

    print(f"v8_ingest root: {v8_ingest_id}")

    # Build reference bindings from the main config (real projects)
    ref_bindings = build_reference_bindings(config_path)
    print(f"Found {len(ref_bindings)} reference v8_ingest paths with existing bindings.")

    bound = 0
    skipped_missing_target = 0
    skipped_no_schema = 0

    for rel_path, ref_folder_id in sorted(ref_bindings.items()):
        print(f"\nPath: v8_ingest/{rel_path}")
        print(f"  Reference folder: {ref_folder_id}")

        schema_uri = get_bound_schema_uri(syn, ref_folder_id)
        if not schema_uri:
            print(f"  ❌ No schema bound on reference folder {ref_folder_id}, skipping")
            skipped_no_schema += 1
            continue

        print(f"  Reference schema URI: {schema_uri}")

        target_id = get_descendant_by_path(syn, v8_ingest_id, rel_path)
        if not target_id:
            print(f"  ❌ Could not find matching folder in testing v8_ingest for path '{rel_path}', skipping")
            skipped_missing_target += 1
            continue

        print(f"  Target folder in testing project: {target_id}")

        try:
            bind_schema_to_entity(syn, target_id, schema_uri)
            print(f"  ✅ Bound {schema_uri} to {target_id}")
            bound += 1
        except Exception as e:
            print(f"  ❌ Error binding schema to {target_id}: {e}")

    print("\nSummary")
    print("=" * 80)
    print(f"Successfully bound:          {bound}")
    print(f"Skipped (no schema on ref): {skipped_no_schema}")
    print(f"Skipped (no target folder): {skipped_missing_target}")
    print("=" * 80)


if __name__ == "__main__":
    main()



