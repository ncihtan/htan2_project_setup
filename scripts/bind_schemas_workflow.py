#!/usr/bin/env python3
"""
Script to bind schemas to project subfolders as part of the GitHub Actions workflow.
Processes schema_binding_config.yml and binds both file-based and record-based schemas
to projects. Collects results and continues on errors to provide a complete summary.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import yaml
from typing import Dict, List, Optional


def map_schema_name_to_file(schema_name: str, schema_version: str = "v1.0.0") -> str:
    """Map schema name from config to expected schema file name pattern."""
    version_suffix = f"-{schema_version}"

    if schema_name == "DigitalPathology":
        return f"HTAN.DigitalPathologyData{version_suffix}-schema.json"
    elif schema_name == "Biospecimen":
        return f"HTAN.BiospecimenData{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel1":
        return f"HTAN.scRNALevel1{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel2":
        return f"HTAN.scRNALevel2{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel3_4":
        return f"HTAN.scRNALevel3and4{version_suffix}-schema.json"
    elif schema_name.startswith("SpatialOmics"):
        level_part = schema_name.replace("SpatialOmics", "Spatial")
        return f"HTAN.{level_part}{version_suffix}-schema.json"
    else:
        return f"HTAN.{schema_name}{version_suffix}-schema.json"


def find_schema_file(schema_name: str, files: List[str], schema_version: str = "v1.0.0") -> Optional[str]:
    """Find the matching schema file for a given schema name."""
    expected_pattern = map_schema_name_to_file(schema_name, schema_version)
    expected_name = os.path.basename(expected_pattern)

    for file in files:
        if file.lower() == expected_name.lower():
            return f"schemas/{file}"

    expected_core = re.sub(r"^htan\.", "", expected_name.lower())
    expected_core = re.sub(r"-v\d+\.\d+\.\d+-schema\.json$", "", expected_core)
    schema_core = schema_name.lower().replace("_", "")

    for file in files:
        file_lower = file.lower()
        file_core = re.sub(r"^htan\.", "", file_lower)
        file_core = re.sub(r"-v\d+\.\d+\.\d+-schema\.json$", "", file_core)
        if file_core in (expected_core, schema_core):
            return f"schemas/{file}"

    return None


def filter_projects_by_folder_type(projects: List[Dict], folder_types: List[str]) -> List[Dict]:
    """Filter projects list to only include specified folder types."""
    if not folder_types:
        return projects
    return [p for p in projects if any(p.get("subfolder", "").startswith(ft) for ft in folder_types)]


def bind_schema_section(
    schemas: Dict,
    schema_version: str,
    organization_name: str,
    results: Dict,
) -> None:
    """Bind all schemas in a section (file-based or record-based) to their projects."""
    try:
        files = os.listdir("schemas")
    except FileNotFoundError:
        for schema_name, schema_config in schemas.items():
            results["skipped"].append({
                "schema": schema_name,
                "reason": "Schemas directory not found",
                "projects": [p["name"] for p in schema_config.get("projects", [])],
            })
        return

    for schema_name, schema_config in schemas.items():
        print(f'\n{"=" * 80}')
        print(f"Processing schema: {schema_name}")
        print(f'{"=" * 80}')
        print(f"Expected file pattern: {map_schema_name_to_file(schema_name, schema_version)}")
        print("Available files in schemas directory:")
        for file in files:
            print(f"  - {file}")

        schema_file = find_schema_file(schema_name, files, schema_version)
        if not schema_file:
            print(f"❌ Schema file for {schema_name} not found")
            results["skipped"].append({
                "schema": schema_name,
                "reason": "Schema file not found in schemas directory",
                "projects": [p["name"] for p in schema_config.get("projects", [])],
            })
            continue

        print(f"✅ Found schema file: {schema_file}")
        projects = schema_config.get("projects", [])
        print(f"Found {len(projects)} project(s) for this schema")

        timeout = 900 if schema_name == "scRNA_seqLevel3_4" else 300
        print(f"Timeout: {timeout // 60} minutes")

        for project in projects:
            project_name = project["name"]
            synapse_id = project["synapse_id"]
            subfolder = project.get("subfolder", "N/A")

            print(f"\n  Binding {schema_name} to {project_name}")
            print(f"    Folder: {subfolder}")
            print(f"    Synapse ID: {synapse_id}")

            cmd = [
                "python", "scripts/synapse_json_schema_bind.py",
                "-p", schema_file,
                "-t", synapse_id,
                "-n", organization_name,
            ]

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=timeout,
                )
                print("    ✅ Successfully bound")
                results["successful"].append({
                    "schema": schema_name,
                    "project": project_name,
                    "synapse_id": synapse_id,
                    "subfolder": subfolder,
                    "schema_file": schema_file,
                })
                for line in result.stdout.strip().split("\n")[:5]:
                    if line.strip():
                        print(f"      {line}")
            except subprocess.TimeoutExpired:
                error_msg = f"Binding timed out after {timeout // 60} minutes"
                print(f"    ❌ Failed: {error_msg}")
                results["failed"].append({
                    "schema": schema_name,
                    "project": project_name,
                    "synapse_id": synapse_id,
                    "subfolder": subfolder,
                    "error": error_msg,
                })
            except subprocess.CalledProcessError as e:
                error_msg = (e.stderr.strip() if e.stderr else str(e))[:500]
                print(f"    ❌ Failed: {error_msg}")
                results["failed"].append({
                    "schema": schema_name,
                    "project": project_name,
                    "synapse_id": synapse_id,
                    "subfolder": subfolder,
                    "error": error_msg,
                })
            except Exception as e:
                error_msg = str(e)[:500]
                print(f"    ❌ Failed: {error_msg}")
                results["failed"].append({
                    "schema": schema_name,
                    "project": project_name,
                    "synapse_id": synapse_id,
                    "subfolder": subfolder,
                    "error": error_msg,
                })


def main():
    parser = argparse.ArgumentParser(
        description="Bind schemas to project subfolders",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/bind_schemas_workflow.py
  python scripts/bind_schemas_workflow.py --schema-filter scRNA_seqLevel3_4
  python scripts/bind_schemas_workflow.py --folder-type-filter v8_release v8_ingest
        """,
    )
    parser.add_argument("--schema-filter", nargs="+", help="Only bind these schema(s)")
    parser.add_argument("--folder-type-filter", nargs="+", help="Only bind to these folder types")
    parser.add_argument("--schema-version", default=None, help="Schema version (e.g., v1.0.0)")
    parser.add_argument("--config-file", default="schema_binding_config.yml")
    args = parser.parse_args()

    with open(args.config_file, "r") as f:
        config = yaml.safe_load(f)

    file_based_schemas = config["schema_bindings"].get("file_based", {})
    record_based_schemas = config["schema_bindings"].get("record_based", {})
    organization_name = os.environ.get("ORGANIZATION_NAME", "HTAN2Organization")
    schema_version = args.schema_version or os.environ.get("SCHEMA_VERSION", "v1.0.0")

    if args.schema_filter:
        file_based_schemas = {k: v for k, v in file_based_schemas.items() if k in args.schema_filter}
        record_based_schemas = {k: v for k, v in record_based_schemas.items() if k in args.schema_filter}
        print(f"📋 Filtering to schemas: {', '.join(args.schema_filter)}")

    if args.folder_type_filter:
        print(f"📁 Filtering to folder types: {', '.join(args.folder_type_filter)}")
        for schemas in (file_based_schemas, record_based_schemas):
            for schema_name in schemas:
                schemas[schema_name]["projects"] = filter_projects_by_folder_type(
                    schemas[schema_name].get("projects", []), args.folder_type_filter
                )

    results: Dict = {"successful": [], "failed": [], "skipped": []}

    print("=" * 80)
    print("Schema Binding Workflow")
    print("=" * 80)
    print(f"Organization: {organization_name}")
    print(f"File-based schemas: {len(file_based_schemas)}")
    print(f"Record-based schemas: {len(record_based_schemas)}")
    print("=" * 80)

    bind_schema_section(file_based_schemas, schema_version, organization_name, results)

    print("\n" + "=" * 80)
    print("PROCESSING RECORD-BASED SCHEMAS")
    print("=" * 80)

    bind_schema_section(record_based_schemas, schema_version, organization_name, results)

    print("\n" + "=" * 80)
    print("BINDING SUMMARY")
    print("=" * 80)
    print(f"✅ Successful: {len(results['successful'])}")
    print(f"❌ Failed: {len(results['failed'])}")
    print(f"⏭️  Skipped: {len(results['skipped'])}")
    print("=" * 80)

    if results["failed"]:
        print("\n❌ FAILED BINDINGS:")
        print("-" * 80)
        for f in results["failed"]:
            print(f"  Schema: {f['schema']}")
            print(f"  Project: {f['project']} ({f['synapse_id']})")
            print(f"  Subfolder: {f['subfolder']}")
            print(f"  Error: {f['error']}")
            print()

    if results["skipped"]:
        print("\n⏭️  SKIPPED SCHEMAS:")
        print("-" * 80)
        for s in results["skipped"]:
            print(f"  Schema: {s['schema']}")
            print(f"  Reason: {s['reason']}")
            print(f"  Projects affected: {', '.join(s['projects'])}")
            print()

    with open("binding_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Results saved to: binding_results.json")

    if results["failed"]:
        print(f"\n⚠️  Warning: {len(results['failed'])} binding(s) failed")

    return results


if __name__ == "__main__":
    main()
