"""synapse_json_schema_bind.py

Bind an already-registered JSON schema to a Synapse entity (folder).
The schema must already be registered in Synapse before running this script.

Usage:
    python scripts/synapse_json_schema_bind.py -t <entity_id> -p <schema_file>
    python scripts/synapse_json_schema_bind.py -t <entity_id> -l <schema_url>
    python scripts/synapse_json_schema_bind.py -t <entity_id> -p <schema_file> -ar

Arguments:
    -t    Synapse entity ID to bind the schema to
    -l    URL of the JSON schema file
    -p    Local file path of the JSON schema file
    -n    Organization name (default: 'Example Organization')
    -ar   Enable derived annotations (use for Access Requirement schemas)
    --no_bind  Parse schema without binding

author: orion.banks
"""

import argparse
import json
import os

import requests
import synapseclient
from synapseclient.models import Folder


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", type=str, default=None, help="Synapse entity ID to bind schema to.")
    parser.add_argument("-l", type=str, default=None, help="URL of the JSON schema.")
    parser.add_argument("-p", type=str, default=None, help="Local file path of the JSON schema.")
    parser.add_argument(
        "-n",
        type=str,
        default="Example Organization",
        help="Organization name. Default: 'Example Organization'.",
    )
    parser.add_argument(
        "-ar",
        action="store_true",
        default=None,
        help="Enable derived annotations (for Access Requirement schemas).",
    )
    parser.add_argument(
        "--no_bind",
        action="store_true",
        default=None,
        help="Parse schema without binding it.",
    )
    return parser.parse_args()


def get_schema_from_url(url: str, path: str) -> tuple:
    """Load and parse a JSON schema from a URL or local path.
    Returns (schema_json, component_adjusted, base_component, version).

    Expected file name convention:
        Non-AR: HTAN.BulkWESLevel1-v1.0.0-schema.json
        AR:     MC2.AccessRequirement-CA000001-v3.0.2-schema.json
    """
    if url is not None:
        source = requests.get(url)
        schema_json = source.json()
        schema = url
    else:
        with open(path, "r") as f:
            schema_json = json.load(f)
        schema = path

    schema_info = schema.split("/")[-1]

    if "-" in schema_info and len(schema_info.split("-")) >= 2:
        base_component = schema_info.split(".")[1].split("-")[0]
        if base_component == "AccessRequirement":
            component = "".join(schema_info.split("-")[0:-2]).split(".")[1]
            version = schema_info.split("-")[-2]
        else:
            component = base_component
            version = schema_info.split("-")[1]
    else:
        base_component = schema_info.split(".")[0].replace("_", "")
        component = base_component
        version = "1.0.0"

    print(f"JSON schema {component} {version} successfully acquired")
    return schema_json, component, base_component, version


def get_schema_uri(schema_org_name: str, schema_type: str, version: str) -> str:
    """Construct the registered schema URI from org name, type, and version."""
    num_version = version.lstrip("v")
    return "-".join([schema_org_name.replace(" ", ""), schema_type, num_version])


def bind_schema_to_entity(
    syn: synapseclient.Synapse,
    schema_uri: str,
    entity_id: str,
    enable_derived_annotations: bool = False,
) -> None:
    """Bind a registered schema URI to a Synapse entity using the new model-based API."""
    folder = Folder(id=entity_id)
    folder.bind_schema(
        json_schema_uri=schema_uri,
        enable_derived_annotations=enable_derived_annotations,
        synapse_client=syn,
    )


def main():
    args = get_args()
    target, url, path, org_name = args.t, args.l, args.p, args.n
    includes_ar = args.ar
    no_bind = args.no_bind

    if url is None and path is None:
        print("❗ No schema source provided (-l or -p). Nothing to do.")
        return

    schema_json, component_adjusted, base_component, version = get_schema_from_url(url, path)

    if no_bind:
        print("✅ Schema processed (--no_bind flag set, skipping binding)")
        return

    if target is None:
        print("❗ No target entity ID provided (-t). Nothing to bind.")
        return

    syn = synapseclient.Synapse()
    username = os.environ.get("SYNAPSE_USERNAME")
    auth_token = os.environ.get("SYNAPSE_PAT")
    if username and auth_token:
        syn.login(username, authToken=auth_token)
    else:
        syn.login()
    print(f"Logged in to Synapse")

    schema_uri = get_schema_uri(org_name, component_adjusted, version)
    print(f"Binding schema: {schema_uri} → {target}")

    enable_derived = base_component == "AccessRequirement" or includes_ar is not None
    bind_schema_to_entity(syn, schema_uri, target, enable_derived_annotations=enable_derived)
    print(f"✅ Schema {component_adjusted} {version} successfully bound to entity {target}")


if __name__ == "__main__":
    main()
