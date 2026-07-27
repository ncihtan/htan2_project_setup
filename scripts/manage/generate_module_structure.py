#!/usr/bin/env python3
"""
Generate the committed module-structure artifact from the data-model schemas.

Reads:
  - htan2_synapse/module_registry.yml  (hand-maintained placement + naming registry)
  - a schemas/ directory of downloaded data-model JSON schemas
      (files named HTAN.<class>-<version>-schema.json)

Writes:
  - htan2_synapse/module_structure.yml (committed artifact consumed by htan2_synapse.config)

The registry is the source of truth for WHERE each schema goes and WHAT it is called.
The downloaded schemas decide WHICH assays actually exist in this data-model version and,
via the presence of a FILENAME property, whether each is file-based or record-based.

Fail-loud policy: a schema present in schemas/ whose class token is not in the registry,
or whose detected file/record kind disagrees with the registry, aborts with exit code 1.
This is deliberately the opposite of the previous behaviour, where an unregistered assay
(MSI, scATAC, MolecularAssignment) was silently dropped.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_REGISTRY = REPO_ROOT / "htan2_synapse" / "module_registry.yml"
DEFAULT_OUTPUT = REPO_ROOT / "htan2_synapse" / "module_structure.yml"

# HTAN.<class>-<version>-schema.json  ->  captures <class>
_FILE_RE = re.compile(r"^HTAN\.(?P<token>.+)-v[0-9][^-]*-schema\.json$")


def detect_kind(schema_path: Path) -> str:
    """file-based iff the schema declares a FILENAME property, else record-based."""
    with open(schema_path) as f:
        schema = json.load(f)
    properties = schema.get("properties") or {}
    return "file" if "FILENAME" in properties else "record"


def scan_schema_files(schemas_dir: Path) -> dict:
    """Return {class_token: Path} for every HTAN.<class>-<ver>-schema.json in the dir."""
    found = {}
    for name in sorted(os.listdir(schemas_dir)):
        m = _FILE_RE.match(name)
        if m:
            found[m.group("token")] = schemas_dir / name
    return found


def build_structure(registry: dict, present: dict):
    """Resolve the registry against the schemas present. Returns (structure, warnings).

    Raises SystemExit(1) on unregistered schemas or file/record kind mismatches.
    """
    # class_token -> (module_name, entry) for everything the registry knows about
    registered = {}
    for module_name, module in registry["modules"].items():
        for entry in module["entries"]:
            registered[entry["class"]] = (module_name, entry)

    unknown = sorted(t for t in present if t not in registered)
    mismatches = []
    warnings = []

    structure_modules = {}
    for module_name, module in registry["modules"].items():
        resolved_entries = []
        for entry in module["entries"]:
            token = entry["class"]
            if token not in present:
                warnings.append(
                    f"{module_name}/{entry.get('subfolder') or '(module)'}: "
                    f"schema HTAN.{token} not shipped in this version — skipped"
                )
                continue
            detected = detect_kind(present[token])
            if detected != entry["kind"]:
                mismatches.append(
                    f"HTAN.{token}: registry says '{entry['kind']}' but schema "
                    f"{'has' if detected == 'file' else 'lacks'} a FILENAME property "
                    f"(detected '{detected}')"
                )
            resolved_entries.append({
                "subfolder": entry.get("subfolder"),
                "kind": entry["kind"],
                "schema_name": entry["schema_name"],
                "class": token,
                "bind_to_module": bool(entry.get("bind_to_module", False)),
            })
        if resolved_entries:
            structure_modules[module_name] = {
                "umbrella": module.get("umbrella"),
                "entries": resolved_entries,
            }

    if unknown or mismatches:
        print("\n❌ Module structure generation failed.\n", file=sys.stderr)
        if unknown:
            print("Unregistered schema(s) present in the data model:", file=sys.stderr)
            for t in unknown:
                print(f"  - HTAN.{t}", file=sys.stderr)
            print(
                "\nAdd a row for each to htan2_synapse/module_registry.yml "
                "(placement, kind, schema_name, class) so it is created and bound "
                "instead of silently dropped.\n",
                file=sys.stderr,
            )
        if mismatches:
            print("File/record kind mismatch (data model changed?):", file=sys.stderr)
            for msg in mismatches:
                print(f"  - {msg}", file=sys.stderr)
            print("", file=sys.stderr)
        sys.exit(1)

    return structure_modules, warnings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schemas-dir", default="schemas",
                        help="Directory of downloaded data-model schemas (default: schemas)")
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY),
                        help="Path to module_registry.yml")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help="Path to write module_structure.yml")
    parser.add_argument("--schema-version", required=True,
                        help="Data-model schema version stamped into the artifact (e.g. v2.0.0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the resolved structure without writing the artifact")
    args = parser.parse_args()

    schemas_dir = Path(args.schemas_dir)
    if not schemas_dir.is_dir():
        print(f"❌ schemas dir not found: {schemas_dir}", file=sys.stderr)
        sys.exit(1)

    with open(args.registry) as f:
        registry = yaml.safe_load(f)

    present = scan_schema_files(schemas_dir)
    if not present:
        print(f"❌ no HTAN.*-schema.json files found in {schemas_dir}", file=sys.stderr)
        sys.exit(1)

    structure_modules, warnings = build_structure(registry, present)

    artifact = {"schema_version": args.schema_version, "modules": structure_modules}

    for w in warnings:
        print(f"⚠  {w}")
    n_modules = len(structure_modules)
    n_entries = sum(len(m["entries"]) for m in structure_modules.values())
    print(f"✓ Resolved {n_entries} schema(s) across {n_modules} module(s) "
          f"from {len(present)} downloaded schema file(s).")

    if args.dry_run:
        print("\n--- module_structure.yml (dry run, not written) ---")
        print(yaml.safe_dump(artifact, sort_keys=False, default_flow_style=False))
        return

    with open(args.output, "w") as f:
        yaml.safe_dump(artifact, f, sort_keys=False, default_flow_style=False)
    print(f"✓ Wrote {args.output}")


if __name__ == "__main__":
    main()
