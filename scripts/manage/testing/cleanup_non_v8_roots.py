#!/usr/bin/env python3
"""
Delete all top-level entities in a project that are *not* v8_* folders.

Specifically, for a given project, this keeps only:
  - v8_ingest
  - v8_staging
  - v8_release
and deletes any other top-level folders/files.

Usage:
    python scripts/manage/cleanup_non_v8_roots.py --project-id syn63834783
"""

import argparse
from typing import Set

import synapseclient


KEEP_NAMES: Set[str] = {"v8_ingest", "v8_staging", "v8_release"}


def cleanup_project_roots(syn: synapseclient.Synapse, project_id: str) -> None:
    """Delete all top-level entities in a project except the v8_* roots."""
    print(f"Project: {project_id}")
    print(f"Keeping only: {', '.join(sorted(KEEP_NAMES))}")
    print("=" * 80)

    kept = 0
    deleted = 0

    # Use high-level iterator (handles paging) and then delete via syn.delete
    for child in syn.getChildren(project_id):
        name = child.get("name")
        eid = child.get("id")
        if name in KEEP_NAMES:
            print(f"  ✅ Keeping: {name} ({eid})")
            kept += 1
            continue

        try:
            print(f"  🗑  Deleting: {name} ({eid})")
            syn.delete(eid)
            deleted += 1
        except Exception as e:
            print(f"  ❌ Error deleting {eid}: {e}")

    print("\nSummary")
    print("=" * 80)
    print(f"Kept:    {kept}")
    print(f"Deleted: {deleted}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete all non-v8_* top-level entities in a project."
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="Synapse project ID (e.g., syn63834783)",
    )
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    syn.login()

    cleanup_project_roots(syn, args.project_id)


if __name__ == "__main__":
    main()


