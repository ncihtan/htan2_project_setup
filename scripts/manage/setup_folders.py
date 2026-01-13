#!/usr/bin/env python3
"""
Master script for setting up folders for a new version.
This script orchestrates the complete folder setup process:
1. Creates folders with proper structure
2. Sets access permissions
3. Updates schema binding file with real IDs
4. Merges into main config (staging only)

Usage:
    python scripts/manage/setup_folders.py --version 8
"""

import argparse
import sys
import subprocess
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n{'='*80}")
    print(f"{description}")
    print(f"{'='*80}")
    print(f"Running: {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"\n❌ Error: {description} failed")
        sys.exit(1)
    
    print(f"✓ {description} completed successfully")


def main():
    parser = argparse.ArgumentParser(
        description="Complete folder setup for a new version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script performs the complete folder setup workflow:
1. Creates folders (ingest, staging, release) with all modules
2. Sets access permissions for all folders
3. Updates schema binding file with real Synapse IDs
4. Merges staging folder bindings into schema_binding_config.yml

Example:
    python scripts/manage/setup_folders.py --version 8
        """
    )
    parser.add_argument(
        "--version",
        type=str,
        required=True,
        help="Version number (e.g., 8 or v8). Will create v8_ingest, v8_staging, v8_release"
    )
    parser.add_argument(
        "--skip-permissions",
        action="store_true",
        help="Skip permission setting step"
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="Skip merging into schema_binding_config.yml"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - show what would be done without making changes"
    )
    
    args = parser.parse_args()
    
    # Normalize version
    version = args.version
    if not version.startswith('v'):
        try:
            int(version)
            version = f"v{version}"
        except ValueError:
            # If version is not purely numeric, assume it's already in the desired format
            pass
    
    print("="*80)
    print("HTAN2 Folder Setup - Complete Workflow")
    print("="*80)
    print(f"Version: {version}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("="*80)
    
    # Step 1: Create folders
    print("\n📁 Step 1: Creating folders...")
    create_cmd = [
        "python", "scripts/manage/create_project_folders.py",
        "--version", version
    ]
    if args.dry_run:
        create_cmd.append("--dry-run")
    
    run_command(create_cmd, "Folder Creation")
    
    # Step 2: Set permissions
    if not args.skip_permissions:
        print("\n🔐 Step 2: Setting permissions...")
        for folder_type in ["ingest", "staging", "release"]:
            perm_cmd = [
                "python", "scripts/manage/update_folder_permissions.py",
                "--version", version,
                "--folder-type", f"{version}_{folder_type}"
            ]
            if args.dry_run:
                perm_cmd.append("--dry-run")
            
            run_command(perm_cmd, f"Setting permissions for {version}_{folder_type}")
    
    # Step 3: Update schema bindings with real IDs for all folder types
    print("\n🔗 Step 3: Updating schema bindings with real IDs...")
    for folder_type in ["ingest", "staging", "release"]:
        update_cmd = [
            "python", "scripts/manage/update_schema_bindings.py",
            "--version", version,
            "--folder-type", folder_type
        ]
        if args.dry_run:
            update_cmd.append("--dry-run")
        
        run_command(update_cmd, f"Schema Binding Update for {version}_{folder_type}")
    
    # Step 4: Merge into main config (all folder types)
    if not args.skip_merge:
        print("\n📋 Step 4: Merging into schema_binding_config.yml...")
        # Merge all folder types (ingest, staging, release)
        merge_cmd = [
            "python", "merge_schema_bindings.py",
            "--schema-binding-file", f"schema_binding_{version}.yml"
            # No folder-type-filter - merge all folder types
        ]
        if args.dry_run:
            merge_cmd.append("--dry-run")
        
        run_command(merge_cmd, "Config Merge (all folder types)")
    
    print("\n" + "="*80)
    print("✅ COMPLETE - Folder Setup Finished")
    print("="*80)
    print(f"\nGenerated files:")
    print(f"  - folder_structure_{version}.yml")
    print(f"  - schema_binding_{version}.yml")
    print(f"  - schema_binding_config.yml (updated with {version}_staging bindings)")
    print(f"\nNext steps:")
    print(f"  1. Review the generated files")
    print(f"  2. When schemas are released, the GitHub Action will automatically bind them")
    print(f"  3. Or manually trigger: Actions → 'Bind Schemas to HTAN2 Projects'")
    print("="*80)


if __name__ == "__main__":
    main()

