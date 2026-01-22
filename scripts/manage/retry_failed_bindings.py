#!/usr/bin/env python3
"""
Script to retry failed schema bindings from binding_results.json files.

This script extracts failed bindings from multiple binding_results.json files
and retries them with a longer timeout.

Usage:
    python scripts/manage/retry_failed_bindings.py --results-files file1.json file2.json file3.json
"""

import json
import subprocess
import argparse
import os
import sys
from typing import List, Dict


def load_failed_bindings(results_files: List[str]) -> List[Dict]:
    """Load all failed bindings from the results files."""
    failed_bindings = []
    
    for results_file in results_files:
        if not os.path.exists(results_file):
            print(f"⚠️  Warning: {results_file} not found, skipping")
            continue
            
        with open(results_file, 'r') as f:
            results = json.load(f)
        
        failed = results.get("failed", [])
        print(f"Found {len(failed)} failed bindings in {results_file}")
        failed_bindings.extend(failed)
    
    # Remove duplicates (same schema + project + synapse_id)
    seen = set()
    unique_failed = []
    for binding in failed_bindings:
        key = (binding["schema"], binding["project"], binding["synapse_id"])
        if key not in seen:
            seen.add(key)
            unique_failed.append(binding)
    
    return unique_failed


def retry_binding(binding: Dict, organization_name: str = "HTAN2Organization", timeout: int = 1800):
    """Retry a single binding with a longer timeout."""
    schema_name = binding["schema"]
    synapse_id = binding["synapse_id"]
    project = binding["project"]
    subfolder = binding.get("subfolder", "N/A")
    schema_file = binding.get("schema_file", None)
    
    # If schema_file is not in the binding, construct it
    if not schema_file:
        if schema_name == "scRNA_seqLevel3_4":
            schema_file = "schemas/HTAN.scRNALevel3_4-v1.0.0-schema.json"
        else:
            print(f"⚠️  Warning: Cannot determine schema file for {schema_name}")
            return False
    
    print(f"\n{'='*80}")
    print(f"Retrying: {schema_name} - {project}")
    print(f"  Synapse ID: {synapse_id}")
    print(f"  Subfolder: {subfolder}")
    print(f"  Schema file: {schema_file}")
    print(f"  Timeout: {timeout // 60} minutes")
    print(f"{'='*80}")
    
    # Check if schema file exists
    if not os.path.exists(schema_file):
        print(f"❌ Schema file not found: {schema_file}")
        return False
    
    # Run the binding script
    cmd = [
        'python', 'scripts/synapse_json_schema_bind.py',
        '-p', schema_file,
        '-t', synapse_id,
        '-n', organization_name,
        '--create_fileview'  # Create fileview this time
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout
        )
        print(f"✅ Successfully bound {schema_name} to {project}")
        # Print first few lines of output
        output_lines = result.stdout.strip().split('\n')
        for line in output_lines[:10]:
            if line.strip():
                print(f"  {line}")
        return True
    except subprocess.TimeoutExpired:
        timeout_minutes = timeout // 60
        print(f"❌ Binding timed out after {timeout_minutes} minutes")
        print(f"   This schema may be too large or there may be a Synapse API issue")
        return False
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip() if e.stderr else str(e)
        print(f"❌ Failed: {error_msg}")
        # Print first few lines of stderr
        if e.stderr:
            error_lines = e.stderr.strip().split('\n')
            for line in error_lines[:10]:
                if line.strip():
                    print(f"  {line}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Retry failed schema bindings from binding_results.json files"
    )
    parser.add_argument(
        '--results-files',
        nargs='+',
        required=True,
        help='Paths to binding_results.json files'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=1800,  # 30 minutes
        help='Timeout in seconds (default: 1800 = 30 minutes)'
    )
    parser.add_argument(
        '--organization-name',
        default=os.environ.get('ORGANIZATION_NAME', 'HTAN2Organization'),
        help='Organization name (default: HTAN2Organization or ORGANIZATION_NAME env var)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be retried without actually retrying'
    )
    
    args = parser.parse_args()
    
    # Load failed bindings
    print("="*80)
    print("Loading failed bindings...")
    print("="*80)
    failed_bindings = load_failed_bindings(args.results_files)
    
    if not failed_bindings:
        print("\n✅ No failed bindings found!")
        return
    
    print(f"\nFound {len(failed_bindings)} unique failed binding(s) to retry")
    
    if args.dry_run:
        print("\n" + "="*80)
        print("DRY RUN MODE - Would retry the following bindings:")
        print("="*80)
        for i, binding in enumerate(failed_bindings, 1):
            print(f"\n{i}. {binding['schema']} - {binding['project']}")
            print(f"   Synapse ID: {binding['synapse_id']}")
            print(f"   Subfolder: {binding.get('subfolder', 'N/A')}")
            print(f"   Error: {binding.get('error', 'Unknown')}")
        print("\nRun without --dry-run to actually retry bindings.")
        return
    
    # Retry each binding
    print("\n" + "="*80)
    print("RETRYING FAILED BINDINGS")
    print("="*80)
    
    results = {
        'successful': [],
        'failed': []
    }
    
    for i, binding in enumerate(failed_bindings, 1):
        print(f"\n[{i}/{len(failed_bindings)}]")
        success = retry_binding(
            binding,
            organization_name=args.organization_name,
            timeout=args.timeout
        )
        
        if success:
            results['successful'].append(binding)
        else:
            results['failed'].append(binding)
    
    # Summary
    print("\n" + "="*80)
    print("RETRY SUMMARY")
    print("="*80)
    print(f"✅ Successfully retried: {len(results['successful'])}")
    print(f"❌ Still failed: {len(results['failed'])}")
    print("="*80)
    
    if results['failed']:
        print("\n❌ BINDINGS THAT STILL FAILED:")
        print("-" * 80)
        for failure in results['failed']:
            print(f"  {failure['schema']} - {failure['project']} ({failure['synapse_id']})")
    
    # Save results
    output_file = 'retry_binding_results.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {output_file}")
    
    if results['failed']:
        print(f"\n⚠️  Warning: {len(results['failed'])} binding(s) still failed")
        print("You may need to investigate these further or increase the timeout")
        sys.exit(1)


if __name__ == "__main__":
    main()

