#!/usr/bin/env python3
"""
Script to bind all failed scRNA_seqLevel3_4 bindings using the new schema name scRNALevel34.

This script reads failed bindings from binding_results.json files and binds them
using the corrected schema name (scRNALevel34 instead of scRNALevel3_4).
"""

import json
import synapseclient
import os
import sys
import argparse
from typing import List, Dict


def bind_schema_to_entity(syn, entity_id: str, schema_uri: str) -> bool:
    """Bind a schema to an entity using REST API."""
    try:
        request_body = {
            'entityId': entity_id,
            'schema$id': schema_uri
        }
        
        syn.restPUT(
            f'/entity/{entity_id}/schema/binding',
            body=json.dumps(request_body)
        )
        return True
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def create_fileview_and_wiki(syn, entity_id: str, schema_name: str):
    """Create fileview and wiki for an entity. Returns fileview_id if successful."""
    try:
        # Load schema
        schema_file = 'schemas/HTAN.scRNALevel3_4-v1.0.0-schema.json'
        with open(schema_file, 'r') as f:
            schema_json = json.load(f)
        
        # Import functions
        import sys
        sys.path.insert(0, 'scripts')
        from synapse_json_schema_bind import create_fileview_from_schema, create_wiki_with_fileview_id
        
        # Create fileview
        fileview_id = create_fileview_from_schema(syn, schema_json, entity_id, schema_name)
        
        if fileview_id:
            # Create wiki
            create_wiki_with_fileview_id(syn, entity_id, fileview_id, schema_name)
            return fileview_id
        return None
    except Exception as e:
        print(f"  ❌ Error creating fileview/wiki: {e}")
        return None


def process_failed_bindings(results_files: List[str], create_fileview: bool = True):
    """Process all failed bindings from the results files."""
    
    # Load failed bindings
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
    
    # Filter to only scRNA_seqLevel3_4
    scRNA_failed = [b for b in failed_bindings if b.get("schema") == "scRNA_seqLevel3_4"]
    
    # Remove duplicates
    seen = set()
    unique_failed = []
    for binding in scRNA_failed:
        key = (binding["schema"], binding["project"], binding["synapse_id"])
        if key not in seen:
            seen.add(key)
            unique_failed.append(binding)
    
    print(f"\nFound {len(unique_failed)} unique scRNA_seqLevel3_4 failed bindings to retry")
    
    # Configure Synapse
    syn = synapseclient.Synapse()
    syn.repoEndpoint = 'https://repo-prod.prod.sagebase.org/repo/v1'
    syn.authEndpoint = 'https://repo-prod.prod.sagebase.org/auth/v1'
    syn.fileHandleEndpoint = 'https://repo-prod.prod.sagebase.org/file/v1'
    syn.portalEndpoint = 'https://repo-prod.prod.sagebase.org/portal/v1'
    
    print("\nLogging in to Synapse...")
    username = os.environ.get('SYNAPSE_USERNAME')
    auth_token = os.environ.get('SYNAPSE_PAT')
    
    if username and auth_token:
        syn.login(username, authToken=auth_token)
    else:
        syn.login()
    
    print("✅ Successfully logged in\n")
    
    # Use the new schema name
    schema_uri = 'HTAN2Organization-scRNALevel34-1.0.0'
    schema_name = 'scRNALevel34'
    
    # Verify schema exists
    try:
        syn.restGET(f'/schema/type/registered/{schema_uri}')
        print(f"✅ Verified schema exists: {schema_uri}\n")
    except Exception as e:
        print(f"❌ Error: Schema {schema_uri} not found: {e}")
        return
    
    # Process each binding
    results = {
        'successful': [],
        'failed': []
    }
    
    print("="*80)
    print("PROCESSING FAILED BINDINGS")
    print("="*80)
    
    for i, binding in enumerate(unique_failed, 1):
        entity_id = binding["synapse_id"]
        project = binding["project"]
        subfolder = binding.get("subfolder", "N/A")
        
        print(f"\n[{i}/{len(unique_failed)}] {project} - {subfolder}")
        print(f"  Entity ID: {entity_id}")
        
        # Bind schema
        print(f"  Binding schema...")
        if bind_schema_to_entity(syn, entity_id, schema_uri):
            print(f"  ✅ Schema bound successfully")
            
            fileview_id = None
            # Create fileview and wiki if requested
            if create_fileview:
                print(f"  Creating fileview and wiki...")
                fileview_id = create_fileview_and_wiki(syn, entity_id, schema_name)
                if fileview_id:
                    print(f"  ✅ Fileview and wiki created (Fileview ID: {fileview_id})")
                else:
                    print(f"  ⚠️  Schema bound but fileview/wiki creation failed")
            
            # Add fileview_id to the result
            success_record = binding.copy()
            if fileview_id:
                success_record['fileview_id'] = fileview_id
            results['successful'].append(success_record)
        else:
            print(f"  ❌ Binding failed")
            results['failed'].append(binding)
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✅ Successfully bound: {len(results['successful'])}")
    print(f"❌ Failed: {len(results['failed'])}")
    print("="*80)
    
    if results['failed']:
        print("\n❌ BINDINGS THAT FAILED:")
        for failure in results['failed']:
            print(f"  {failure['project']} - {failure['synapse_id']}")
    
    # Save results
    output_file = 'scRNA34_binding_results.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {output_file}")
    
    if results['failed']:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Bind all failed scRNA_seqLevel3_4 bindings using the new schema name"
    )
    parser.add_argument(
        '--results-files',
        nargs='+',
        required=True,
        help='Paths to binding_results.json files'
    )
    parser.add_argument(
        '--no-fileview',
        action='store_true',
        help='Skip fileview and wiki creation (just bind schemas)'
    )
    
    args = parser.parse_args()
    
    process_failed_bindings(args.results_files, create_fileview=not args.no_fileview)


if __name__ == "__main__":
    main()

