#!/usr/bin/env python3
"""
Script to update wikis for all schema bindings from binding_results.json.

This script reads a binding_results.json file and updates wikis for all entities
that have fileviews, using the new short wiki format.

Usage:
    python scripts/manage/update_wikis.py --results-file path/to/binding_results.json
"""

import synapseclient
import argparse
import json
import os
import sys
import re


def extract_fileview_from_wiki(syn, entity_id: str) -> str:
    """Extract fileview ID from existing wiki if it exists."""
    try:
        wiki = syn.getWiki(entity_id)
        wiki_content = wiki.markdown
        
        # Look for fileview ID patterns in the wiki
        # Pattern 1: "Fileview ID: syn12345678"
        match = re.search(r'Fileview ID:\s*(syn\d+)', wiki_content, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Pattern 2: "syn12345678" in a link or text
        match = re.search(r'(syn\d{8,})', wiki_content)
        if match:
            return match.group(1)
            
    except synapseclient.core.exceptions.SynapseHTTPError as e:
        if e.response.status_code == 404:
            # No wiki exists
            return None
    except Exception:
        pass
    
    return None


def find_fileview_in_entity(syn, entity_id: str) -> str:
    """Find a fileview associated with an entity by checking its children."""
    try:
        # Method 1: Get all children and check their types
        children = list(syn.getChildren(entity_id))
        
        for child in children:
            try:
                # Get the full entity to check its type
                entity = syn.get(child.id, downloadFile=False)
                # Check if it's a fileview (EntityViewSchema)
                if hasattr(entity, 'concreteType'):
                    if 'EntityView' in entity.concreteType:
                        return entity.id
            except Exception as e:
                # Skip if we can't get the entity
                continue
        
        # Method 2: Try querying for EntityView types
        try:
            # Query for EntityView types that are children of this entity
            query = f"SELECT id, name FROM entity WHERE parentId='{entity_id}'"
            results = syn.tableQuery(query)
            df = results.asDataFrame()
            
            # Check each result to see if it's a fileview
            for _, row in df.iterrows():
                try:
                    entity = syn.get(row['id'], downloadFile=False)
                    if hasattr(entity, 'concreteType') and 'EntityView' in entity.concreteType:
                        return row['id']
                except Exception:
                    continue
        except Exception as e:
            # Query might fail if no table exists, that's okay
            pass
            
    except Exception as e:
        print(f"  Error finding fileview: {e}")
    
    return None


def update_wiki(syn, entity_id: str, fileview_id: str, schema_name: str):
    """Update the wiki on an entity with the new short format."""
    
    print(f"  Updating wiki for entity {entity_id} with fileview {fileview_id}")
    
    # Create wiki content with hyperlink
    fileview_url = f"https://www.synapse.org/#!Synapse:{fileview_id}"
    wiki_content = f"""The data is displayed in a fileview with columns extracted from the JSON schema:

Fileview ID: {fileview_id}

[View Fileview →]({fileview_url})

Schema Documentation: https://htan2-data-model.readthedocs.io/en/latest/index.html
"""
    
    try:
        # Check if wiki already exists
        try:
            existing_wiki = syn.getWiki(entity_id)
            # Update existing wiki
            existing_wiki.markdown = wiki_content
            syn.store(existing_wiki)
            print(f"  ✅ Updated existing wiki on entity {entity_id}")
        except synapseclient.core.exceptions.SynapseHTTPError as e:
            if e.response.status_code == 404:
                # Wiki doesn't exist, create new one
                syn.store(synapseclient.Wiki(
                    owner=entity_id,
                    title=f"{schema_name} Data View",
                    markdown=wiki_content
                ))
                print(f"  ✅ Created new wiki on entity {entity_id}")
            else:
                raise
        
    except Exception as e:
        print(f"  ❌ Error updating wiki: {e}")
        return False
    
    return True


def process_binding_results(syn, results_file: str, include_failed: bool = False):
    """Process binding results and update wikis."""
    
    print(f"Reading binding results from: {results_file}")
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    successful = results.get("successful", [])
    failed = results.get("failed", [])
    
    print(f"\nFound {len(successful)} successful bindings")
    if include_failed:
        print(f"Found {len(failed)} failed bindings (will attempt to update wikis if fileviews exist)")
    
    total_updated = 0
    total_not_found = 0
    total_errors = 0
    
    # Process successful bindings
    print("\n" + "="*80)
    print("Processing successful bindings...")
    print("="*80)
    
    for i, binding in enumerate(successful, 1):
        entity_id = binding["synapse_id"]
        schema_name = binding["schema"]
        project = binding["project"]
        
        print(f"\n[{i}/{len(successful)}] {schema_name} - {project} (synapse_id: {entity_id})")
        
        # Try to extract fileview ID from existing wiki first
        fileview_id = extract_fileview_from_wiki(syn, entity_id)
        
        # If not found in wiki, try to find it as a child entity
        if not fileview_id:
            fileview_id = find_fileview_in_entity(syn, entity_id)
        
        if fileview_id:
            if update_wiki(syn, entity_id, fileview_id, schema_name):
                total_updated += 1
            else:
                total_errors += 1
        else:
            print(f"  ⚠️  No fileview found for entity {entity_id}")
            total_not_found += 1
    
    # Process failed bindings if requested
    if include_failed and failed:
        print("\n" + "="*80)
        print("Processing failed bindings (attempting to update wikis if fileviews exist)...")
        print("="*80)
        
        for i, binding in enumerate(failed, 1):
            entity_id = binding["synapse_id"]
            schema_name = binding["schema"]
            project = binding.get("project", "Unknown")
            error = binding.get("error", "Unknown error")
            
            print(f"\n[{i}/{len(failed)}] {schema_name} - {project} (synapse_id: {entity_id})")
            print(f"  Error: {error}")
            
            # Try to extract fileview ID from existing wiki first
            fileview_id = extract_fileview_from_wiki(syn, entity_id)
            
            # If not found in wiki, try to find it as a child entity
            if not fileview_id:
                fileview_id = find_fileview_in_entity(syn, entity_id)
            
            if fileview_id:
                print(f"  Found fileview: {fileview_id}")
                if update_wiki(syn, entity_id, fileview_id, schema_name):
                    total_updated += 1
                else:
                    total_errors += 1
            else:
                print(f"  ⚠️  No fileview found for entity {entity_id}")
                total_not_found += 1
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✅ Successfully updated: {total_updated}")
    print(f"⚠️  No fileview found: {total_not_found}")
    print(f"❌ Errors: {total_errors}")
    print("="*80)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Update wikis for all schema bindings from binding_results.json"
    )
    parser.add_argument(
        "--results-file",
        type=str,
        required=True,
        help="Path to binding_results.json file"
    )
    parser.add_argument(
        "--include-failed",
        action="store_true",
        help="Also attempt to update wikis for failed bindings (if fileviews exist)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without actually updating"
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.results_file):
        print(f"Error: Results file not found: {args.results_file}")
        sys.exit(1)
    
    if args.dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No changes will be made")
        print("=" * 80)
        print()
        
        with open(args.results_file, 'r') as f:
            results = json.load(f)
        
        successful = results.get("successful", [])
        failed = results.get("failed", [])
        
        print(f"Would process {len(successful)} successful bindings")
        if args.include_failed:
            print(f"Would process {len(failed)} failed bindings")
        
        print("\nRun without --dry-run to actually update wikis.")
        return
    
    # Configure Synapse client
    syn = synapseclient.Synapse()
    syn.repoEndpoint = 'https://repo-prod.prod.sagebase.org/repo/v1'
    syn.authEndpoint = 'https://repo-prod.prod.sagebase.org/auth/v1'
    syn.fileHandleEndpoint = 'https://repo-prod.prod.sagebase.org/file/v1'
    syn.portalEndpoint = 'https://repo-prod.prod.sagebase.org/portal/v1'
    
    print("Logging in to Synapse...")
    
    # Use credentials from environment variables if available
    username = os.environ.get('SYNAPSE_USERNAME')
    auth_token = os.environ.get('SYNAPSE_PAT')
    
    if username and auth_token:
        print(f"Using username and auth token for authentication")
        syn.login(username, authToken=auth_token)
    else:
        print("No credentials found in environment, attempting default login")
        syn.login()
    
    print("✅ Successfully logged in to Synapse\n")
    
    # Process results
    process_binding_results(syn, args.results_file, args.include_failed)


if __name__ == "__main__":
    main()

