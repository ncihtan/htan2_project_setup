#!/usr/bin/env python3
"""
Extract fileview IDs from wikis and update the results JSON file.
"""

import json
import synapseclient
import os
import sys
import re
from typing import Dict, List


def extract_fileview_from_wiki(syn, entity_id: str) -> str:
    """Extract fileview ID from existing wiki if it exists."""
    try:
        wiki = syn.getWiki(entity_id)
        wiki_content = wiki.markdown
        
        # Look for fileview ID patterns in the wiki
        # Pattern: "Fileview ID: syn12345678"
        match = re.search(r'Fileview ID:\s*(syn\d+)', wiki_content, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Pattern: "syn12345678" in a link or text
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


def update_results_with_fileviews(results_file: str):
    """Update results file with fileview IDs extracted from wikis."""
    
    print(f"Loading results from: {results_file}")
    with open(results_file, 'r') as f:
        results = json.load(f)
    
    # Configure Synapse
    syn = synapseclient.Synapse()
    syn.repoEndpoint = 'https://repo-prod.prod.sagebase.org/repo/v1'
    syn.authEndpoint = 'https://repo-prod.prod.sagebase.org/auth/v1'
    syn.fileHandleEndpoint = 'https://repo-prod.prod.sagebase.org/file/v1'
    syn.portalEndpoint = 'https://repo-prod.prod.sagebase.org/portal/v1'
    
    print("Logging in to Synapse...")
    username = os.environ.get('SYNAPSE_USERNAME')
    auth_token = os.environ.get('SYNAPSE_PAT')
    
    if username and auth_token:
        syn.login(username, authToken=auth_token)
    else:
        syn.login()
    
    print("✅ Successfully logged in\n")
    
    # Update successful bindings with fileview IDs
    successful = results.get("successful", [])
    print(f"Extracting fileview IDs for {len(successful)} successful bindings...\n")
    
    updated_count = 0
    for i, binding in enumerate(successful, 1):
        entity_id = binding["synapse_id"]
        project = binding["project"]
        
        # Skip if already has fileview_id
        if "fileview_id" in binding:
            continue
        
        print(f"[{i}/{len(successful)}] {project} - {entity_id}")
        fileview_id = extract_fileview_from_wiki(syn, entity_id)
        
        if fileview_id:
            binding["fileview_id"] = fileview_id
            updated_count += 1
            print(f"  ✅ Found fileview: {fileview_id}")
        else:
            print(f"  ⚠️  No fileview found in wiki")
    
    # Save updated results
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Updated {updated_count} bindings with fileview IDs")
    print(f"Results saved to: {results_file}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract fileview IDs from wikis and update results JSON"
    )
    parser.add_argument(
        '--results-file',
        required=True,
        help='Path to results JSON file to update'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.results_file):
        print(f"Error: Results file not found: {args.results_file}")
        sys.exit(1)
    
    update_results_with_fileviews(args.results_file)


if __name__ == "__main__":
    main()

