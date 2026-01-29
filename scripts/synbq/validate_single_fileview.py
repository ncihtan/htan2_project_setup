#!/usr/bin/env python3
"""
Get validation status for files in a single EntityView using REST API endpoints.

Usage:
    python scripts/manage/validate_single_fileview.py --folder-id syn72644024 --fileview-id syn72644059
"""

import synapseclient
import json
import pandas as pd
import argparse


def main():
    parser = argparse.ArgumentParser(description="Validate files in a single fileview")
    parser.add_argument("--folder-id", required=True, help="Synapse ID of the folder (for validation endpoint)")
    parser.add_argument("--fileview-id", required=True, help="Synapse ID of the fileview (for querying files)")
    parser.add_argument("--output", default=None, help="Output CSV filename (default: validation_status_{FOLDER_ID}.csv)")
    args = parser.parse_args()
    
    # Login to Synapse
    syn = synapseclient.Synapse()
    syn.repoEndpoint = 'https://repo-prod.prod.sagebase.org/repo/v1'
    syn.authEndpoint = 'https://repo-prod.prod.sagebase.org/auth/v1'
    syn.fileHandleEndpoint = 'https://repo-prod.prod.sagebase.org/file/v1'
    syn.portalEndpoint = 'https://repo-prod.prod.sagebase.org/portal/v1'
    syn.login()
    
    FOLDER_ID = args.folder_id
    ENTITY_VIEW_ID = args.fileview_id
    
    # Get invalid files
    invalid_files = {f['objectId']: f for f in syn.restPOST(
        f"/entity/{FOLDER_ID}/schema/validation/invalid", 
        body=json.dumps({})
    ).get('page', [])}
    
    # Query fileview
    df = syn.tableQuery(f"SELECT id, name FROM {ENTITY_VIEW_ID}").asDataFrame()
    
    # Add validation columns
    df['is_valid'] = ~df['id'].isin(invalid_files)
    df['validation_error_message'] = df['id'].map(lambda x: invalid_files.get(x, {}).get('validationErrorMessage', ''))
    df['all_validation_messages'] = df['id'].map(lambda x: '; '.join(invalid_files.get(x, {}).get('allValidationMessages', [])))
    df['validated_on'] = df['id'].map(lambda x: invalid_files.get(x, {}).get('validatedOn', ''))
    
    # Rename columns and save
    df = df.rename(columns={'id': 'file_id', 'name': 'file_name'})
    
    output_file = args.output or f"validation_status_{FOLDER_ID}.csv"
    df.to_csv(output_file, index=False)
    
    # Print summary
    print(f"Total: {len(df)}, Valid: {df['is_valid'].sum()}, Invalid: {(~df['is_valid']).sum()}")
    
    if (~df['is_valid']).any():
        print("\nInvalid files:")
        for _, r in df[~df['is_valid']].iterrows():
            print(f"  {r['file_name']}: {r['validation_error_message']}")
    
    print(f"\nExported to: {output_file}")


if __name__ == "__main__":
    main()

