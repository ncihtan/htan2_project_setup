#!/usr/bin/env python3
"""
Validate files in all fileviews from schema_binding_config.yml.
Generates one CSV per fileview with validation status.

Usage:
    python scripts/manage/validate_fileviews.py [--config-file CONFIG] [--output-dir DIR]
"""

import synapseclient
import yaml
import argparse
import json
import pandas as pd
from pathlib import Path


def get_validation_status(syn, folder_id: str):
    """Get validation status for files in a folder."""
    try:
        response = syn.restPOST(f"/entity/{folder_id}/schema/validation/invalid", body=json.dumps({}))
        return {f['objectId']: f for f in response.get('page', [])}
    except Exception as e:
        print(f"  ⚠️  Error getting validation for {folder_id}: {e}")
        return {}


def validate_fileview(syn, folder_id: str, fileview_id: str, output_dir: Path):
    """Validate files in a fileview and save to CSV."""
    print(f"  Validating {fileview_id} (folder: {folder_id})...")
    
    try:
        invalid_files = get_validation_status(syn, folder_id)
        
        try:
            df = syn.tableQuery(f"SELECT id, name FROM {fileview_id}").asDataFrame()
        except Exception as e:
            print(f"  ⚠️  Error querying {fileview_id}: {e}")
            return None
        
        if df.empty:
            df = pd.DataFrame(columns=['file_id', 'file_name', 'is_valid', 'validation_error_message', 
                                      'all_validation_messages', 'validated_on'])
        else:
            df['is_valid'] = ~df['id'].isin(invalid_files)
            df['validation_error_message'] = df['id'].map(lambda x: invalid_files.get(x, {}).get('validationErrorMessage', ''))
            df['all_validation_messages'] = df['id'].map(lambda x: '; '.join(invalid_files.get(x, {}).get('allValidationMessages', [])))
            df['validated_on'] = df['id'].map(lambda x: invalid_files.get(x, {}).get('validatedOn', ''))
            df = df.rename(columns={'id': 'file_id', 'name': 'file_name'})
        
        output_file = output_dir / f"validation_status_{fileview_id}.csv"
        df.to_csv(output_file, index=False)
        
        valid = df['is_valid'].sum() if 'is_valid' in df.columns else 0
        invalid = len(df) - valid
        print(f"    ✅ Total: {len(df)}, Valid: {valid}, Invalid: {invalid}")
        
        if invalid > 0:
            for _, row in df[~df['is_valid']].head(3).iterrows():
                print(f"      - {row['file_name']}: {row.get('validation_error_message', 'N/A')}")
            if invalid > 3:
                print(f"      ... and {invalid - 3} more")
        
        return str(output_file)
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None


def process_projects(syn, projects, schema_name, output_dir, results):
    """Process a list of projects and validate their fileviews."""
    for project in projects:
        folder_id = project.get('synapse_id')
        fileview_id = project.get('fileview_id')
        project_name = project.get('name', 'Unknown')
        subfolder = project.get('subfolder', 'N/A')
        
        if not folder_id or not fileview_id:
            print(f"  ⏭️  Skipping {project_name}: missing folder_id or fileview_id")
            results['skipped'].append({'schema': schema_name, 'project': project_name, 'subfolder': subfolder})
            continue
        
        print(f"  {project_name} ({subfolder})")
        output_file = validate_fileview(syn, folder_id, fileview_id, output_dir)
        
        result = {'schema': schema_name, 'project': project_name, 'subfolder': subfolder, 
                 'folder_id': folder_id, 'fileview_id': fileview_id}
        if output_file:
            result['output_file'] = output_file
            results['successful'].append(result)
        else:
            results['failed'].append(result)


def main():
    parser = argparse.ArgumentParser(description="Validate files in all fileviews from config")
    parser.add_argument("--config-file", default="schema_binding_config.yml", help="Config YAML file")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: current dir)")
    args = parser.parse_args()
    
    if not Path(args.config_file).exists():
        print(f"Error: Config file not found: {args.config_file}")
        return
    
    output_dir = Path(args.output_dir) if args.output_dir else Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("="*80)
    print("Validate All Fileviews")
    print(f"Config: {args.config_file}")
    print(f"Output: {output_dir}")
    print("="*80)
    print()
    
    # Login
    syn = synapseclient.Synapse()
    syn.repoEndpoint = 'https://repo-prod.prod.sagebase.org/repo/v1'
    syn.authEndpoint = 'https://repo-prod.prod.sagebase.org/auth/v1'
    syn.fileHandleEndpoint = 'https://repo-prod.prod.sagebase.org/file/v1'
    syn.portalEndpoint = 'https://repo-prod.prod.sagebase.org/portal/v1'
    syn.login()
    print("✅ Logged in\n")
    
    # Load config
    with open(args.config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    schema_bindings = config.get('schema_bindings', {})
    results = {'successful': [], 'failed': [], 'skipped': []}
    
    # Process all schemas
    for schema_type in ['file_based', 'record_based']:
        print(f"\n{'='*80}\nProcessing {schema_type} schemas...\n{'='*80}")
        schemas = schema_bindings.get(schema_type, {})
        
        for schema_name, schema_config in schemas.items():
            projects = schema_config.get('projects', [])
            print(f"\n{schema_name}: {len(projects)} project(s)")
            process_projects(syn, projects, schema_name, output_dir, results)
    
    # Summary
    print(f"\n{'='*80}\nSUMMARY\n{'='*80}")
    print(f"✅ Successful: {len(results['successful'])}")
    print(f"❌ Failed: {len(results['failed'])}")
    print(f"⏭️  Skipped: {len(results['skipped'])}")
    print(f"\n✅ CSV files saved to: {output_dir}")


if __name__ == "__main__":
    main()
