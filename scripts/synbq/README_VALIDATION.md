# Fileview Validation Scripts

## validate_single_fileview.py

Validate a single fileview.

```bash
python scripts/synbq/validate_single_fileview.py --folder-id syn72644024 --fileview-id syn72644059
```

**Output:** `validation_status_{FOLDER_ID}.csv` with columns:
- `file_id`, `file_name`, `is_valid`, `validation_error_message`, `all_validation_messages`, `validated_on`

## validate_fileviews.py

Validate all fileviews from `schema_binding_config.yml`.

```bash
# Validate all fileviews (outputs to current directory)
python scripts/synbq/validate_fileviews.py

# Custom output directory
python scripts/synbq/validate_fileviews.py --output-dir validation_results

# Custom config file
python scripts/synbq/validate_fileviews.py --config-file schema_binding_v8.yml
```

**Output:** One CSV per fileview: `validation_status_{FILEVIEW_ID}.csv`

## update_fileview_ids.py

Extract fileview IDs from wikis and update config.

```bash
python scripts/manage/update_fileview_ids.py --config-file schema_binding_config.yml
```

