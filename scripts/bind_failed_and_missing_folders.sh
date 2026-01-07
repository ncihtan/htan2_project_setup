#!/bin/bash
# Script to bind schemas for failed bindings and missing folder types (release, ingest)

set -e

VERSION="v8"
SCHEMA_VERSION="v1.0.0"
FAILED_SCHEMAS="scRNA_seqLevel3_4 Biospecimen"

echo "=========================================="
echo "Binding Schemas for Failed + Missing Folders"
echo "=========================================="
echo "Version: $VERSION"
echo "Schema Version: $SCHEMA_VERSION"
echo "Failed/Skipped Schemas: $FAILED_SCHEMAS"
echo "Folder Types: ${VERSION}_release, ${VERSION}_ingest"
echo "=========================================="
echo ""

# Step 1: Generate schema bindings for release and ingest folders
echo "Step 1: Generating schema bindings for release and ingest folders..."
python scripts/manage/update_schema_bindings.py \
    --version "$VERSION" \
    --folder-type release ingest

# Step 2: Merge release and ingest bindings into config
echo ""
echo "Step 2: Merging release and ingest bindings into schema_binding_config.yml..."
python merge_schema_bindings.py \
    --schema-binding-file "schema_binding_${VERSION}.yml"

# Step 3: Download schemas (if needed)
if [ ! -d "schemas" ] || [ -z "$(ls -A schemas)" ]; then
    echo ""
    echo "Step 3: Downloading schemas..."
    mkdir -p schemas
    
    REPO="ncihtan/htan2-data-model"
    SCHEMA_FILES=$(curl -s "https://api.github.com/repos/$REPO/contents/JSON_Schemas/$SCHEMA_VERSION" | \
      jq -r '.[].name | select(endswith(".json"))' | \
      sort)
    
    for schema_file in $SCHEMA_FILES; do
      echo "Downloading $schema_file..."
      curl -L -H "Accept: application/vnd.github.v3.raw" \
        "https://api.github.com/repos/$REPO/contents/JSON_Schemas/$SCHEMA_VERSION/$schema_file" \
        -o "schemas/$schema_file"
    done
else
    echo ""
    echo "Step 3: Schemas directory exists, skipping download..."
fi

# Step 4: Bind only the failed/skipped schemas for release and ingest folders
echo ""
echo "Step 4: Binding $FAILED_SCHEMAS to ${VERSION}_release and ${VERSION}_ingest folders..."
python scripts/bind_schemas_workflow.py \
    --schema-filter $FAILED_SCHEMAS \
    --folder-type-filter "${VERSION}_release" "${VERSION}_ingest" \
    --schema-version "$SCHEMA_VERSION"

echo ""
echo "=========================================="
echo "Complete!"
echo "=========================================="
echo "Results saved to: binding_results.json"

