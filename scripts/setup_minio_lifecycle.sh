#!/bin/bash

# What: Apply MinIO S3 lifecycle rules for automatic artifact expiration.
#
# Inputs:
# - MINIO_ENDPOINT: MinIO endpoint URL (e.g., http://localhost:9000, default from env or localhost)
# - MINIO_ACCESS_KEY: MinIO access key (from env or required as arg)
# - MINIO_SECRET_KEY: MinIO secret key (from env or required as arg)
# - BUCKET_NAME: Target bucket name (default: kionas)
# - POLICY_FILE: Path to lifecycle policy JSON (default: ./setup_minio_lifecycle.json)
# - DRY_RUN: If "true", shows what would be applied without making changes (default: false)
#
# Output:
# - Applies lifecycle rules to MinIO bucket
# - Prints status and error messages
#
# Details:
# - Uses AWS CLI v2 with S3-compatible endpoint
# - Lifecycle rules expire objects by prefix and age
# - Rules are defense-in-depth alongside janitor cleanup task
#
# Usage:
#   ./setup_minio_lifecycle.sh
#   ./setup_minio_lifecycle.sh --dry-run
#   ./setup_minio_lifecycle.sh --endpoint http://minio:9000 --bucket my-bucket
#   MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin ./setup_minio_lifecycle.sh

set -e

# Defaults
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
BUCKET_NAME="${BUCKET_NAME:-kionas}"
POLICY_FILE="${POLICY_FILE:-./setup_minio_lifecycle.json}"
DRY_RUN=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --endpoint)
            MINIO_ENDPOINT="$2"
            shift 2
            ;;
        --bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        --policy)
            POLICY_FILE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate prerequisites
if [[ ! -f "$POLICY_FILE" ]]; then
    echo "ERROR: Lifecycle policy file not found: $POLICY_FILE"
    exit 1
fi

# Get credentials from environment
if [[ -z "$MINIO_ACCESS_KEY" ]]; then
    echo "ERROR: MINIO_ACCESS_KEY not set in environment"
    exit 1
fi

if [[ -z "$MINIO_SECRET_KEY" ]]; then
    echo "ERROR: MINIO_SECRET_KEY not set in environment"
    exit 1
fi

# Check if AWS CLI is available
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI v2 is required but not found"
    echo "Please install it from: https://aws.amazon.com/cli/"
    exit 1
fi

AWS_VERSION=$(aws --version 2>&1 | head -1)
echo "Using AWS CLI: $AWS_VERSION"

# Read and validate policy
if ! jq . "$POLICY_FILE" > /dev/null 2>&1; then
    echo "ERROR: Failed to parse lifecycle policy JSON: $POLICY_FILE"
    exit 1
fi

RULE_COUNT=$(jq '.Rules | length' "$POLICY_FILE")
echo "Loaded lifecycle policy from: $POLICY_FILE"
echo "Policy contains $RULE_COUNT rules"

# Export credentials for AWS CLI
export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY"

# Convert policy to compact JSON
POLICY_JSON=$(jq -c . "$POLICY_FILE")

if [[ "$DRY_RUN" == "true" ]]; then
    echo ""
    echo "=== DRY RUN MODE ==="
    echo "Would apply to endpoint: $MINIO_ENDPOINT"
    echo "Would apply to bucket: $BUCKET_NAME"
    echo ""
    echo "Lifecycle rules to be applied:"
    jq '.Rules[] | "\t- \(.ID): prefix='\''​\(.Filter.Prefix)'\'​' expiration=\(.Expiration.Days) days (Status: \(.Status))"' "$POLICY_FILE"
    echo ""
    echo "Dry run complete. No changes made."
    exit 0
fi

# Apply lifecycle configuration
echo ""
echo "Applying lifecycle configuration..."
echo "Endpoint: $MINIO_ENDPOINT"
echo "Bucket: $BUCKET_NAME"

if aws s3api put-bucket-lifecycle-configuration \
    --endpoint-url "$MINIO_ENDPOINT" \
    --bucket "$BUCKET_NAME" \
    --lifecycle-configuration "$POLICY_JSON" 2>&1; then
    
    echo "✓ Successfully applied lifecycle rules to bucket: $BUCKET_NAME"
    echo ""
    echo "Applied rules:"
    jq '.Rules[] | "\t- \(.ID): prefix='\''​\(.Filter.Prefix)'\'​' expires in \(.Expiration.Days) day(s)"' "$POLICY_FILE"
else
    echo "ERROR: Failed to apply lifecycle configuration"
    exit 1
fi

# Verify lifecycle configuration
echo ""
echo "Verifying lifecycle configuration..."

if VERIFIED=$(aws s3api get-bucket-lifecycle-configuration \
    --endpoint-url "$MINIO_ENDPOINT" \
    --bucket "$BUCKET_NAME" 2>&1); then
    
    echo "✓ Verification successful"
    echo "Current lifecycle rules:"
    echo "$VERIFIED" | jq '.Rules[] | "\t- \(.ID): \(.Filter.Prefix) → \(.Expiration.Days) days (Status: \(.Status))"'
else
    echo "⚠ WARNING: Could not verify lifecycle configuration"
    echo "  Rules may have been applied successfully, but verification failed"
    echo "  Error: $VERIFIED"
fi

echo ""
echo "✓ Lifecycle configuration setup complete!"
exit 0
