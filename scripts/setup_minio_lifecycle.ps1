# What: Apply MinIO S3 lifecycle rules for automatic artifact expiration.
#
# Inputs:
# - $MinioEndpoint: MinIO endpoint URL (e.g., http://localhost:9000)
# - $AccessKey: MinIO access key
# - $SecretKey: MinIO secret key
# - $BucketName: Target bucket name (default: kionas)
# - $PolicyFile: Path to lifecycle policy JSON (default: ./setup_minio_lifecycle.json)
# - $DryRun: If $true, shows what would be applied without making changes (default: $false)
#
# Output:
# - Applies lifecycle rules to MinIO bucket
# - Prints status and error messages
#
# Details:
# - Uses AWS CLI v2 with S3-compatible endpoint
# - Lifecycle rules expire objects by prefix and age
# - Rules are defense-in-depth alongside janitor cleanup task

param(
    [Parameter(Mandatory=$false)]
    [string]$MinioEndpoint = "http://localhost:9000",
    
    [Parameter(Mandatory=$false)]
    [string]$AccessKey = $null,
    
    [Parameter(Mandatory=$false)]
    [string]$SecretKey = $null,
    
    [Parameter(Mandatory=$false)]
    [string]$BucketName = "kionas",
    
    [Parameter(Mandatory=$false)]
    [string]$PolicyFile = "./setup_minio_lifecycle.json",
    
    [Parameter(Mandatory=$false)]
    [bool]$DryRun = $false
)

# Validate prerequisites
if (-not (Test-Path $PolicyFile)) {
    Write-Error "Lifecycle policy file not found: $PolicyFile"
    exit 1
}

# Get credentials from environment if not provided
if (-not $AccessKey) {
    $AccessKey = $env:MINIO_ACCESS_KEY
    if (-not $AccessKey) {
        Write-Error "MINIO_ACCESS_KEY not provided and not set in environment"
        exit 1
    }
}

if (-not $SecretKey) {
    $SecretKey = $env:MINIO_SECRET_KEY
    if (-not $SecretKey) {
        Write-Error "MINIO_SECRET_KEY not provided and not set in environment"
        exit 1
    }
}

# Check if AWS CLI is available
try {
    $awsVersion = aws --version 2>&1 | Select-Object -First 1
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI not available"
    }
    Write-Host "Using AWS CLI: $awsVersion"
}
catch {
    Write-Error "AWS CLI v2 is required. Please install it from: https://aws.amazon.com/cli/"
    exit 1
}

# Read lifecycle policy
try {
    $policy = Get-Content $PolicyFile | ConvertFrom-Json
    Write-Host "Loaded lifecycle policy from: $PolicyFile"
    Write-Host "Policy contains $($policy.Rules.Count) rules"
}
catch {
    Write-Error "Failed to parse lifecycle policy JSON: $_"
    exit 1
}

# Configure AWS credentials for MinIO
$env:AWS_ACCESS_KEY_ID = $AccessKey
$env:AWS_SECRET_ACCESS_KEY = $SecretKey

# Prepare command
$jsonPolicy = $policy | ConvertTo-Json -Depth 10 -Compress

$command = "aws s3api put-bucket-lifecycle-configuration --endpoint-url $MinioEndpoint --bucket $BucketName --lifecycle-configuration $jsonPolicy"

if ($DryRun) {
    Write-Host "`n=== DRY RUN MODE ===" -ForegroundColor Yellow
    Write-Host "Would execute:" -ForegroundColor Yellow
    Write-Host $command -ForegroundColor Cyan
    Write-Host "`nLifecycle rules to be applied:" -ForegroundColor Yellow
    $policy.Rules | ForEach-Object {
        Write-Host "  - $($_.ID): prefix='$($_.Filter.Prefix)' expiration=$($_.Expiration.Days) days (Status: $($_.Status))" -ForegroundColor Cyan
    }
    Write-Host "`nDry run complete. No changes made." -ForegroundColor Yellow
    exit 0
}

# Apply lifecycle configuration
Write-Host "`nApplying lifecycle configuration..." -ForegroundColor Green

try {
    # Note: PowerShell's Invoke-WebRequest or direct aws command is more reliable
    $result = & aws s3api put-bucket-lifecycle-configuration `
        --endpoint-url $MinioEndpoint `
        --bucket $BucketName `
        --lifecycle-configuration $jsonPolicy 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Successfully applied lifecycle rules to bucket: $BucketName" -ForegroundColor Green
        Write-Host "`nApplied rules:" -ForegroundColor Green
        $policy.Rules | ForEach-Object {
            Write-Host "  - $($_.ID): prefix='$($_.Filter.Prefix)' expires in $($_.Expiration.Days) day(s)" -ForegroundColor Green
        }
    }
    else {
        Write-Error "Failed to apply lifecycle configuration: $result"
        exit 1
    }
}
catch {
    Write-Error "Error applying lifecycle configuration: $_"
    exit 1
}

# Verify lifecycle configuration
Write-Host "`nVerifying lifecycle configuration..."

try {
    $verification = & aws s3api get-bucket-lifecycle-configuration `
        --endpoint-url $MinioEndpoint `
        --bucket $BucketName 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Verification successful" -ForegroundColor Green
        Write-Host "Current lifecycle rules:" -ForegroundColor Green
        $verifiedPolicy = $verification | ConvertFrom-Json
        $verifiedPolicy.Rules | ForEach-Object {
            Write-Host "  - $($_.ID): $($_.Filter.Prefix) → $($_.Expiration.Days) days (Status: $($_.Status))" -ForegroundColor Green
        }
    }
    else {
        Write-Warning "Could not verify lifecycle configuration, but it may have been applied successfully"
    }
}
catch {
    Write-Warning "Verification check failed (but rules may have been applied): $_"
}

Write-Host "`n✓ Lifecycle configuration setup complete!" -ForegroundColor Green
exit 0
