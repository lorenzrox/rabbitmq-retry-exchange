#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$OUTPUT_DIR       = Join-Path $PSScriptRoot "dist"
$RABBITMQ_VERSION = "4.3.4"

New-Item -ItemType Directory -Force -Path $OUTPUT_DIR | Out-Null
Remove-Item -Recurse -Force "$OUTPUT_DIR\*" -ErrorAction SilentlyContinue

Write-Host "Building plugin..."
docker build `
    --build-arg RABBITMQ_VERSION=$RABBITMQ_VERSION `
    -f Dockerfile.build `
    --output "type=local,dest=$OUTPUT_DIR" `
    $PSScriptRoot

Write-Host "Done. Plugin saved to: $OUTPUT_DIR"