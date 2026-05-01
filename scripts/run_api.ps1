# run_api.ps1
# Start the OChain v2 FastAPI server on port 5051.
#
# Usage:
#   .\scripts\run_api.ps1
#   .\scripts\run_api.ps1 -Port 5052 -Workers 2
#
# Requires: uvicorn (pip install ochain-v2[dev])

param(
    [int]$Port    = 5051,
    [string]$Host = "0.0.0.0",
    [int]$Workers = 1,
    [switch]$Reload
)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Cmd = "uvicorn ochain_v2.api.main:app --host $Host --port $Port --workers $Workers"
if ($Reload) { $Cmd += " --reload" }

Write-Host "[OChain v2] Starting API server on http://${Host}:${Port}"
Write-Host "[OChain v2] Docs: http://localhost:${Port}/docs"
Write-Host "[OChain v2] Command: $Cmd"
Write-Host ""

Invoke-Expression $Cmd
