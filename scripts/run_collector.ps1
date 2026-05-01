# run_collector.ps1
# Start the OChain v2 option chain collector.
#
# Usage:
#   .\scripts\run_collector.ps1
#   .\scripts\run_collector.ps1 -Broker fixture -Symbols NIFTY BANKNIFTY
#   .\scripts\run_collector.ps1 -DryRun
#
# Requires: pip install ochain-v2   (add [dhan] for live trading)

param(
    [string]$Config  = "config/settings.yaml",
    [string]$Broker  = "",          # dhan | fixture (overrides settings.yaml)
    [string[]]$Symbols = @(),       # space-separated, e.g. NIFTY BANKNIFTY
    [switch]$DryRun
)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Cmd = "python -m ochain_v2 collector --config `"$Config`""
if ($Broker)   { $Cmd += " --broker $Broker" }
if ($Symbols)  { $Cmd += " --symbols " + ($Symbols -join " ") }
if ($DryRun)   { $Cmd += " --dry-run" }

Write-Host "[OChain v2] Starting collector"
Write-Host "[OChain v2] Config  : $Config"
if ($Broker)  { Write-Host "[OChain v2] Broker  : $Broker" }
if ($Symbols) { Write-Host "[OChain v2] Symbols : $($Symbols -join ', ')" }
if ($DryRun)  { Write-Host "[OChain v2] Mode    : DRY RUN (no writes)" }
Write-Host "[OChain v2] Command : $Cmd"
Write-Host ""

Invoke-Expression $Cmd
