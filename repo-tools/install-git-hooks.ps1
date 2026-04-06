$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$sourceHook = Join-Path $repoRoot ".githooks\pre-commit"
$gitDir = Join-Path $repoRoot ".git"
$hooksDir = Join-Path $gitDir "hooks"
$targetHook = Join-Path $hooksDir "pre-commit"

if (-not (Test-Path $gitDir)) {
    throw "No .git directory found at '$gitDir'. Run this from a cloned repository."
}

if (-not (Test-Path $sourceHook)) {
    throw "Source hook not found: '$sourceHook'"
}

if (-not (Test-Path $hooksDir)) {
    New-Item -ItemType Directory -Path $hooksDir | Out-Null
}

Copy-Item -Path $sourceHook -Destination $targetHook -Force
Write-Host "Installed git hook: $targetHook"
