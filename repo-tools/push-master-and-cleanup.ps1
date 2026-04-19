[CmdletBinding()]
param(
    [string]$BaseBranch = "master",
    [string]$WorktreeRoot = "D:\Jacobs\Georgia-Statewide-Data-Pipeline-worktrees"
)

$ErrorActionPreference = "Stop"

$repoRoot = (& git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) {
    throw "Not inside a git repository."
}

Push-Location $repoRoot
try {
    $currentBranch = (& git branch --show-current).Trim()
    if ($currentBranch -ne $BaseBranch) {
        throw "Current branch is '$currentBranch'. Switch to '$BaseBranch' before running this script."
    }

    $status = & git status --porcelain
    if ($status) {
        throw "Base branch worktree is dirty. Commit or stash changes before pushing and cleaning up."
    }

    Write-Host "Pushing $BaseBranch to origin..."
    & git push origin $BaseBranch
    if ($LASTEXITCODE -ne 0) {
        throw "Push failed. Worktrees were not cleaned up."
    }

    Write-Host "Cleaning up merged worktrees..."
    & powershell -ExecutionPolicy Bypass -File (Join-Path $repoRoot "repo-tools\cleanup-worktrees.ps1") -BaseBranch $BaseBranch -WorktreeRoot $WorktreeRoot -RequireRemoteBaseMatch -Apply
    if ($LASTEXITCODE -ne 0) {
        throw "Cleanup failed after push."
    }
}
finally {
    Pop-Location
}
