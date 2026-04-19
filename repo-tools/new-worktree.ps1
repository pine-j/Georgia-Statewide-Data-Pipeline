[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Agent,

    [Parameter(Mandatory = $true)]
    [string]$Task,

    [string]$BaseBranch = "master",
    [string]$WorktreeRoot = "D:\Jacobs\Georgia-Statewide-Data-Pipeline-worktrees"
)

$ErrorActionPreference = "Stop"

function Get-RepoRoot {
    $root = (& git rev-parse --show-toplevel).Trim()
    if (-not $root) {
        throw "Not inside a git repository."
    }
    return $root
}

function Convert-ToSlug {
    param([string]$Value)

    $slug = $Value.ToLowerInvariant()
    $slug = $slug -replace "[^a-z0-9]+", "-"
    $slug = $slug.Trim("-")

    if (-not $slug) {
        throw "Value '$Value' does not produce a valid slug."
    }

    return $slug
}

$repoRoot = Get-RepoRoot
$agentSlug = Convert-ToSlug $Agent
$taskSlug = Convert-ToSlug $Task
$worktreeName = "$agentSlug-$taskSlug"
$branchName = "worktree/$agentSlug/$taskSlug"
if ([System.IO.Path]::IsPathRooted($WorktreeRoot)) {
    $worktreeRootPath = $WorktreeRoot
} else {
    $worktreeRootPath = Join-Path $repoRoot $WorktreeRoot
}
$worktreePath = Join-Path $worktreeRootPath $worktreeName

Push-Location $repoRoot
try {
    & git show-ref --verify --quiet "refs/heads/$BaseBranch"
    if ($LASTEXITCODE -ne 0) {
        throw "Base branch '$BaseBranch' does not exist locally."
    }

    & git show-ref --verify --quiet "refs/heads/$branchName"
    if ($LASTEXITCODE -eq 0) {
        throw "Branch '$branchName' already exists."
    }

    if (Test-Path -LiteralPath $worktreePath) {
        throw "Worktree path already exists: $worktreePath"
    }

    if (-not (Test-Path -LiteralPath $worktreeRootPath)) {
        New-Item -ItemType Directory -Path $worktreeRootPath | Out-Null
    }

    Write-Host "Creating worktree '$worktreeName' from $BaseBranch..."
    & git worktree add "$worktreePath" -b "$branchName" "$BaseBranch"
    if ($LASTEXITCODE -ne 0) {
        throw "git worktree add failed."
    }

    Write-Host ""
    Write-Host "Created:"
    Write-Host "  Worktree: $worktreePath"
    Write-Host "  Branch:   $branchName"
    Write-Host ""
    Write-Host "Next steps:"
    Write-Host "  cd `"$worktreePath`""
    Write-Host "  git status"
}
finally {
    Pop-Location
}
