[CmdletBinding()]
param(
    [string]$BaseBranch = "master",
    [string]$WorktreeRoot = ".worktrees",
    [switch]$RequireRemoteBaseMatch,
    [switch]$Apply
)

$ErrorActionPreference = "Stop"

function Get-RepoRoot {
    $root = (& git rev-parse --show-toplevel).Trim()
    if (-not $root) {
        throw "Not inside a git repository."
    }
    return $root
}

function Get-WorktreeEntries {
    $lines = & git worktree list --porcelain
    $entries = @()
    $current = @{}

    foreach ($line in $lines) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            if ($current.ContainsKey("worktree")) {
                $entries += [pscustomobject]$current
            }
            $current = @{}
            continue
        }

        $parts = $line.Split(" ", 2)
        $key = $parts[0]
        $value = if ($parts.Length -gt 1) { $parts[1] } else { $true }
        $current[$key] = $value
    }

    if ($current.ContainsKey("worktree")) {
        $entries += [pscustomobject]$current
    }

    return $entries
}

function Test-BranchMergedIntoBase {
    param(
        [string]$Branch,
        [string]$BaseBranch
    )

    & git merge-base --is-ancestor "refs/heads/$Branch" "refs/heads/$BaseBranch" *> $null
    return $LASTEXITCODE -eq 0
}

function Get-BranchNameFromRef {
    param([string]$BranchRef)

    if (-not $BranchRef) {
        return $null
    }

    return ($BranchRef -replace "^refs/heads/", "")
}

function Get-RemoteBranchSha {
    param(
        [string]$RemoteName,
        [string]$BranchName
    )

    $line = (& git ls-remote --heads $RemoteName $BranchName | Select-Object -First 1)
    if (-not $line) {
        return $null
    }

    return (($line -split "\s+")[0]).Trim()
}

$repoRoot = Get-RepoRoot
$managedRoot = Join-Path $repoRoot $WorktreeRoot

Push-Location $repoRoot
try {
    & git show-ref --verify --quiet "refs/heads/$BaseBranch"
    if ($LASTEXITCODE -ne 0) {
        throw "Base branch '$BaseBranch' does not exist locally."
    }

    if ($RequireRemoteBaseMatch) {
        & git remote get-url origin *> $null
        if ($LASTEXITCODE -ne 0) {
            throw "RequireRemoteBaseMatch was set, but no 'origin' remote is configured."
        }

        $localBaseSha = (& git rev-parse "refs/heads/$BaseBranch").Trim()
        $remoteBaseSha = Get-RemoteBranchSha -RemoteName "origin" -BranchName $BaseBranch

        if (-not $remoteBaseSha) {
            throw "Could not resolve origin/$BaseBranch."
        }

        if ($localBaseSha -ne $remoteBaseSha) {
            throw "Local $BaseBranch is not yet pushed to origin. Push $BaseBranch before retiring worktrees."
        }
    }

    $entries = Get-WorktreeEntries
    $retired = 0

    foreach ($entry in $entries) {
        $worktreePath = $entry.worktree
        $resolvedPath = try { (Resolve-Path -LiteralPath $worktreePath).Path } catch { $worktreePath }

        if ($resolvedPath -eq $repoRoot) {
            continue
        }

        if (-not ($resolvedPath.StartsWith($managedRoot, [System.StringComparison]::OrdinalIgnoreCase))) {
            Write-Host "Skipping unmanaged worktree: $resolvedPath"
            continue
        }

        $branch = Get-BranchNameFromRef $entry.branch
        if (-not $branch) {
            Write-Host "Skipping detached worktree: $resolvedPath"
            continue
        }

        if ($branch -eq $BaseBranch) {
            Write-Host "Skipping base branch worktree: $resolvedPath"
            continue
        }

        $status = & git -C $resolvedPath status --porcelain
        if ($status) {
            Write-Host "Skipping dirty worktree [$branch]: $resolvedPath"
            continue
        }

        if (-not (Test-BranchMergedIntoBase -Branch $branch -BaseBranch $BaseBranch)) {
            Write-Host "Keeping unmerged worktree [$branch]: $resolvedPath"
            continue
        }

        if (-not $Apply) {
            Write-Host "[dry-run] Would remove worktree [$branch]: $resolvedPath"
            Write-Host "[dry-run] Would delete local branch: $branch"
            continue
        }

        Write-Host "Removing worktree [$branch]: $resolvedPath"
        & git worktree remove "$resolvedPath"

        Write-Host "Deleting merged local branch: $branch"
        & git branch -d "$branch"
        $retired += 1
    }

    if ($Apply) {
        Write-Host "Pruning stale worktree metadata..."
        & git worktree prune
        Write-Host "Retired $retired worktree(s)."
    } else {
        Write-Host "Dry run complete. Re-run with -Apply to remove merged worktrees."
    }
}
finally {
    Pop-Location
}
