# Repo Tools

This directory contains local repository-maintenance tooling.

- `install-git-hooks.ps1`: installs the repository `pre-commit` hook from `.githooks/`
- `new-worktree.ps1`: creates a task-specific git worktree and branch
- `cleanup-worktrees.ps1`: removes clean, merged managed worktrees
- `push-master-and-cleanup.ps1`: pushes `master`, then runs managed worktree cleanup

These are local developer utilities. They are not GitHub Actions code and do not belong in `.github/`.
