# Georgia Statewide Data Pipeline - Project Setup

Last updated: 2026-03-27

## Project Location

- Repo root: `D:\Jacobs\Georgia-Statewide-Data-Pipeline`

## What This Project Contains

- A Palantir Foundry code repository for Python transforms
- Gradle-based Foundry project scaffold in `foundry/`
- Python transforms package in `foundry/transforms-python/` (`myproject.pipeline` + `myproject.datasets`)
- CI config for pull requests/merge queue in `.github/workflows/ci.yml`

## Tooling and Runtime

- Git repository
- PowerShell shell on Windows
- Python (minimum noted in docs: 3.7+, CI uses 3.12)
- Gradle wrapper (`foundry/gradlew.bat`) for Foundry publish flow
- Foundry/Maestro tooling (`palantir-transforms-sdk`, `maestro`)

Key transform/runtime dependencies are declared in `foundry/transforms-python/conda_recipe/meta.yaml`:
- `transforms`
- `transforms-expectations`
- `transforms-preview`
- `transforms-verbs`
- `foundry-transforms-lib-python`
- `transforms-external-systems` (pip)

## First-Time Setup

### 1) Clone and open repo

```powershell
cd "D:\Jacobs"
git clone https://jacobs.palantirfoundry.com/stemma/git/ri.stemma.main.repository.91eaab98-945d-4496-891a-60255e03337b "Georgia-Statewide-Data-Pipeline"
cd "Georgia-Statewide-Data-Pipeline"
```

### 2) Install local Foundry tooling

```powershell
pip install palantir-transforms-sdk
```

### 3) Install project environment

```powershell
cd "foundry"
maestro env install
```

### 4) Install git hooks (99 MB staged-file guard)

```powershell
cd "D:\Jacobs\Georgia-Statewide-Data-Pipeline"
.\scripts\install-git-hooks.ps1
```

## Daily Run Commands

### Sync branch and create feature branch

```powershell
git checkout master
git pull origin master
git checkout -b "feature/short-description"
```

### Validate/publish flow used by Foundry CI config

```powershell
cd "foundry"
.\gradlew.bat --no-daemon --build-cache --stacktrace patch publish
```

### Local code inspection commands

```powershell
git status
git diff
```

Notes:
- No dedicated `dev`, `test`, or `lint` scripts are currently enabled in this repo.
- `pytest`/format/lint plugins are present as commented options in `foundry/transforms-python/build.gradle` but not enabled.
- A pre-commit hook blocks commits when any staged file exceeds 99 MB.

## Data and Artifact Notes

- Source-of-truth transform code is under `foundry/transforms-python/src/myproject/`.
- Data folder layout at repo root:
  - `01-Raw-Data/` (ignored in git except `.gitkeep`)
  - `02-Data-Staging/` (tracked)
  - `03-Processed-Data/` (tracked)
- Root `.gitignore` ignores `foundry/` from the outer repo context (`foundry/` is managed separately).
- Inside `foundry/`, generated/build artifacts are ignored (`.gradle/`, `build/`, `out/`, generated sources, caches).
- Do not commit credentials/tokens; Foundry token is required for clone/push auth.

## Branch and Merge Workflow

- Default branch is `master` (see `foundry/gradle.properties` and CI trigger).
- Never commit directly to `master`; work from a feature branch.
- Merge queue is configured (GitHub `merge_group` event in `.github/workflows/ci.yml`).
- PR and queue commands:

```powershell
gh pr create --base master --fill
gh pr merge --auto --merge
```

- If merge conflicts occur, rebase on latest `master` and push:

```powershell
git pull --rebase origin master
git push --force-with-lease
```

## Core Folder Layout

- `00-Project-Management/` project documentation
- `.github/workflows/` CI workflow(s)
- `foundry/` Foundry-managed Gradle project
- `foundry/transforms-python/` Python transforms project
- `foundry/docs/` local setup and push workflow notes

## Primary In-Repo References

- `.claude/CLAUDE.md`
- `.github/workflows/ci.yml`
- `foundry/docs/local-development-setup.md`
- `foundry/docs/pushing-changes-to-foundry.md`
- `foundry/transforms-python/README.md`
- `foundry/transforms-python/conda_recipe/meta.yaml`
