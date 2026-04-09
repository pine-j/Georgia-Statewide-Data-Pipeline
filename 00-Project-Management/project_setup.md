# Georgia Statewide Data Pipeline - Project Setup

Last updated: 2026-04-05

## Project Location

- Repo root: `D:\Jacobs\Georgia-Statewide-Data-Pipeline`

## What This Project Contains

- Project planning and assessment documentation in `00-Project-Management/`
- Raw, staged, and processed data folders at the repo root
- Local-first statewide web application in `04-Webapp/`
- RAPTOR integration staging in `05-RAPTOR-Integration/`
- CI workflow for pull requests and merge queue in `.github/workflows/ci.yml`

## Tooling and Runtime

- Git repository
- PowerShell shell on Windows
- Python for staging scripts and the web API backend
- Node.js and npm for the web frontend
- Docker Desktop optional for the local webapp stack
- Local repo utilities in `repo-tools/` for hooks and worktree management

Current webapp stack under `04-Webapp/`:
- Frontend: Vite + React + TypeScript
- Backend: FastAPI + SQLAlchemy
- Database: local PostGIS via Docker Compose
- Mapping: MapLibre

## First-Time Setup

### 1) Clone and open repo

```powershell
cd "D:\Jacobs"
git clone <repository-url> "Georgia-Statewide-Data-Pipeline"
cd "Georgia-Statewide-Data-Pipeline"
```

### 2) Install git hooks (99 MB staged-file guard)

```powershell
.\repo-tools\install-git-hooks.ps1
```

### 3) Set up the local webapp environment

Use one of the following approaches when working in `04-Webapp/`.

#### Option A: Docker Compose

```powershell
cd "04-Webapp"
docker compose up --build
```

Services:
- Frontend: `http://localhost:5173`
- Backend: `http://localhost:8000`
- OpenAPI: `http://localhost:8000/docs`
- PostGIS: `localhost:5432`

#### Option B: Run backend and frontend separately

Backend:

```powershell
cd "04-Webapp\backend"
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Frontend:

```powershell
cd "04-Webapp\frontend"
npm install
npm run dev
```

Tracked environment templates:
- `04-Webapp/backend/.env.example`
- `04-Webapp/frontend/.env.example`

## Daily Run Commands

### Sync local `master`

```powershell
git checkout master
git pull origin master
```

### Create a dedicated task worktree

```powershell
.\repo-tools\new-worktree.ps1 -Agent codex -Task short-description
```

This creates a local feature branch in a dedicated worktree under `.worktrees/`.
Keep the main checkout reserved for `master` integration work.

### Local code inspection commands

```powershell
git status
git diff
```

### Common local webapp commands

```powershell
cd "04-Webapp"
docker compose up --build
```

```powershell
cd "04-Webapp\frontend"
npm run build
```

Notes:
- The root repo does not currently expose a single top-level `dev`, `test`, or `lint` command.
- Frontend commands live in `04-Webapp/frontend/package.json`.
- Backend dependencies live in `04-Webapp/backend/requirements.txt`.
- A pre-commit hook blocks commits when any staged file exceeds 99 MB.

## Data and Artifact Notes

- Data folder layout at repo root:
  - `01-Raw-Data/` (ignored in git except `.gitkeep`)
  - `02-Data-Staging/` (tracked)
  - `03-Processed-Data/` (tracked)
- The living data inventory is `01-Raw-Data/Georgia_Data_Inventory.csv`.
- Root `.gitignore` ignores large raw files, database artifacts, generated frontend output, virtual environments, and local worktrees.
- Do not commit credentials, `.env` files, database dumps, or generated artifacts unless explicitly required.

## Branch and Merge Workflow

- Default branch is `master`.
- Do not do feature work in the main checkout.
- Each agent or sub-agent works in its own local feature branch and dedicated worktree.
- Do not push feature branches by default.
- Merge completed task branches locally into `master`.
- Push `master` to remote after the merge milestone.
- Retire merged worktrees immediately after `master` is pushed.

### Recommended task flow

1. Update local `master`
2. Create a dedicated worktree:

```powershell
.\repo-tools\new-worktree.ps1 -Agent codex -Task short-description
```

3. Change into the new worktree and do the task there
4. Commit locally in the worktree as needed
5. Return to the main checkout on `master`
6. Merge the completed feature branch locally into `master`
7. Push `master` and retire merged worktrees:

```powershell
.\repo-tools\push-master-and-cleanup.ps1
```

### Worktree cleanup commands

```powershell
# preview cleanup only
.\repo-tools\cleanup-worktrees.ps1

# apply cleanup after master has been pushed
.\repo-tools\cleanup-worktrees.ps1 -RequireRemoteBaseMatch -Apply
```

## Core Folder Layout

- `00-Project-Management/` project documentation
- `.github/workflows/` CI workflow(s)
- `01-Raw-Data/` raw downloads and the living data inventory
- `02-Data-Staging/` ETL scripts, config, staging databases, and GeoPackage generation
- `03-Processed-Data/` processed outputs
- `04-Webapp/` local-first statewide web app source tree
- `05-RAPTOR-Integration/` Georgia RAPTOR integration staging
- `repo-tools/` local repository-maintenance scripts

## Primary In-Repo References

- `AGENT.md`
- `.github/workflows/ci.yml`
- `04-Webapp/README.md`
- `repo-tools/README.md`
- `repo-tools/new-worktree.ps1`
- `repo-tools/push-master-and-cleanup.ps1`
- `repo-tools/cleanup-worktrees.ps1`

## Assessment And Options References

Open these from the VS Code editor or Markdown preview:

- [Roadway Gap-Fill and Supplement Strategy](./Pipeline-Documetation/phase-1-Supplement-Docs/roadway-gap-fill-consolidated.md)
