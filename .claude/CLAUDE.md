# Georgia Statewide Data Pipeline

## Branching Workflow

Multiple agents may run concurrently. Each agent must:

1. Start from latest master: `git checkout master && git pull`
2. Create a feature branch: `git checkout -b descriptive-branch-name`
3. Do all work and commit on the feature branch
4. Push: `git push -u origin descriptive-branch-name`
5. Open a PR and auto-merge (see Merge to Main below)

Never commit directly to master.

## Context Exclusions

Ignore the `foundry/` folder for all AI context purposes. Do not read, summarize, or use files under `foundry/` when forming responses or making decisions.

## Project Structure

```
00-Project-Management/
├── project_setup.md                    # Tooling, setup, daily commands
└── Project_Plan/
    ├── README.md                       # Overview, scoring categories, architecture
    ├── phase-1-foundation.md           # Active RAPTOR phases (1-8)
    ├── ...
    ├── phase-8-raptor-integration.md
    └── archive/                        # Post-RAPTOR phases (9-16), revisit later
```

## Data Inventory

The data inventory CSV is at `01-Raw-Data/Georgia_Data_Inventory.csv`. Add new datasets to this file as they are discovered. This is a living document tracking all known data sources for the Georgia pipeline.

## Commits and Pushes

Commit at milestones (not every tiny change). Push after every commit.

## PR Writing Style

The project lead is NOT a developer. PR titles and descriptions must be in plain English describing what changed from a user's perspective. Never use `--fill`. Use this structure:

- **Title:** one plain sentence about what changed
- **Body:**
  - **What changed** — plain language bullets
  - **Why** — plain language
  - **Technical details** — optional, for the record

## Merge to Main

A GitHub merge queue is configured on `master`. Never merge locally or push directly to master. Instead:

1. Open a PR and enable auto-merge:
   ```
   gh pr create --base master --title "..." --body "..." && gh pr merge --auto --merge
   ```
   If a PR already exists: `gh pr merge --auto --merge`
2. Done. GitHub runs CI, queues the merge, and merges automatically.
3. On failure: fix the issue on the branch, push, queue re-tests.
   For merge conflicts:
   ```
   git pull --rebase origin master && git push --force-with-lease
   ```
