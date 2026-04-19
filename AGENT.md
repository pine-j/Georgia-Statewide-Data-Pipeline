# Agent Guidance

## Canonical Guidance File

`AGENT.md` is the single in-repo agent guidance file. Do not recreate `CLAUDE.md` or `.claude/` guidance files.

## Branching Workflow

Multiple agents may run concurrently. Each agent must:

1. Start from latest `master`: `git checkout master && git pull`
2. Create a dedicated git worktree before making changes
3. Create a feature branch inside that worktree: `git checkout -b descriptive-branch-name`
4. Do all work and commit in that worktree on the feature branch
5. Merge the finished feature branch locally into `master`
6. Push `master` to remote after the merge milestone
7. Retire the merged worktree and delete the local feature branch

Never commit directly to `master`.

If you inherit a checkout where `master` is already ahead of `origin/master`,
treat those local commits as existing state to preserve. Do not rewrite,
discard, or "fix" that history unless the user explicitly asks.

## Worktree Requirement

Every agent and sub-agent must create and use its own git worktree when starting a task. Do not have two agents work from the same checkout when they may touch overlapping files.

Purpose:

- isolate each agent's changes
- reduce file-level clashes between concurrent agents
- make cleanup and merge decisions explicit

Standard location:

- create agent worktrees under `.worktrees/`
- use a clear folder name such as `.worktrees/<agent>-<task>`
- preferred creation command: `.\repo-tools\new-worktree.ps1 -Agent <agent> -Task <task>`

Expected lifecycle:

1. Create a dedicated worktree for the task, preferably with:
   `git worktree add .worktrees/<agent>-<task> -b <branch-name> master`
2. Create or switch to the task branch inside that worktree
3. Complete the task there and verify the changes
4. Commit the changes from that worktree
5. Merge the branch locally into `master` when the task is complete
6. Push `master` to remote
7. Remove the worktree after the task is finished and the changes are safely integrated

Retirement rule:

- do not delete a worktree on a timer
- only retire a worktree when its branch is merged into local `master`, `master` has been pushed to remote, and the worktree is clean
- after retirement, delete the local feature branch so only `master` and active task branches remain

Cleanup command:

- preview cleanup: `.\repo-tools\cleanup-worktrees.ps1`
- apply cleanup: `.\repo-tools\cleanup-worktrees.ps1 -Apply`
- standard end-of-merge command: `.\repo-tools\push-master-and-cleanup.ps1`

## Worktree Data Access

Raw inputs and staged outputs are gitignored and exist only in the main repo working tree. They are not copied into `.worktrees/*` checkouts when `git worktree add` runs.

- Read gitignored data from the main repo via absolute path. Examples: `D:\Jacobs\Georgia-Statewide-Data-Pipeline\01-Raw-Data\...`, `...\02-Data-Staging\staged\...`. Do not copy these files into the worktree.
- Never write into the main repo's gitignored data dirs from a worktree. Other worktrees may be reading the same files concurrently; overwriting them corrupts other agents' reads. Regenerate into a worktree-local scratch dir (for example `_scratch/staged/`) and point the code at that override via an explicit argument or config. Exclude the scratch dir via `.git/info/exclude` so it does not land on the feature branch.
- Do not run the full staging pipeline from a worktree. Full pipeline runs happen only from the main repo working tree, and only when no other agent is actively reading staged data. Single-step reruns with an output override pointed at `_scratch/` are fine.

Optional convenience for pipeline-heavy tasks: junction the read-only input dirs into the worktree so scripts that use relative paths work unchanged:

```
mklink /J ".worktrees\<agent>-<task>\01-Raw-Data" "D:\Jacobs\Georgia-Statewide-Data-Pipeline\01-Raw-Data"
```

Do not junction `02-Data-Staging/staged/` or any other writable data dir — keep writes worktree-local.

## Context Exclusions

Ignore the `.tmp/` and `.playwright-mcp/` folders for project decisions and
summaries. Treat them as scratch/reference space, not project source-of-truth.

## Current Repo Structure

- `00-Project-Management/project_setup.md`: tooling, setup, and daily commands
- `00-Project-Management/Project_Plan/README.md`: project overview, architecture, and active phase index
- `00-Project-Management/Project_Plan/phase-1-foundation.md`
- `00-Project-Management/Project_Plan/phase-2-connectivity.md`
- `00-Project-Management/Project_Plan/phase-3-socioeconomic.md`
- `00-Project-Management/Project_Plan/phase-4-safety.md`
- `00-Project-Management/Project_Plan/phase-5-asset-preservation.md`
- `00-Project-Management/Project_Plan/phase-6-mobility.md`
- `00-Project-Management/Project_Plan/phase-7-sharepoint.md`
- `00-Project-Management/Project_Plan/phase-8-raptor-integration.md`
- `00-Project-Management/Pipeline-Documentation/`: pipeline phase docs, data dictionary, and supplementary notes
- `01-Raw-Data/`: raw downloads and the living Georgia data inventory
- `02-Data-Staging/`: ETL scripts, config, staging databases, and GeoPackage generation
- `03-Processed-Data/`: processed outputs
- `04-Webapp/`: active Georgia Statewide Web App source tree
- `05-RAPTOR-Integration/states/Georgia/`: Georgia RAPTOR category code staged for later integration

## Documentation Conventions

For repo-internal documentation links, use relative Markdown paths so they open correctly in VS Code and its Markdown preview. Avoid absolute local paths such as `d:/...` and avoid `file://` links for local repo files. External URLs are fine when the target is intentionally outside the repository.

When file names or documentation folders change, update the affected indexes and cross-links in the same change.

Pipeline documentation under `00-Project-Management/Pipeline-Documentation/` is a documentation-first record. Preserve findings and provenance even when no implementation decision has been made yet.

## Data Inventory

The data inventory CSV is at `01-Raw-Data/Georgia_Data_Inventory.csv`. Add new datasets there as they are discovered. Treat it as the living inventory of known Georgia pipeline data sources.

Keep dataset-specific download scripts with the raw dataset they fetch. Place those entrypoints under the relevant `01-Raw-Data/<dataset>/` folder, preferably `01-Raw-Data/<dataset>/scripts/`. Reserve `02-Data-Staging/` for post-download ETL work such as normalization, enrichment, validation, database loading, and staged artifact generation.

## Segment Slicing Guardrail

Do NOT slice, split, or re-segment roadway geometries at new milepoint boundaries without explicit user approval. The base network segments from the GDOT geodatabase should be preserved as-is unless there is a confirmed, empirically verified reason that the source data intervals do not align. Before proposing any geometry slicing logic, present the evidence (mismatched interval counts, examples) and get confirmation.

## Data Source Preference

Wherever possible, prefer official GDOT data as the first-choice source for Georgia datasets, boundaries, and attributes. Only use derived, federal, third-party, or fallback sources when an official GDOT source is unavailable, inaccessible, or materially incomplete, and document that choice clearly.

When supplementing the roadway network from a secondary source, preserve explicit source provenance for both geometry and attributes.

## Local-First Web App

`04-Webapp/` is intentionally local-first:

- frontend: Vite + React + TypeScript
- backend: FastAPI
- default current backend data mode: staged SQLite + GeoPackage reads from `02-Data-Staging/`
- local database target: PostGIS
- mapping: MapLibre
- runtime: local Docker Compose or direct local development commands

Do not introduce remote-hosting or cloud-specific assumptions into the Georgia web app unless the user explicitly asks for them.

The current default backend runtime serves staged roadway data directly from
`02-Data-Staging/`. Keep that path working unless the user explicitly asks to
move more of the app onto PostGIS-backed tables or services.

## Repo Hygiene

Respect the root `.gitignore`. Do not reintroduce ignored local-tooling folders such as `.claude/` or `.cursor/`, and do not commit generated webapp artifacts or staging outputs unless the user explicitly wants them tracked.

## Commits and Pushes

Commit locally as needed while working.

Treat a local merge into `master` as the milestone that requires a remote push.

Practical rule:

- local commits do not need to be pushed one-by-one
- do not push feature branches by default
- push `master` after the feature work has been merged locally into `master`
- push earlier only when backup, handoff, or collaboration requires it

## PR Writing Style

If a remote PR is explicitly needed, the project lead is not a developer. PR titles and descriptions must be in plain English describing what changed from a user's perspective. Never use `--fill`.

Use this structure:

- Title: one plain sentence about what changed
- Body:
  - What changed: plain language bullets
  - Why: plain language
  - Technical details: optional, for the record

## Merge to Main

Default workflow:

1. Finish the task in its dedicated worktree and commit locally.
2. Merge the task branch locally into `master`.
3. Push `master` to remote.
4. Run `.\repo-tools\push-master-and-cleanup.ps1` or push `master` and then run `.\repo-tools\cleanup-worktrees.ps1 -RequireRemoteBaseMatch -Apply`.
5. Confirm the merged worktree is retired and the local feature branch is deleted.

Use remote PRs only when the user explicitly wants that workflow.
