# Agent Guidance

## HARD RULE — Never Delete Raw Data

**DO NOT delete, overwrite, or lose any file under `01-Raw-Data/` or any staged output under `02-Data-Staging/` (databases, spatial, tables).** These files take a long time to download and regenerate. This rule has been violated before — it must not happen again.

This means:
- Before running `git worktree remove`: enumerate and copy ALL gitignored data files back to the main repo. No exceptions.
- Before running `git clean`, `rm -rf`, or any destructive command: verify it will not touch raw data or staged outputs.
- If you are unsure whether an action will delete data files, stop and ask the user.

Violation of this rule has caused costly multi-hour re-downloads twice. Treat any action that could destroy these files as **blocked until explicitly confirmed by the user**.

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

- create agent worktrees under `D:\Jacobs\Georgia-Statewide-Data-Pipeline-worktrees\` (a sibling folder to the main repo, not inside it)
- use a clear folder name such as `D:\Jacobs\Georgia-Statewide-Data-Pipeline-worktrees\<agent>-<task>`
- preferred creation command: `.\repo-tools\new-worktree.ps1 -Agent <agent> -Task <task>` (the script now defaults `-WorktreeRoot` to the sibling folder)
- the legacy in-repo `.worktrees/` path is deprecated; do not create new worktrees there

Expected lifecycle:

1. Create a dedicated worktree for the task, preferably with:
   `git worktree add D:\Jacobs\Georgia-Statewide-Data-Pipeline-worktrees\<agent>-<task> -b <branch-name> master`
2. Create or switch to the task branch inside that worktree
3. Complete the task there and verify the changes
4. Commit the changes from that worktree
5. Merge the branch locally into `master` when the task is complete
6. Push `master` to remote
7. Remove the worktree after the task is finished and the changes are safely integrated

Retirement rule:

- do not delete a worktree on a timer
- only retire a worktree when its branch is merged into local `master`, `master` has been pushed to remote, and the worktree is clean
- **BLOCKING PREREQUISITE before `git worktree remove`:** run `find <worktree>/01-Raw-Data -type f` and `find <worktree>/02-Data-Staging/{databases,spatial,tables} -type f` to enumerate every gitignored data file. Copy each one to the main repo working tree (`D:/Jacobs/Georgia-Statewide-Data-Pipeline/`) at the equivalent relative path. Log the files copied. Only after this copy is verified may you run `git worktree remove`. Skipping this step has caused data loss twice — it is not optional.
- after retirement, delete the local feature branch so only `master` and active task branches remain

Cleanup command:

- preview cleanup: `.\repo-tools\cleanup-worktrees.ps1`
- apply cleanup: `.\repo-tools\cleanup-worktrees.ps1 -Apply`
- standard end-of-merge command: `.\repo-tools\push-master-and-cleanup.ps1`

## Worktree Data Access

Raw inputs and staged outputs are gitignored and exist only in the main repo working tree. They are not copied into sibling worktree checkouts (`D:\Jacobs\Georgia-Statewide-Data-Pipeline-worktrees\*`) when `git worktree add` runs.

**CRITICAL — gitignored files are destroyed on worktree removal.** Any raw data file downloaded into a worktree (e.g. under `01-Raw-Data/`) is gitignored and lives only in that worktree's working tree. When `git worktree remove` runs, those files are permanently deleted. They are NOT in the main repo and NOT on any branch. **This has caused data loss twice. Treat this as a hard blocker — never remove a worktree without copying data files first.**

Rules that follow from this:

- **MANDATORY:** Before declaring a workstream complete and merging, copy ALL gitignored data files (GDBs, cached boundaries, HPMS downloads, GPAS outputs, staged databases, spatial files, CSVs — everything under `01-Raw-Data/` and `02-Data-Staging/{databases,spatial,tables}`) into the **main repo working tree** at the equivalent path (`D:/Jacobs/Georgia-Statewide-Data-Pipeline/...`). Do this before `git worktree remove`. Enumerate files explicitly with `find` — do not rely on memory of what was downloaded.
- When a script depends on gitignored source files, verify those files exist in the main repo working tree before running the script. Do not assume a prior agent left them there.
- If a required gitignored file is missing from the main repo working tree, re-download it before proceeding. Document the re-download in your report so the orchestrator knows it happened.

- Read gitignored data from the main repo via absolute path. Examples: `D:\Jacobs\Georgia-Statewide-Data-Pipeline\01-Raw-Data\...`, `...\02-Data-Staging\staged\...`. Do not copy these files into the worktree.
- Never write into the main repo's gitignored data dirs from a worktree. Other worktrees may be reading the same files concurrently; overwriting them corrupts other agents' reads. Regenerate into a worktree-local scratch dir (for example `_scratch/staged/`) and point the code at that override via an explicit argument or config. Exclude the scratch dir via `.git/info/exclude` so it does not land on the feature branch.
- Do not run the full staging pipeline from a worktree. Full pipeline runs happen only from the main repo working tree, and only when no other agent is actively reading staged data. Single-step reruns with an output override pointed at `_scratch/` are fine.

Optional convenience for pipeline-heavy tasks: junction the read-only input dirs into the worktree so scripts that use relative paths work unchanged:

```
mklink /J "D:\Jacobs\Georgia-Statewide-Data-Pipeline-worktrees\<agent>-<task>\01-Raw-Data" "D:\Jacobs\Georgia-Statewide-Data-Pipeline\01-Raw-Data"
```

Do not junction `02-Data-Staging/staged/` or any other writable data dir — keep writes worktree-local.

## Delivery Folder

`D:\Jacobs\GA-Pipeline-Delivery-Folder` is a git-free, attribution-free snapshot of pipeline data outputs (01-Raw-Data, 02-Data-Staging, 03-Processed-Data) intended for delivery to GDOT and other stakeholders. It must never contain git history, `.claude/` or `.git/` directories, `AGENT.md`, `CLAUDE.md`, development scripts, or any reference to AI tooling. When refreshing this folder, copy only data artifacts — not repo infrastructure.

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

This is a solo-developer repo. The default merge workflow is local (see §Merge to Main below) — do NOT open a GitHub PR unless the user explicitly asks for one. When an agent says "ready for merge review" or "merge to master," that means: merge the branch locally, not open a PR.

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
