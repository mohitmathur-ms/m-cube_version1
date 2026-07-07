"""Collect commit metadata + a size-capped diff for one push, as JSON.

Runs git itself so all the range/truncation logic is testable locally (no
fragile bash JSON escaping). Writes the metadata file that generate_report.py
consumes.

Env contract:
  AFTER_SHA       Tip commit of the push (default: `git rev-parse HEAD`).
  BEFORE_SHA      Previous tip (push event `before`). Empty / all-zeros / a
                  missing object -> falls back to the parent of AFTER, or the
                  empty tree for a brand-new branch.
  BRANCH          Branch name (default: current symbolic ref).
  REPO            "owner/name" for building commit URLs (optional).
  SERVER_URL      Git host base (default: https://github.com).
  DIFF_CAP_BYTES  Max bytes of unified diff to keep (default: 60000).
  REPORT_META     Output JSON path (default: out/_meta.json).
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ZERO = "0" * 40
DIFF_CAP = int(os.environ.get("DIFF_CAP_BYTES", "60000"))


def git(*args: str, check: bool = True) -> str:
    res = subprocess.run(["git", *args], capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {res.stderr.strip()}")
    return res.stdout


def _object_exists(ref: str) -> bool:
    return subprocess.run(["git", "cat-file", "-e", ref], capture_output=True).returncode == 0


def resolve_range() -> tuple[str, str]:
    after = os.environ.get("AFTER_SHA", "").strip() or git("rev-parse", "HEAD").strip()
    before = os.environ.get("BEFORE_SHA", "").strip()

    empty_tree = git("hash-object", "-t", "tree", os.devnull).strip()
    parent_exists = subprocess.run(
        ["git", "rev-parse", "--verify", "-q", f"{after}^"], capture_output=True
    ).returncode == 0

    if not before or before == ZERO or not _object_exists(before):
        before = git("rev-parse", f"{after}^").strip() if parent_exists else empty_tree
    return before, after


def main() -> int:
    before, after = resolve_range()
    branch = os.environ.get("BRANCH", "").strip() or git(
        "rev-parse", "--abbrev-ref", "HEAD"
    ).strip()
    repo = os.environ.get("REPO", "").strip()
    server = os.environ.get("SERVER_URL", "https://github.com").rstrip("/")

    diffstat = git("diff", "--stat", before, after, check=False)
    files_raw = git("diff", "--name-only", before, after, check=False)
    changed_files = [f for f in files_raw.splitlines() if f.strip()]
    full_diff = git("diff", before, after, check=False)

    truncated = len(full_diff.encode("utf-8")) > DIFF_CAP
    if truncated:
        full_diff = full_diff.encode("utf-8")[:DIFF_CAP].decode("utf-8", "ignore")

    # Commit message(s) across the range; fall back to the tip commit.
    msgs = git("log", "--format=- %s", f"{before}..{after}", check=False).strip()
    if not msgs:
        msgs = git("log", "-1", "--format=- %s", after, check=False).strip()
    author = git("log", "-1", "--format=%an", after, check=False).strip()

    meta = {
        "sha": after,
        "short_sha": after[:7],
        "branch": branch,
        "author": author,
        "commit_messages": msgs,
        "diffstat": diffstat,
        "changed_files": changed_files,
        "diff": full_diff,
        "diff_truncated": truncated,
        "commit_url": f"{server}/{repo}/commit/{after}" if repo else "",
    }

    out = Path(os.environ.get("REPORT_META", "out/_meta.json"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[collect_meta] {len(changed_files)} files, diff {'truncated ' if truncated else ''}-> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
