#!/usr/bin/env bash
#
# Initialize the SparkSkew-Benchmark git repository.
#
# Usage:
#   bash scripts/bootstrap_repo.sh
#
# This script is idempotent:
#   - creates .git only if missing;
#   - stages all non-ignored files;
#   - commits only if there are staged changes;
#   - renames the current branch to main;
#   - prints gh auth status when GitHub CLI is available.
#
# It does NOT create a remote or push. Those are explicit, outward-facing
# actions and are performed separately after authentication is confirmed.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v git >/dev/null 2>&1; then
  echo "ERROR: git is not installed or not on PATH." >&2
  echo "Install Git for Windows: https://git-scm.com/download/win" >&2
  exit 1
fi

if [ ! -d .git ]; then
  echo "[bootstrap] git init"
  git init
else
  echo "[bootstrap] .git already exists; keeping history"
fi

# Make the script executable in the index on platforms that support the bit.
chmod +x scripts/bootstrap_repo.sh 2>/dev/null || true

echo "[bootstrap] staging files"
git add -A

if git diff --cached --quiet; then
  echo "[bootstrap] nothing to commit"
else
  echo "[bootstrap] committing initial release"
  git commit -m "feat: initial open-source release of SparkSkew-Benchmark"
fi

echo "[bootstrap] renaming branch to main"
git branch -M main

echo
echo "[bootstrap] git status"
git status --short --branch

echo
if command -v gh >/dev/null 2>&1; then
  echo "[bootstrap] gh auth status"
  gh auth status || true
else
  echo "[bootstrap] gh is not installed or not on PATH."
  echo "  Install: winget install --id GitHub.cli -e"
  echo "  Or download: https://github.com/cli/cli/releases/latest"
fi

echo
echo "[bootstrap] done. Next: authenticate gh, create the public repo, push."
