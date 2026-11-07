#!/bin/bash
# PiFitness bootstrap stub (D16).
# Usage: bash bootstrap.sh [--ref REF] [--dry-run]
set -e
REF="main"
DRY_RUN=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref) REF="$2"; shift 2 ;;
    --ref=*) REF="${1#--ref=}"; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done
PROJECT_DIR="/home/god/PiFitness"
DEPLOY_DIR="/tmp/pifitness-deploy"
rm -rf "$DEPLOY_DIR"; mkdir -p "$DEPLOY_DIR"
git -C "$PROJECT_DIR" fetch origin "$REF"
git -C "$PROJECT_DIR" archive "origin/$REF" deploy/ | tar -x -C "$DEPLOY_DIR" --strip-components=1
chmod +x "$DEPLOY_DIR"/*.sh 2>/dev/null || true
printf 'Change summary (origin/%s):\n' "$REF"
git -C "$PROJECT_DIR" log --oneline "HEAD..origin/$REF" 2>/dev/null || true
if $DRY_RUN; then
  echo "DRY-RUN: would execute deploy/scripts/deploy.sh"
  exit 0
fi
exec bash "$DEPLOY_DIR/deploy.sh"
