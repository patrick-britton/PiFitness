
#!/usr/bin/env bash
set -Eeuo pipefail

# Ensure ~/.local/bin is on PATH (for user-installed tools like uv)
if [[ -d "$HOME/.local/bin" && ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    export PATH="$HOME/.local/bin:$PATH"
fi

REPO="/home/god/PiFitness"
BOOTSTRAP_DIR="/home/god/pifitness-bootstrap"

TEST_ENV="/home/god/.config/pifitness/test.env"

echo
echo "================================"
echo "       PiFitness Bootstrap"
echo "================================"
echo
echo "1) PRD"
echo "2) TEST"
echo "Q) Quit"
echo

while true; do
    read -r -p "Select environment: " choice

    case "${choice,,}" in
        1|p|prd)
            echo
            echo "PRD deployment is not yet enabled."
            echo "The new bootstrap has not been merged into main."
            exit 0
            ;;
        2|t|test|tst)
            ENVIRONMENT="test"
            BRANCH="feature-test"
            ENV_FILE="$TEST_ENV"
            break
            ;;
        q|quit)
            echo "Cancelled."
            exit 0
            ;;
        *)
            echo "Invalid selection."
            ;;
    esac
done

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: TEST environment file not found: $ENV_FILE" >&2
    exit 1
fi

echo
echo "Fetching origin/$BRANCH..."

git -C "$REPO" fetch --no-tags origin \
    "refs/heads/$BRANCH:refs/remotes/origin/$BRANCH"

COMMIT=$(git -C "$REPO" rev-parse --verify \
    "refs/remotes/origin/$BRANCH^{commit}")

# Create a private temporary directory for the exact
# deployment tooling contained in this commit.
umask 077
mkdir -p "$BOOTSTRAP_DIR"

TMP_DIR=$(mktemp -d "$BOOTSTRAP_DIR/bootstrap.XXXXXX")

cleanup() {
    local exit_status=$?
    trap - EXIT

    rm -rf -- "$TMP_DIR" ||
        echo "WARNING: Could not remove temporary directory: $TMP_DIR" >&2

    exit "$exit_status"
}

trap cleanup EXIT

# Extract the deployment tooling from the requested commit:
#   deploy/scripts  the scripts and their supporting libraries (required)
#   deploy/systemd  the unit files the commit defines (autodeploy.sh reads the
#                   unit lists from it, before any checkout exists). Optional:
#                   a commit without that directory simply defines no units.
# Helpers find deploy/systemd as a sibling of deploy/scripts (common.py,
# systemd_unit_dir): the two names must agree.
EXTRACT_PATHS=(deploy/scripts)
if git -C "$REPO" cat-file -e "$COMMIT:deploy/systemd" 2>/dev/null; then
    EXTRACT_PATHS+=(deploy/systemd)
fi
git -C "$REPO" archive "$COMMIT" "${EXTRACT_PATHS[@]}" |
    tar -xf - -C "$TMP_DIR"

SCRIPT="$TMP_DIR/deploy/scripts/autodeploy.sh"

[[ -f "$SCRIPT" ]] ||
    { echo "ERROR: Bootstrap script missing from commit." >&2; exit 1; }


echo
echo "Environment: $ENVIRONMENT"
echo "Branch:      $BRANCH"
echo "Commit:      $COMMIT"
echo "Environment: $ENV_FILE"
echo

bash -n "$SCRIPT"

echo "Bootstrap and supporting library syntax verified."

echo
echo "Executing $ENVIRONMENT bootstrap..."

bash "$SCRIPT" \
    --environment "$ENVIRONMENT" \
    --commit "$COMMIT"
