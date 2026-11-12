#!/usr/bin/env bash
# ensure_binary.sh - Package-agnostic binary installation helper.
# Ensures a single binary is installed at a given path with an expected version.
# Called by bootstrap.sh, deploy.sh, test-up.sh with dbmate (and any future
# binary) parameters. The orchestrator stays dumb; this module is truthful.
#
# Usage:
#   source "$SCRIPT_DIR/lib/ensure_binary.sh"
#   ensure_binary "/usr/local/bin/dbmate" "2.36.0" \
#       "https://github.com/amacneil/dbmate/releases/download/v2.36.0/dbmate-linux-arm64" "dbmate"

set -euo pipefail

ensure_binary() {
    local BIN_PATH="$1"
    local EXPECTED_VERSION="$2"
    local DOWNLOAD_URL="$3"
    local TOOL_NAME="$4"

    [[ -n "$BIN_PATH" && -n "$EXPECTED_VERSION" && -n "$DOWNLOAD_URL" && -n "$TOOL_NAME" ]] || {
        echo "ERROR: ensure_binary requires BIN_PATH, EXPECTED_VERSION, DOWNLOAD_URL, TOOL_NAME" >&2
        return 1
    }

    if [[ -x "$BIN_PATH" ]]; then
        local INSTALLED_VERSION
        INSTALLED_VERSION="$("$BIN_PATH" --version 2>/dev/null | awk '{print $NF}')" || {
            echo "ERROR: $TOOL_NAME exists at $BIN_PATH but --version failed." >&2
            return 1
        }
        [[ "$INSTALLED_VERSION" == "$EXPECTED_VERSION" ]] || {
            echo "ERROR: $TOOL_NAME version mismatch: expected $EXPECTED_VERSION, got $INSTALLED_VERSION" >&2
            return 1
        }
        echo "$TOOL_NAME $INSTALLED_VERSION already installed at $BIN_PATH."
        return 0
    fi

    echo "Installing $TOOL_NAME migration tool..."
    if ! command -v curl >/dev/null 2>&1; then
        echo "ERROR: curl is required to download $TOOL_NAME" >&2
        return 1
    fi

    if ! curl -fsSL "$DOWNLOAD_URL" -o "$BIN_PATH"; then
        echo "ERROR: Failed to download $TOOL_NAME from $DOWNLOAD_URL" >&2
        return 1
    fi
    chmod +x "$BIN_PATH"

    local INSTALLED_VERSION
    INSTALLED_VERSION="$("$BIN_PATH" --version 2>/dev/null | awk '{print $NF}')" || {
        echo "ERROR: $TOOL_NAME installed but --version failed." >&2
        return 1
    }
    [[ "$INSTALLED_VERSION" == "$EXPECTED_VERSION" ]] || {
        echo "ERROR: $TOOL_NAME version mismatch: expected $EXPECTED_VERSION, got $INSTALLED_VERSION" >&2
        return 1
    }
    echo "$TOOL_NAME $INSTALLED_VERSION installed."
    return 0
}
