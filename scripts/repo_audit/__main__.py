import sys
from pathlib import Path

# Allow both `python scripts/repo_audit ...` (directory execution) and `python -m repo_audit`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from repo_audit.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
