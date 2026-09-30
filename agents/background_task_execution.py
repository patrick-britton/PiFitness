#!/usr/bin/env python3
import signal
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))  # /home/god/PiFitness

from backend_functions.ultimate_task_executioner_v2 import ultimate_task_executioner


def _handle_sigterm(signum, frame):
    print("Received SIGTERM — shutting down cleanly", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_sigterm)
    ultimate_task_executioner()