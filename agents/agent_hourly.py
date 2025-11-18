#!/usr/bin/env python3
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))  # /home/god/PiFitness

from backend_functions.task_execution import task_executioner

if __name__ == "__main__":
    task_executioner()