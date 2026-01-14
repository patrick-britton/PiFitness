import os
import subprocess
from dotenv import load_dotenv
from typing import List, Dict


def _run_systemctl_status(unit: str) -> List[str]:
    """
    Returns systemctl status output as a list of lines.
    """
    result = subprocess.run(
        ["systemctl", "status", unit, "--no-pager"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.splitlines()

def _extract_lines(lines: List[str], prefixes: List[str]) -> Dict[str, str]:
    """
    Extracts first matching lines that start with given prefixes.
    """
    extracted = {}
    for prefix in prefixes:
        for line in lines:
            if line.strip().startswith(prefix):
                extracted[prefix] = line.strip()
                break
    return extracted

def _extract_log_lines(lines: List[str], max_lines: int = 5) -> List[str]:
    """
    Extracts recent log lines from systemctl status output.
    """
    log_lines = []
    for line in lines:
        if line.strip().startswith(("Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")):
            log_lines.append(line.rstrip())

    return log_lines[-max_lines:]

def get_runtime_status() -> Dict:
    """
    Main function you call from Streamlit.
    """
    box = os.getenv("BOX", "unknown")

    if box == "local":
        return {
            "mode": "local",
            "message": "Running locally"
        }

    if box == "pi5":
        timer_lines = _run_systemctl_status("pifitness_agent.timer")
        service_lines = _run_systemctl_status("pifitness_agent.service")

        timer_info = _extract_lines(
            timer_lines,
            prefixes=["Active:", "Trigger:"]
        )

        service_info = _extract_lines(
            service_lines,
            prefixes=["Loaded:", "Active:"]
        )

        service_logs = _extract_log_lines(service_lines)

        return {
            "mode": "pi5",
            "timer": timer_info,
            "service": service_info,
            "service_logs": service_logs,
        }

    return {
        "mode": "unknown",
        "message": f"Unrecognized BOX value: {box}"
    }