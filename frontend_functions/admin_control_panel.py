import streamlit as st
import subprocess
import datetime
from typing import Dict


def render_systemd_control_panel():
    """
    Streamlit control panel for a systemd service + timer.
    """

    # ------------------------------------------------------------
    # DEFINE YOUR UNITS HERE (ONLY PLACE YOU NEED TO EDIT)
    # ------------------------------------------------------------
    SERVICE_NAME = "pifitness_agent.service"
    TIMER_NAME   = "pifitness_agent.timer"
    LOG_LINES    = 100
    # ------------------------------------------------------------

    def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )

    def systemctl(action: str, unit: str):
        return run_cmd(["sudo", "systemctl", action, unit])

    def get_is_active(unit: str) -> str:
        result = run_cmd(["systemctl", "is-active", unit])
        return result.stdout.strip()

    def get_is_enabled(unit: str) -> str:
        result = run_cmd(["systemctl", "is-enabled", unit])
        return result.stdout.strip()

    def get_timer_info(unit: str) -> Dict[str, str]:
        """
        Returns parsed NextElapseUSecRealtime and LastTriggerUSecRealtime.
        """
        result = run_cmd([
            "systemctl",
            "show",
            unit,
            "--property=NextElapseUSecRealtime",
            "--property=LastTriggerUSecRealtime",
            "--property=Result"
        ])

        props = {}
        for line in result.stdout.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                props[k] = v
        return props

    def parse_systemd_time(value: str) -> datetime.datetime | None:
        """
        systemd returns timestamps like:
        Mon 2026-02-15 12:00:00 UTC
        or empty string if none scheduled.
        """
        if not value:
            return None

        try:
            # Remove weekday
            parts = value.split(" ", 1)
            if len(parts) == 2:
                value = parts[1]

            return datetime.datetime.strptime(
                value.strip(),
                "%Y-%m-%d %H:%M:%S %Z"
            )
        except Exception:
            return None

    def get_logs(unit: str, lines: int) -> str:
        result = run_cmd([
            "journalctl",
            "-u", unit,
            "-n", str(lines),
            "--no-pager",
            "--output=short-iso"
        ])
        return result.stdout

    # ============================================================
    # UI
    # ============================================================

    st.subheader("Systemd Control Panel")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Service Status")
        service_active = get_is_active(SERVICE_NAME)
        service_enabled = get_is_enabled(SERVICE_NAME)

        st.write("Active:", service_active)
        st.write("Enabled:", service_enabled)

    with col2:
        st.markdown("### Timer Status")
        timer_active = get_is_active(TIMER_NAME)
        timer_enabled = get_is_enabled(TIMER_NAME)

        st.write("Active:", timer_active)
        st.write("Enabled:", timer_enabled)

    # ------------------------------------------------------------
    # Timer Execution Info
    # ------------------------------------------------------------

    st.markdown("### Timer Execution Info")

    timer_props = get_timer_info(TIMER_NAME)

    next_run = parse_systemd_time(timer_props.get("NextElapseUSecRealtime", ""))
    last_run = parse_systemd_time(timer_props.get("LastTriggerUSecRealtime", ""))

    now = datetime.datetime.utcnow()

    if next_run:
        delta_next = next_run - now
        st.write("Next Run:", next_run, f"({delta_next})")
    else:
        st.write("Next Run: None scheduled")

    if last_run:
        delta_last = now - last_run
        st.write("Last Run:", last_run, f"({delta_last} ago)")
    else:
        st.write("Last Run: Never")

    # ------------------------------------------------------------
    # Control Buttons
    # ------------------------------------------------------------

    st.markdown("### Controls")

    c1, c2, c3, c4, c5 = st.columns(5)

    if c1.button("Start"):
        systemctl("start", SERVICE_NAME)
        systemctl("start", TIMER_NAME)
        st.rerun()

    if c2.button("Stop"):
        systemctl("stop", TIMER_NAME)
        systemctl("stop", SERVICE_NAME)
        st.rerun()

    if c3.button("Restart"):
        systemctl("restart", SERVICE_NAME)
        systemctl("restart", TIMER_NAME)
        st.rerun()

    if c4.button("Reload"):
        systemctl("daemon-reload", "")
        st.rerun()

    if c5.button("Reset Failed"):
        systemctl("reset-failed", SERVICE_NAME)
        systemctl("reset-failed", TIMER_NAME)
        st.rerun()

    # ------------------------------------------------------------
    # Logs
    # ------------------------------------------------------------

    st.markdown(f"### Last {LOG_LINES} Log Entries")

    logs = get_logs(SERVICE_NAME, LOG_LINES)

    st.text_area(
        "Logs",
        logs,
        height=400
    )
