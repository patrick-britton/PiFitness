#!/usr/bin/env python3
"""
deploy/scripts/lib/step_preflight.py  --  Step 1 of the TEST and PRD runs.

    step_preflight.py --run-dir <dir>

Proves, WITHOUT changing anything, that the run can succeed, and (TEST only)
collects the one decision the developer may have to make.  Strictly read-only.

Uses only what exists before checkout: autodeploy.conf and the Pi env file.
NO DEFAULTS: machine-specific values (paths, DB settings, backup dir/glob, ...) are read
ONLY from the Pi env file (test.env and prd.env have IDENTICAL keys; a TEST run reads PRD,
read-only, through the single load_prd_source() in common.py); repo-specific values (units, tools, table/marker names,
timeouts, ...) ONLY from autodeploy.conf. A missing file, a malformed line or a missing
key fails the step; nothing is guessed or hard-coded here.

TEST run                                   PRD run
  config + env keys                          config + env keys
  terminal (required)                        terminal (reported only)
  tools                                      tools
  paths                                      paths
  units (defined + running + pairing)        units (defined + running + pairing)
  PRD database + migrations                  PRD database (reachable/writable)
  TEST database, markers, comparison         PRD state marker (stale dirty?)
  newest nightly snapshot                    plan
  classify -> decide (prompt if compatible)
  plan

Decision handed to later steps (written to run.state):
    DB_CLASSIFICATION   compatible | incompatible | unusable | n/a
    DB_CLASSIFICATION_REASON
    DB_ACTION           keep | restore_snapshot | fresh_clone | none
    DB_SNAPSHOT_FILE    newest nightly dump (TEST), else empty
    DB_SNAPSHOT_USABLE  yes | no
    DB_SNAPSHOT_MIGRATIONS_MATCH_PRD   yes | no | unknown
    TEST_STATE_AT_START / PRD_STATE_AT_START   clean|dirty|migrated|absent|unreadable
    PRD_STALE_DIRTY     yes | no   (PRD run)
    PRD_BASELINE_DUMP   recorded baseline when PRD_STALE_DIRTY=yes
    SERVICES_LIST       *.service files in the commit's deploy/systemd (space-separated)
    TIMERS_LIST         *.timer files in the commit's deploy/systemd   (space-separated)
    ACTIVE_SERVICES     every PiFitness service / timer running on the Pi when preflight ran
    ACTIVE_TIMERS       (UNIT_NAME_GLOB), including units named like the listed ones: what a
                        later step stops and restores. Space-separated; empty when none.
                        Stop timers first, then services. Start and restore use timers plus
                        services with no same-named timer; a timer-paired service is never
                        started directly (see common.startable_units).
    PREFLIGHT           PASS

Exit: 0 pass, 1 fail (REASON:/WHERE: printed).  Never 10 (reserved).
Python 3 standard library only.
"""
import difflib
import glob
import os
import re
import shutil
import sys
import time

from common import (ACTIVE_UNIT_STATES as ACTIVE_STATES, HelperError, Parser, PgError,
                    PgTarget, discover_unit_files, evidence, human_age,
                    human_size, info, is_inside, list_matching_units, load_context,
                    load_prd_source, nearest_existing_ancestor, open_tty, orphan_timers,
                    parse_kv_file, psql_rows, read_db_marker, run_cmd, standalone_services,
                    startable_units, step_main, systemd_unit_dir, update_state, warn,
                    words, writable_dir)
from unitctl import CONF_KEYS as UNIT_CONF_KEYS, CONF_MAY_BE_EMPTY as UNIT_CONF_EMPTY_OK, UnitEnv
import fnmatch

INTRINSIC_TOOLS = ("psql", "pg_dump", "pg_restore", "systemctl")  # this helper needs them
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
PLAIN_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TOC_TABLE_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+TABLE\s+(?!DATA\s)(\S+)\s+(\S+)\s")
SAMPLE_LINES = 20
TOOLS_CONF = "tools.conf"       # beside autodeploy.conf; step_dependencies.py reads the same file


def read_installable_tools(script_dir):
    """INSTALLABLE_TOOLS from tools.conf -> (path, [names])."""
    path = os.path.join(script_dir, TOOLS_CONF)
    values = parse_kv_file(path)
    if "INSTALLABLE_TOOLS" not in values:
        raise HelperError("key INSTALLABLE_TOOLS is not in tools.conf", path)
    return path, words(values["INSTALLABLE_TOOLS"])


def parse_args(argv):
    parser = Parser(prog="step_preflight.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def normalize_schema(text):
    """Schema-only dump -> comparable lines (drop comments, SETs, \\restrict tokens)."""
    lines = []
    for raw in text.splitlines():
        line = raw.rstrip()
        if (not line or line.startswith("--") or line.startswith("\\restrict")
                or line.startswith("\\unrestrict") or line.startswith("SET ")
                or line.startswith("SELECT pg_catalog.set_config")):
            continue
        lines.append(line)
    return lines


def toc_has_table(toc_text, schema, table):
    """True if a pg_restore --list TOC defines TABLE <schema>.<table> (exact schema)."""
    for line in toc_text.splitlines():
        match = TOC_TABLE_RE.match(line)
        if match and match.group(1) == schema and match.group(2) == table:
            return True
    return False


def parse_migration_versions(dump_text, schema, table):
    """Versions from the COPY block of <schema>.<table> in a pg_restore text dump.

    Matches the exact schema-qualified name, so a same-named table in another schema
    (e.g. the legacy _migrations schema) can never be picked up.
    """
    versions, inside = [], False
    prefix = "COPY %s.%s " % (schema, table)
    for line in dump_text.splitlines():
        if not inside:
            if line.startswith(prefix):
                inside = True
            continue
        if line == "\\.":
            return versions
        versions.append(line.split("\t")[0])
    return versions if inside else None


class Preflight:
    def __init__(self, ctx):
        self.ctx = ctx
        self.is_test = ctx.environment == "test"
        self.per = "TEST" if self.is_test else "PRD"
        self.decision = {}
        self.unit_state = {}            # unit -> (LoadState, ActiveState), for the listed units
        self.services_list = []         # *.service the commit defines (deploy/systemd)
        self.timers_list = []           # *.timer the commit defines
        self.defined_units = []         # timers + services of those two lists
        self.start_units = []           # what a deploy starts: standalone services, then timers
        self.restore_units = []         # what a restore starts (same rule, from the active set)
        self.active_services = []       # PiFitness services running at the start
        self.active_timers = []         # PiFitness timers running at the start
        self.active_units = []          # timers + services, in stop order
        self.test_target = None
        self.prd_target = None
        self.prd_versions = None
        self.snapshot = {"file": "", "usable": False, "versions": None}

    # ------------------------------------------------------------------ run
    def run(self):
        stages = [("Configuration and Pi env file", self.stage_config),
                  ("Terminal", self.stage_terminal),
                  ("Required tools", self.stage_tools),
                  ("Paths", self.stage_paths),
                  ("systemd units", self.stage_units),
                  ("PRD database", self.stage_prd_database)]
        if self.is_test:
            stages += [("TEST database", self.stage_test_database),
                       ("Newest nightly snapshot", self.stage_snapshot),
                       ("Decision", self.stage_decide)]
        else:
            stages += [("PRD state marker", self.stage_prd_marker)]
        stages.append(("Plan", self.stage_plan))

        total = len(stages)
        for number, (title, function) in enumerate(stages, 1):
            info("(%d/%d) %s ..." % (number, total, title))
            function()
        self.decision["PREFLIGHT"] = "PASS"
        update_state(self.ctx.run_dir, self.decision)
        evidence("preflight", "ready: nothing was changed")

    # ------------------------------------------------------------- 1. config
    def stage_config(self):
        c = self.ctx

        # Pi env file ONLY. test.env and prd.env have identical keys; only values differ.
        env_keys = ["APP_ENV", "LOG_DIR", "PG_HOST", "PG_PORT", "PG_USER", "PG_DB",
                    "APP_DIR", "VENV_DIR", "STATE_FILE", "PG_BACKUP_LOCATION", "BACKUP_GLOB"]
        env_empty_ok = ["PG_PASSWORD"]
        # autodeploy.conf ONLY (repo-specific). No defaults here either.
        conf_keys = ["REQUIRED_TOOLS", "MIGRATIONS_TABLE", "MIGRATIONS_SCHEMA"] + [k for k in UNIT_CONF_KEYS]
        conf_empty_ok = ["PERSISTENT_RESOURCES"] + list(UNIT_CONF_EMPTY_OK)
        if self.is_test:        # only the TEST run restores/compares/snapshots
            conf_keys += ["TEST_DB_MARKER", "BACKUP_WARN_AGE_HOURS",
                          "ENV_FILE_DIR", "PRD_APP_ENV"]
        c.require(env=env_keys, env_may_be_empty=env_empty_ok,
                  conf=conf_keys, conf_may_be_empty=conf_empty_ok)

        # Tools the dependencies step installs (tools.conf): reported present/missing, never failed.
        self.tools_conf_path, self.installable_tools = read_installable_tools(c.script_dir)

        # This run's own env file (the file IS the environment)
        self.log_dir = c.env_value("LOG_DIR")
        self.backup_dir = c.env_value("PG_BACKUP_LOCATION")
        self.app_dir = c.env_value("APP_DIR")
        self.venv_dir = c.env_value("VENV_DIR")
        self.state_file = c.env_value("STATE_FILE")
        self.test_state_file = self.state_file if self.is_test else None

        # autodeploy.conf values
        self.systemctl_cmd = c.conf_words("SYSTEMCTL")

        self.migrations_table = c.conf_value("MIGRATIONS_TABLE")
        self.migrations_schema = c.conf_value("MIGRATIONS_SCHEMA")
        for key, value in (("MIGRATIONS_TABLE", self.migrations_table),
                           ("MIGRATIONS_SCHEMA", self.migrations_schema)):
            if not PLAIN_IDENT_RE.match(value):
                raise HelperError("%s must be a plain, unqualified name: %r" % (key, value),
                                  c.conf_path)
        self.migrations_fqn = "%s.%s" % (self.migrations_schema, self.migrations_table)

        self.nightly_dir = self.nightly_glob = None
        if self.is_test:
            self.marker = c.conf_value("TEST_DB_MARKER")
            try:
                self.warn_age_hours = float(c.conf_value("BACKUP_WARN_AGE_HOURS"))
                if self.warn_age_hours <= 0:
                    raise ValueError
            except ValueError:
                raise HelperError("BACKUP_WARN_AGE_HOURS must be a positive number (got %r)"
                                  % c.conf_value("BACKUP_WARN_AGE_HOURS"), c.conf_path)

            # PRD is only ever READ by a TEST run, and only through this one loader.
            source = load_prd_source(c)
            self.prd_target = source.target
            self.nightly_dir = source.backup_dir
            self.nightly_glob = source.backup_glob
            self.test_target = PgTarget.from_values("TEST", c.envvals, c.env_path)
            evidence("prd_source_env_file", "%s (read-only use: DB connection + nightly dumps)"
                     % source.env_path)
        else:
            self.prd_target = PgTarget.from_values("PRD", c.envvals, c.env_path)

        evidence("environment", c.environment)
        evidence("app_env", c.env_value("APP_ENV"))
        evidence("commit", c.commit)
        evidence("config_file", c.conf_path)
        evidence("env_file", c.env_path)
        evidence("tools_file", self.tools_conf_path)
        evidence("required_keys_checked", "%d (all present)" % (
            len(env_keys) + len(env_empty_ok) + len(conf_keys) + len(conf_empty_ok)))

    # ----------------------------------------------------------- 2. terminal
    def stage_terminal(self):
        tty = open_tty()
        available = tty is not None
        if tty is not None:
            tty.close()
        evidence("terminal_available", "yes" if available else "no")
        if self.is_test:
            # /dev/tty is used by step_manual_gate for the GO/NO-GO prompt.
            # When no tty is present (e.g. launched via DevHub), step_manual_gate
            # falls back to reading from stdin, which DevHub pipes interactively.
            if not available:
                evidence("terminal_mode", "stdin (DevHub interactive mode)")
            else:
                evidence("terminal_mode", "tty (direct terminal)")
        if not self.is_test:
            evidence("terminal_required", "no (PRD runs prompt nobody)")

    # -------------------------------------------------------------- 3. tools
    def stage_tools(self):
        installable = set(self.installable_tools)
        required = list(INTRINSIC_TOOLS)
        for tool in self.ctx.conf_words("REQUIRED_TOOLS"):
            if tool not in required and tool not in installable:
                required.append(tool)
        reported_only = [t for t in self.installable_tools if t not in required]

        missing = []
        for tool in required + reported_only:
            path = shutil.which(tool)
            if path is None:
                if tool in reported_only:
                    evidence("tool_" + tool, "MISSING (installed by the dependencies step)")
                else:
                    evidence("tool_" + tool, "MISSING")
                    missing.append(tool)
                continue
            version = run_cmd([path, "--version"], timeout=15)
            line = (version.stdout.strip().splitlines() or version.stderr.strip().splitlines()
                    or ["version unknown"])[0]
            evidence("tool_" + tool, "%s (%s)" % (path, line.strip()))
        if missing:
            raise HelperError("required tool(s) not found on PATH: %s" % ", ".join(missing),
                              "PATH=%s" % os.environ.get("PATH", ""))

    # -------------------------------------------------------------- 4. paths
    def stage_paths(self):
        problems = []

        def need_dir(label, path):
            ok = writable_dir(path)
            evidence(label, "%s (%s)" % (path, "exists, writable" if ok else "MISSING or NOT WRITABLE"))
            if not ok:
                problems.append("%s: %s" % (label, path))

        def need_creatable(label, path):
            ancestor = nearest_existing_ancestor(path)
            exists = os.path.exists(path)
            ok = writable_dir(path if os.path.isdir(path) else ancestor)
            note = "exists" if exists else "will be created under %s" % ancestor
            evidence(label, "%s (%s; %s)" % (path, note, "writable" if ok else "NOT WRITABLE"))
            if not ok:
                problems.append("%s: %s" % (label, path))

        need_dir("log_dir", self.log_dir)
        need_dir("backup_dir", self.backup_dir)
        if self.is_test:        # nightlies are only READ; they may live elsewhere than baselines
            readable = os.path.isdir(self.nightly_dir) and os.access(self.nightly_dir, os.R_OK | os.X_OK)
            evidence("nightly_dir", "%s (%s)" % (self.nightly_dir,
                                                  "exists, readable" if readable else "MISSING or NOT READABLE"))
            if not readable:
                problems.append("nightly_dir: %s" % self.nightly_dir)
        need_dir("app_dir_parent", os.path.dirname(os.path.abspath(self.app_dir)))
        evidence("app_dir", "%s (%s)" % (self.app_dir, "exists" if os.path.isdir(self.app_dir)
                                          else "does not exist yet"))
        need_creatable("venv_dir", self.venv_dir)
        need_creatable("state_file_dir", os.path.dirname(os.path.abspath(self.state_file)))
        if os.path.exists(self.state_file) and not os.access(self.state_file, os.W_OK):
            problems.append("state file is not writable: %s" % self.state_file)

        for label, path in (("venv_dir", self.venv_dir), ("state_file", self.state_file)):
            if is_inside(path, self.app_dir):
                problems.append("%s is INSIDE the app dir and would be destroyed by the "
                                "checkout swap: %s" % (label, path))
        if problems:
            raise HelperError("path check failed: %s" % "; ".join(problems),
                              "%s / %s" % (self.ctx.conf_path, self.ctx.env_path))

        resources = self.ctx.conf_words("PERSISTENT_RESOURCES")
        if not resources:
            evidence("persistent_resources", "none configured")
        for item in resources:
            path = item if os.path.isabs(item) else os.path.join(self.app_dir, item)
            if os.path.exists(path):
                evidence("persistent_resource", "%s (present)" % path)
            else:
                warn("persistent resource not found: %s (the checkout step will have nothing to carry)" % path)
                evidence("persistent_resource", "%s (MISSING)" % path)

    # -------------------------------------------------------------- 5. units
    def systemctl_show(self, unit):
        argv = self.systemctl_cmd + [
            "show", "--property=LoadState,ActiveState", unit]
        result = run_cmd(argv, timeout=30)
        if not result.ok:
            raise HelperError("cannot query unit %s: %s" % (unit, result.first_error_line()),
                              result.command())
        values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        return values.get("LoadState", "unknown"), values.get("ActiveState", "unknown")

    def stage_units(self):
        """Work out the two unit sets at run time. Nothing is listed in configuration.

        SERVICES_LIST / TIMERS_LIST   the *.service / *.timer files in the commit's
                                      deploy/systemd: the units the commit defines
                                      (what a deploy starts).
        ACTIVE_SERVICES / ACTIVE_TIMERS  every PiFitness unit running on the Pi right now
                                      (UNIT_NAME_GLOB says which units are PiFitness's),
                                      including units named like the listed ones: what a
                                      later step stops and restores.

        Timer/service pairing: X.timer is paired with X.service. An orphan timer fails
        the step. Stop order is timers then services; start and restore use timers plus
        services that have no same-named timer (a paired service is never started
        directly).
        """
        c = self.ctx
        pattern = c.conf_value("UNIT_NAME_GLOB")
        if pattern[0] in "*?[":
            raise HelperError("UNIT_NAME_GLOB must start with literal text (got %r): a pattern "
                              "that matches unrelated units would have them stopped" % pattern,
                              c.conf_path)

        # --- units the commit defines
        unit_dir = systemd_unit_dir(c.script_dir)
        services, timers, ignored, note = discover_unit_files(unit_dir)
        self.services_list, self.timers_list = services, timers
        self.defined_units = timers + services
        evidence("units_dir", "%s (%s)" % (unit_dir, note or "%d service(s), %d timer(s)"
                                           % (len(services), len(timers))))
        evidence("services_list", " ".join(services) or "none")
        evidence("timers_list", " ".join(timers) or "none")
        for entry, reason in ignored:
            evidence("units_dir_ignored", "%s (%s)" % (entry, reason))
        if note and self.is_test:
            warn("this commit defines no systemd units (%s in %s): either the commit has no "
                 "deploy/systemd, or the launcher did not extract it. A TEST run would have "
                 "nothing to start." % (note, unit_dir))

        # --- pairing: every timer needs a same-named service
        orphans = orphan_timers(services, timers)
        if orphans:
            raise HelperError("timer(s) in deploy/systemd have no same-named .service: %s"
                              % ", ".join(orphans), unit_dir)
        self.start_units = startable_units(services, timers)
        evidence("start_units", " ".join(self.start_units) or "none")
        evidence("paired_services_not_started_directly", " ".join(
            s for s in services if s not in self.start_units) or "none")

        # --- state of each defined unit. Units are INSTALLED by the start step, so
        #     "not found" is normal here and is only reported.
        for unit in self.defined_units:
            load, active = self.unit_state[unit] = self.systemctl_show(unit)
            evidence("unit_" + unit, "%s, %s" % (load, active))

        # --- units running on the Pi now
        matching = list_matching_units(self.systemctl_cmd, pattern)
        evidence("unit_name_glob", "%s (%d loaded unit(s) match)" % (pattern, len(matching)))
        defined = set(self.defined_units)
        for unit, load, active in matching:
            if unit not in defined:                      # defined ones were shown above
                evidence("unit_" + unit, "%s, %s" % (load, active))
        # Everything active is recorded, including units named like the commit's own:
        # they are what is serving now, and they are what gets stopped and restored.
        running = [u for u, _load, active in matching if active in ACTIVE_STATES]
        self.active_services = [u for u in running if u.endswith(".service")]
        self.active_timers = [u for u in running if u.endswith(".timer")]
        self.active_units = self.active_timers + self.active_services      # stop order
        evidence("active_services", " ".join(self.active_services) or "none")
        evidence("active_timers", " ".join(self.active_timers) or "none")

        # Restore set: active timers plus active services with no same-named timer. A
        # timer counts whether or not it was active (a service whose timer was stopped
        # by hand is still timer-paired and is not started directly).
        known_timers = set(timers) | {u for u, _l, _a in matching if u.endswith(".timer")}
        self.restore_units = (standalone_services(self.active_services, known_timers)
                              + self.active_timers)
        evidence("restore_units", " ".join(self.restore_units) or "none")

        self.decision.update({
            "SERVICES_LIST": " ".join(self.services_list),
            "TIMERS_LIST": " ".join(self.timers_list),
            "ACTIVE_SERVICES": " ".join(self.active_services),
            "ACTIVE_TIMERS": " ".join(self.active_timers)})

        self.check_unit_handling(pattern, unit_dir)

    def check_unit_handling(self, pattern, unit_dir):
        """Read-only proof that stop -> move aside -> install -> start can succeed."""
        c = self.ctx
        ue = UnitEnv(c)
        evidence("unit_install_dir", ue.install_dir)
        evidence("privilege_cmd", " ".join(ue.priv) or "(none: running as the unit owner)")
        ue.check_privilege()
        evidence("unit_dir_writable_with_privilege", "yes")

        # every unit the commit defines must be findable (and so displaceable) next time
        strays = [u for u in self.defined_units if not fnmatch.fnmatchcase(u, pattern)]
        if strays:
            raise HelperError("unit(s) in deploy/systemd do not match UNIT_NAME_GLOB %r, so a "
                              "later run could not find and replace them: %s"
                              % (pattern, ", ".join(strays)), unit_dir)

        # the commit's units must render completely for THIS environment
        if self.defined_units:
            ue.render(self.defined_units)
            evidence("units_render", "ok (placeholders: %s)" % (" ".join(ue.placeholders) or "none"))
        else:
            warn("this commit defines no units: the start step will refuse to run")

        # every existing PiFitness unit file must be one the stop step can move aside
        displaced = ue.plan_displaced()
        evidence("unit_files_to_move_aside", " ".join(e["name"] for e in displaced) or "none")
        self.decision["UNITS_TO_DISPLACE"] = " ".join(e["name"] for e in displaced)

    # ------------------------------------------------- applied migrations
    def read_applied_versions(self, target, label):
        """Versions dbmate has applied in <target>; [] when the table does not exist yet.

        A missing table is the normal state before the FIRST dbmate run: dbmate creates it
        itself and applies every migration in the repo. Nothing is created here.

        Only <MIGRATIONS_SCHEMA>.<MIGRATIONS_TABLE> is ever consulted, always schema-qualified,
        so a same-named table in another schema (the legacy _migrations schema) is ignored.
        dbmate puts its table in the connection's current_schema(); if that is not
        MIGRATIONS_SCHEMA, dbmate would create/read the table somewhere else, so fail now.
        Connection or permission errors still fail the step: only "table absent"
        (decided by to_regclass, not by matching error text) means "nothing applied".
        """
        try:
            current = psql_rows(target, "SELECT current_schema()")[0][0]
            if current != self.migrations_schema:
                raise HelperError(
                    "%s connects with current_schema() = %s but MIGRATIONS_SCHEMA is %s; dbmate "
                    "would keep its table in %s" % (label, current or "(none)",
                                                    self.migrations_schema, current or "(none)"),
                    "role search_path of %s" % target.describe())
            exists = psql_rows(target, "SELECT to_regclass('%s') IS NOT NULL"
                               % self.migrations_fqn)[0][0] == "t"
            if not exists:
                evidence("%s_migrations_table" % label.lower(),
                         "%s absent (treated as 0 applied: first dbmate run)" % self.migrations_fqn)
                return []
            rows = psql_rows(target, "SELECT version FROM %s ORDER BY version" % self.migrations_fqn)
        except PgError as exc:
            raise HelperError("cannot read %s migrations table %s: %s"
                              % (label, self.migrations_fqn, exc.reason), exc.where)
        evidence("%s_migrations_table" % label.lower(), "%s present" % self.migrations_fqn)
        return [r[0] for r in rows]

    # ----------------------------------------------------------- 6. PRD DB
    def stage_prd_database(self):
        prd = self.prd_target
        try:
            rows = psql_rows(prd, (
                "SELECT current_database(), current_user, current_setting('server_version'), "
                "current_setting('transaction_read_only'), pg_is_in_recovery()::text, "
                "coalesce(shobj_description((SELECT oid FROM pg_database "
                "WHERE datname = current_database()), 'pg_database'), '')"))
        except PgError as exc:
            raise HelperError("PRD database is not reachable: %s" % exc.reason, exc.where)
        dbname, user, version, read_only, in_recovery, comment = rows[0]
        evidence("prd_database", "%s (PostgreSQL %s)" % (prd.describe(), version))
        evidence("prd_read_only", read_only)
        evidence("prd_in_recovery", in_recovery)
        if read_only != "off" or in_recovery != "false":
            raise HelperError("PRD database is not writable (read_only=%s, in_recovery=%s)"
                              % (read_only, in_recovery), prd.describe())
        if self.is_test and comment == self.marker:
            raise HelperError("the PRD database carries the TEST marker; refusing to continue",
                              prd.describe())

        # PRD may be un-migrated (no table yet): that is "0 applied", never a failure.
        self.prd_versions = self.read_applied_versions(prd, "PRD")
        evidence("prd_migrations", "latest %s, count %d" % (
            self.prd_versions[-1] if self.prd_versions else "(none)", len(self.prd_versions)))

        if self.is_test:
            test = self.test_target
            if test.same_database_as(prd):
                raise HelperError("the TEST connection target is the PRD database "
                                  "(%s); refusing to continue" % prd.describe(),
                                  "PG_HOST/PG_PORT/PG_DB in %s and the PRD env file" % self.ctx.env_path)
            evidence("test_target", test.describe())
            evidence("test_target_is_prd", "no")

    # ------------------------------------------------------ PRD marker (PRD)
    def stage_prd_marker(self):
        state, values = read_db_marker(self.state_file)
        evidence("prd_state_file", self.state_file)
        evidence("prd_state_at_start", state)
        self.decision["PRD_STATE_AT_START"] = state
        self.decision["PRD_STALE_DIRTY"] = "yes" if state == "dirty" else "no"
        self.decision["DB_CLASSIFICATION"] = "n/a"
        self.decision["DB_ACTION"] = "none"
        if state == "unreadable":
            warn("PRD state file is not in a recognised format; treating it as a hint only")
        if state == "dirty":
            baseline = values.get("baseline_dump", "")
            if not baseline or not os.path.isfile(baseline):
                raise HelperError("PRD state marker is 'dirty' (interrupted run) but its recorded "
                                  "baseline dump is %s, so the database cannot be recovered" % (
                                      "missing: " + baseline if baseline else "not recorded"),
                                  self.state_file)
            warn("stale 'dirty' PRD marker from an interrupted run: PRD step 5 will RESTORE the "
                 "database from the recorded baseline")
            evidence("prd_baseline_dump", "%s (%s)" % (baseline, human_size(os.path.getsize(baseline))))
            self.decision["PRD_BASELINE_DUMP"] = baseline
        else:
            evidence("prd_stale_dirty", "no")

    # --------------------------------------------------------- TEST database
    def stage_test_database(self):
        test, unusable = self.test_target, []
        state, values = read_db_marker(self.test_state_file)
        evidence("test_state_at_start", state)
        self.decision["TEST_STATE_AT_START"] = state
        if state == "unreadable":
            warn("TEST state file is not in a recognised format; it is only a hint, "
                 "the database itself is inspected")
        if state == "dirty":
            unusable.append("TEST state file says 'dirty' (interrupted restore or crash)")

        self.test_versions = None
        self.schema_diff = None
        if not unusable:
            self.inspect_test(unusable)
        else:
            evidence("test_database", "not inspected (%s)" % unusable[0])

        baseline = values.get("baseline_dump", "")
        if not unusable:
            if baseline and os.path.isfile(baseline):
                age = time.time() - os.path.getmtime(baseline)
                evidence("test_clean_baseline", "%s (%s, %s old)" % (
                    baseline, human_size(os.path.getsize(baseline)), human_age(age)))
            else:
                unusable.append("no clean baseline dump recorded/found (%s), so a rollback "
                                "would be impossible" % (baseline or "baseline_dump not in state file"))

        self.unusable = unusable
        if unusable:
            for reason in unusable:
                evidence("test_unusable_because", reason)
        self.compare()

    def inspect_test(self, unusable):
        test = self.test_target
        try:
            rows = psql_rows(test, (
                "SELECT current_database(), coalesce(shobj_description((SELECT oid FROM pg_database "
                "WHERE datname = current_database()), 'pg_database'), '')"))
        except PgError as exc:
            unusable.append("TEST database unreachable or missing: %s" % exc.reason)
            evidence("test_database", "NOT REACHABLE (%s)" % exc.reason)
            return
        dbname, comment = rows[0]
        evidence("test_database", test.describe())
        if dbname == self.prd_target.dbname and test.same_database_as(self.prd_target):
            raise HelperError("TEST connection reports the PRD database; refusing to continue",
                              test.describe())
        if comment != self.marker:
            unusable.append("TEST database lacks the TEST marker")
        evidence("test_marker", "present" if comment == self.marker else "MISSING")

        try:
            self.test_versions = self.read_applied_versions(test, "TEST")
        except HelperError as exc:
            unusable.append("TEST migrations unreadable: %s" % exc.reason)
            self.test_versions = None
            return

        prd_schema = self.dump_schema(self.prd_target, fatal=True)
        test_schema = self.dump_schema(test, fatal=False)
        if test_schema is None:
            unusable.append("TEST schema could not be dumped")
            self.test_versions = None
            return
        diff = [l for l in difflib.unified_diff(prd_schema, test_schema, "PRD", "TEST",
                                                n=0, lineterm="")
                if l[:1] in "+-" and not l.startswith(("+++", "---"))]
        self.schema_diff = diff

    def dump_schema(self, target, fatal):
        argv = ["pg_dump", "--schema-only", "--no-owner", "--no-privileges",
                "--no-tablespaces", "--no-security-labels", "--no-password"]
        result = run_cmd(argv, extra_env=target.env(), timeout=180)
        if not result.ok:
            if fatal:
                raise HelperError("pg_dump --schema-only of %s failed: %s"
                                  % (target.label, result.first_error_line()), result.command())
            return None
        return normalize_schema(result.stdout)

    def compare(self):
        prd = self.prd_versions
        test = self.test_versions
        if test is None:
            evidence("test_migrations", "not available")
            return
        test_only = sorted(set(test) - set(prd))
        prd_only = sorted(set(prd) - set(test))
        self.test_only, self.prd_only = test_only, prd_only
        evidence("test_migrations", "latest %s, count %d" % (test[-1] if test else "(none)", len(test)))
        evidence("versions_in_test_not_prd", ", ".join(test_only) or "none")
        evidence("versions_in_prd_not_test", ", ".join(prd_only) or "none")
        evidence("schema_diff_lines", len(self.schema_diff))
        if self.schema_diff:
            info("schema differences (- only in PRD, + only in TEST), first %d of %d:"
                 % (min(SAMPLE_LINES, len(self.schema_diff)), len(self.schema_diff)))
            for line in self.schema_diff[:SAMPLE_LINES]:
                info("    " + line[:140])

    # ---------------------------------------------------------- snapshot
    def stage_snapshot(self):
        pattern = os.path.join(self.nightly_dir, self.nightly_glob)
        files = [f for f in glob.glob(pattern) if os.path.isfile(f)]
        evidence("nightly_snapshots_found", len(files))
        if not files:
            evidence("newest_snapshot", "none (%s)" % pattern)
            return
        newest = max(files, key=os.path.getmtime)
        size = os.path.getsize(newest)
        age = time.time() - os.path.getmtime(newest)
        evidence("newest_snapshot", "%s (%s, %s old)" % (os.path.basename(newest),
                                                          human_size(size), human_age(age)))
        self.snapshot["file"] = newest
        limit = self.warn_age_hours
        if age > limit * 3600:
            warn("newest nightly snapshot is older than %g hours" % limit)

        problem, toc_text = None, ""
        if size == 0:
            problem = "file is empty"
        else:
            toc = run_cmd(["pg_restore", "--list", newest], timeout=120)
            if not toc.ok:
                problem = "pg_restore cannot read it: %s" % toc.first_error_line()
            else:
                toc_text = toc.stdout
        if problem:
            evidence("snapshot_usable", "no (%s)" % problem)
            warn("newest nightly snapshot is not usable: %s" % problem)
            return
        self.snapshot["usable"] = True
        evidence("snapshot_usable", "yes")

        if not toc_has_table(toc_text, self.migrations_schema, self.migrations_table):
            versions = []       # snapshot predates dbmate: 0 applied
            evidence("snapshot_migrations_table", "%s absent (treated as 0 applied)" % self.migrations_fqn)
        else:
            data = run_cmd(["pg_restore", "--data-only", "--schema=" + self.migrations_schema,
                            "--table=" + self.migrations_table, "--file=-", newest], timeout=300)
            versions = (parse_migration_versions(data.stdout, self.migrations_schema,
                                                 self.migrations_table) if data.ok else None)
        self.snapshot["versions"] = versions
        if versions is None:
            evidence("snapshot_migrations", "could not be read")
            warn("could not read migration versions from the snapshot")
            return
        evidence("snapshot_migrations", "latest %s, count %d" % (
            versions[-1] if versions else "(none)", len(versions)))
        if set(versions) != set(self.prd_versions):
            snap_only = sorted(set(versions) - set(self.prd_versions))
            live_only = sorted(set(self.prd_versions) - set(versions))
            evidence("snapshot_matches_live_prd", "no")
            warn("snapshot migration versions differ from live PRD (only in snapshot: %s; only in PRD: %s); "
                 "a restore from it will need migrations applied" % (
                     ", ".join(snap_only) or "none", ", ".join(live_only) or "none"))
        else:
            evidence("snapshot_matches_live_prd", "yes")

    # ------------------------------------------------------------- decide
    def stage_decide(self):
        snap = self.snapshot
        if self.unusable:
            cls, reason = "unusable", "; ".join(self.unusable)
        elif self.test_only:
            cls, reason = "incompatible", "TEST has %d migration(s) PRD does not have" % len(self.test_only)
        elif not self.prd_only and self.schema_diff:
            cls, reason = "incompatible", ("TEST and PRD have the same migrations but the schema "
                                           "differs (%d lines)" % len(self.schema_diff))
        else:
            cls = "compatible"
            reason = ("TEST is identical to PRD" if not self.prd_only
                      else "TEST is behind PRD by %d migration(s)" % len(self.prd_only))
        evidence("test_classification", "%s (%s)" % (cls, reason))

        if cls == "compatible":
            action = "keep"
            evidence("decision_source", "default (keep)")
        else:
            action = "restore_snapshot"
            evidence("decision_source", "automatic (%s)" % cls)

        if action != "keep" and not snap["usable"]:
            raise HelperError("database action '%s' needs a usable PRD nightly backup but there is "
                              "none in %s" % (action, self.nightly_dir),
                              os.path.join(self.nightly_dir, self.nightly_glob))
        evidence("decision", "database action = %s" % action)
        self.decision.update({
            "DB_CLASSIFICATION": cls, "DB_CLASSIFICATION_REASON": reason, "DB_ACTION": action,
            "DB_SNAPSHOT_FILE": snap["file"], "DB_SNAPSHOT_USABLE": "yes" if snap["usable"] else "no",
            "DB_SNAPSHOT_MIGRATIONS_MATCH_PRD": (
                "unknown" if snap["versions"] is None
                else "yes" if set(snap["versions"]) == set(self.prd_versions) else "no")})

    # --------------------------------------------------------------- plan
    def stage_plan(self):
        prd_active = self.active_units
        if self.is_test:
            action = self.decision["DB_ACTION"]
            text = {"keep": "keep the current TEST database",
                    "restore_snapshot": "restore TEST from %s" % os.path.basename(self.snapshot["file"]),
                    "fresh_clone": "take a fresh clone of PRD into TEST"}[action]
            evidence("plan_database", text)
            evidence("plan_prd_units_stopped_at_step_7", ", ".join(prd_active) or "none active")
            evidence("plan_unit_files_moved_aside_at_step_7", self.decision.get("UNITS_TO_DISPLACE") or "none")
            evidence("plan_units_installed_and_started_at_step_8", ", ".join(self.start_units) or "none")
            evidence("plan_units_restored_on_exit", ", ".join(self.restore_units) or "none (restore set is re-polled at step 7)")
            evidence("plan_input", "no further input needed until step 10 (GO / NO-GO via DevHub or terminal)")
        else:
            evidence("plan_prd_units_stopped_at_step_2", ", ".join(prd_active) or "none active")
            evidence("plan_unit_files_moved_aside_at_step_2", self.decision.get("UNITS_TO_DISPLACE") or "none")
            evidence("plan_units_installed_and_started_at_step_7", ", ".join(self.start_units) or "none")
            evidence("plan_on_success", "old units stay stopped and their files are deleted")
            evidence("plan_on_failure", "old units restored (files, enabled state, running set)")
            evidence("plan_baseline", "decided in step 5 (only if migrations are pending)")
            evidence("plan_input", "none: PRD runs prompt nobody; any failure rolls back automatically")


def main():
    args = parse_args(sys.argv[1:])
    Preflight(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
