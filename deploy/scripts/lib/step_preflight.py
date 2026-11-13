#!/usr/bin/env python3
"""
deploy/scripts/lib/step_preflight.py  --  Step 1 of the TEST and PRD runs.

    step_preflight.py --run-dir <dir>

Proves, WITHOUT changing anything, that the run can succeed, and (TEST only)
collects the one decision the developer may have to make.  The single
exception to "read-only": if any TEST unit is active it runs test-down.sh,
because that is a remediation, not a decision (requirements 4.6 / D19).

Uses only what exists before checkout: autodeploy.conf and the Pi env file.

TEST run                                   PRD run
  config + env keys                          config + env keys
  terminal (required)                        terminal (reported only)
  tools                                      tools
  paths                                      paths
  units exist (TEST + PRD)                   units exist (PRD)
  PRD database + migrations                  PRD database (reachable/writable)
  auto test-down                             PRD state marker (stale dirty?)
  TEST database, markers, comparison         auto test-down
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

from common import (HelperError, Parser, PgError, PgTarget, ask_tty, evidence,
                    human_age, human_size, info, is_inside, load_context,
                    nearest_existing_ancestor, open_tty, parse_kv_file,
                    psql_rows, read_db_marker, run_cmd, step_main, update_state,
                    warn, words, writable_dir)

INTRINSIC_TOOLS = ("psql", "pg_dump", "pg_restore", "systemctl")  # this helper needs them
ACTIVE_STATES = ("active", "activating", "reloading", "deactivating")
DEFAULT_PRD_ENV = "/home/god/.config/pifitness/prd.env"
DEFAULT_BACKUP_DIR = "/home/god/Documents/DB_Backups"
DEFAULT_BACKUP_GLOB = "personal_fitness_[0-9]*.dump"
DEFAULT_MARKER = "pifitness-test-database"
DEFAULT_MIGRATIONS_TABLE = "schema_migrations"
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
SAMPLE_LINES = 20


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


def parse_migration_versions(dump_text, table):
    """Versions from the COPY block of the migrations table in a pg_restore text dump."""
    versions, inside = [], False
    short = table.split(".")[-1]
    for line in dump_text.splitlines():
        if not inside:
            if line.startswith("COPY ") and (" %s " % short in line or "." + short + " " in line):
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
        self.unit_state = {}
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
            stages += [("Remediation: TEST units", self.stage_remediate),
                       ("TEST database", self.stage_test_database),
                       ("Newest nightly snapshot", self.stage_snapshot),
                       ("Decision", self.stage_decide)]
        else:
            stages += [("PRD state marker", self.stage_prd_marker),
                       ("Remediation: TEST units", self.stage_remediate)]
        stages.append(("Plan", self.stage_plan))

        total = len(stages)
        for number, (title, function) in enumerate(stages, 1):
            info("(%d/%d) %s ..." % (number, total, title))
            function()
        self.decision["PREFLIGHT"] = "PASS"
        update_state(self.ctx.run_dir, self.decision)
        evidence("preflight", "ready: nothing was changed%s" % (
            " except TEST units stopped by test-down.sh" if self.test_down_ran else ""))

    test_down_ran = False

    # ------------------------------------------------------------- 1. config
    def stage_config(self):
        c = self.ctx
        per = self.per
        non_empty = ["LOG_DIR", "PG_HOST", "PG_PORT",
                     "PG_USER", "PG_DB", "TEST_SERVICES", "PRD_SERVICES",
                     per + "_APP_DIR", per + "_VENV_DIR", per + "_STATE_FILE"]
        present = ["PG_PASSWORD", "TEST_TIMERS", "PRD_TIMERS"]
        missing = c.missing(non_empty) + c.missing_keys(present)
        if missing:
            raise HelperError("required configuration is missing or empty: %s" % ", ".join(missing),
                              "%s  and  %s" % (c.conf_path, c.env_path))

        self.log_dir = c.get("LOG_DIR")
        self.backup_dir = c.get("PIFITNESS_PRD_BACKUP_DIR") or c.get("DEFAULT_BACKUP_DIR", DEFAULT_BACKUP_DIR)
        self.backup_glob = c.get("PIFITNESS_PRD_BACKUP_GLOB", DEFAULT_BACKUP_GLOB)
        self.app_dir = c.get(per + "_APP_DIR")
        self.venv_dir = c.get(per + "_VENV_DIR")
        self.state_file = c.get(per + "_STATE_FILE")
        self.test_state_file = c.get("TEST_STATE_FILE")
        self.test_units = c.words("TEST_TIMERS") + c.words("TEST_SERVICES")
        self.prd_units = c.words("PRD_TIMERS") + c.words("PRD_SERVICES")
        self.marker = c.get("TEST_DB_MARKER", DEFAULT_MARKER)
        self.migrations_table = c.get("MIGRATIONS_TABLE", DEFAULT_MIGRATIONS_TABLE)
        if not IDENT_RE.match(self.migrations_table):
            raise HelperError("MIGRATIONS_TABLE is not a plain table name: %r" % self.migrations_table,
                              c.conf_path)

        if self.is_test:
            prd_env_path = c.get("PRD_ENV_FILE", DEFAULT_PRD_ENV)
            prd_values = parse_kv_file(prd_env_path)
            gaps = [k for k in ("PG_HOST", "PG_PORT", "PG_USER", "PG_DB") if not prd_values.get(k)]
            if "PG_PASSWORD" not in prd_values:
                gaps.append("PG_PASSWORD")
            if gaps:
                raise HelperError("PRD env file is missing: %s" % ", ".join(gaps), prd_env_path)
            self.prd_target = PgTarget.from_values("PRD", prd_values)
            self.test_target = PgTarget.from_values("TEST", c.merged())
            evidence("prd_env_file", prd_env_path)
        else:
            self.prd_target = PgTarget.from_values("PRD", c.merged())

        evidence("environment", c.environment)
        evidence("commit", c.commit)
        evidence("config_file", c.conf_path)
        evidence("env_file", c.env_path)
        evidence("required_keys_checked", "%d (all present)" % (len(non_empty) + len(present)))

    # ----------------------------------------------------------- 2. terminal
    def stage_terminal(self):
        tty = open_tty()
        available = tty is not None
        if tty is not None:
            tty.close()
        evidence("terminal_available", "yes" if available else "no")
        if self.is_test and not available:
            raise HelperError("no interactive terminal: TEST runs need one for the GO/NO-GO "
                              "prompt (and the database prompt)", "open('/dev/tty')")
        if not self.is_test:
            evidence("terminal_required", "no (PRD runs prompt nobody)")

    # -------------------------------------------------------------- 3. tools
    def stage_tools(self):
        installable = set(self.ctx.words("INSTALLABLE_TOOLS"))
        required = list(INTRINSIC_TOOLS)
        for tool in self.ctx.words("REQUIRED_TOOLS"):
            if tool not in required and tool not in installable:
                required.append(tool)
        reported_only = [t for t in self.ctx.words("INSTALLABLE_TOOLS") if t not in required]

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

        resources = self.ctx.words("PERSISTENT_RESOURCES")
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
        argv = self.ctx.words("SYSTEMCTL", "systemctl") + [
            "show", "--property=LoadState,ActiveState", unit]
        result = run_cmd(argv, timeout=30)
        if not result.ok:
            raise HelperError("cannot query unit %s: %s" % (unit, result.first_error_line()),
                              result.command())
        values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        return values.get("LoadState", "unknown"), values.get("ActiveState", "unknown")

    def stage_units(self):
        must_exist = self.prd_units + (self.test_units if self.is_test else [])
        check = list(dict.fromkeys(must_exist + self.test_units))
        missing = []
        for unit in check:
            load, active = self.unit_state[unit] = self.systemctl_show(unit)
            evidence("unit_" + unit, "%s, %s" % (load, active))
            if unit in must_exist and load != "loaded":
                missing.append("%s (%s)" % (unit, load))
        if missing:
            raise HelperError("systemd unit(s) listed in config do not exist: %s" % ", ".join(missing),
                              "TEST_SERVICES/TEST_TIMERS/PRD_SERVICES/PRD_TIMERS in %s" % self.ctx.conf_path)
        prd_active = [u for u in self.prd_units if self.unit_state[u][1] in ACTIVE_STATES]
        evidence("prd_units_active", ", ".join(prd_active) or "none")

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

        if self.is_test:
            test = self.test_target
            if test.same_database_as(prd):
                raise HelperError("the TEST connection target is the PRD database "
                                  "(%s); refusing to continue" % prd.describe(),
                                  "PG_HOST/PG_PORT/PG_DB in %s and the PRD env file" % self.ctx.env_path)
            evidence("test_target", test.describe())
            evidence("test_target_is_prd", "no")
            try:
                rows = psql_rows(prd, "SELECT version FROM %s ORDER BY version" % self.migrations_table)
            except PgError as exc:
                raise HelperError("cannot read PRD migrations table %s: %s"
                                  % (self.migrations_table, exc.reason), exc.where)
            self.prd_versions = [r[0] for r in rows]
            evidence("prd_migrations", "latest %s, count %d" % (
                self.prd_versions[-1] if self.prd_versions else "(none)", len(self.prd_versions)))

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

    # ----------------------------------------------------- remediation
    def stage_remediate(self):
        active = [u for u in self.test_units if self.unit_state[u][1] in ACTIVE_STATES]
        if self.test_state_file:
            evidence("test_state_file", self.test_state_file)
        if not active:
            evidence("test_units_active", "none; test-down.sh not needed")
            return

        script = self.ctx.get("TEST_DOWN_SCRIPT", "test-down.sh")
        if not os.path.isabs(script):
            script = os.path.join(self.ctx.script_dir, script)
        if not os.path.isfile(script):
            raise HelperError("TEST units are active (%s) but the test-down script does not exist"
                              % ", ".join(active), script)
        info("TEST units are active; running test-down.sh to return to a known state")
        result = run_cmd(["bash", script], timeout=int(self.ctx.get("TEST_DOWN_TIMEOUT", "180")),
                         echo_prefix="    ", heartbeat=30)
        self.test_down_ran = True
        verdict = "PASS" if result.ok else "FAIL"
        evidence("test_units_active", "%s; ran test-down.sh: %s (%.1fs)" % (
            ", ".join(active), verdict, result.seconds))
        if not result.ok:
            raise HelperError("test-down.sh failed: %s" % result.first_error_line(), result.command())

        deadline = time.monotonic() + 30
        while True:
            still = [u for u in self.test_units if self.systemctl_show(u)[1] in ACTIVE_STATES]
            if not still or time.monotonic() > deadline:
                break
            time.sleep(1)
        if still:
            raise HelperError("TEST units still active after test-down.sh: %s" % ", ".join(still),
                              "systemctl show " + " ".join(still))
        evidence("test_units_after", "all inactive")

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
            rows = psql_rows(test, "SELECT version FROM %s ORDER BY version" % self.migrations_table)
        except PgError as exc:
            unusable.append("TEST migrations table unreadable: %s" % exc.reason)
            return
        self.test_versions = [r[0] for r in rows]

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
        pattern = os.path.join(self.backup_dir, self.backup_glob)
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
        limit = float(self.ctx.get("BACKUP_WARN_AGE_HOURS", "36"))
        if age > limit * 3600:
            warn("newest nightly snapshot is older than %g hours" % limit)

        problem = None
        if size == 0:
            problem = "file is empty"
        else:
            toc = run_cmd(["pg_restore", "--list", newest], timeout=120)
            if not toc.ok:
                problem = "pg_restore cannot read it: %s" % toc.first_error_line()
        if problem:
            evidence("snapshot_usable", "no (%s)" % problem)
            warn("newest nightly snapshot is not usable: %s" % problem)
            return
        self.snapshot["usable"] = True
        evidence("snapshot_usable", "yes")

        data = run_cmd(["pg_restore", "--data-only", "--table=" + self.migrations_table.split(".")[-1],
                        "--file=-", newest], timeout=300)
        versions = parse_migration_versions(data.stdout, self.migrations_table) if data.ok else None
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
            prompt = ("\nTEST database is compatible with PRD (%s).\n"
                      "  [K] Keep  [R] Restore most recent nightly  [C] Take a fresh clone\n"
                      "Choice: " % reason)
            evidence("prompt", "database choice [K]eep / [R]estore nightly / [C]lone (waits indefinitely)")
            answer = ask_tty(prompt, {"K", "R", "C"})
            evidence("answer", answer)
            action = {"K": "keep", "R": "restore_snapshot", "C": "fresh_clone"}[answer]
            evidence("decision_source", "developer")
        else:
            action = "restore_snapshot"
            evidence("decision_source", "automatic (%s)" % cls)

        if action != "keep" and not snap["usable"]:
            raise HelperError("database action '%s' needs a usable PRD nightly backup but there is "
                              "none in %s" % (action, self.backup_dir),
                              os.path.join(self.backup_dir, self.backup_glob))
        evidence("decision", "database action = %s" % action)
        self.decision.update({
            "DB_CLASSIFICATION": cls, "DB_CLASSIFICATION_REASON": reason, "DB_ACTION": action,
            "DB_SNAPSHOT_FILE": snap["file"], "DB_SNAPSHOT_USABLE": "yes" if snap["usable"] else "no",
            "DB_SNAPSHOT_MIGRATIONS_MATCH_PRD": (
                "unknown" if snap["versions"] is None
                else "yes" if set(snap["versions"]) == set(self.prd_versions) else "no")})

    # --------------------------------------------------------------- plan
    def stage_plan(self):
        prd_active = [u for u in self.prd_units if self.unit_state[u][1] in ACTIVE_STATES]
        if self.is_test:
            action = self.decision["DB_ACTION"]
            text = {"keep": "keep the current TEST database",
                    "restore_snapshot": "restore TEST from %s" % os.path.basename(self.snapshot["file"]),
                    "fresh_clone": "take a fresh clone of PRD into TEST"}[action]
            evidence("plan_database", text)
            evidence("plan_prd_units_stopped_at_step_7", ", ".join(prd_active) or "none active")
            evidence("plan_input", "no further input is needed until step 10 (GO / NO-GO)")
        else:
            evidence("plan_prd_units_stopped_at_step_2", ", ".join(prd_active) or "none active")
            evidence("plan_baseline", "decided in step 5 (only if migrations are pending)")
            evidence("plan_input", "none: PRD runs prompt nobody; any failure rolls back automatically")


def main():
    args = parse_args(sys.argv[1:])
    Preflight(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
