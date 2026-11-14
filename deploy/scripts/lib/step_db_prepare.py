#!/usr/bin/env python3
"""
deploy/scripts/lib/step_db_prepare.py  --  TEST Step 4: database clone / restore.

    step_db_prepare.py --run-dir <dir>

Objective (spec TEST Step 4 + 4.4): a running TEST database that is a faithful, correctly
marked copy of PRD, with a clean baseline dump saved.

Reads
    run.state           DB_ACTION (keep | restore_snapshot | fresh_clone), DB_SNAPSHOT_FILE,
                        DB_SNAPSHOT_MIGRATIONS_MATCH_PRD   (all written by step_preflight)
    Pi env file         APP_ENV, PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DB, STATE_FILE,
                        PG_BACKUP_LOCATION, BACKUP_GLOB
    autodeploy.conf     TEST_DB_MARKER (+ ENV_FILE_DIR, PRD_APP_ENV, TEST_APP_ENV, which
                        common.load_prd_source and expected_app_env need)
    prd.env             ONLY through common.load_prd_source(): a read-only PRD connection
                        (pg_dump for a fresh clone, SELECTs for the comparison).

What it does
    keep             SKIPPED (not required). Verifies the clean baseline recorded in STATE_FILE
                     still exists and prints its age.
    otherwise        1. checks (all read-only, before anything is changed):
                          TEST is not PRD; the TEST role may drop/create/restore; every
                          extension in the dump can be created; the source dump is readable
                          (fresh_clone: pg_dump -Fc of PRD, which only reads PRD)
                       2. registers the undo action, sets the state marker to dirty
                       3. DROP DATABASE (if it exists), CREATE DATABASE, pg_restore
                       4. COMMENT ON DATABASE = TEST_DB_MARKER; proves the TEST role has
                          full access to what was restored
                       5. compares key-table row counts and the schema with PRD
                       6. pg_dump -Fc the result to test_baseline_<UTC>.dump in
                          PG_BACKUP_LOCATION, records baseline_dump=<path> and sets the
                          marker to clean in ONE atomic write
    On failure       the marker is LEFT dirty, the cause is reported, nothing is retried. The
                     next run's preflight classifies the database as Unusable and restores it.

Restore options (constants): pg_restore --no-owner --no-privileges, so every restored object is
owned by the TEST role and no PRD role needs to exist on the TEST server. pg_restore errors are
fatal, except the well-known harmless "COMMENT ON EXTENSION plpgsql" ownership error that a
non-superuser gets; that one is reported as a WARNING.

UNDO CONTRACT  (undo action name: test_db  ->  lib/undo_test_db.py)   *** NOT BUILT YET ***
    Registered by the first step that can make the TEST database dirty: this step (when it
    restores) and, later, the migrate step. register_undo is idempotent.
    undo_test_db.py --run-dir <dir> --outcome success|incomplete|failure --exit-status <n>
      * outcome == success                          -> exit 2, "SKIPPED: GO, database kept"
      * STATE_FILE marker is not dirty              -> exit 2, nothing to undo
      * run.state DB_PREPARE_STATE == running       -> exit 2, "TEST step 4 did not finish: the
                                                       database stays dirty for the next run"
                                                       (spec 4.4: step 4 failing is not rolled back)
      * otherwise: restore the EXACT file recorded as baseline_dump in STATE_FILE (never "the
        newest"), using TestDatabase.rebuild(dump) from THIS module, then set the marker to
        clean (keeping baseline_dump). A missing baseline or a failed restore is a FAIL.
    run.state keys this step maintains for it:
        DB_PREPARE_STATE   running (set after the undo is registered, before the first change)
                           complete (set when the step passed OR was skipped as 'keep')
        TEST_BASELINE_DUMP path of the baseline after this step
        DB_PREPARE_SOURCE  backup filename or 'fresh clone'

TEST and PRD run the same kind of code: there is no `if environment ==` here. The write target is
protected by checks on the data (database name, server identity, role), not by environment name.

Exit: 0 pass, 1 fail (REASON:/WHERE: printed), 2 skipped.  Never 10 (reserved).
Python 3 standard library only.
"""
import difflib
import fnmatch
import os
import re
import sys
import time

from common import (HelperError, Parser, PgTarget, SkipStep, evidence, expected_app_env,
                    human_age, human_size, info, load_context, load_prd_source, psql_rows,
                    read_db_marker, register_undo, run_cmd, step_main, update_db_marker,
                    update_state, warn, writable_dir)
from step_preflight import normalize_schema      # the SAME normalisation preflight compared with

# Constants: these never vary, so they are not configuration.
MAINT_DB = "postgres"                        # database to connect to while dropping/creating TEST
PROTECTED_DBS = ("postgres", "template0", "template1")   # never dropped, whatever PG_DB says
BASELINE_PREFIX = "test_baseline_"           # spec 4.4 (CLEAN_BASELINE_PREFIX default)
CLONE_PREFIX = "test_clone_source_"          # temporary PRD dump for a fresh clone; deleted after
UNDO_NAME = "test_db"                        # lib/undo_test_db.py (contract above)
STATE_KEY = "DB_PREPARE_STATE"
ACTIONS = ("restore_snapshot", "fresh_clone")
LONG_TIMEOUT = 7200                          # dump / restore, seconds
QUERY_TIMEOUT = 300
HEARTBEAT = 30                               # "still running (Ns)" after this many silent seconds
KEY_TABLES = 5                               # row counts compared for the N biggest tables
SAMPLE_LINES = 20

TOC_EXTENSION_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+EXTENSION\s+-\s+(\S+)")

KEY_TABLES_SQL = (
    "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
    "AND n.nspname !~ '^pg_' AND NOT EXISTS (SELECT 1 FROM pg_depend d "
    "WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e') "
    "ORDER BY pg_total_relation_size(c.oid) DESC LIMIT %d" % KEY_TABLES)

def _all_privileges(function, oid, privileges):
    """SQL: the role holds EVERY listed privilege. (A comma list in has_*_privilege means ANY.)"""
    return "(" + " AND ".join("%s(current_user, %s, '%s')" % (function, oid, p)
                              for p in privileges) + ")"


_USER_REL = ("FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
             "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname !~ '^pg_' ")

ACCESS_SQL = (
    "SELECT "
    "(SELECT count(*) " + _USER_REL + "AND c.relkind IN ('r', 'p') AND NOT "
    + _all_privileges("has_table_privilege", "c.oid", ("SELECT", "INSERT", "UPDATE", "DELETE")) + "), "
    "(SELECT count(*) " + _USER_REL + "AND c.relkind = 'S' AND NOT "
    + _all_privileges("has_sequence_privilege", "c.oid", ("USAGE", "SELECT", "UPDATE")) + "), "
    "(SELECT count(*) " + _USER_REL + "AND c.relkind IN ('r', 'p', 'S', 'v', 'm') "
    "AND NOT pg_has_role(current_user, c.relowner, 'MEMBER')), "
    "(SELECT count(*) " + _USER_REL + "AND c.relkind IN ('r', 'p')), "
    "(" + _all_privileges("has_database_privilege", "current_database()", ("CONNECT", "CREATE")) + ")::text")


# ----------------------------------------------------------------------------
# Small pure helpers
# ----------------------------------------------------------------------------
def quote_ident(name):
    return '"%s"' % name.replace('"', '""')


def quote_literal(text):
    return "'%s'" % text.replace("'", "''")


def utc_stamp():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def elapsed(seconds):
    seconds = int(seconds)
    return "%dm%02ds" % divmod(seconds, 60) if seconds >= 60 else "%ds" % seconds


def reason_from(result):
    """One-line cause from a CmdResult of a command whose output was echoed (merged)."""
    if result.timed_out:
        return "timed out after %s" % elapsed(result.seconds)
    lines = [l.strip() for l in (result.stdout + "\n" + result.stderr).splitlines() if l.strip()]
    for line in lines:
        if "error" in line.lower():
            return line
    return lines[-1] if lines else "exit status %d" % result.rc


def toc_extensions(toc_text):
    """Extension names a `pg_restore --list` TOC will CREATE."""
    return [m.group(1) for m in (TOC_EXTENSION_RE.match(l) for l in toc_text.splitlines()) if m]


def classify_restore_errors(output):
    """pg_restore output -> (tolerated, fatal) lists of one-line descriptions.

    The only tolerated error is COMMENT ON EXTENSION plpgsql ("must be owner of extension"),
    which a non-superuser always gets and which changes nothing that matters.
    """
    tolerated, fatal = [], []
    lines = output.splitlines()
    last_toc, i = "", 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("pg_restore: from TOC entry"):
            last_toc = line
        elif line.startswith("pg_restore: error:"):
            block, j = [line], i + 1
            while j < len(lines) and not lines[j].startswith("pg_restore:"):
                block.append(lines[j].strip())
                j += 1
            text = " ".join(block)
            context = (last_toc + " " + text).lower()
            if "extension plpgsql" in context and "comment" in context:
                tolerated.append(text[:200])
            else:
                fatal.append(text[:300])
            last_toc, i = "", j - 1
        i += 1
    return tolerated, fatal


def schema_lines(target):
    argv = ["pg_dump", "--schema-only", "--no-owner", "--no-privileges", "--no-tablespaces",
            "--no-security-labels", "--no-password"]
    result = run_cmd(argv, extra_env=target.env(), timeout=600)
    if not result.ok:
        raise HelperError("pg_dump --schema-only of %s failed: %s"
                          % (target.label, result.first_error_line()), result.command())
    return normalize_schema(result.stdout)


# ----------------------------------------------------------------------------
# The TEST database and everything that may write to it.
# Importable: undo_test_db.py will call rebuild() with the recorded baseline.
# ----------------------------------------------------------------------------
class TestDatabase:
    __test__ = False                # not a pytest class

    def __init__(self, ctx):
        self.ctx = ctx
        ctx.require(
            env=["APP_ENV", "PG_HOST", "PG_PORT", "PG_USER", "PG_DB", "STATE_FILE",
                 "PG_BACKUP_LOCATION", "BACKUP_GLOB"],
            env_may_be_empty=["PG_PASSWORD"],
            conf=["TEST_DB_MARKER", "ENV_FILE_DIR", "PRD_APP_ENV", "TEST_APP_ENV"])
        self.marker = ctx.conf_value("TEST_DB_MARKER")
        self.state_file = ctx.env_value("STATE_FILE")
        self.backup_dir = ctx.env_value("PG_BACKUP_LOCATION")
        self.backup_glob = ctx.env_value("BACKUP_GLOB")
        self.test = PgTarget.from_values("TEST", ctx.envvals, ctx.env_path)
        self.prd = load_prd_source(ctx).target          # read-only use, nothing else
        self.maint = PgTarget(self.test.label, self.test.host, self.test.port, self.test.user,
                              self.test.password, MAINT_DB)

    # --------------------------------------------------------------- safety
    def verify_not_prd(self):
        """Refuse unless the write target is demonstrably not PRD. -> True if same server."""
        c, test, prd = self.ctx, self.test, self.prd
        expected = expected_app_env(c.conf, c.conf_path, c.environment)
        if c.env_value("APP_ENV") != expected:
            raise HelperError("APP_ENV is %r but this environment must carry %r; refusing to "
                              "write" % (c.env_value("APP_ENV"), expected), c.env_path)
        if test.same_database_as(prd):
            raise HelperError("the TEST connection target is the PRD database (%s); refusing "
                              "to write" % prd.describe(), c.env_path)
        if test.dbname in PROTECTED_DBS:
            raise HelperError("PG_DB is %r, a system database that is never dropped" % test.dbname,
                              c.env_path)
        if test.user == prd.user:
            raise HelperError("TEST and PRD use the same database role (%s); TEST needs its own "
                              "role" % test.user, c.env_path)
        started = "SELECT pg_postmaster_start_time()::text"
        same_server = psql_rows(self.maint, started)[0][0] == psql_rows(prd, started)[0][0]
        if same_server and test.dbname == prd.dbname:
            raise HelperError("TEST and PRD resolve to the same server and the same database "
                              "name (%s); refusing to write" % test.dbname, c.env_path)
        return same_server

    # ----------------------------------------------------------- privileges
    def check_privileges(self):
        """Prove the TEST role can drop, create and restore. -> dict of facts for evidence."""
        test, maint = self.test, self.maint
        try:
            role = psql_rows(maint, "SELECT rolsuper::text, rolcreatedb::text FROM pg_roles "
                                    "WHERE rolname = current_user")
            exists = psql_rows(maint, "SELECT pg_has_role(current_user, datdba, 'MEMBER')::text "
                                      "FROM pg_database WHERE datname = %s"
                               % quote_literal(test.dbname))
        except HelperError as exc:
            raise HelperError("cannot use the maintenance database %r as role %s: %s"
                              % (MAINT_DB, test.user, exc.reason), exc.where)
        if not role:
            raise HelperError("role %s was not found in pg_roles" % test.user, maint.describe())
        superuser, createdb = role[0][0] == "true", role[0][1] == "true"
        db_exists = bool(exists)
        owns = bool(exists) and exists[0][0] == "true"
        if not superuser:
            if not createdb:
                raise HelperError("role %s lacks CREATEDB, so it cannot create the TEST database "
                                  "(as a PostgreSQL superuser: ALTER ROLE %s CREATEDB)"
                                  % (test.user, quote_ident(test.user)), maint.describe())
            if db_exists and not owns:
                raise HelperError("role %s does not own database %s, so it cannot drop it"
                                  % (test.user, test.dbname), maint.describe())
        return {"superuser": superuser, "createdb": createdb, "db_exists": db_exists,
                "owns": owns or superuser}

    def check_extensions(self, toc_text, superuser):
        """Every extension the dump creates must exist here and be creatable by the TEST role."""
        found = toc_extensions(toc_text)
        for name in found:
            rows = psql_rows(self.maint, (
                "SELECT v.superuser::text, v.trusted::text FROM pg_available_extension_versions v "
                "JOIN pg_available_extensions e ON e.name = v.name AND e.default_version = v.version "
                "WHERE e.name = %s") % quote_literal(name))
            if not rows:
                raise HelperError("the dump creates extension %s but it is not installed on the "
                                  "TEST server" % name, self.maint.describe())
            needs_super, trusted = rows[0][0] == "true", rows[0][1] == "true"
            if needs_super and not trusted and not superuser:
                raise HelperError("the dump creates extension %s, which only a superuser may create, "
                                  "and role %s is not one" % (name, self.test.user),
                                  self.maint.describe())
        return found

    # -------------------------------------------------------------- rebuild
    def rebuild(self, dump):
        """Drop TEST, create it empty, pg_restore `dump` into it, set the TEST marker.

        The only code that writes to the TEST database. Verifies the target is not PRD first.
        """
        test, maint, name = self.test, self.maint, self.test.dbname
        self.verify_not_prd()
        quoted = quote_ident(name)
        exists = psql_rows(maint, "SELECT 1 FROM pg_database WHERE datname = %s" % quote_literal(name))
        if exists:
            info("dropping database %s (other sessions are terminated)" % name)
            psql_rows(maint, "DROP DATABASE %s WITH (FORCE)" % quoted, timeout=QUERY_TIMEOUT)
        info("creating database %s" % name)
        psql_rows(maint, "CREATE DATABASE %s TEMPLATE template0" % quoted, timeout=QUERY_TIMEOUT)

        info("pg_restore %s  (%s)" % (os.path.basename(dump), human_size(os.path.getsize(dump))))
        argv = ["pg_restore", "--no-owner", "--no-privileges", "--no-password",
                "--dbname=%s" % name, dump]
        result = run_cmd(argv, extra_env=test.env(), timeout=LONG_TIMEOUT,
                         echo_prefix="    ", heartbeat=HEARTBEAT)
        tolerated, fatal = classify_restore_errors(result.stdout)
        if result.timed_out or fatal or (not result.ok and not tolerated):
            raise HelperError("pg_restore failed: %s" % (fatal[0] if fatal else reason_from(result)),
                              result.command())
        for text in tolerated:
            warn("ignored harmless pg_restore error: %s" % text)
        evidence("restore_seconds", "%.0f (%s)" % (result.seconds, elapsed(result.seconds)))

        psql_rows(test, "COMMENT ON DATABASE %s IS %s" % (quoted, quote_literal(self.marker)))
        read_back = psql_rows(test, "SELECT coalesce(shobj_description((SELECT oid FROM pg_database "
                                    "WHERE datname = current_database()), 'pg_database'), '')")
        if not read_back or read_back[0][0] != self.marker:
            raise HelperError("the TEST marker could not be set on the database comment", test.describe())

    # --------------------------------------------------------------- access
    def check_app_access(self):
        """Proof (not assumption) that the TEST role has full access to what was restored."""
        row = psql_rows(self.test, ACCESS_SQL)[0]
        tables_lacking, sequences_lacking, foreign_owned, tables, db_ok = (
            int(row[0]), int(row[1]), int(row[2]), int(row[3]), row[4] == "true")
        problems = []
        if not db_ok:
            problems.append("no CONNECT/CREATE on the database")
        if tables_lacking:
            problems.append("%d table(s) lack SELECT/INSERT/UPDATE/DELETE" % tables_lacking)
        if sequences_lacking:
            problems.append("%d sequence(s) lack USAGE/SELECT/UPDATE" % sequences_lacking)
        if foreign_owned:
            problems.append("%d object(s) are owned by a role the TEST role is not a member of"
                            % foreign_owned)
        if problems:
            raise HelperError("role %s does not have full access to the restored database: %s"
                              % (self.test.user, "; ".join(problems)), self.test.describe())
        return tables


# ----------------------------------------------------------------------------
# The step
# ----------------------------------------------------------------------------
def parse_args(argv):
    parser = Parser(prog="step_db_prepare.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


class DbPrepare:
    def __init__(self, ctx):
        self.ctx = ctx
        self.db = TestDatabase(ctx)
        self.started = time.monotonic()
        self.action = ""
        self.dump = ""                  # the dump pg_restore reads
        self.source_label = ""
        self.temp_files = []            # removed in run()'s finally
        self.armed = False              # True once the marker has been set dirty
        self.superuser = False
        self.previous_baseline = ""
        self.baseline = ""

    # ------------------------------------------------------------------ run
    def run(self):
        stages = [("Decision", self.stage_decision),
                  ("Write target and privileges", self.stage_target),
                  ("Source dump", self.stage_source),
                  ("Arm: undo action, state marker dirty", self.stage_arm),
                  ("Drop, create and restore TEST", self.stage_rebuild),
                  ("Marker and access", self.stage_access),
                  ("Compare with PRD", self.stage_compare),
                  ("Clean baseline", self.stage_baseline)]
        total = len(stages)
        try:
            for number, (title, function) in enumerate(stages, 1):
                info("(%d/%d) %s ...  [elapsed %s]" % (number, total, title,
                                                       elapsed(time.monotonic() - self.started)))
                function()
        except SkipStep:
            raise
        except BaseException:
            if self.armed:
                warn("the TEST state marker is left 'dirty' (nothing is retried automatically); "
                     "the next run's preflight treats the database as Unusable and restores it")
            raise
        finally:
            self.discard_temp_files()
        evidence("prd_database", "PRD database was not modified")
        evidence("elapsed_total", elapsed(time.monotonic() - self.started))

    def discard_temp_files(self):
        for path in self.temp_files:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    info("removed temporary file %s" % path)
            except OSError as exc:
                warn("could not remove temporary file %s: %s" % (path, exc.strerror))

    # ------------------------------------------------------------- 1. decide
    def stage_decision(self):
        state = self.ctx.state
        self.action = state.get("DB_ACTION", "")
        evidence("db_action", self.action or "(not in run.state)")
        if self.action == "keep":
            self.keep()                                   # raises SkipStep
        if self.action not in ACTIONS:
            raise HelperError("run.state has DB_ACTION=%r; expected keep, restore_snapshot or "
                              "fresh_clone (written by step_preflight)" % self.action,
                              os.path.join(self.ctx.run_dir, "run.state"))
        if self.action == "restore_snapshot":
            snapshot = state.get("DB_SNAPSHOT_FILE", "")
            if not snapshot or not os.path.isfile(snapshot):
                raise HelperError("the nightly snapshot chosen by preflight is not there: %s"
                                  % (snapshot or "(DB_SNAPSHOT_FILE empty)"),
                                  os.path.join(self.ctx.run_dir, "run.state"))
            self.dump = snapshot
            self.source_label = os.path.basename(snapshot)
        else:
            self.source_label = "fresh clone"
        evidence("source", self.source_label)
        if not writable_dir(self.db.backup_dir):
            raise HelperError("backup directory is missing or not writable", self.db.backup_dir)

    def keep(self):
        marker_state, values = read_db_marker(self.db.state_file)
        baseline = values.get("baseline_dump", "")
        evidence("test_state_marker", marker_state)
        if not baseline or not os.path.isfile(baseline):
            raise HelperError("the TEST database was kept, but its clean baseline is %s; a rollback "
                              "would be impossible" % ("missing: " + baseline if baseline
                                                       else "not recorded in the state file"),
                              self.db.state_file)
        age = time.time() - os.path.getmtime(baseline)
        evidence("test_clean_baseline", "%s (%s, %s old)" % (
            baseline, human_size(os.path.getsize(baseline)), human_age(age)))
        update_state(self.ctx.run_dir, {STATE_KEY: "complete", "TEST_BASELINE_DUMP": baseline,
                                        "DB_PREPARE_SOURCE": "kept"})
        evidence("prd_database", "PRD database was not modified")
        raise SkipStep("not required")

    # ------------------------------------------------------ 2. write target
    def stage_target(self):
        db, test = self.db, self.db.test
        same_server = db.verify_not_prd()
        evidence("test_database", test.describe())
        evidence("test_host_port_name", "%s:%s/%s" % (test.host, test.port, test.dbname))
        evidence("write_target_is_prd", "no (APP_ENV, database name, server and role checked)")
        evidence("prd_source", db.prd.describe() + " (read-only use)")
        facts = db.check_privileges()
        self.superuser = facts["superuser"]
        evidence("app_role", "%s (superuser=%s, createdb=%s)" % (
            test.user, "yes" if facts["superuser"] else "no", "yes" if facts["createdb"] else "no"))
        evidence("app_role_differs_from_prd_role", "yes (%s vs %s)" % (test.user, db.prd.user))
        evidence("test_database_exists", "yes" if facts["db_exists"] else "no (will be created)")
        evidence("app_role_may_drop_create", "yes")
        if same_server and facts["superuser"]:
            warn("TEST and PRD are on the same PostgreSQL server and the TEST role is a superuser, "
                 "so the TEST role could technically reach PRD")
        evidence("same_server_as_prd", "yes" if same_server else "no")

    # --------------------------------------------------------- 3. source dump
    def stage_source(self):
        db = self.db
        if self.action == "fresh_clone":
            final = os.path.join(db.backup_dir, "%s%s.dump" % (CLONE_PREFIX, utc_stamp()))
            self.guard_name(final)
            partial = final + ".partial"
            self.temp_files += [partial, final]
            info("pg_dump -Fc of PRD %s (reads PRD only; this takes a while)" % db.prd.describe())
            argv = ["pg_dump", "-Fc", "--no-password", "--file", partial]
            result = run_cmd(argv, extra_env=db.prd.env(), timeout=LONG_TIMEOUT,
                             echo_prefix="    ", heartbeat=HEARTBEAT)
            if not result.ok:
                raise HelperError("pg_dump of PRD failed: %s" % reason_from(result), result.command())
            os.replace(partial, final)
            self.dump = final
            evidence("fresh_dump_seconds", "%.0f (%s)" % (result.seconds, elapsed(result.seconds)))
        size = os.path.getsize(self.dump)
        evidence("source_dump", "%s (%s)" % (self.dump, human_size(size)))
        if size == 0:
            raise HelperError("the source dump is empty", self.dump)
        toc = run_cmd(["pg_restore", "--list", self.dump], timeout=300)
        if not toc.ok:
            raise HelperError("pg_restore cannot read the source dump: %s" % toc.first_error_line(),
                              toc.command())
        found = db.check_extensions(toc.stdout, self.superuser)
        evidence("dump_extensions", " ".join(found) or "none")
        evidence("extensions_creatable_by_app_role", "yes")

    def guard_name(self, path):
        """Files this step writes must never match the nightly glob (they would pollute it)."""
        if fnmatch.fnmatch(os.path.basename(path), self.db.backup_glob):
            raise HelperError("%s would match BACKUP_GLOB %r and be mistaken for a nightly dump"
                              % (os.path.basename(path), self.db.backup_glob), self.db.backup_dir)

    # -------------------------------------------------------------- 4. arm
    def stage_arm(self):
        db, run_dir = self.db, self.ctx.run_dir
        db.verify_not_prd()                                      # before ANY write (spec step 5)
        _state, values = read_db_marker(db.state_file)
        self.previous_baseline = values.get("baseline_dump", "")
        register_undo(run_dir, UNDO_NAME)                        # BEFORE changing anything
        update_state(run_dir, {STATE_KEY: "running", "DB_PREPARE_SOURCE": self.source_label})
        update_db_marker(db.state_file, "dirty")                 # BEFORE the overwrite
        self.armed = True
        evidence("undo_registered", UNDO_NAME)
        evidence("test_state_marker", "dirty (set before the TEST database is overwritten)")

    # ---------------------------------------------------------- 5. rebuild
    def stage_rebuild(self):
        self.db.rebuild(self.dump)
        evidence("test_marker", "%s (database comment)" % self.db.marker)

    # ----------------------------------------------------------- 6. access
    def stage_access(self):
        tables = self.db.check_app_access()
        evidence("app_user_privileges", "%s has CONNECT/CREATE and full access to all %d restored "
                                         "table(s) and sequences" % (self.db.test.user, tables))

    # ---------------------------------------------------------- 7. compare
    def stage_compare(self):
        db = self.db
        try:
            tables = psql_rows(db.test, KEY_TABLES_SQL)
        except HelperError as exc:
            warn("key tables could not be listed: %s" % exc.reason)
            tables = []
        if not tables:
            evidence("row_counts", "no user tables found")
        for schema, table in tables:
            qualified = "%s.%s" % (quote_ident(schema), quote_ident(table))
            counts = []
            for target in (db.test, db.prd):
                try:
                    counts.append(int(psql_rows(target, "SELECT count(*) FROM %s" % qualified,
                                                timeout=QUERY_TIMEOUT)[0][0]))
                except HelperError as exc:
                    counts.append(None)
                    warn("count of %s in %s failed: %s" % (qualified, target.label, exc.reason))
            t_count, p_count = counts
            diff = "n/a" if None in counts else "%+d" % (t_count - p_count)
            evidence("rows_%s.%s" % (schema, table), "TEST %s, PRD %s (TEST minus PRD: %s)"
                     % ("n/a" if t_count is None else t_count, "n/a" if p_count is None else p_count, diff))
        evidence("row_counts_note", "PRD stays live and a nightly snapshot is older than PRD, "
                                    "so counts may differ; they are evidence, not a pass/fail test")

        prd_schema, test_schema = schema_lines(db.prd), schema_lines(db.test)
        diff = [l for l in difflib.unified_diff(prd_schema, test_schema, "PRD", "TEST", n=0, lineterm="")
                if l[:1] in "+-" and not l.startswith(("+++", "---"))]
        evidence("schema_diff_lines", len(diff))
        if not diff:
            return
        info("schema differences (- only in PRD, + only in TEST), first %d of %d:"
             % (min(SAMPLE_LINES, len(diff)), len(diff)))
        for line in diff[:SAMPLE_LINES]:
            info("    " + line[:140])
        # Spec: the schema diff must be zero. The one relaxation: a nightly snapshot that preflight
        # already flagged as behind live PRD (migration versions differ or unknown) may differ.
        behind = (self.action == "restore_snapshot"
                  and self.ctx.state.get("DB_SNAPSHOT_MIGRATIONS_MATCH_PRD", "") != "yes")
        if behind:
            warn("TEST differs from live PRD (%d line(s)); allowed because preflight flagged this "
                 "snapshot as not matching live PRD" % len(diff))
            return
        raise HelperError("the restored TEST schema differs from PRD (%d line(s); spec requires zero)"
                          % len(diff), "pg_dump --schema-only PRD vs TEST")

    # ---------------------------------------------------------- 8. baseline
    def stage_baseline(self):
        db, run_dir = self.db, self.ctx.run_dir
        final = os.path.join(db.backup_dir, "%s%s.dump" % (BASELINE_PREFIX, utc_stamp()))
        self.guard_name(final)
        partial = final + ".partial"
        self.temp_files.append(partial)
        info("pg_dump -Fc of the restored TEST database -> %s" % os.path.basename(final))
        result = run_cmd(["pg_dump", "-Fc", "--no-password", "--file", partial],
                         extra_env=db.test.env(), timeout=LONG_TIMEOUT,
                         echo_prefix="    ", heartbeat=HEARTBEAT)
        if not result.ok:
            raise HelperError("pg_dump of the TEST baseline failed: %s" % reason_from(result),
                              result.command())
        if os.path.getsize(partial) == 0:
            raise HelperError("the baseline dump is empty", partial)
        toc = run_cmd(["pg_restore", "--list", partial], timeout=300)
        if not toc.ok:
            raise HelperError("the baseline dump is not readable by pg_restore: %s"
                              % toc.first_error_line(), toc.command())
        os.replace(partial, final)
        self.baseline = final
        # Record the path and set clean in ONE atomic write (spec: record, then clean).
        update_db_marker(db.state_file, "clean", baseline_dump=final)
        update_state(run_dir, {STATE_KEY: "complete", "TEST_BASELINE_DUMP": final})
        evidence("test_state_marker", "clean")
        evidence("baseline_dump", "%s (%s)" % (final, human_size(os.path.getsize(final))))
        evidence("baseline_recorded_in", "%s (baseline_dump=...)" % db.state_file)

        old = self.previous_baseline
        if (old and old != final and os.path.isfile(old)
                and os.path.dirname(os.path.abspath(old)) == os.path.abspath(db.backup_dir)
                and os.path.basename(old).startswith(BASELINE_PREFIX)):
            try:
                os.remove(old)
                evidence("previous_baseline_removed", old)
            except OSError as exc:
                warn("could not remove the previous baseline %s: %s" % (old, exc.strerror))


def main():
    args = parse_args(sys.argv[1:])
    DbPrepare(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
