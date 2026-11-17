#!/usr/bin/env python3
"""
deploy/scripts/lib/step_migrate.py  --  TEST Step 5: apply pending migrations (dbmate).

    step_migrate.py --run-dir <dir>

Objective (spec TEST Step 5): the TEST database is migrated to the schema in the checked-out commit.

Reads
    autodeploy.conf     MIGRATIONS_DIR (path inside the repo checkout, relative; NEW key),
                        MIGRATIONS_TABLE, MIGRATIONS_SCHEMA (already there)
    Pi env file         APP_DIR, STATE_FILE and the PG_* values (through TestDatabase)
    run.state           DB_PREPARE_STATE (must not be 'running')

What it does
    1. checks, all read-only:  MIGRATIONS_DIR exists inside APP_DIR and holds *.sql files; the write
       target is not PRD (TestDatabase.verify_not_prd, same check db_prepare uses); the state marker is
       'clean'; works out the pending migrations itself (files in the directory minus the versions in
       the migrations table, read with SQL) and prints them.
    2. nothing pending -> reports it and PASSES; the database is not touched and nothing is armed.
    3. pending -> registers the undo action (test_db), sets the marker to dirty, runs `dbmate up`
       against TEST only, then `dbmate status` and a re-read of the table as evidence.
    On failure       reports the migration that was being applied and dbmate's error. The marker stays
                     dirty, so the exit handler (undo_test_db.py) restores the baseline recorded in the
                     state file; its output shows 'rolled_back_to'. This step does not restore anything.
    On success       the marker stays dirty (spec 4.4: it also covers the tests and manual testing that
                     follow). Rollback happens on every exit except GO.

Why pending is computed here and not with `dbmate status`: I have not verified that `dbmate status`
leaves a database without a migrations table untouched, and spec 4.4 says the marker must be dirty
BEFORE anything can write to TEST. The directory listing + one SELECT is read-only by construction.
`dbmate up` (the only writer) is the canonical tool.

dbmate is run with --no-dump-schema (otherwise it writes schema.sql into the checkout and the tree is
no longer clean), the connection in DATABASE_URL (the password never appears on a command line) and the
migrations table the preflight already reads.

NOT BUILT YET: the PRD run. PRD_STEPS also names `migrate`, but PRD needs a PRD baseline undo, not
test_db. Until that is designed, this step refuses to run when the environment is not test.

Exit: 0 pass, 1 fail (REASON:/WHERE: printed).  Never 10 (reserved).
Python 3 standard library only.
"""
import os
import re
import sys
import time
from urllib.parse import quote

from common import (HelperError, Parser, PgTarget, evidence, expected_app_env, info,
                    is_inside, load_context, norm_host, psql_rows, read_db_marker,
                    register_undo, run_cmd, step_main, update_db_marker, warn)
from step_db_prepare import STATE_KEY, TestDatabase, elapsed, reason_from

MIGRATION_FILE_RE = re.compile(r"^(\d+).*\.sql$")      # dbmate's own rule: version = leading digits
APPLYING_RE = re.compile(r"Applying:\s*(\S+)")
LONG_TIMEOUT = 300
HEARTBEAT = 30
LIST_LIMIT = 40


def parse_args(argv):
    parser = Parser(prog="step_migrate.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def migration_files(directory):
    """-> sorted [(version, filename)] of the *.sql files dbmate would consider."""
    found, seen = [], {}
    for name in sorted(os.listdir(directory)):
        match = MIGRATION_FILE_RE.match(name)
        if not match or not os.path.isfile(os.path.join(directory, name)):
            continue
        version = match.group(1)
        if version in seen:
            raise HelperError("two migration files share version %s: %s and %s"
                              % (version, seen[version], name), directory)
        seen[version] = name
        found.append((version, name))
    return sorted(found)


def database_url(target):
    """postgres:// URL for dbmate. TCP only; user and password are percent-encoded."""
    if target.host.startswith("/"):
        raise HelperError("PG_HOST is a socket directory (%s); step_migrate supports TCP hosts only"
                          % target.host, "PG_HOST")
    login = quote(target.user, safe="")
    if target.password:
        login += ":" + quote(target.password, safe="")
    # Local server: do not insist on TLS. Anything else: leave the choice to dbmate's default.
    query = "?sslmode=disable" if norm_host(target.host) == "local" else ""
    return "postgres://%s@%s:%s/%s%s" % (login, target.host, target.port,
                                         quote(target.dbname, safe=""), query)


class Migrate:
    def __init__(self, ctx):
        self.ctx = ctx
        ctx.require(conf=["MIGRATIONS_DIR", "MIGRATIONS_TABLE", "MIGRATIONS_SCHEMA"],
                    env=["APP_DIR", "STATE_FILE"])
        self.state_file = ctx.env_value("STATE_FILE")
        self.fqn = "%s.%s" % (ctx.conf_value("MIGRATIONS_SCHEMA"), ctx.conf_value("MIGRATIONS_TABLE"))

        if ctx.is_test:
            self.db = TestDatabase(ctx)
            self.target = self.db.test
            self.undo_name = "test_db"
        else:
            ctx.require(env=["APP_ENV", "PG_HOST", "PG_PORT", "PG_USER", "PG_DB"],
                        env_may_be_empty=["PG_PASSWORD"])
            self.db = None
            self.target = PgTarget.from_values("PRD", ctx.envvals, ctx.env_path)
            self.undo_name = "prd_db"

        self.started = time.monotonic()
        self.files = []
        self.pending = []
        self.directory = ""

    def run(self):
        for number, (title, function) in enumerate((("Checks", self.stage_checks),
                                                    ("Pending migrations", self.stage_pending),
                                                    ("Apply", self.stage_apply),
                                                    ("Verify", self.stage_verify)), 1):
            info("(%d/4) %s ...  [elapsed %s]" % (number, title,
                                                  elapsed(time.monotonic() - self.started)))
            if function() == "done":
                break
        if self.ctx.is_test:
            evidence("prd_database", "PRD database was not modified")
        else:
            evidence("prd_database", "PRD database migrated to current schema")

    # ------------------------------------------------------------- 1. checks
    def stage_checks(self):
        c, db = self.ctx, self.db
        relative = c.conf_value("MIGRATIONS_DIR")
        app_dir = c.env_value("APP_DIR")
        self.directory = os.path.normpath(os.path.join(app_dir, relative))
        evidence("migrations_dir", "%s (MIGRATIONS_DIR=%s)" % (self.directory, relative))
        if os.path.isabs(relative) or not is_inside(self.directory, app_dir) \
                or os.path.realpath(self.directory) == os.path.realpath(app_dir):
            raise HelperError("MIGRATIONS_DIR must be a relative path inside the repo checkout "
                              "(got %r)" % relative, c.conf_path)
        if not os.path.isdir(self.directory):
            raise HelperError("MIGRATIONS_DIR does not exist in the checkout: %s" % self.directory,
                              c.conf_path)
        self.files = migration_files(self.directory)
        evidence("migration_files", len(self.files))
        if not self.files:
            raise HelperError("no <digits>_*.sql migration files in %s" % self.directory,
                              self.directory)

        if self.db is not None:
            run_state = c.state.get(STATE_KEY, "")
            if run_state == "running":
                raise HelperError("run.state says the TEST database step did not finish "
                                  "(%s=running); refusing to migrate" % STATE_KEY,
                                  os.path.join(c.run_dir, "run.state"))
            self.db.verify_not_prd()
            evidence("write_target_is_prd", "no (APP_ENV, database name, server and role checked)")
            evidence("test_database", self.target.describe())
            marker, _values = read_db_marker(self.state_file)
            evidence("test_state_marker", marker)
            if marker != "clean":
                raise HelperError("the TEST state marker is %r, not 'clean': migrating now would not "
                                  "have a trustworthy rollback baseline (a run restores the database "
                                  "first when TEST is not clean)" % marker, self.state_file)
        else:
            expected = expected_app_env(c.conf, c.conf_path, "prd")
            if c.env_value("APP_ENV") != expected:
                raise HelperError("APP_ENV is %r but PRD must carry %r; refusing to migrate"
                                  % (c.env_value("APP_ENV"), expected), c.env_path)
            evidence("write_target_is_prd", "yes (confirmed APP_ENV=%s, target=%s)" % (expected, self.target.describe()))
            evidence("prd_database_target", self.target.describe())
            marker, _values = read_db_marker(self.state_file)
            evidence("prd_state_marker", marker)
            if marker not in ("clean", "dirty"):
                raise HelperError("the PRD state marker is %r, not 'clean' or 'dirty'" % marker, self.state_file)

    # ---------------------------------------------------------- 2. pending
    def stage_pending(self):
        target = self.target
        exists = psql_rows(target, "SELECT to_regclass('%s') IS NOT NULL" % self.fqn)[0][0] == "t"
        applied = []
        if exists:
            applied = [r[0] for r in psql_rows(target, "SELECT version FROM %s ORDER BY version"
                                                % self.fqn)]
        else:
            evidence("migrations_table", "%s absent (first dbmate run: every migration is pending)"
                     % self.fqn)
        known = {v for v, _n in self.files}
        unknown = sorted(set(applied) - known)
        if unknown:
            warn("applied in %s but not in this commit's migrations directory: %s"
                 % (self.ctx.environment.upper(), ", ".join(unknown[:10])))
        self.pending = [(v, n) for v, n in self.files if v not in set(applied)]
        evidence("applied_before", "%d (latest %s)" % (len(applied), applied[-1] if applied else "none"))
        evidence("pending_before", len(self.pending))
        for _version, name in self.pending[:LIST_LIMIT]:
            info("    pending: %s" % name)
        if len(self.pending) > LIST_LIMIT:
            info("    ... and %d more" % (len(self.pending) - LIST_LIMIT))
        if not self.pending:
            evidence("migrate", "nothing pending: the %s database was not touched"
                     % self.ctx.environment.upper())
            return "done"

    # ------------------------------------------------------------- 3. apply
    def dbmate(self, *command, echo=True):
        argv = ["dbmate", "--no-dump-schema", "--migrations-dir", self.directory,
                "--migrations-table", self.fqn] + list(command)
        env = dict(self.target.env(), DATABASE_URL=database_url(self.target))
        return run_cmd(argv, extra_env=env, timeout=LONG_TIMEOUT,
                       echo_prefix="    " if echo else None, heartbeat=HEARTBEAT)

    def stage_apply(self):
        run_dir = self.ctx.run_dir
        if self.db is not None:
            self.db.verify_not_prd()                              # again, right before the write
        register_undo(run_dir, self.undo_name)                    # BEFORE changing anything
        update_db_marker(self.state_file, "dirty")                # BEFORE dbmate up
        evidence("undo_registered", self.undo_name)
        evidence("state_marker", "dirty (set before dbmate up)")
        result = self.dbmate("up")
        applying = APPLYING_RE.findall(result.stdout)
        if not result.ok:
            failing = applying[-1] if applying else "(unknown: dbmate printed no 'Applying:' line)"
            evidence("migration_failed", failing)
            evidence("error", reason_from(result))
            warn("the state marker is left 'dirty': the exit handler restores the "
                 "baseline recorded in %s (see 'rolled_back_to' in the undo output)" % self.state_file)
            raise HelperError("dbmate up failed on %s: %s" % (failing, reason_from(result)),
                              result.command())
        evidence("migrations_applied", "%d (%s)" % (len(applying), ", ".join(applying[:LIST_LIMIT])))
        evidence("dbmate_up_seconds", "%.1f" % result.seconds)

    # ------------------------------------------------------------ 4. verify
    def stage_verify(self):
        status = self.dbmate("status")
        if not status.ok:
            raise HelperError("dbmate status failed: %s" % reason_from(status), status.command())
        rows = psql_rows(self.target, "SELECT version FROM %s ORDER BY version" % self.fqn)
        applied = {r[0] for r in rows}
        still = [n for v, n in self.files if v not in applied]
        evidence("applied_after", "%d (latest %s)" % (len(rows), rows[-1][0] if rows else "none"))
        evidence("pending_after", len(still))
        if still:
            raise HelperError("migrations are still pending after dbmate up: %s"
                              % ", ".join(still[:10]), self.fqn)


def main():
    args = parse_args(sys.argv[1:])
    Migrate(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
