#!/usr/bin/env python3
"""
deploy/scripts/lib/step_db_baseline.py  --  PRD Step 5: Take PRD rollback baseline.

    step_db_baseline.py --run-dir <dir>

Objective (spec PRD Step 5): A pinned PRD dump file that the exit handler can restore
if the deploy fails.

Required behavior:
1. With the checkout now available, check pending migrations against PRD.
2. If no migrations pending: SKIPPED (no migrations pending), do not take a dump,
   leave the state clean.
3. If migrations pending and state is clean: take a pg_dump -Fc from PRD to
   prd_baseline_<UTC>.dump, record the path in the state file (baseline_dump=<path>),
   then set state to dirty. Register undo 'prd_db' BEFORE changing anything.
4. If state is already dirty (crash recovery): restore the PRD database from the
   recorded baseline dump instead of capturing a new one. On success, set state to clean
   so Step 6 can proceed.
5. Heartbeat printed at intervals during dump/restore.

Exit: 0 pass, 1 fail, 2 skipped (SkipStep). Python 3 standard library only.
"""
import os
import sys
import time

from common import (HelperError, Parser, PgTarget, SkipStep, evidence, expected_app_env,
                    human_size, info, is_inside, load_context, psql_rows, read_db_marker,
                    register_undo, run_cmd, step_main, update_db_marker, update_state,
                    warn, writable_dir)
from step_db_prepare import (HEARTBEAT, LONG_TIMEOUT, MAINT_DB, QUERY_TIMEOUT,
                             classify_restore_errors, elapsed, quote_ident,
                             quote_literal, reason_from, utc_stamp)
from step_migrate import migration_files

PRD_BASELINE_PREFIX = "prd_baseline_"
LIST_LIMIT = 40


def parse_args(argv):
    parser = Parser(prog="step_db_baseline.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def rebuild_database(target, maint, dump):
    """Drop and recreate target database from dump file with safety checks and error classification."""
    name = target.dbname
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
    result = run_cmd(argv, extra_env=target.env(), timeout=LONG_TIMEOUT,
                     echo_prefix="    ", heartbeat=HEARTBEAT)
    tolerated, fatal = classify_restore_errors(result.stdout)
    if result.timed_out or fatal or (not result.ok and not tolerated):
        raise HelperError("pg_restore failed: %s" % (fatal[0] if fatal else reason_from(result)),
                          result.command())
    for text in tolerated:
        warn("ignored harmless pg_restore error: %s" % text)
    evidence("restore_seconds", "%.0f (%s)" % (result.seconds, elapsed(result.seconds)))


class Baseline:
    def __init__(self, ctx):
        self.ctx = ctx
        if not ctx.is_prd:
            raise SkipStep("step_db_baseline is only run for PRD deployments")

        ctx.require(conf=["MIGRATIONS_DIR", "MIGRATIONS_TABLE", "MIGRATIONS_SCHEMA"],
                    env=["APP_DIR", "STATE_FILE", "PG_BACKUP_LOCATION", "APP_ENV",
                         "PG_HOST", "PG_PORT", "PG_USER", "PG_DB"],
                    env_may_be_empty=["PG_PASSWORD"])

        expected = expected_app_env(ctx.conf, ctx.conf_path, "prd")
        if ctx.env_value("APP_ENV") != expected:
            raise HelperError("APP_ENV is %r but PRD must carry %r; refusing to baseline"
                              % (ctx.env_value("APP_ENV"), expected), ctx.env_path)

        self.target = PgTarget.from_values("PRD", ctx.envvals, ctx.env_path)
        self.maint = self.target.for_db(MAINT_DB)
        self.state_file = ctx.env_value("STATE_FILE")
        self.backup_dir = ctx.env_value("PG_BACKUP_LOCATION")
        self.app_dir = ctx.env_value("APP_DIR")
        self.fqn = "%s.%s" % (ctx.conf_value("MIGRATIONS_SCHEMA"), ctx.conf_value("MIGRATIONS_TABLE"))

        relative = ctx.conf_value("MIGRATIONS_DIR")
        self.directory = os.path.normpath(os.path.join(self.app_dir, relative))
        if os.path.isabs(relative) or not is_inside(self.directory, self.app_dir) \
                or os.path.realpath(self.directory) == os.path.realpath(self.app_dir):
            raise HelperError("MIGRATIONS_DIR must be a relative path inside the repo checkout "
                              "(got %r)" % relative, ctx.conf_path)
        if not os.path.isdir(self.directory):
            raise HelperError("MIGRATIONS_DIR does not exist in the checkout: %s" % self.directory,
                              ctx.conf_path)

        self.files = migration_files(self.directory)
        self.started = time.monotonic()
        self.pending = []

    def run(self):
        self.stage_recovery()
        self.stage_pending()
        self.stage_capture()

    def stage_recovery(self):
        info("(1/3) State marker check / crash recovery ...")
        marker, values = read_db_marker(self.state_file)
        evidence("prd_state_file", self.state_file)
        evidence("prd_state_before", marker)
        evidence("prd_database_target", self.target.describe())

        if marker == "dirty":
            baseline = values.get("baseline_dump", "")
            if not baseline or not os.path.isfile(baseline):
                raise HelperError("PRD state is 'dirty' (interrupted run) but recorded baseline "
                                  "dump is %s" % ("missing: " + baseline if baseline else "not recorded"),
                                  self.state_file)
            size = os.path.getsize(baseline)
            if size == 0:
                raise HelperError("recorded PRD baseline dump is empty: %s" % baseline, self.state_file)

            toc = run_cmd(["pg_restore", "--list", baseline], timeout=300)
            if not toc.ok:
                raise HelperError("recorded PRD baseline dump is unreadable by pg_restore: %s"
                                  % toc.first_error_line(), toc.command())

            info("CRASH RECOVERY: PRD database is 'dirty'; restoring from %s ..."
                 % os.path.basename(baseline))
            rebuild_database(self.target, self.maint, baseline)
            update_db_marker(self.state_file, "clean")
            evidence("decision", "restore")
            evidence("prd_database", "PRD database restored to %s" % baseline)
            evidence("prd_state_after", "clean")

    def stage_pending(self):
        info("(2/3) Check pending migrations ...")
        exists = psql_rows(self.target, "SELECT to_regclass('%s') IS NOT NULL" % self.fqn)[0][0] == "t"
        applied = []
        if exists:
            applied = [r[0] for r in psql_rows(self.target, "SELECT version FROM %s ORDER BY version"
                                                % self.fqn)]
        else:
            evidence("migrations_table", "%s absent (every migration is pending)" % self.fqn)

        known = {v for v, _n in self.files}
        unknown = sorted(set(applied) - known)
        if unknown:
            warn("applied in PRD but not in this commit's migrations directory: %s"
                 % ", ".join(unknown[:10]))

        self.pending = [(v, n) for v, n in self.files if v not in set(applied)]
        evidence("applied_before", "%d (latest %s)" % (len(applied), applied[-1] if applied else "none"))
        evidence("pending_before", len(self.pending))
        for _version, name in self.pending[:LIST_LIMIT]:
            info("    pending: %s" % name)
        if len(self.pending) > LIST_LIMIT:
            info("    ... and %d more" % (len(self.pending) - LIST_LIMIT))

        if not self.pending:
            marker, _values = read_db_marker(self.state_file)
            evidence("decision", "skip")
            evidence("prd_database", "PRD database was not written by this step")
            evidence("prd_state_after", marker)
            raise SkipStep("no migrations pending")

    def stage_capture(self):
        info("(3/3) Capture PRD rollback baseline ...")
        evidence("decision", "capture")
        run_dir = self.ctx.run_dir
        register_undo(run_dir, "prd_db")
        evidence("undo_registered", "prd_db")

        if not writable_dir(self.backup_dir):
            raise HelperError("backup directory is missing or not writable", self.backup_dir)
        backup_dir = self.backup_dir
        prefix = self.ctx.conf_value("PRD_BASELINE_PREFIX") or PRD_BASELINE_PREFIX
        filename = "%s%s.dump" % (prefix, utc_stamp())
        final_dump = os.path.join(backup_dir, filename)
        partial_dump = final_dump + ".partial"

        info("capturing PRD baseline dump to %s ..." % filename)
        argv = ["pg_dump", "-Fc", "--no-owner", "--no-privileges", "--no-password",
                "--file=%s" % partial_dump, self.target.dbname]
        result = run_cmd(argv, extra_env=self.target.env(), timeout=LONG_TIMEOUT,
                         echo_prefix="    ", heartbeat=HEARTBEAT)
        if not result.ok:
            if os.path.exists(partial_dump):
                try:
                    os.remove(partial_dump)
                except OSError:
                    pass
            raise HelperError("pg_dump failed: %s" % reason_from(result), result.command())

        os.replace(partial_dump, final_dump)

        # Prove dump is readable
        toc = run_cmd(["pg_restore", "--list", final_dump], timeout=300)
        if not toc.ok:
            raise HelperError("pg_dump produced unreadable dump: %s" % toc.first_error_line(),
                              final_dump)

        dump_size = os.path.getsize(final_dump)
        update_db_marker(self.state_file, "dirty", baseline_dump=final_dump)
        update_state(run_dir, {"PRD_BASELINE_DUMP": final_dump})

        evidence("baseline_dump", "%s (%s)" % (final_dump, human_size(dump_size)))
        evidence("prd_state_after", "dirty")
        evidence("elapsed_seconds", "%.0f (%s)" % (result.seconds, elapsed(result.seconds)))
        evidence("prd_database", "PRD database was not written by this step")


def main():
    args = parse_args(sys.argv[1:])
    Baseline(load_context(args.run_dir)).run()


if __name__ == "__main__":
    sys.exit(step_main(main))
