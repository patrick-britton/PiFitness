#!/usr/bin/env python3
"""
deploy/scripts/lib/common.py  --  small shared toolkit for the autodeploy helpers.

Not executable on its own; helpers do `from common import ...` (Python puts the
helper's own directory on sys.path, so no packaging is needed).

Contents
--------
HelperError(reason, where)   raise to fail; becomes REASON:/WHERE: lines
Parser                       argparse that raises HelperError instead of exiting
Reporter(tag, stream)        say()/evidence() with a "[tag]" prefix; remembers lines
run_stages(...)              runs "(i/N) title ... OK (0.1s)" stage lists
guarded(...)                 runs a body, prints a uniform FAIL block, returns 0/1
parse_kv_file(path)          KEY=value file -> dict (never echoes file content)
write_text_atomic(path, text)
write_state(path, dict)      KEY=shlex-quoted-value lines, atomic
read_state(run_dir)          -> dict from <run_dir>/run.state
update_state(run_dir, dict)  read-modify-write run.state atomically

Added for cleanup.py and the step helpers
-----------------------------------------
Output           out(), dotted_line(), evidence(), info(), warn()
Run files        read_steps(), read_results(), read_undo(), register_undo()
Subprocesses     run_cmd() -> CmdResult (timeouts, optional live echo),
                 run_streaming() (runs a helper with heartbeat; used by cleanup)
                 print_failure()
Step protocol    SkipStep, step_main()  (maps outcomes to exit 0/1/2/130)
Run context      load_context(run_dir) -> RunContext (state + conf + Pi env file)
                 STRICT: env values come ONLY from the Pi env file, conf values ONLY from
                 autodeploy.conf; nothing has a default; a missing key fails the run.
Terminal         open_tty(), ask_tty()
PostgreSQL       PgTarget, PgError, psql_rows(), norm_host()
DB state marker  read_db_marker(), update_db_marker()   (TEST/PRD state files)
Filesystem       writable_dir(), nearest_existing_ancestor(), is_inside()
Formatting       human_size(), human_age(), words()

Python 3 standard library only.
"""
import argparse
import os
import queue
import re
import shlex
import subprocess
import sys
import threading
import time
import traceback

KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class HelperError(Exception):
    """A failure with a one-line reason and a pointer to where it happened."""

    def __init__(self, reason, where=""):
        super().__init__(reason)
        self.reason = reason
        self.where = where


class Parser(argparse.ArgumentParser):
    """argparse that reports problems as HelperError (uniform FAIL output)."""

    def error(self, message):
        raise HelperError(message, "command line: " + " ".join(sys.argv[1:]))


# ----------------------------------------------------------------------------
# Progress output
# ----------------------------------------------------------------------------
class Reporter:
    """Prints '[tag] message' lines immediately and remembers them."""

    def __init__(self, tag, stream=None):
        self.tag = tag
        self.stream = stream if stream is not None else sys.stdout
        self.lines = []

    def say(self, message=""):
        line = "%s %s" % (self.tag, message) if message else ""
        self.lines.append(line)
        print(line, file=self.stream, flush=True)

    def evidence(self, pairs):
        for key, value in pairs:
            self.say("    EVIDENCE: %s = %s" % (key, value))


def run_stages(reporter, stages, ctx):
    """stages = [(title, function)]; function(ctx) -> [(key, value), ...]."""
    total = len(stages)
    for index, (title, function) in enumerate(stages, 1):
        ctx["stage"] = "(%d/%d) %s" % (index, total, title)
        reporter.say("%s ..." % ctx["stage"])
        started = time.monotonic()
        reporter.evidence(function(ctx))
        reporter.say("%s ... OK (%.1fs)" % (ctx["stage"], time.monotonic() - started))


def guarded(reporter, ctx, body, failure_note=""):
    """Run body(); on any exception print a FAIL block and return 1, else 0."""
    try:
        body()
        return 0
    except HelperError as exc:
        reporter.say("FAIL at stage %s" % ctx.get("stage", "(startup)"))
        reporter.say("REASON: %s" % exc.reason)
        if exc.where:
            reporter.say("WHERE: %s" % exc.where)
    except Exception as exc:  # a bug in the helper: still be informative
        reporter.say("FAIL at stage %s" % ctx.get("stage", "(startup)"))
        reporter.say("REASON: unexpected %s: %s" % (type(exc).__name__, exc))
        for line in traceback.format_exc().rstrip().splitlines():
            reporter.say("    " + line)
    if failure_note:
        reporter.say(failure_note)
    return 1


# ----------------------------------------------------------------------------
# Files and state
# ----------------------------------------------------------------------------
def parse_kv_file(path):
    """Parse KEY=value lines into a dict. Never echoes file content in errors.

    Accepts optional 'export ', '#' comments, quoted values. No $VAR expansion;
    an unquoted value may not contain spaces.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except OSError as exc:
        raise HelperError("cannot read file: %s" % exc.strerror, path)

    values = {}
    for number, raw in enumerate(lines, 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not KEY_RE.match(key):
            raise HelperError("line is not KEY=value", "%s:%d" % (path, number))
        try:
            tokens = shlex.split(value)
        except ValueError as exc:
            raise HelperError("cannot parse value for %s (%s)" % (key, exc),
                              "%s:%d" % (path, number))
        for index, token in enumerate(tokens):      # trailing "# comment"
            if token.startswith("#"):
                tokens = tokens[:index]
                break
        if len(tokens) > 1:
            raise HelperError("value for %s has unquoted spaces; wrap it in quotes"
                              % key, "%s:%d" % (path, number))
        values[key] = tokens[0] if tokens else ""
    return values


def check_keys(values, required=(), may_be_empty=()):
    """-> (absent, empty) key lists. `required` must be present AND non-empty;
    `may_be_empty` must be present but may have an empty value."""
    absent = [k for k in list(required) + list(may_be_empty) if k not in values]
    empty = [k for k in required if k in values and not values[k]]
    return absent, empty


def describe_gaps(absent, empty):
    parts = []
    if absent:
        parts.append("missing: %s" % ", ".join(absent))
    if empty:
        parts.append("empty: %s" % ", ".join(empty))
    return "; ".join(parts)


def write_text_atomic(path, text, mode=0o600):
    tmp = "%s.tmp.%d" % (path, os.getpid())
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except OSError as exc:
        raise HelperError("cannot write file: %s" % exc.strerror, path)


def write_state(path, values):
    text = "".join("%s=%s\n" % (k, shlex.quote(str(v))) for k, v in values.items())
    write_text_atomic(path, text)


def state_path(run_dir):
    return os.path.join(run_dir, "run.state")


def read_state(run_dir):
    return parse_kv_file(state_path(run_dir))


def update_state(run_dir, updates):
    """Merge updates into run.state atomically (single writer assumed)."""
    state = read_state(run_dir)
    state.update({k: str(v) for k, v in updates.items()})
    write_state(state_path(run_dir), state)
    return state


# ============================================================================
# Everything below was added for cleanup.py and the step helpers.
# ============================================================================
PYTHON = "/usr/bin/python3"


# ----------------------------------------------------------------------------
# Plain output (no "[tag]" prefix)
# ----------------------------------------------------------------------------
def out(text=""):
    print(text, flush=True)


def dotted_line(label, status, width=60):
    """'[STEP 1/10] preflight ........ PASS (1.2s)' - same look everywhere."""
    dots = max(3, width - len(label) - 1)
    return "%s %s %s" % (label, "." * dots, status)


def evidence(key, value):
    """Step-helper protocol line:  EVIDENCE: <key> = <value>  (column 0)."""
    print("EVIDENCE: %s = %s" % (key, value), flush=True)


def info(message):
    """'What the step is doing' line, indented under the step banner."""
    print("  %s" % message, flush=True)


def warn(message):
    print("WARNING: %s" % message, flush=True)


# ----------------------------------------------------------------------------
# Formatting helpers
# ----------------------------------------------------------------------------
def words(value):
    """Space-separated config value -> list (quotes honoured)."""
    return shlex.split(value or "")


def human_size(num_bytes):
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return "%d B" % size if unit == "B" else "%.1f %s" % (size, unit)
        size /= 1024.0


def human_age(seconds):
    seconds = max(0, int(seconds))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes = rest // 60
    if days:
        return "%dd %dh" % (days, hours)
    if hours:
        return "%dh %dm" % (hours, minutes)
    return "%dm" % minutes


# ----------------------------------------------------------------------------
# Run-directory files (steps, results, undo)
# ----------------------------------------------------------------------------
def _read_lines(path, required=False):
    try:
        with open(path, encoding="utf-8") as handle:
            return [line.rstrip("\n") for line in handle]
    except FileNotFoundError:
        if required:
            raise HelperError("file does not exist", path)
        return []
    except OSError as exc:
        raise HelperError("cannot read file: %s" % exc.strerror, path)


def read_steps(run_dir):
    """Ordered step names for this run (the 'steps' file)."""
    lines = _read_lines(os.path.join(run_dir, "steps"), required=True)
    return [line.strip() for line in lines if line.strip()]


def read_results(run_dir):
    """Rows appended by run_step.py: number<TAB>name<TAB>STATUS<TAB>seconds<TAB>detail.

    Missing file -> []. Malformed lines are skipped (cleanup must never crash on
    a half-written line after a crash).
    """
    rows = []
    for line in _read_lines(os.path.join(run_dir, "results")):
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        try:
            number = int(parts[0])
            seconds = float(parts[3]) if len(parts) > 3 and parts[3] else 0.0
        except ValueError:
            continue
        rows.append({"number": number, "name": parts[1], "status": parts[2],
                     "seconds": seconds,
                     "detail": parts[4] if len(parts) > 4 else ""})
    return rows


UNDO_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def read_undo(run_dir):
    """Undo names in the order they were registered (cleanup reverses them)."""
    names = []
    for line in _read_lines(os.path.join(run_dir, "undo")):
        name = line.strip()
        if name and name not in names:
            names.append(name)
    return names


def register_undo(run_dir, name):
    """Register lib/undo_<name>.py. Call BEFORE the step changes anything.

    Idempotent. Appended and fsync'd so it survives a crash a moment later.
    """
    if not UNDO_NAME_RE.match(name):
        raise HelperError("invalid undo name %r" % name, "register_undo")
    if name in read_undo(run_dir):
        return
    path = os.path.join(run_dir, "undo")
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (name + "\n").encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise HelperError("cannot register undo action: %s" % exc.strerror, path)


# ----------------------------------------------------------------------------
# Subprocesses
# ----------------------------------------------------------------------------
class CmdResult:
    def __init__(self, argv, rc, stdout, stderr, seconds, timed_out=False):
        self.argv = argv
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr
        self.seconds = seconds
        self.timed_out = timed_out

    @property
    def ok(self):
        return self.rc == 0 and not self.timed_out

    def command(self):
        return " ".join(shlex.quote(a) for a in self.argv)

    def first_error_line(self):
        if self.timed_out:
            return "timed out after %.0fs" % self.seconds
        for text in (self.stderr, self.stdout):
            for line in text.splitlines():
                if line.strip():
                    return line.strip()
        return "exit status %d" % self.rc

    def tail(self, count=10):
        lines = [l for l in (self.stdout + "\n" + self.stderr).splitlines() if l.strip()]
        return lines[-count:]


def run_cmd(argv, extra_env=None, timeout=None, cwd=None, echo_prefix=None,
            heartbeat=None):
    """Run a command; never raises for a non-zero exit.

    echo_prefix=None : output captured silently (stdout and stderr separate).
    echo_prefix="  " : stdout+stderr merged, echoed live line by line with that
                       prefix (and captured in .stdout); heartbeat=N prints a
                       "still running" line after N silent seconds.
    A missing binary gives rc 127, an unrunnable one rc 126, a timeout rc 124
    with timed_out=True (the child is killed).
    """
    env = os.environ.copy()
    env.update(extra_env or {})
    started = time.monotonic()
    merge = echo_prefix is not None
    try:
        proc = subprocess.Popen(
            argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT if merge else subprocess.PIPE,
            text=True, errors="replace", env=env, cwd=cwd)
    except FileNotFoundError:
        return CmdResult(argv, 127, "", "command not found: %s" % argv[0], 0.0)
    except OSError as exc:
        return CmdResult(argv, 126, "", "cannot execute %s: %s" % (argv[0], exc.strerror), 0.0)

    if not merge:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
            return CmdResult(argv, proc.returncode, stdout, stderr,
                             time.monotonic() - started)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            return CmdResult(argv, 124, stdout or "", stderr or "",
                             time.monotonic() - started, timed_out=True)

    lines_q = queue.Queue()

    def pump():
        for line in proc.stdout:
            lines_q.put(line.rstrip("\n"))
        lines_q.put(None)

    threading.Thread(target=pump, daemon=True).start()
    captured, timed_out = [], False
    last_output = time.monotonic()
    while True:
        try:
            item = lines_q.get(timeout=1.0)
        except queue.Empty:
            now = time.monotonic()
            if timeout and now - started > timeout and not timed_out:
                timed_out = True
                proc.kill()
            elif heartbeat and now - last_output >= heartbeat:
                out("%s... still running (%ds)" % (echo_prefix, now - started))
                last_output = now
            continue
        if item is None:
            break
        captured.append(item)
        out(echo_prefix + item)
        last_output = time.monotonic()
    rc = proc.wait()
    return CmdResult(argv, 124 if timed_out else rc, "\n".join(captured), "",
                     time.monotonic() - started, timed_out)


_REASON_RE = re.compile(r"(?:^|\s)REASON:\s*(.*)$")
_WHERE_RE = re.compile(r"(?:^|\s)WHERE:\s*(.*)$")
_SKIPPED_RE = re.compile(r"(?:^|\s)SKIPPED:\s*(.*)$")


def run_streaming(helper_path, args, name, heartbeat_seconds=30):
    """Run a helper script with /usr/bin/python3, echoing its output live.

    Returns {"rc", "seconds", "skipped", "reason", "where", "lines"}.
    Used by cleanup.py for undo helpers (they inherit its SIGINT/SIGTERM ignore).
    """
    result = run_cmd([PYTHON, helper_path] + list(args),
                     extra_env={"PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1"},
                     echo_prefix="  ", heartbeat=heartbeat_seconds)
    reason = where = skipped = ""
    lines = result.stdout.splitlines()
    for line in lines:
        for regex, slot in ((_REASON_RE, "reason"), (_WHERE_RE, "where"),
                            (_SKIPPED_RE, "skipped")):
            match = regex.search(line)
            if match:
                if slot == "reason":
                    reason = match.group(1).strip()
                elif slot == "where":
                    where = match.group(1).strip()
                else:
                    skipped = match.group(1).strip()
    if result.rc in (126, 127):          # could not even start
        reason = reason or result.stderr
        where = where or helper_path
    return {"rc": result.rc, "seconds": result.seconds, "skipped": skipped,
            "reason": reason, "where": where, "lines": lines}


def print_failure(result, helper, label):
    """Make sure a failed helper's REASON/WHERE are visible; return a short detail.

    The helper's own output was already echoed live, so only gaps are filled.
    """
    reason = result.get("reason") or ""
    if not reason:
        reason = "%s exited with status %d and printed no REASON line" % (label, result["rc"])
        out("  REASON: %s" % reason)
    if not result.get("where"):
        out("  WHERE: %s" % helper)
    return reason if len(reason) <= 70 else reason[:67] + "..."


# ----------------------------------------------------------------------------
# Step-helper protocol
# ----------------------------------------------------------------------------
class SkipStep(Exception):
    """Raise from a step to finish with 'SKIPPED: <reason>' and exit code 2."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def step_main(body):
    """Run a step helper's body and return its exit code.

    0 pass / 1 fail (REASON:, WHERE: printed) / 2 skipped / 130 interrupted.
    Never returns 10 (reserved for run_step.py).  Usage:
        sys.exit(step_main(main))
    """
    try:
        body()
        return 0
    except SkipStep as exc:
        print("SKIPPED: %s" % exc.reason, flush=True)
        return 2
    except HelperError as exc:
        print("REASON: %s" % exc.reason, flush=True)
        if exc.where:
            print("WHERE: %s" % exc.where, flush=True)
        return 1
    except KeyboardInterrupt:
        print("REASON: interrupted", flush=True)
        return 130
    except Exception as exc:  # a bug in the helper: still be informative
        frame = traceback.extract_tb(exc.__traceback__)[-1]
        print("REASON: unexpected %s: %s" % (type(exc).__name__, exc), flush=True)
        print("WHERE: %s:%d in %s" % (frame.filename, frame.lineno, frame.name), flush=True)
        for line in traceback.format_exc().rstrip().splitlines():
            print("    " + line, flush=True)
        return 1


# ----------------------------------------------------------------------------
# Run context: run.state + autodeploy.conf + the Pi env file
# ----------------------------------------------------------------------------
class RunContext:
    """run.state + autodeploy.conf + the Pi env file, with NO defaults and NO merging.

    Two sources, two meanings:
      Pi env file     (not in git)  machine-specific values: PG_*, *_APP_DIR, *_VENV_DIR,
                                    *_STATE_FILE, *_ENV_FILE, LOG_DIR, backup dir/glob ...
                                    read ONLY with env_value().
      autodeploy.conf (in git)      repo-specific values: unit lists, tools, table and
                                    marker names, timeouts ... read ONLY with
                                    conf_value() / conf_words().
    A key found in the "wrong" file is NOT used. Call require() first so every
    missing key is reported at once; the accessors never fall back to a default.
    """

    def __init__(self, run_dir, state, conf, envvals, conf_path, env_path, script_dir):
        self.run_dir = run_dir
        self.state = state
        self.conf = conf
        self.envvals = envvals
        self.conf_path = conf_path
        self.env_path = env_path
        self.script_dir = script_dir
        self.environment = state["ENVIRONMENT"]
        self.commit = state["COMMIT"]

    def require(self, env=(), conf=(), env_may_be_empty=(), conf_may_be_empty=()):
        """Fail (listing EVERY gap) unless all keys are present in the right file.

        env / conf          must be present and non-empty
        *_may_be_empty      must be present; an empty value is fine
        """
        problems, where = [], []
        for label, values, path, required, optional in (
                ("Pi env file", self.envvals, self.env_path, env, env_may_be_empty),
                ("autodeploy.conf", self.conf, self.conf_path, conf, conf_may_be_empty)):
            absent, empty = check_keys(values, list(dict.fromkeys(required)),
                                       list(dict.fromkeys(optional)))
            if absent or empty:
                problems.append("%s (%s)" % (label, describe_gaps(absent, empty)))
                where.append(path)
        if problems:
            raise HelperError("required configuration is incomplete: %s" % "; ".join(problems),
                              "  and  ".join(where))

    def env_value(self, key):
        """Value from the Pi env file ONLY. Missing key = failure, never a default."""
        if key not in self.envvals:
            raise HelperError("key %s is not in the Pi env file" % key, self.env_path)
        return self.envvals[key]

    def conf_value(self, key):
        """Value from autodeploy.conf ONLY. Missing key = failure, never a default."""
        if key not in self.conf:
            raise HelperError("key %s is not in autodeploy.conf" % key, self.conf_path)
        return self.conf[key]

    def conf_words(self, key):
        return words(self.conf_value(key))


def load_context(run_dir):
    """Re-open the run exactly as new_run.py initialised it.

    The conf/env file paths come ONLY from run.state (written by new_run.py);
    there is no guessing and no fallback location.
    """
    state = read_state(run_dir)
    missing = [k for k in ("ENVIRONMENT", "COMMIT", "CONF_FILE", "ENV_FILE", "SCRIPT_DIR")
               if not state.get(k)]
    if missing:
        raise HelperError("run.state is missing key(s): %s" % ", ".join(missing),
                          state_path(run_dir))
    if state["ENVIRONMENT"] not in ("test", "prd"):
        raise HelperError("run.state has an invalid ENVIRONMENT: %r" % state["ENVIRONMENT"],
                          state_path(run_dir))
    conf_path, env_path = state["CONF_FILE"], state["ENV_FILE"]
    return RunContext(run_dir, state, parse_kv_file(conf_path), parse_kv_file(env_path),
                      conf_path, env_path, state["SCRIPT_DIR"])


# ----------------------------------------------------------------------------
# Terminal (stdout is a pipe to tee, so always go through /dev/tty)
# ----------------------------------------------------------------------------
class Tty:
    """/dev/tty as separate read and write handles.

    open("/dev/tty", "r+") does NOT work: a tty is not seekable, so Python's
    read/write buffering refuses it ("File or stream is not seekable").
    """

    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer

    def write(self, text):
        self._writer.write(text)

    def flush(self):
        self._writer.flush()

    def readline(self):
        return self._reader.readline()

    def close(self):
        for handle in (self._reader, self._writer):
            try:
                handle.close()
            except OSError:
                pass


def open_tty():
    """Return a Tty for /dev/tty, or None if there is no controlling terminal."""
    reader = writer = None
    try:
        reader = open("/dev/tty", "r", encoding="utf-8")
        writer = open("/dev/tty", "w", encoding="utf-8", buffering=1)
        return Tty(reader, writer)
    except (OSError, ValueError):
        for handle in (reader, writer):
            if handle is not None:
                handle.close()
        return None


def ask_tty(prompt, valid):
    """Prompt on /dev/tty until the answer (upper-cased) is in `valid`. No timeout."""
    tty = open_tty()
    if tty is None:
        raise HelperError("no terminal available (/dev/tty cannot be opened)",
                          "open('/dev/tty')")
    try:
        while True:
            tty.write(prompt)
            tty.flush()
            line = tty.readline()
            if line == "":
                raise HelperError("terminal closed while waiting for an answer",
                                  "read('/dev/tty')")
            answer = line.strip().upper()
            if answer in valid:
                return answer
            tty.write("  Please answer one of: %s\n" % ", ".join(sorted(valid)))
    finally:
        tty.close()


# ----------------------------------------------------------------------------
# PostgreSQL
# ----------------------------------------------------------------------------
class PgError(HelperError):
    """psql / pg_* failed. .reason is the first line of the error text."""


LOCAL_HOSTS = ("", "localhost", "127.0.0.1", "::1")
SEP = "\x1f"


def norm_host(host):
    host = (host or "").strip().lower()
    return "local" if host in LOCAL_HOSTS or host.startswith("/") else host


class PgTarget:
    def __init__(self, label, host, port, user, password, dbname):
        self.label = label
        self.host = host
        self.port = str(port)
        self.user = user
        self.password = password
        self.dbname = dbname

    @classmethod
    def from_values(cls, label, values, source):
        """Build a target from a Pi env file's values. No defaults: a missing or
        empty PG_HOST/PG_PORT/PG_USER/PG_DB (or a missing PG_PASSWORD) fails."""
        absent, empty = check_keys(values, ("PG_HOST", "PG_PORT", "PG_USER", "PG_DB"),
                                   may_be_empty=("PG_PASSWORD",))
        if absent or empty:
            raise HelperError("%s database settings are incomplete (%s)"
                              % (label, describe_gaps(absent, empty)), source)
        return cls(label, values["PG_HOST"], values["PG_PORT"], values["PG_USER"],
                   values["PG_PASSWORD"], values["PG_DB"])

    def env(self):
        """Connection settings for libpq tools. The password never goes on a command line."""
        return {"PGHOST": self.host, "PGPORT": self.port, "PGUSER": self.user,
                "PGPASSWORD": self.password, "PGDATABASE": self.dbname,
                "PGCONNECT_TIMEOUT": "10", "PGAPPNAME": "pifitness-autodeploy"}

    def describe(self):
        return "%s:%s/%s as %s" % (self.host, self.port, self.dbname, self.user)

    def same_database_as(self, other):
        return (norm_host(self.host) == norm_host(other.host)
                and self.port == other.port and self.dbname == other.dbname)


def psql_rows(target, sql, timeout=60):
    """Run one SQL statement; return rows as lists of strings (NULL -> '')."""
    argv = ["psql", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1", "-F", SEP, "-c", sql]
    result = run_cmd(argv, extra_env=target.env(), timeout=timeout)
    if not result.ok:
        raise PgError(result.first_error_line(),
                      "psql %s: %s" % (target.describe(), " ".join(sql.split())[:90]))
    return [line.split(SEP) for line in result.stdout.splitlines() if line != ""]


# ----------------------------------------------------------------------------
# TEST / PRD database state marker files
#   state=clean|dirty|migrated
#   baseline_dump=/path/to/the/pinned/dump
# ----------------------------------------------------------------------------
DB_STATES = ("clean", "dirty", "migrated")


def read_db_marker(path):
    """-> (state, values). state is clean|dirty|migrated|absent|unreadable.

    A bare word ("dirty") is also accepted for the state.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return "absent", {}
    except OSError:
        return "unreadable", {}
    token = text.strip().lower()
    if token in DB_STATES:
        return token, {}
    try:
        values = parse_kv_file(path)
    except HelperError:
        return "unreadable", {}
    state = values.get("state", "").strip().lower()
    return (state if state in DB_STATES else "unreadable"), values


def update_db_marker(path, state=None, **extra):
    """Atomically set the state and/or other keys (e.g. baseline_dump), keeping the rest."""
    current_state, values = read_db_marker(path)
    values = dict(values)
    if "state" not in values and current_state in DB_STATES:
        values["state"] = current_state
    if state is not None:
        if state not in DB_STATES:
            raise HelperError("invalid database state %r" % state, path)
        values["state"] = state
    values.update({k: str(v) for k, v in extra.items()})
    write_state(path, values)


# ----------------------------------------------------------------------------
# Filesystem checks (read-only: nothing is created)
# ----------------------------------------------------------------------------
def writable_dir(path):
    """True if path is a directory we can create files in (and not mounted read-only)."""
    try:
        if not os.path.isdir(path) or not os.access(path, os.W_OK | os.X_OK):
            return False
        return not (os.statvfs(path).f_flag & os.ST_RDONLY)
    except OSError:
        return False


def nearest_existing_ancestor(path):
    path = os.path.abspath(path)
    while not os.path.exists(path):
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent
    return path


def is_inside(path, parent):
    path, parent = os.path.realpath(path), os.path.realpath(parent)
    return path == parent or path.startswith(parent.rstrip(os.sep) + os.sep)
