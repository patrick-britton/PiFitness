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

Python 3 standard library only.
"""
import argparse
import os
import re
import shlex
import sys
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
