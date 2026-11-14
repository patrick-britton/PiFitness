#!/usr/bin/env python3
"""
deploy/scripts/lib/step_checkout.py  --  TEST step 2 / PRD step 3: clean application checkout.

    step_checkout.py --run-dir <dir>

OBJECTIVE
    The app directory (APP_DIR from the Pi env file) contains exactly the requested commit's
    files, plus configured persistent resources, plus the environment file wiring, and
    nothing else. It is replaced atomically; the previous checkout is never modified.

    TEST: the previous checkout is deleted as soon as the swap has succeeded; backend/.env
          is a SYMLINK to the TEST env file.
    PRD : the previous checkout is KEPT, and undo_checkout.py (run by the exit handler)
          deletes it on success or swaps it back on any other outcome; backend/.env is a
          COPY of the PRD env file (it survives swap + rollback without a dangling link).

STAGES
    (1/8) Configuration             keys, paths, safety guards (read-only)
    (2/8) Fetch the exact commit    registers the undo, builds <APP_DIR>.stage-<run id>
                                    from the launcher's local repo (see SOURCE)
    (3/8) Verify the checkout       HEAD == requested SHA, clean tree, expected paths
    (4/8) Persistent resources      copied from the previous checkout (it is not modified)
    (5/8) Environment file          symlink (TEST) or copy (PRD) at CHECKOUT_ENV_FILE_PATH
    (6/8) Final checks              untracked files must be 0 apart from the two above
    (7/8) Swap in                   one atomic exchange
    (8/8) Previous checkout         TEST: deleted. PRD: retained for the exit handler.

SOURCE OF THE COMMIT (nothing to configure: no URL, no branch)
    The launcher has already fetched the exact commit (its SHA is --commit) into the repo at
    the PRD app dir before autodeploy.sh starts. This step fetches that one SHA from there:
        PRD run : the repo is this run's own APP_DIR (read before the swap replaces it)
        TEST run: the repo is APP_DIR from prd.env (read via common.load_prd_source)
    Stage 1 proves, read-only, that the path is a git checkout that contains the commit and
    has an 'origin' remote. The new checkout is given the SAME 'origin' URL, so after a swap
    the launcher's next "git fetch origin ..." still works in the new directory.

KEYS (no defaults anywhere; a missing key fails the step before anything is touched)
    Pi env file     APP_DIR  VENV_DIR  STATE_FILE  LOG_DIR
                    (TEST run also: APP_DIR in prd.env, which load_prd_source requires)
    autodeploy.conf CHECKOUT_EXPECTED_PATHS  CHECKOUT_ENV_FILE_PATH  PERSISTENT_RESOURCES (may be empty)

STATE WRITTEN (run.state), all BEFORE anything is changed, so undo_checkout.py can act
even if this process is killed:
    CHECKOUT_APP_DIR  CHECKOUT_STAGE_DIR  CHECKOUT_NEW_DIR_ID  CHECKOUT_KEEP_PREVIOUS
    CHECKOUT_PREVIOUS_EXISTED  (after the swap: CHECKOUT_PREVIOUS_DIR, CHECKOUT_STATUS)

Does not stop or start units, touch the venv, or touch any database.
Python 3 standard library only.
"""
import filecmp
import os
import re
import shutil
import sys
import time

from common import (HelperError, Parser, evidence, info, is_inside, load_context,
                    load_prd_source, register_undo, run_cmd, step_main, update_state, warn,
                    words, writable_dir)
import checkoutlib as cl

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
UNDO_NAME = "checkout"
SAMPLE = 20
TOTAL_STAGES = 8


def parse_args(argv):
    parser = Parser(prog="step_checkout.py", allow_abbrev=False)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    args.run_dir = os.path.abspath(args.run_dir)
    if not os.path.isdir(args.run_dir):
        raise HelperError("run directory does not exist", args.run_dir)
    return args


def mask_url(text):
    """Hide credentials embedded in a URL (https://user:token@host/...)."""
    return re.sub(r"(//)[^/@\s]+@", r"\1***@", text)


def clean_relative(path, label, where):
    """Normalise a repo-relative path; refuse absolute, empty or escaping paths."""
    if os.path.isabs(path):
        raise HelperError("%s must be relative to the app dir: %r" % (label, path), where)
    norm = os.path.normpath(path)
    if norm in (".", "") or norm == ".." or norm.startswith(".." + os.sep):
        raise HelperError("%s must stay inside the app dir: %r" % (label, path), where)
    return norm


def count_files(root):
    """Files under root, not counting the .git directory."""
    total = 0
    for _dirpath, dirnames, filenames in os.walk(root):
        if ".git" in dirnames:
            dirnames.remove(".git")
        total += len(filenames)
    return total


class Checkout:
    def __init__(self, ctx):
        self.ctx = ctx
        self.number = 0
        self.swapped = False
        self.staged = False          # the staging directory exists and is ours
        self.git_env = {"GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "/bin/true"}
        if "GIT_SSH_COMMAND" not in os.environ:
            self.git_env["GIT_SSH_COMMAND"] = "ssh -o BatchMode=yes"

    # ------------------------------------------------------------------ driver
    def run(self):
        stages = [
            ("Configuration", self.stage_config),
            ("Fetch the exact commit", self.stage_fetch),
            ("Verify the checkout", self.stage_verify),
            ("Persistent resources", self.stage_persistent),
            ("Environment file", self.stage_env),
            ("Final checks", self.stage_final),
            ("Swap in", self.stage_swap),
            ("Previous checkout", self.stage_previous),
        ]
        assert len(stages) == TOTAL_STAGES
        try:
            for title, function in stages:
                self.number += 1
                info("(%d/%d) %s ..." % (self.number, TOTAL_STAGES, title))
                function()
        except BaseException:
            # Before the swap: the previous checkout is untouched and still active; just
            # remove the temporary checkout. After the swap the registered undo decides.
            if self.staged and not self.swapped:
                self.discard_stage()
            raise

    def discard_stage(self):
        try:
            if os.path.lexists(self.stage) and cl.is_ours(self.app_dir, self.stage):
                cl.remove_tree(self.stage)
                info("temporary checkout removed: %s" % self.stage)
        except OSError as exc:
            warn("could not remove the temporary checkout %s: %s (the exit handler will retry)"
                 % (self.stage, exc.strerror))

    # --------------------------------------------------------------- 1. config
    def stage_config(self):
        c = self.ctx
        c.require(env=["APP_DIR", "VENV_DIR", "STATE_FILE", "LOG_DIR"],
                  conf=["CHECKOUT_EXPECTED_PATHS", "CHECKOUT_ENV_FILE_PATH"],
                  conf_may_be_empty=["PERSISTENT_RESOURCES"])

        self.sha = c.commit.strip().lower()
        if not SHA_RE.match(self.sha):
            raise HelperError("run.state COMMIT is not 40 hex characters: %r" % c.commit,
                              c.run_dir)
        self.test_run = c.environment == "test"
        self.keep_previous = not self.test_run

        app_dir = c.env_value("APP_DIR").rstrip(os.sep)
        if not os.path.isabs(app_dir):
            raise HelperError("APP_DIR must be an absolute path: %r" % app_dir, c.env_path)
        self.app_dir = os.path.normpath(app_dir)
        parent = os.path.dirname(self.app_dir)
        if parent == self.app_dir or parent == os.sep:
            raise HelperError("APP_DIR is too close to the filesystem root: %s" % self.app_dir,
                              c.env_path)
        if os.path.islink(self.app_dir):
            raise HelperError("APP_DIR is a symbolic link; it must be a real directory "
                              "(a swap would replace the link, not the checkout)", self.app_dir)
        if os.path.lexists(self.app_dir) and not os.path.isdir(self.app_dir):
            raise HelperError("APP_DIR exists but is not a directory", self.app_dir)
        if not writable_dir(parent):
            raise HelperError("the directory holding APP_DIR is not writable", parent)
        self.previous_exists = os.path.isdir(self.app_dir)

        # Things that must survive the swap, or that the swap must not pull out from
        # under a running process, may not live inside APP_DIR.
        for label, path in (("VENV_DIR", c.env_value("VENV_DIR")),
                            ("STATE_FILE", c.env_value("STATE_FILE")),
                            ("LOG_DIR", c.env_value("LOG_DIR")),
                            ("the Pi env file", c.env_path),
                            ("the running scripts (launcher temp folder)", c.script_dir),
                            ("the run directory", c.run_dir)):
            if is_inside(path, self.app_dir):
                raise HelperError("%s is INSIDE the app dir and would be destroyed by the swap: %s"
                                  % (label, path), "%s / %s" % (c.conf_path, c.env_path))

        git = run_cmd(["git", "--version"], timeout=30)
        if not git.ok:
            raise HelperError("git is not usable: %s" % git.first_error_line(), git.command())

        self.resolve_source_repo()

        self.expected_paths = [clean_relative(p, "CHECKOUT_EXPECTED_PATHS entry", c.conf_path)
                               for p in c.conf_words("CHECKOUT_EXPECTED_PATHS")]
        self.env_rel = clean_relative(c.conf_value("CHECKOUT_ENV_FILE_PATH"),
                                      "CHECKOUT_ENV_FILE_PATH", c.conf_path)
        self.env_source = os.path.abspath(c.env_path)

        # Persistent resources: relative to APP_DIR (or absolute). Only those INSIDE the app
        # dir need carrying; an absolute path elsewhere is kept in place by simply not
        # being touched.
        self.resources, self.resources_outside = [], []
        for item in c.conf_words("PERSISTENT_RESOURCES"):
            if os.path.isabs(item):
                if not is_inside(item, self.app_dir):
                    self.resources_outside.append(item)
                    continue
                item = os.path.relpath(os.path.normpath(item), self.app_dir)
            rel = clean_relative(item, "PERSISTENT_RESOURCES entry", c.conf_path)
            if any(rel == r or rel.startswith(r + os.sep) for r in self.resources):
                continue            # already covered by a parent entry
            self.resources = [r for r in self.resources if not r.startswith(rel + os.sep)]
            self.resources.append(rel)

        self.run_id = os.path.basename(c.run_dir)
        if self.run_id.endswith(".d"):
            self.run_id = self.run_id[:-2]
        self.stage = cl.stage_path(self.app_dir, self.run_id)
        if os.path.lexists(self.stage):
            raise HelperError("the staging path already exists", self.stage)
        if os.path.lexists(cl.swap_tmp_path(self.app_dir)):
            raise HelperError("a swap directory from an interrupted run is present; the exit "
                              "handler of that run could not finish", cl.swap_tmp_path(self.app_dir))

        stale = cl.stale_stage_dirs(self.app_dir, keep=(self.stage,))
        for path in stale:
            warn("leftover directory from an earlier interrupted run (not touched): %s" % path)

        evidence("environment", c.environment)
        evidence("requested_sha", self.sha)
        evidence("app_dir", "%s (%s)" % (self.app_dir, "previous checkout present"
                                         if self.previous_exists else "does not exist yet"))
        evidence("source_repo", "%s (contains the commit)" % self.source_repo)
        evidence("origin_remote", "%s (copied onto the new checkout)" % mask_url(self.origin_url))
        evidence("staging_dir", self.stage)
        evidence("env_file_wiring", "%s backend file %s -> %s" % (
            "symlink" if self.test_run else "copy", self.env_rel, self.env_source))
        evidence("previous_checkout_policy", "deleted right after the swap" if self.test_run
                 else "retained until the exit handler decides (success: deleted; else: swapped back)")
        evidence("leftover_stage_dirs", str(len(stale)))

    def resolve_source_repo(self):
        """Find the repo the launcher fetched the commit into; read-only.

        Sets self.source_repo and self.origin_url. Nothing is configured: a PRD run uses its
        own APP_DIR, a TEST run uses APP_DIR from prd.env.
        """
        c = self.ctx
        if self.test_run:
            prd = load_prd_source(c)
            path, whose, where = prd.app_dir, "APP_DIR in prd.env", prd.env_path
        else:
            path, whose, where = self.app_dir, "APP_DIR", c.env_path
        if not os.path.isabs(path):
            raise HelperError("%s must be an absolute path: %r" % (whose, path), where)
        path = os.path.normpath(path.rstrip(os.sep) or os.sep)
        if not os.path.isdir(os.path.join(path, ".git")):
            raise HelperError("%s (%s) is not a git checkout; the launcher fetches the commit "
                              "into it, so it must be one" % (whose, path), where)
        have = self.git(["cat-file", "-e", "%s^{commit}" % self.sha], cwd=path, timeout=60)
        if not have.ok:
            raise HelperError("commit %s is not in %s (%s): %s; the launcher fetches the "
                              "commit into the repo it was started with, so this is not that "
                              "repo" % (self.sha, path, whose, have.first_error_line()), where)
        origin = self.git(["config", "--get", "remote.origin.url"], cwd=path, timeout=60)
        url = origin.stdout.strip() if origin.ok else ""
        if not url:
            raise HelperError("the repo at %s has no 'origin' remote. The launcher's "
                              "'git fetch origin' needs one, and the new checkout could not "
                              "be given one" % path, where)
        self.source_repo, self.origin_url = path, url

    # ---------------------------------------------------------------- git helpers
    def git(self, args, cwd=None, timeout=300, echo=False, extra_env=None):
        env = dict(self.git_env)
        env.update(extra_env or {})
        argv = ["git"] + args
        if echo:
            return run_cmd(argv, extra_env=env, timeout=timeout, cwd=cwd or self.stage,
                           echo_prefix="    git: ", heartbeat=30)
        return run_cmd(argv, extra_env=env, timeout=timeout, cwd=cwd or self.stage)

    def git_fail(self, what, result):
        tail = [mask_url(l) for l in result.tail(6)]
        raise HelperError("%s: %s" % (what, mask_url(result.first_error_line())),
                          "%s%s" % (mask_url(result.command()),
                                    ("  | last output: " + " / ".join(tail)) if tail else ""))

    def head_sha(self, path):
        result = self.git(["rev-parse", "HEAD"], cwd=path, timeout=60)
        if not result.ok:
            self.git_fail("cannot read HEAD of the checkout", result)
        return result.stdout.strip().lower()

    def status_lines(self, path):
        result = self.git(["status", "--porcelain", "--untracked-files=all"], cwd=path, timeout=120)
        if not result.ok:
            self.git_fail("git status failed", result)
        return [line for line in result.stdout.splitlines() if line.strip()]

    # ---------------------------------------------------------- 2. fetch commit
    def stage_fetch(self):
        c = self.ctx
        source = self.source_repo

        # Everything the undo needs is recorded BEFORE the first thing is created.
        register_undo(c.run_dir, UNDO_NAME)
        update_state(c.run_dir, {
            "CHECKOUT_APP_DIR": self.app_dir,
            "CHECKOUT_STAGE_DIR": self.stage,
            "CHECKOUT_NEW_DIR_ID": "",
            "CHECKOUT_KEEP_PREVIOUS": "yes" if self.keep_previous else "no",
            "CHECKOUT_PREVIOUS_EXISTED": "yes" if self.previous_exists else "no",
            "CHECKOUT_STATUS": "staging",
        })

        try:
            os.mkdir(self.stage)
        except OSError as exc:
            raise HelperError("cannot create the staging directory: %s" % exc.strerror, self.stage)
        self.staged = True
        self.new_id = cl.dir_id(self.stage)
        update_state(c.run_dir, {"CHECKOUT_NEW_DIR_ID": self.new_id})

        if self.previous_exists and os.stat(self.stage).st_dev != os.stat(self.app_dir).st_dev:
            raise HelperError("the staging directory and APP_DIR are on different filesystems; "
                              "an atomic swap is impossible", "%s vs %s" % (self.stage, self.app_dir))

        init = self.git(["init", "-q", self.stage], cwd=os.path.dirname(self.stage), timeout=60)
        if not init.ok:
            self.git_fail("git init failed", init)

        # Fetch only the one commit, from the launcher's local repo. If a shallow fetch is
        # refused, retry the same SHA without --depth (still one commit's worth, locally).
        method = "shallow fetch of the exact SHA from the local repo"
        shallow = self.git(["fetch", "--no-tags", "--depth", "1", source, self.sha],
                           timeout=900, echo=True)
        if not shallow.ok:
            warn("shallow fetch failed (%s); retrying without --depth"
                 % mask_url(shallow.first_error_line()))
            full = self.git(["fetch", "--no-tags", source, self.sha], timeout=900, echo=True)
            if not full.ok:
                self.git_fail("cannot fetch the commit from the local repo %s" % source, full)
            method = "fetch of the exact SHA from the local repo (no --depth)"
        have = self.git(["cat-file", "-e", "%s^{commit}" % self.sha], timeout=60)
        if not have.ok:
            raise HelperError("commit %s is not available after fetching from %s"
                              % (self.sha, source), mask_url(source))
        checkout = self.git(["-c", "advice.detachedHead=false", "checkout", "-q", "--detach",
                             "--force", self.sha], timeout=600)
        if not checkout.ok:
            self.git_fail("git checkout of the commit failed", checkout)
        if os.path.exists(os.path.join(self.stage, ".gitmodules")):
            raise HelperError("the commit uses git submodules, which this step does not fetch; "
                              "a silent partial checkout is refused", self.stage)
        # Give the new checkout the same 'origin' as the repo it replaces, so the launcher's
        # next "git fetch origin ..." (run inside this directory) keeps working after a swap.
        remote = self.git(["remote", "add", "origin", self.origin_url], timeout=60)
        if not remote.ok:
            self.git_fail("cannot set the origin remote on the new checkout", remote)
        evidence("fetch_method", method)
        evidence("origin_remote_set", mask_url(self.origin_url))

    # ------------------------------------------------------------- 3. verify
    def stage_verify(self):
        actual = self.head_sha(self.stage)
        if actual != self.sha:
            raise HelperError("new checkout is at %s, not the requested %s" % (actual, self.sha),
                              self.stage)
        dirty = self.status_lines(self.stage)
        if dirty:
            raise HelperError("new checkout is not clean (%d path(s), e.g. %s)"
                              % (len(dirty), dirty[0]), "git status in " + self.stage)
        missing = [p for p in self.expected_paths
                   if not os.path.exists(os.path.join(self.stage, p))]
        if missing:
            raise HelperError("expected top-level path(s) missing from the commit: %s"
                              % ", ".join(missing), "CHECKOUT_EXPECTED_PATHS in " + self.ctx.conf_path)
        tracked = self.git(["ls-files"], timeout=120)
        tracked_count = len(tracked.stdout.splitlines()) if tracked.ok else -1

        evidence("actual_sha", actual)
        evidence("clean_tree", "yes (git status --porcelain is empty)")
        evidence("expected_paths", "%s (all present)" % " ".join(self.expected_paths))
        evidence("file_count", "%d on disk, %d tracked" % (count_files(self.stage), tracked_count))
        self.compare_running_scripts()

    def compare_running_scripts(self):
        """The scripts running now came from the launcher's extract of this commit; confirm."""
        src_root = os.path.join(self.stage, "deploy", "scripts")
        run_root = self.ctx.script_dir
        if not os.path.isdir(src_root):
            warn("the commit has no deploy/scripts directory")
            return
        same = differ = missing = 0
        examples = []
        for dirpath, dirnames, filenames in os.walk(src_root):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            for name in filenames:
                rel = os.path.relpath(os.path.join(dirpath, name), src_root)
                mine = os.path.join(run_root, rel)
                if not os.path.isfile(mine):
                    missing += 1
                    examples.append(rel + " (not in the running copy)")
                elif filecmp.cmp(os.path.join(dirpath, name), mine, shallow=False):
                    same += 1
                else:
                    differ += 1
                    examples.append(rel + " (differs)")
        evidence("running_scripts_match_checkout", "%d identical, %d differ, %d missing"
                 % (same, differ, missing))
        if differ or missing:
            warn("the running deploy scripts are not byte-identical to this commit's "
                 "deploy/scripts: %s" % ", ".join(examples[:SAMPLE]))

    # ------------------------------------------------- 4. persistent resources
    def stage_persistent(self):
        carried = []
        for rel in self.resources:
            src = os.path.join(self.app_dir, rel)
            dst = os.path.join(self.stage, rel)
            if not os.path.lexists(src):
                warn("persistent resource not present, nothing to carry: %s" % src)
                continue
            if os.path.lexists(dst):
                raise HelperError("persistent resource %s collides with a file in the commit" % rel,
                                  "PERSISTENT_RESOURCES in " + self.ctx.conf_path)
            try:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                if os.path.islink(src):
                    os.symlink(os.readlink(src), dst)
                elif os.path.isdir(src):
                    shutil.copytree(src, dst, symlinks=True)
                else:
                    shutil.copy2(src, dst)
            except (OSError, shutil.Error) as exc:
                raise HelperError("cannot carry persistent resource %s: %s" % (rel, exc), src)
            carried.append(rel)
        self.carried = carried
        evidence("persistent_resources_preserved", " ".join(carried) if carried else "none")
        if self.resources_outside:
            evidence("persistent_resources_outside_app_dir",
                     "%s (left in place)" % " ".join(self.resources_outside))

    # ------------------------------------------------------- 5. env file wiring
    def stage_env(self):
        if not os.path.isfile(self.env_source):
            raise HelperError("the Pi env file is not a regular file", self.env_source)
        link = os.path.join(self.stage, self.env_rel)
        parent = os.path.dirname(link)
        if not os.path.isdir(parent):
            raise HelperError("directory for the env file does not exist in the commit: %s"
                              % os.path.relpath(parent, self.stage), "CHECKOUT_ENV_FILE_PATH")
        if os.path.lexists(link):
            raise HelperError("%s already exists in the commit; refusing to overwrite it"
                              % self.env_rel, self.stage)
        try:
            if self.test_run:
                os.symlink(self.env_source, link)
                if os.path.realpath(link) != os.path.realpath(self.env_source):
                    raise HelperError("symlink does not resolve to the env file", link)
                how = "symlink %s -> %s" % (self.env_rel, self.env_source)
            else:
                shutil.copyfile(self.env_source, link)
                os.chmod(link, 0o600)
                if not filecmp.cmp(self.env_source, link, shallow=False):
                    raise HelperError("copied env file differs from its source", link)
                how = "copy %s -> %s (mode 600)" % (self.env_source, self.env_rel)
        except OSError as exc:
            raise HelperError("cannot wire the env file: %s" % exc.strerror, link)
        evidence("env_file", how)

    # ------------------------------------------------------------ 6. final checks
    def stage_final(self):
        actual = self.head_sha(self.stage)
        if actual != self.sha:
            raise HelperError("HEAD moved to %s" % actual, self.stage)
        allowed = [self.env_rel] + list(self.carried)
        stray, changed = [], []
        for line in self.status_lines(self.stage):
            code, path = line[:2], line[3:].strip().strip('"')
            if code == "??":
                if not any(path == a or path.startswith(a + os.sep) for a in allowed):
                    stray.append(path)
            else:
                changed.append(line)
        if changed:
            raise HelperError("tracked files differ from the commit (%d, e.g. %s)"
                              % (len(changed), changed[0]), "git status in " + self.stage)
        if stray:
            raise HelperError("%d untracked file(s) beyond the persistent resources and env file "
                              "(e.g. %s)" % (len(stray), stray[0]), "git status in " + self.stage)
        evidence("untracked_files", "0 (apart from persistent resources and %s)" % self.env_rel)
        evidence("file_count_final", str(count_files(self.stage)))

    # ----------------------------------------------------------------- 7. swap
    def stage_swap(self):
        c = self.ctx
        update_state(c.run_dir, {"CHECKOUT_STATUS": "swapping"})
        try:
            method = cl.swap_in(self.stage, self.app_dir)
        except OSError as exc:
            raise HelperError("swap failed: %s" % exc.strerror, "%s <-> %s" % (self.stage, self.app_dir))
        self.swapped = True
        update_state(c.run_dir, {"CHECKOUT_STATUS": "swapped",
                                 "CHECKOUT_PREVIOUS_DIR": self.stage if self.previous_exists else ""})
        if not cl.has_id(self.app_dir, self.new_id):
            raise HelperError("after the swap APP_DIR is not the new checkout", self.app_dir)
        live = self.head_sha(self.app_dir)
        if live != self.sha:
            raise HelperError("after the swap APP_DIR is at %s, not %s" % (live, self.sha),
                              self.app_dir)
        evidence("swap_method", method)
        evidence("live_checkout", "%s at %s" % (self.app_dir, live))

    # ------------------------------------------------------ 8. previous checkout
    def stage_previous(self):
        c = self.ctx
        if not self.previous_exists:
            evidence("previous_checkout", "none (APP_DIR did not exist before this run)")
            update_state(c.run_dir, {"CHECKOUT_STATUS": "done"})
            return

        previous = self.stage          # the exchange left the old tree at the staging path
        try:
            old_names = set(os.listdir(previous)) - {".git"}
            new_names = set(os.listdir(self.app_dir)) - {".git"}
            only_old = sorted(old_names - new_names)
        except OSError:
            only_old = []
        if self.test_run:
            if only_old:
                shown = " ".join(only_old[:SAMPLE]) + (" ..." if len(only_old) > SAMPLE else "")
                warn("previous TEST checkout held top-level entries that are NOT in the new one "
                     "and are being discarded: %s" % shown)
            try:
                cl.remove_tree(previous)
                evidence("old_checkout_removed", previous)
            except OSError as exc:
                warn("could not delete the previous checkout %s: %s; the exit handler will retry"
                     % (previous, exc.strerror))
                evidence("old_checkout_removed", "NO (%s)" % exc.strerror)
        else:
            evidence("previous_checkout_retained", "%s (undo_checkout deletes it on success, "
                                                   "swaps it back otherwise)" % previous)
        update_state(c.run_dir, {"CHECKOUT_STATUS": "done"})
        evidence("checkout", "ready: %s is commit %s" % (self.app_dir, self.sha[:12]))


def main():
    args = parse_args(sys.argv[1:])
    started = time.monotonic()
    Checkout(load_context(args.run_dir)).run()
    info("checkout complete (%.1fs)" % (time.monotonic() - started))


if __name__ == "__main__":
    sys.exit(step_main(main))
