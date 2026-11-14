#!/usr/bin/env python3
"""
deploy/scripts/lib/checkoutlib.py  --  directory-swap helpers shared by
step_checkout.py (does the swap) and undo_checkout.py (reverses / tidies it).

Not executable on its own. Python 3 standard library only.

Layout around the app directory <APP_DIR> (all siblings, so the same filesystem):

    <APP_DIR>                      the live checkout
    <APP_DIR>.stage-<run id>       the new checkout while it is built; after the swap it
                                   holds the PREVIOUS checkout (TEST: deleted at once;
                                   PRD: kept until the exit handler decides)
    <APP_DIR>.swap-tmp             only used by the three-rename fallback

The swap is one renameat2(RENAME_EXCHANGE) call: the live path is never absent and a
reader sees either the old tree or the new one. A directory keeps its inode across a
rename, so "is the new checkout live?" is answered by comparing inodes, which stays
true even if the process dies between two lines (the id is recorded in run.state
BEFORE the swap).
"""
import ctypes
import errno
import os
import shutil
import stat

STAGE_INFIX = ".stage-"
SWAP_SUFFIX = ".swap-tmp"

_RENAME_EXCHANGE = 2
_AT_FDCWD = -100


def stage_path(app_dir, run_id):
    return "%s%s%s" % (app_dir, STAGE_INFIX, run_id)


def swap_tmp_path(app_dir):
    return app_dir + SWAP_SUFFIX


def dir_id(path):
    """'<st_dev>:<st_ino>' of the directory itself (a symlink is not followed)."""
    info = os.lstat(path)
    return "%d:%d" % (info.st_dev, info.st_ino)


def has_id(path, ident):
    """True if path exists and is the directory recorded as ident."""
    try:
        return bool(ident) and dir_id(path) == ident
    except OSError:
        return False


def is_ours(app_dir, path):
    """True only for the sibling names this feature creates (guards every deletion)."""
    if os.path.dirname(path) != os.path.dirname(app_dir):
        return False
    base, app = os.path.basename(path), os.path.basename(app_dir)
    return base.startswith(app + STAGE_INFIX) or base == app + SWAP_SUFFIX


def stale_stage_dirs(app_dir, keep=()):
    """Other <APP_DIR>.stage-* siblings (left by a run that died before it could clean up)."""
    parent, prefix = os.path.dirname(app_dir), os.path.basename(app_dir) + STAGE_INFIX
    try:
        names = sorted(os.listdir(parent))
    except OSError:
        return []
    return [os.path.join(parent, n) for n in names
            if n.startswith(prefix) and os.path.join(parent, n) not in keep]


def remove_tree(path):
    """Delete a directory tree (or a lone file/symlink), fixing read-only permissions."""
    def onerror(func, target, _exc):
        try:
            parent = os.path.dirname(target)
            os.chmod(parent, os.stat(parent).st_mode | stat.S_IWUSR | stat.S_IXUSR)
            if os.path.isdir(target) and not os.path.islink(target):
                os.chmod(target, os.stat(target).st_mode | stat.S_IRWXU)
            func(target)
        except OSError:
            raise

    if os.path.islink(path) or os.path.isfile(path):
        os.unlink(path)
    elif os.path.isdir(path):
        shutil.rmtree(path, onerror=onerror)


def exchange(first, second):
    """Atomically swap two paths (Linux renameat2 RENAME_EXCHANGE). Raises OSError."""
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        fn = libc.renameat2
    except (OSError, AttributeError):
        raise OSError(errno.ENOSYS, "renameat2 is not available")
    fn.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    fn.restype = ctypes.c_int
    if fn(_AT_FDCWD, os.fsencode(first), _AT_FDCWD, os.fsencode(second), _RENAME_EXCHANGE) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


def swap_in(new_dir, app_dir):
    """Make new_dir the live app_dir. Returns a description of the method used.

    Afterwards new_dir's path holds whatever app_dir held before (if anything was there).
    Works in both directions: swap_in(previous, app_dir) puts the previous tree back.
    """
    if not os.path.lexists(app_dir):
        os.rename(new_dir, app_dir)
        return "rename (nothing was at the app dir)"
    try:
        exchange(new_dir, app_dir)
        return "atomic exchange (renameat2)"
    except OSError as exc:
        if exc.errno not in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP):
            raise
    tmp = swap_tmp_path(app_dir)
    if os.path.lexists(tmp):
        raise OSError(errno.EEXIST, "leftover swap directory exists: %s" % tmp)
    os.rename(app_dir, tmp)
    os.rename(new_dir, app_dir)
    os.rename(tmp, new_dir)
    return "three renames (renameat2 unsupported; the app dir was absent for an instant)"
