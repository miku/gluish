# coding: utf-8

"""
Path related functions.
"""

from gluish.utils import random_string
import hashlib
import os
import tempfile
import time
import warnings


def unlink(path):
    """
    Unlink path, if it exists, otherwise do nothing.
    """
    try:
        os.remove(path)
    except OSError as err:
        if err.errno == 2:
            pass
        else:
            raise


def fileage(path):
    """
    Return the file age in seconds.
    """
    return time.time() - os.stat(path).st_mtime


def size(path):
    """
    Return the size of a directory tree or a single file.
    """
    if not os.path.isdir(path):
        return os.path.getsize(path)
    total_size = os.path.getsize(path)
    for item in os.listdir(path):
        itempath = os.path.join(path, item)
        if os.path.isfile(itempath):
            total_size += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            total_size += size(itempath)
    return total_size


def size_fmt(num):
    """
    Format integer as human readables capacities.
    """
    for unit in ('b', 'K', 'M', 'G', 'T', 'P', 'E'):
        if num < 1024.0:
            return "%3.1f%s" % (num, unit)
        num /= 1024.0


def wc(path):
    """
    Like `wc -l`.
    """
    with open(path) as handle:
        num_lines = sum(1 for line in handle)
    return num_lines


def filehash(path, blocksize=65536):
    """
    Compute the sha1 hash of the file contents.
    """
    hasher = hashlib.sha1()
    with open(path, 'rb') as handle:
        buf = handle.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = handle.read(blocksize)
    return hasher.hexdigest()


def touch(path, times=None):
    """
    Unix touch in Python.
    """
    with file(path, 'a'):
        os.utime(path, times)


def which(program):
    """
    Return `None` if no executable can be found.
    """
    def is_exe(fpath):
        """ Is `fpath` executable? ` """
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

def iterfiles(directory='.', fun=None):
    """
    Yield paths below a directory, optionally filter the path through a function
    given in `fun`.
    """
    if fun is None:
        fun = lambda path: True
    for root, dirs, files in os.walk(directory):
        for f in files:
            path = os.path.join(root, f)
            if fun(path):
                yield path

# backwards compat
findfiles = iterfiles

def copyregions(src, dst, seekmap):
    """
    Copy regions of a source file to a target. The regions are given in the
    iterable `seekmap`, which must contain (offset, length) tuples.

    `src` (r) and `dst` (w) must be open files handles.

    More: http://stackoverflow.com/q/18551592/89391
    """
    for offset, length in sorted(seekmap):
        src.seek(offset)
        dst.write(src.read(length))


def random_tmp_path(prefix='gluish'):
    """
    Return a random path, that is located under the system's tmp dir. This
    is just a path, nothing gets touched or created.

    Just use:

        tempfile.mktemp(prefix='gluish-')

    instead.
    """
    warnings.warn("deprecated", DeprecationWarning)
    return os.path.join(tempfile.gettempdir(), '%s-%s' % (prefix, random_string()))


def random_tmp_dir(create=False, prefix='gluish'):
    """
    Same as random path, just with the option to create a directory, when
    `create` is `True`.
    """
    warnings.warn("deprecated", DeprecationWarning)
    path = tempfile.mkdtemp(prefix='gluish-')
    if create and not os.path.exists(path):
        os.makedirs(path)
    return path


def splitext(path):
    for ext in ('.tar.gz', '.tar.bz2'):
        if path.endswith(ext):
            return path[:-len(ext)], path[-len(ext):]
    return os.path.splitext(path)
