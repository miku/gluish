#!/usr/bin/env python
# coding: utf-8

import luigi
import os

__all__ = ['Executable']

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

class Executable(luigi.Task):
    """
    Checks, whether an external executable is available. This task will consider
    itself complete, only if the executable `name` is found in $PATH.
    """
    name = luigi.Parameter()
    message = luigi.Parameter(default="")

    def run(self):
        """ Only run if, task is not complete. """
        raise RuntimeError('external program required: %s (%s)' % (self.name, self.message))

    def complete(self):
        return which(self.name) is not None
