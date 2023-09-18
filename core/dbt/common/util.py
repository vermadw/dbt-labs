import hashlib
import sys


def md5(string, charset="utf-8"):
    if sys.version_info >= (3, 9):
        return hashlib.md5(string.encode(charset), usedforsecurity=False).hexdigest()
    else:
        return hashlib.md5(string.encode(charset)).hexdigest()
