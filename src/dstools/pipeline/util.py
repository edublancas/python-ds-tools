from difflib import Differ


def diff_strings(a, b):
    """Compute the diff between two strings
    """
    d = Differ()

    if a is None and b is None:
        return '[Both a and b are None]'

    out = ''

    if a is None:
        out += '[a is None]\n'
    elif b is None:
        out += '[a is None]\n'

    a = '' if a is None else a
    b = '' if b is None else b

    result = d.compare(a.splitlines(keepends=True),
                       b.splitlines(keepends=True))
    out += ''.join(result)

    return out
