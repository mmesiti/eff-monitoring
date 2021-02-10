import subprocess


def _read_manual():
    """ Reads up the man pages, returns list of lines."""
    cmd = "man sacct"
    output = subprocess.run(cmd.split(), capture_output=True)

    return output.stdout.decode("utf-8").split("\n")


def _get_startline(manpage_lines):
    """Finds line where the list of fields starts"""
    for i, line in enumerate(manpage_lines):
        if "Fields available:" in line:
            return i + 1


def _get_endline(manpage_lines, startline):
    """Finds line where the list of fields ends"""
    for i, line in enumerate(manpage_lines):
        if "NOTE: " in line and i > startline:
            return i


def _get_all_fields(manpage_lines, startline, endline):
    return "".join(manpage_lines[startline:endline]).split()


def get_all_fields():
    manpage_lines = _read_manual()
    startline = _get_startline(manpage_lines)
    endline = _get_endline(manpage_lines, startline)

    return _get_all_fields(manpage_lines, startline, endline)
