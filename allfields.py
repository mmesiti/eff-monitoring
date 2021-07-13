import subprocess


def get_all_fields():
    cmd = "sacct -e"
    return (subprocess.run(cmd.split(),
                           capture_output=True).stdout.decode("utf-8").split())
