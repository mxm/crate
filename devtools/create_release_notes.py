
import argparse
from datetime import datetime
import os.path
import re

release_note_header = 'Breaking Changes\n'
path_to_release_notes = 'blackbox/docs/release_notes/'

def create_release_notes(version, minimum, release_date, file):
    release_notes_file = create_release_notes_file(version)

    print("VERSION: {}".format(version))
    print("RELEASE DATE: {}\n".format(release_date.strftime('%Y/%m/%d')))

    lines = write_release_notes_header([], version, release_date)
    lines = append_upgrade_warning(lines, version, minimum)
    change_lines = copy_changes(lines, file)
    release_notes_file.write("\n".join(lines))
    release_notes_file.writelines(change_lines)
    release_notes_file.close()

    print("Release Notes written at {}.".format(release_notes_file.name))
    print("Please check the release notes for any irregularities or specific detail that is needed!")

def copy_changes(lines, file):
    with open(get_path(file), "r") as f:
        change_lines = f.readlines()

    return change_lines[change_lines.index(release_note_header):]



def get_path(file):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", file))

def create_release_notes_file(version):
    return open(get_path(path_to_release_notes + "{}.txt".format(version)), 'w')

def write_release_notes_header(lines, version, release_date):
    lines.append(".. _version_{}:\n".format(version))

    lines.append("=============")
    lines.append("Version {}".format(version))
    lines.append("=============\n")

    lines.append("Released on " + release_date.strftime('%Y/%m/%d') + ".\n\n")
    return lines

def append_upgrade_warning(lines, version, minimum):
    lines.append(".. NOTE::\n")
    lines.append("   If you are upgrading a cluster, you must be running CrateDB {} or higher".format(minimum))
    lines.append("   before you upgrade to {}.\n".format(version))

    if (version.is_patch()):
        lines.append("   If you want to perform a :ref:`cluster_upgrade`, your current CrateDB version")
        lines.append("   number must be :ref:`version_{}` or higher. Any upgrade from a version".format(version.base_version()))
        lines.append("   prior to this will require a full cluster restart.\n")
    else:
        lines.append("   You cannot perform a :ref:`cluster_upgrade` to this version. Any upgrade to")
        lines.append("   this version will require a full cluster restart.\n")

    lines.append(".. warning::\n")
    lines.append("   Before upgrading, you should `back up your data`_.\n")
    lines.append(".. _back up your data: https://crate.io/a/backing-up-and-restoring-crate/\n\n")
    return lines

def main():
    parser = argparse.ArgumentParser(prog='create_release_notes.py', description=__doc__)
    parser.add_argument('-v', '--version', type=str, required=True)
    parser.add_argument('-d', '--date', type=str, help="Release date (YYYY/MM/DD). Default is today.")
    parser.add_argument('-m', '--minimum', type=str, required=True,
                        help="The minimum required version to upgrade to this version.")
    parser.add_argument('-f', '--file', type=str, default="CHANGES.txt",
                        help="The changelogs file from the root CrateDB directory. Default is CHANGES.txt")
    args = parser.parse_args()

    release_date = datetime.today() if args.date == None else datetime.strptime(args.date, '%Y/%m/%d')
    create_release_notes(Version(args.version), Version(args.minimum), release_date, args.file)

class Version:
    def __init__(self, version):
        r = re.compile('^(\d+.\d+.\d+)$')
        if r.match(version) is not None:
            self.major, self.minor, self.patch = version.split('.')
        else:
            raise SyntaxError('Version number is not in the format X.Y.Z')

    def __str__(self):
        return self.major + '.' + self.minor + '.' + self.patch

    def base_version(self):
        return self.major + '.' + self.minor + '.0'

    def is_patch(self):
        return int(self.patch) > 0

if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
