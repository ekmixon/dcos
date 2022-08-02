#!/opt/mesosphere/bin/python3

import json
import os
import shutil
import sys


def main(old_dir, new_dir, networks):
    '''
    Moves all the directories in networks from old_dir to new_dir

    @type old_dir: str, old CNI directory
    @type new_dir: str, new CNI directory
    @type networks: list, names of the directories to move
    '''
    print(f'Upgrading CNI directory from {old_dir} to {new_dir}')
    for name in networks:
        src = os.path.join(old_dir, name)
        if not os.path.exists(src):
            print(f'{src} already moved')
        else:
            dst = os.path.join(new_dir, name)
            shutil.move(src, dst)
            print(f'{src} moved to {dst}')
    print('CNI upgrade completed')


def readfile(filename):
    with open(filename) as f:
        data = json.loads(f.read())
        return list(data['names'])


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage: ./upgrage_cni.py <old-cni-dir> <new-cni-dir> <file-with-network-names')
        sys.exit(1)

    try:
        networks = readfile(sys.argv[3])
        main(sys.argv[1], sys.argv[2], networks)
    except Exception as e:
        print(
            f'ERROR: An exception occurred while upgrading the CNI directory {e}',
            file=sys.stderr,
        )

        sys.exit(1)
