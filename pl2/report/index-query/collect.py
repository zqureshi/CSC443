import re
import os
import shlex
import subprocess
from os import path

FIRST_EXP = 10
LAST_EXP = 25

CSVIN = "in.csv"
HEAPFILE = "out.heap"

CSV2HEAP = path.abspath('csv2idxfile')
QUERY = path.abspath('query_idx_heapfile')

TIME_RE = re.compile(r'^TIME: (\d+) milliseconds', re.I | re.M)


def runcmd(args):
    """
    Run the command specified by the given argument. `args` can be a string or
    a list specifying the command to execute and its arguments.
    """
    if type(args) is str:
        args = shlex.split(args)
    args = map(str, args)
    with open(os.devnull, 'w') as fnull:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=fnull)
        return proc.stdout.read()


def csv2heap(page_size):
    """
    Execute `heapfile2csv` and return the amount of time it took.
    """
    args = [CSV2HEAP, CSVIN, HEAPFILE, page_size]
    out = runcmd(args)
    return int(TIME_RE.search(out).group(1))


def query(page_size):
    args = [QUERY, HEAPFILE, "000000000", "999999999", page_size]
    out = runcmd(args)
    return int(TIME_RE.search(out).group(1))


def main():
    # 1MB, 512K, 256K, 128K, 64K, 32K, 16K, 4K
    for exp in [20, 19, 18, 17, 16, 15, 14, 12]:
        page_size = 2 ** exp
        csv2heap(page_size)
        q_time = query(page_size)
        print ",".join(map(str, [page_size, q_time]))

if __name__ == '__main__':
    main()
