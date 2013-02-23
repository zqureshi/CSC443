import re
import os
import csv
import shlex
import argparse
import subprocess
from os import path

FIRST_EXP = 10
LAST_EXP = 25

CSVFILE = "csv.txt"
PAGEFILE = "page.bin"

READ = path.abspath('read_fixed_len_pages')
WRITE = path.abspath('write_fixed_len_pages')

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


def write(page_size):
    """
    Execute `write_fixed_len_pages` and return the amount of time it took.
    """
    args = [WRITE, CSVFILE, PAGEFILE, page_size]
    out = runcmd(args)
    return int(TIME_RE.search(out).group(1))


def read(page_size):
    """
    Execute `read_fixed_len_pages` and return the amount of time it took.
    """
    args = [READ, PAGEFILE, page_size]
    out = runcmd(args)
    return int(TIME_RE.search(out).group(1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--first-exponent', '-f', type=int, default=FIRST_EXP,
                        dest='fexp', metavar='INT')
    parser.add_argument('--last-exponent', '-l', type=int, default=LAST_EXP,
                        dest='lexp', metavar='INT')
    parser.add_argument('file', type=argparse.FileType('wb'), metavar='FILE')

    args = parser.parse_args()
    writer = csv.writer(args.file)

    for exp in xrange(args.fexp, args.lexp + 1):
        page_size = 2 ** exp
        w_time = write(page_size)
        r_time = read(page_size)
        writer.writerow([page_size, w_time, r_time])

if __name__ == '__main__':
    main()
