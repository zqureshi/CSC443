import os
import csv
import shlex
import argparse
import subprocess
from os import path

FIRST_EXP = 10
LAST_EXP  = 25
FILE_SIZE = 104857600

GET_HISTOGRAM      = path.abspath('get_histogram')
CREATE_RANDOM_FILE = path.abspath('create_random_file')

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

def get_histogram(name, blk):
    """
    Execute `get_histogram` with the given block size and return the amount of
    time it took.
    """
    args = [GET_HISTOGRAM, name, blk]
    return int(runcmd(args).split()[1])

def create_random_file(name, blk):
    """
    Use `create_random_file` to create a file using the given block size and
    return the amount of time it took to complete the operation.
    """
    args = [CREATE_RANDOM_FILE, name, FILE_SIZE, blk]
    return int(runcmd(args).split()[1])

def main():
    parser = argparse.ArgumentParser(
            description=('Runs create_random_file and get_histogram on the '
                'with varying block sizes and outputs a CSV in the format: '
                'BLOCK SIZE, CREATE TIME, GET TIME'))

    parser.add_argument('--tmp', '-t', metavar='FILE', default='noise',
            help=('Name of temporary file against which the experiment will '
                'be executed.'), dest='tmp')

    parser.add_argument('--first-exponent', '-f', type=int, default=FIRST_EXP,
            help='Exponent of 2 at which the block sizes will begin.',
            dest='fexp', metavar='INT')

    parser.add_argument('--last-exponent', '-l', type=int, default=LAST_EXP,
            help='Exponent of 2 at which the block sizes will end.',
            dest='lexp', metavar='INT')

    parser.add_argument('--repeat', '-r', type=int, default=1,
            help=('Number of times a command is executed with a specific '
                'buffer size. Run times are averaged and rounded off to the '
                'nearest integer. Default: 1'))

    parser.add_argument('file', type=argparse.FileType('wb'), metavar='FILE',
            help='File to which the collected data will be written.')

    args = parser.parse_args()
    noise = args.tmp
    repeat = args.repeat
    writer = csv.writer(args.file)

    for exp in xrange(args.fexp, args.lexp + 1):
        block_size = 2 ** exp

        write_times = [create_random_file(noise, block_size)
                for i in xrange(repeat)]

        read_times  = [get_histogram(noise, block_size)
                for i in xrange(repeat)]

        write_time = sum(write_times) / repeat
        read_time  = sum(read_times)  / repeat

        writer.writerow([block_size, write_time, read_time])
        os.remove(noise)

if __name__ == '__main__':
    main()
