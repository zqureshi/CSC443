import csv
import argparse
import operator
from numpy import log2
import matplotlib.pyplot as plt

SND = operator.itemgetter(1)
FILE_SIZE = 104857600

def main():

    parser = argparse.ArgumentParser(
            description='Plot the data collected by collect.py')

    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE',
            help="File containing the data collected by collect.py.")
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
            default='File Size: 100MB', help='Title for the plot.')
    parser.add_argument('--no-max', '-n', action='store_false',
            help="Don't mark the maximum read rate in the plot.",
            dest='mark_max')
    parser.add_argument('--output', '-o', metavar='FILE', default=None,
            help=('Output file containing the plot. If absent, the plot will '
                'be shown in a window.'))

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    block_sizes, wtimes, rtimes = zip(*rows)

    # Rate of writing in bytes per second.
    # x B/msec = 1000 * x B/sec
    to_rate = lambda t: FILE_SIZE * 1000.0/ float(t) 

    write_rates = [to_rate(t) for t in wtimes]
    read_rates  = [to_rate(t) for t in rtimes]

    # Block sizes with the maximum read and write rates.
    max_write = max(zip(block_sizes, write_rates), key=SND)
    max_read  = max(zip(block_sizes,  read_rates), key=SND)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    w_plot = fig.add_subplot(111)
    (w_line,) = w_plot.semilogx(block_sizes, write_rates, 'b.-', basex=2)
    w_plot.set_xlabel('Block size')
    w_plot.set_ylabel('Write rate (bytes per second)', color='b')

    # Mark off block size with the maximum write rate.
    if args.mark_max:
        w_plot.plot([max_write[0]] * 2 + [w_plot.get_xlim()[0]],
                    [w_plot.get_ylim()[0]] + [max_write[1]] * 2,
                    color='b', linestyle=':')
        w_plot.annotate('$2^{%d}$' % log2(max_write[0]), fontsize=15,
                xy=max_write, verticalalignment='bottom',
                horizontalalignment='center')

    # Color the write time ticks blue
    for t in w_plot.get_yticklabels():
        t.set_color('b')

    # Plot read times
    r_plot = w_plot.twinx()
    (r_line,) = r_plot.semilogx(block_sizes, read_rates, 'g.-', basex=2)
    r_plot.set_ylabel('Read rate (bytes per second)', color='g')

    # Mark off block size with maximum read rate.
    if args.mark_max:
        r_plot.plot([max_read[0]] * 2 + [r_plot.get_xlim()[1]],
                    [r_plot.get_ylim()[1]] + [max_read[1]] * 2,
                    color='g', linestyle=':')
        r_plot.annotate('$2^{%d}$' % log2(max_read[0]), fontsize=15,
                xy=max_read, verticalalignment='top',
                horizontalalignment='center')
    
    # Color the read time ticks greed
    for t in r_plot.get_yticklabels():
        t.set_color('g')

    # Add a legend
    fig.legend((w_line, r_line), ('Write', 'Read'), loc='lower right')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
