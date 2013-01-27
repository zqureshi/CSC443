import csv
import argparse
import operator
from numpy import log2
import matplotlib.pyplot as plt

SND = operator.itemgetter(1)

def main():

    parser = argparse.ArgumentParser(
            description='Plot the data collected by collect.py')

    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE',
            help="File containing the data collected by collect.py.")
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
            default='File Size: 100MB', help='Title for the plot.')
    parser.add_argument('--output', '-o', metavar='FILE', default=None,
            help=('Output file containing the plot. If absent, the plot will '
                'be shown in a window.'))

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    block_sizes, write_times, read_times = zip(*rows)

    # Block sizes with the minimum write and read times
    min_write = min(zip(block_sizes, write_times), key=SND)[0]
    min_read  = min(zip(block_sizes,  read_times), key=SND)[0]

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    w_plot = fig.add_subplot(111)
    (w_line,) = w_plot.semilogx(block_sizes, write_times, 'b.-', basex=2)
    w_plot.set_xlabel('Block size')
    w_plot.set_ylabel('Write time (in milliseconds)', color='b')

    # Mark off block size with minimum write times
    w_plot.axvline(min_write, color='b', linestyle=':', ymin=0.05)
    w_plot.annotate('$2^{%d}$' % log2(min_write), xytext=(0, 3), fontsize=15,
            xy=(min_write, w_plot.get_ylim()[0]), textcoords='offset points',
            verticalalignment='bottom', horizontalalignment='center')

    # Color the write time ticks blue
    for t in w_plot.get_yticklabels():
        t.set_color('b')

    # Plot read times
    r_plot = w_plot.twinx()
    (r_line,) = r_plot.semilogx(block_sizes, read_times, 'g.-', basex=2)
    r_plot.set_ylabel('Read time (in milliseconds)', color='g')
    
    # Mark off block size with minimum read times
    r_plot.axvline(min_read, color='g', linestyle=':', ymax=0.95)
    r_plot.annotate('$2^{%d}$' % log2(min_read), xytext=(0, -3), fontsize=15,
            xy=(min_read, r_plot.get_ylim()[1]), textcoords='offset points',
            verticalalignment='top', horizontalalignment='center')

    # Color the read time ticks greed
    for t in r_plot.get_yticklabels():
        t.set_color('g')

    # Add a legend
    fig.legend((w_line, r_line), ('Write', 'Read'))

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
