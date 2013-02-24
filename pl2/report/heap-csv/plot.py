import csv
import argparse
import operator
from numpy import log2
import matplotlib.pyplot as plt

SND = operator.itemgetter(1)
RECORD_COUNT = 50000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE')
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
                        default='Number of records: 50000')
    parser.add_argument('--no-max', '-n', action='store_false',
                        dest='mark_max')
    parser.add_argument('--output', '-o', metavar='FILE', default=None)

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    page_sizes, c2h_times, h2c_times = zip(*rows)

    # Time in milliseconds to Rate of writing in records per second.
    to_rate = lambda t: RECORD_COUNT * 1000.0 / float(t)

    c2h_rates = [to_rate(t) for t in c2h_times]
    h2c_rates = [to_rate(t) for t in h2c_times]

    # Page sizes with the maximum read and write rates.
    max_c2h = max(zip(page_sizes, c2h_rates), key=SND)
    max_h2c = max(zip(page_sizes, h2c_rates), key=SND)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    c2h_plot = fig.add_subplot(111)
    (c2h_line,) = c2h_plot.semilogx(page_sizes, c2h_rates, 'b.-', basex=2,
                                    linewidth=2)
    c2h_plot.set_xlabel('Page size (bytes)')
    c2h_plot.set_ylabel('Records per second', color='b')

    # Mark off block size with the maximum write rate.
    if args.mark_max:
        c2h_plot.plot([max_c2h[0]] * 2 + [c2h_plot.get_xlim()[0]],
                      [c2h_plot.get_ylim()[0]] + [max_c2h[1]] * 2,
                      color='b', linestyle=':', linewidth=2)
        c2h_plot.annotate('$2^{%d}$' % log2(max_c2h[0]), fontsize=15,
                          xy=max_c2h, verticalalignment='top',
                          horizontalalignment='left')

    # Color the write time ticks blue
    for t in c2h_plot.get_yticklabels():
        t.set_color('b')

    # Plot read times
    h2c_plot = c2h_plot.twinx()
    (h2c_line,) = h2c_plot.semilogx(page_sizes, h2c_rates, 'g.-', basex=2,
                                    linewidth=2)
    h2c_plot.set_ylabel('Records per second', color='g')

    # Add a little wiggle room to the read time Y limits
    if False:
        lo, hi = h2c_plot.get_ylim()
        h2c_plot.set_ylim([lo, hi*1.025])

    # Mark off block size with maximum read rate.
    if args.mark_max:
        h2c_plot.plot([max_h2c[0]] * 2 + [h2c_plot.get_xlim()[1]],
                      [h2c_plot.get_ylim()[0]] + [max_h2c[1]] * 2,
                      color='g', linestyle=':', linewidth=2)
        h2c_plot.annotate('$2^{%d}$' % log2(max_h2c[0]), fontsize=15,
                          xy=max_h2c, verticalalignment='bottom',
                          horizontalalignment='center')

    # Color the read time ticks greed
    for t in h2c_plot.get_yticklabels():
        t.set_color('g')

    # Add a legend
    fig.legend((c2h_line, h2c_line), ('csv2heapfile', 'heapfile2csv'),
               loc='lower right')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
