import csv
import argparse
import operator
from numpy import log2
import matplotlib.pyplot as plt


SND = operator.itemgetter(1)
RECORD_COUNT = 1000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE')
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
                        default='Number of records: 1000')
    parser.add_argument('--no-max', '-n', action='store_false',
                        dest='mark_max')
    parser.add_argument('--output', '-o', metavar='FILE', default=None)

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    page_sizes, wtimes, rtimes = zip(*rows)

    # Time in milliseconds to Rate of writing in records per second.
    to_rate = lambda t: RECORD_COUNT * 1000.0 / float(t)

    read_rates = [to_rate(t) for t in rtimes]
    write_rates = [to_rate(t) for t in wtimes]

    # Page sizes with the maximum read and write rates.
    max_read = max(zip(page_sizes,  read_rates), key=SND)
    max_write = max(zip(page_sizes, write_rates), key=SND)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    w_plot = fig.add_subplot(111)
    (w_line,) = w_plot.semilogx(page_sizes, write_rates, 'b.-', basex=2,
                                linewidth=2)
    w_plot.set_xlabel('Page size (bytes)')
    w_plot.set_ylabel('Write rate (records per second)', color='b')

    # Mark off block size with the maximum write rate.
    if args.mark_max:
        w_plot.plot([max_write[0]] * 2 + [w_plot.get_xlim()[0]],
                    [w_plot.get_ylim()[0]] + [max_write[1]] * 2,
                    color='b', linestyle=':', linewidth=2)
        w_plot.annotate('$2^{%d}$' % log2(max_write[0]), fontsize=15,
                        xy=max_write, verticalalignment='bottom',
                        horizontalalignment='center')

    # Color the write time ticks blue
    for t in w_plot.get_yticklabels():
        t.set_color('b')

    # Plot read times
    r_plot = w_plot.twinx()
    (r_line,) = r_plot.semilogx(page_sizes, read_rates, 'g.-', basex=2,
                                linewidth=2)
    r_plot.set_ylabel('Read rate (records per second)', color='g')

    # Add a little wiggle room to the read time Y limits
    if False:
        lo, hi = r_plot.get_ylim()
        r_plot.set_ylim([lo, hi*1.025])

    # Mark off block size with maximum read rate.
    if args.mark_max:
        r_plot.plot([max_read[0]] * 2 + [r_plot.get_xlim()[1]],
                    [r_plot.get_ylim()[0]] + [max_read[1]] * 2,
                    color='g', linestyle=':', linewidth=2)
        r_plot.annotate('$2^{%d}$' % log2(max_read[0]), fontsize=15,
                        xy=max_read, verticalalignment='bottom',
                        horizontalalignment='center')

    # Color the read time ticks greed
    for t in r_plot.get_yticklabels():
        t.set_color('g')

    # Add a legend
    fig.legend((w_line, r_line), ('write_fixed_len_page',
                                  'read_fixed_len_page'),
               loc='lower right')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
