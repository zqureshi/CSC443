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
    page_sizes, q_times = zip(*rows)

    # Time in milliseconds to Rate of writing in records per second.
    to_rate = lambda t: RECORD_COUNT * 1000.0 / float(t)
    q_rates = map(to_rate, q_times)

    # Page sizes with the maximum read and write rates.
    max_q = max(zip(page_sizes, q_rates), key=SND)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    q_plot = fig.add_subplot(111)

    (q_line,) = q_plot.semilogx(page_sizes, q_rates, 'b-', basex=2,
                                linewidth=2)
    q_plot.set_xlabel('Page size (bytes)')
    q_plot.set_ylabel('Records per second', color='b')

    # Sqlite with index
    sql_line = q_plot.axhline(190624.33281483516, color='g', linestyle=':',
                              linewidth=2)

    # Mark off block size with the maximum write rate.
    if args.mark_max:
        q_plot.plot([max_q[0]] * 2 + [q_plot.get_xlim()[0]],
                    [q_plot.get_ylim()[0]] + [max_q[1]] * 2,
                    color='b', linestyle=':', linewidth=2)
        q_plot.annotate('$2^{%d}$' % log2(max_q[0]), fontsize=15,
                        xy=max_q, verticalalignment='top',
                        horizontalalignment='left')

    # Color the write time ticks blue
    # for t in q_plot.get_yticklabels():
    #     t.set_color('b')

    # Add a legend
    fig.legend((q_line, sql_line), ('query', 'sqlite'), loc='lower right')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
