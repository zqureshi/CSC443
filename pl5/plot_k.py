import csv
import argparse
import operator
import matplotlib.pyplot as plt

SND = operator.itemgetter(1)
RECORD_COUNT = 5825422


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE')
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
                        default='Number of records: 5825422; Memory: 10MB')
    parser.add_argument('--no-max', '-n', action='store_false',
                        dest='mark_max')
    parser.add_argument('--output', '-o', metavar='FILE', default=None)

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    ks, times = zip(*rows)

    # Time in milliseconds to Rate of writing in records per second.
    to_rate = lambda t: RECORD_COUNT * 1000.0 / float(t)
    k_rates = map(to_rate, times)

    # Page sizes with the maximum read and write rates.
    max_k = max(zip(ks, k_rates), key=SND)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    k_plot = fig.add_subplot(111)

    k_plot.semilogx(ks, k_rates, 'b-', basex=2, linewidth=2, label='msort')
    k_plot.set_xlabel('$k$')
    k_plot.set_ylabel('Records per second')

    # Mark off block size with the maximum write rate.
    if args.mark_max:
        k_plot.plot([max_k[0]] * 2 + [k_plot.get_xlim()[0]],
                    [k_plot.get_ylim()[0]] + [max_k[1]] * 2,
                    color='b', linestyle=':', linewidth=2)
        k_plot.annotate(str(max_k[0]), fontsize=15,
                        xy=max_k, verticalalignment='bottom',
                        horizontalalignment='left')

    # Add a legend
    k_plot.legend(loc='best')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
