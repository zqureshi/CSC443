import csv
import argparse
import operator
import matplotlib.pyplot as plt

SND = operator.itemgetter(1)
RECORD_COUNT = 5825422


def in_mb(byte):
    return byte / (1024 * 1024)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE')
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
                        default='Number of records: 5825422; $k$: 2048')
    parser.add_argument('--no-max', '-n', action='store_false',
                        dest='mark_max')
    parser.add_argument('--output', '-o', metavar='FILE', default=None)

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    mems, times = zip(*rows)

    # Time in milliseconds to Rate of writing in records per second.
    to_rate = lambda t: RECORD_COUNT * 1000.0 / float(t)
    mem_rates = map(to_rate, times)

    # Page sizes with the maximum read and write rates.
    max_mem = max(zip(mems, mem_rates), key=SND)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    m_plt = fig.add_subplot(111)

    m_plt.semilogx(mems, mem_rates, 'b-', basex=2, linewidth=2, label='msort')
    m_plt.set_xlabel('Memory Capacity (in bytes)')
    m_plt.set_ylabel('Records per second')

    # Mark off block size with the maximum write rate.
    if args.mark_max:
        m_plt.plot([max_mem[0]] * 2 + [m_plt.get_xlim()[0]],
                   [m_plt.get_ylim()[0]] + [max_mem[1]] * 2,
                   color='b', linestyle=':', linewidth=2)
        m_plt.annotate('%d MB' % in_mb(max_mem[0]), fontsize=15,
                       xy=max_mem, verticalalignment='bottom',
                       horizontalalignment='left')

    # Add a legend
    m_plt.legend(loc='best')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()

