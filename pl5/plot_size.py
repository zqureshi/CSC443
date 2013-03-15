import csv
import argparse
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE')
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
                        default='Memory Capacity: 8MB; $k$: 2048')
    parser.add_argument('--output', '-o', metavar='FILE', default=None)

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    sizes, times = zip(*rows)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    f_plt = fig.add_subplot(111)

    f_plt.loglog(sizes, times, 'b-', basex=2, basey=2, linewidth=2,
                 label='msort')
    f_plt.set_xlabel('Number of records')
    f_plt.set_ylabel('Time (in milliseconds)')

    # Add a legend
    f_plt.legend(loc='best')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
