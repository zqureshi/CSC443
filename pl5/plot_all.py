import csv
import argparse
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'), metavar='FILE')
    parser.add_argument('--title', '-t', type=str, metavar='TITLE',
                        default='bsort vs msort')
    parser.add_argument('--output', '-o', metavar='FILE', default=None)

    args = parser.parse_args()

    rows = (map(int, row) for row in csv.reader(args.file))
    sizes, btree, goodm, badm = zip(*rows)

    fig = plt.figure()
    fig.suptitle(args.title)

    # Plot write times
    a_plt = fig.add_subplot(111)

    a_plt.loglog(sizes, btree, 'r-', basex=2, linewidth=2, basey=2,
                 label='bsort')
    a_plt.loglog(sizes, goodm, 'g-', basex=2, linewidth=2, basey=2,
                 label='msort; $k=2048$; capacity=8MB')
    a_plt.loglog(sizes, badm, 'b-', basex=2, linewidth=2, basey=2,
                 label='msort; $k=2$; capacity=32kB')

    a_plt.set_xlabel('Number of records')
    a_plt.set_ylabel('Time (in milliseconds)')

    # Add a legend
    a_plt.legend(loc='best')

    if args.output is not None:
        fig.savefig(args.output)
    else:
        plt.show()

if __name__ == '__main__':
    main()
