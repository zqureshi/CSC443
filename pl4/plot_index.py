import argparse
import csv
import matplotlib.pyplot as plt

PARSER = argparse.ArgumentParser()
PARSER.add_argument('-o', metavar='OUTPUT', default=None, dest='output')
args = PARSER.parse_args()

# let's use meaningful labels of 'AA-AZ', ...
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
labels = ["AA -- %sZ" % x for x in letters]

xdata = range(26)

# let's plot it
fig = plt.figure()
ax = plt.axes()


def ydata(filename):
    with open(filename, 'r') as f:
        return [int(row[1]) for row in csv.reader(f)]

plt.semilogy(xdata, ydata("heap.index.txt"), 'g', label="Heap file")
plt.semilogy(xdata, ydata('sqlite.index.txt'), 'b', label="SQLite")
plt.semilogy(xdata, ydata('leveldb.index.txt'), 'r', label="B+ Tree", basey=2)


# matplotlib allows you to rotate the labels and
# adjust the ticker density
plt.xticks(rotation=70)
ax.xaxis.set_ticks(xdata)
ax.xaxis.set_ticklabels(labels)
fig.subplots_adjust(bottom=0.15)  # a little trick to add some space for the
                                  # longish labels

# add some titles
plt.legend(loc='best')
plt.xlabel('Letters')
plt.ylabel('Time (in milliseconds)')
plt.suptitle('With Index; 100,000 Records')

# save your plot and see it
if args.output is not None:
    fig.savefig(args.output)
else:
    plt.show()
