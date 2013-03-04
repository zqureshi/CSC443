import argparse
import csv
import matplotlib.pyplot as plt

PARSER = argparse.ArgumentParser()
PARSER.add_argument('name', metavar='NAME')
PARSER.add_argument('-l', action='store_true', dest='logy')
PARSER.add_argument('-o', metavar='OUTPUT', default=None, dest='output')
args = PARSER.parse_args()
name = args.name

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

plotter = plt.semilogy if args.logy else plt.plot
plotter(xdata, ydata("%s.data.txt" % name), 'g', label="No Index")
plotter(xdata, ydata("%s.index.txt" % name), 'b', label="With Index")

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
plt.suptitle('%s with and Without Index; 100,000 Records' % name.capitalize())

# save your plot and see it
if args.output is not None:
    fig.savefig(args.output)
else:
    plt.show()
