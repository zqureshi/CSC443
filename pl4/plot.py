import argparse
import csv
import matplotlib.pyplot as plt

PARSER = argparse.ArgumentParser()
PARSER.add_argument('file', metavar='FILE', type=argparse.FileType('r'))
PARSER.add_argument('-t', metavar='TITLE', type=str, dest='title',
                    default='Number of records: 100,000')
PARSER.add_argument('-o', metavar='OUTPUT', default=None, dest='output')
args = PARSER.parse_args()

# let's use meaningful labels of 'AA-AZ', ...
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
labels = ["AA -- %sZ" % x for x in letters]

# here is the data series (i, i^2) for i=0 .. 25
rows = (map(int, row) for row in csv.reader(args.file))
xdata, ydata = zip(*rows)

# let's plot it
fig = plt.figure()
ax = plt.axes()
plt.plot(xdata, ydata, '*-')

# matplotlib allows you to rotate the labels and
# adjust the ticker density
plt.xticks(rotation=70)
ax.xaxis.set_ticks(xdata)
ax.xaxis.set_ticklabels(labels)
fig.subplots_adjust(bottom=0.15)  # a little trick to add some space for the
                                  # longish labels

# add some titles
plt.xlabel('Letters')
plt.ylabel('Time (in milliseconds)')
plt.suptitle(args.title)

# save your plot and see it
if args.output is not None:
    fig.savefig(args.output)
else:
    plt.show()
