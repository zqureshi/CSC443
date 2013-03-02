import string
import random
import argparse

# LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
LETTERS = string.digits

PARSER = argparse.ArgumentParser()
PARSER.add_argument('--attr-len', '-a', metavar='SIZE', type=int,
                    dest='attr', default=9)
PARSER.add_argument('tuples', metavar='COUNT', type=int)
PARSER.add_argument('file', metavar='FILE', type=argparse.FileType('w'))
args = PARSER.parse_args()

for i in xrange(args.tuples):
    row = [''.join([random.choice(LETTERS) for j in xrange(args.attr)])
           for a in xrange(100)]
    print >>args.file, ",".join(row)

print "Generated %d random tuples in %s." % (args.tuples, args.file.name)
