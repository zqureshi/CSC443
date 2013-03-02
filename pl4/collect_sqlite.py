import os
import csv
import time
import sys
import sqlite3
from os import path

CREATE = ("CREATE TABLE T (" +
          ", ".join("A%d CHARACTER(10)" % i for i in xrange(1, 101)) +
          ")")

CREATE_INDEX = ("CREATE INDEX I ON T (A1)")

INSERT = ("INSERT INTO T VALUES (" +
          ", ".join("?" for i in xrange(1, 101)) +
          ")")

QUERY = ("SELECT SUBSTR(A2, 1, 5), COUNT(*) FROM T "
         "WHERE A1 >= ? AND A1 <= ? "
         "GROUP BY SUBSTR(A2, 1, 5)")

QUERY_IDX = ("SELECT SUBSTR(A2, 1, 5), COUNT(*) FROM T INDEXED BY I "
             "WHERE A1 >= ? AND A1 <= ? "
             "GROUP BY SUBSTR(A2, 1, 5)")

CSV = 'in.csv'
DB = 'out.sqlite.db'


def now():
    return int(round(time.time() * 1000))


def stderr(s):
    print >>sys.stderr, s


def stderr_e(s):
    print >>sys.stderr, s,


def main():
    print "SQLite:"
    if path.isfile(DB):
        stderr("Removing existing database.")
        os.unlink(DB)

    with sqlite3.connect(DB) as con:
        stderr("Creating database")
        con.execute(CREATE)

        with open(CSV, 'r') as f:
            con.executemany(INSERT, csv.reader(f))

        stderr_e("Querying")
        for c in xrange(26):
            char = chr(c + ord('A'))
            start_time = now()
            all(con.execute(QUERY, ('AA', '%sZ' % char)))
            end_time = now()

            stderr_e(char)
            print "%d,%d" % (c, end_time - start_time)
        stderr_e ("")

    print "\nSQLite with index:"

    if path.isfile(DB):
        stderr("Removing existing database.")
        os.unlink(DB)

    with sqlite3.connect(DB) as con:
        stderr("Creating database")
        con.execute(CREATE)
        con.execute(CREATE_INDEX)

        with open(CSV, 'r') as f:
            con.executemany(INSERT, csv.reader(f))

        stderr_e("Querying")
        for c in xrange(26):
            char = chr(c + ord('A'))
            start_time = now()
            all(con.execute(QUERY_IDX, ('AA', '%sZ' % char)))
            end_time = now()

            stderr_e(char)
            print "%d,%d" % (c, end_time - start_time)

if __name__ == '__main__':
    main()

