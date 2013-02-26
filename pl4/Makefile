CC = g++
CCFLAGS = -Wall -g -lpthread
INCLUDES = -I. -I./include
TARGETS = csv2leveldb query_leveldb

## Project Build
.PHONY: all clean

all: $(TARGETS) $(OBJS)

query_leveldb: query_leveldb.o libleveldb.a
	$(CC) -o $@ $(CCFLAGS) $(INCLUDES) $^

csv2leveldb: csv2leveldb.o libleveldb.a
	$(CC) -o $@ $(CCFLAGS) $(INCLUDES) $^

%.o: src/%.cc
	$(CC) -c -o $@ $(CCFLAGS) $(INCLUDES) $^

clean:
	rm -rf $(OBJS) $(TARGETS) *.o

# LevelDB

.PHONY: leveldb clean-leveldb

leveldb: libleveldb.a

clean-leveldb:
	rm -rf leveldb leveldb-1.9.0 libleveldb.a

libleveldb.a:
	curl https://leveldb.googlecode.com/files/leveldb-1.9.0.tar.gz | tar xvz
	cp -R leveldb-1.9.0/include/leveldb leveldb
	cd leveldb-1.9.0; make
	mv leveldb-1.9.0/libleveldb.a .
