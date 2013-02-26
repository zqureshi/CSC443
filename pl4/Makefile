CC = g++
CCFLAGS = -Wall -g -Wextra
INCLUDES = -I. -I./include
OBJECTS = serializer.o
TARGETS = csv2leveldb query_leveldb csv2idx_leveldb query_idx_leveldb
LDFLAGS  = -lpthread -lsnappy

## Project Build
.PHONY: all clean

all: $(TARGETS) $(OBJS)

query_leveldb: query_leveldb.o serializer.o libleveldb.a
	$(CC) -o $@ $(CCFLAGS) $(LDFLAGS) $(INCLUDES) $^

query_idx_leveldb: query_idx_leveldb.o libleveldb.a
	$(CC) -o $@ $(CCFLAGS) $(LDFLAGS) $(INCLUDES) $^

csv2leveldb: csv2leveldb.o serializer.o libleveldb.a
	$(CC) -o $@ $(CCFLAGS) $(LDFLAGS) $(INCLUDES) $^

csv2idx_leveldb: csv2idx_leveldb.o libleveldb.a
	$(CC) -o $@ $(CCFLAGS) $(LDFLAGS) $(INCLUDES) $^

%.o: src/%.cc
	$(CC) -c -o $@ $(CCFLAGS) $(INCLUDES) $^

clean:
	rm -rf $(OBJECTS) $(TARGETS) *.o

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
