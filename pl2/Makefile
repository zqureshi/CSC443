CC = g++
CCFLAGS = -Wall -g # -pedantic
OBJS = serializer.o pagemanager.o heapmanager.o library.o

INCLUDES_TEST = -I./
LIBS_TEST = -pthread -lgtest -L.
TARGETS = write_fixed_len_pages read_fixed_len_pages \
		  # csv2heapfile heapfile2csv query
# ^ Uncomment when heapmanager is done

TARGETS_TEST = test_serializer test_pagemanager test_heapmanager

PAGEDEPS = serializer.o pagemanager.o library.o
HEAPDEPS = $(PAGEDEPS) heapmanager.o

## Project Build
.PHONY: all clean

all: $(TARGETS) $(OBJS)

read_fixed_len_pages: read_fixed_len_pages.o $(PAGEDEPS)
	$(CC) -o $@ $(CCFLAGS) $^

write_fixed_len_pages: write_fixed_len_pages.o $(PAGEDEPS)
	$(CC) -o $@ $(CCFLAGS) $^

csv2heapfile: csv2heapfile.o $(HEAPDEPS)
	$(CC) -o $@ $(CCFLAGS) $^

heapfile2csv: heapfile2csv.o $(HEAPDEPS)
	$(CC) -o $@ $(CCFLAGS) $^

query: query.o $(HEAPDEPS)
	$(CC) -o $@ $(CCFLAGS) $^

%.o: %.cc
	$(CC) -c -o $@ $(CCFLAGS) $^

clean: clean-test
	rm -rf $(OBJS) $(TARGETS) *.o

## Testing Framework
.PHONY: test run-test clean-test

test: $(TARGETS_TEST)

run-test: test
	for test in $(TARGETS_TEST); do ./$${test} && echo; done

test_%: tests/%.cc tests/runner.cc $(OBJS)
	$(CC) -o $@ $(CCFLAGS) $(INCLUDES_TEST) $^ $(LIBS_TEST)

clean-test:
	rm -rf $(TARGETS_TEST) *.dSYM *.test.heap

## Google Test
.PHONY: gtest clean-gtest

gtest: libgtest.a

clean-gtest:
	rm -rf gtest libgtest.a

libgtest.a:
	mkdir -p tmp
	cd tmp; wget http://googletest.googlecode.com/files/gtest-1.6.0.zip
	cd tmp; unzip gtest-1.6.0.zip
	cd tmp/gtest-1.6.0; cmake .; make
	cp tmp/gtest-1.6.0/libgtest.a .
	cp -R tmp/gtest-1.6.0/include/gtest gtest
	rm -rf tmp
