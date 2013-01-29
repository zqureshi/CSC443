CC = g++
CCFLAGS = -Wall -g
OBJS = serializer.o

INCLUDES_TEST = -I./
LIBS_TEST = -pthread -lgtest -L.
TARGETS_TEST = test_serializer

## Project Build
.PHONY: all clean

all: $(OBJS)

%.o: %.cc
	$(CC) -c -o $@ $(CPPFLAGS) $^

clean: clean-test
	rm -rf $(OBJS)

## Testing Framework
.PHONY: test run-test clean-test

test: $(TARGETS_TEST)

run-test: test
	for test in $(TARGETS_TEST); do ./$${test}; done

test_serializer: tests/serializer.cc serializer.o
	$(CC) -o $@ $(CCFLAGS) $(INCLUDES_TEST) $^ $(LIBS_TEST)

clean-test:
	rm -rf $(TARGETS_TEST) *.dSYM

## Google Test
.PHONY: gtest clean-gtest

gtest: libgtest.a

clean-gtest:
	rm -rf gtest libgtest.a tmp

libgtest.a:
	mkdir -p tmp
	cd tmp; wget http://googletest.googlecode.com/files/gtest-1.6.0.zip
	cd tmp; unzip gtest-1.6.0.zip
	cd tmp/gtest-1.6.0; cmake .; make
	cp tmp/gtest-1.6.0/libgtest.a .
	cp -R tmp/gtest-1.6.0/include/gtest gtest
