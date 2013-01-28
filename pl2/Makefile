CC = g++
CCFLAGS = -Wall -g -std=c++03
OBJS = serializer.o

INCLUDES_TEST = -I./
LIBS_TEST = -pthread -lgtest
TARGETS_TEST = test_serializer

## Project Build
.PHONY: all clean

all: $(OBJS)

serializer.o: serializer.cc
	$(CC) -c -o $@ $(CCFLAGS) $^

clean:
	rm -rf $(OBJS) $(TARGETS_TEST)

## Testing Framework
.PHONY: test run-test clean-test

test: $(TARGETS_TEST)

run-test: test
	for test in $(TARGETS_TEST); do ./$${test}; done

test_serializer: tests/serializer.cc serializer.o
	$(CC) -o $@ $(CCFLAGS) $(INCLUDES_TEST) $^ $(LIBS_TEST)

clean-test:
	rm -rf $(TARGETS_TEST)
