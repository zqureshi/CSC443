CC = g++
CCFLAGS = -Wall -g
OBJS = serializer.o

INCLUDES_TEST = -I./
LIBS_TEST = -pthread -lgtest
TARGETS_TEST = test_serializer

## Project Build
.PHONY: all clean

all: $(OBJS)

%.o: %.cc
	$(CC) -c -o $@ $(CPPFLAGS) $^

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
