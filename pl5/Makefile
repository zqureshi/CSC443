TARGETS = mkunsorted msort bsort collect_K collect_mem collect_file
GHCOPTS = 

.PHONY: all clean

all: $(TARGETS)

clean:
	rm -f $(TARGETS) $(TARGETS:%=%.hi) $(TARGETS:%=%.o)

%: %.hs
	ghc $(GHCOPTS) --make $^
