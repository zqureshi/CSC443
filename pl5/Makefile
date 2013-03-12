TARGETS = mkunsorted msort bsort
GHCOPTS = ""

.PHONY: all clean

all: $(TARGETS)

clean:
	rm -f $(TARGETS) $(TARGETS:%=%.hi) $(TARGETS:%=%.o)

%: %.hs
	ghc $(GHCOPTS) --make $^
