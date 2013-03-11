TARGETS = mkunsorted msort

.PHONY: all clean

all: $(TARGETS)

clean:
	rm -f $(TARGETS) $(TARGETS:%=%.hi) $(TARGETS:%=%.o)

%: %.hs
	ghc --make $^
