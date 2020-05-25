# stackdb top makefile
export PREFIX = $(shell pwd)

all stackdb:
	cd src && $(MAKE) all

test:
	cd test && $(MAKE) test

# run a simple helloworld to do minimal testing
helloworld:
	@cd src && $(MAKE)  --no-print-directory $@

clean:
	cd src && $(MAKE) clean
	cd test && $(MAKE) clean

.DEFAULT: all
.PHONY: test