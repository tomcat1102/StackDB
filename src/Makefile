# stackdb makefile
ifndef PREFIX
 $(error Please use outermost makefile in stackdb directory)
endif

# feature flags
export DEFINES = -D HAVE_FDATASYNC \
				 -D HAVE_O_CLOEXEC
				 
# compiler and make flags
export CXX = g++
export CXXFLAGS = -Wall -g -I$(PREFIX)/src -I../include $(DEFINES)
export MAKEFLAGS = --warn-undefined-variables
# suppress warnings for these
export CPPFLAGS=
export LDFLAGS=
export TARGET_ARCH=
export LOADLIBES=
export LDLIBS=


SOURCES = $(shell find . -name '*.cpp')
OBJECTS = $(patsubst ./%.cpp, %.o, $(SOURCES))
BASENAMES = $(patsubst ./%.cpp, %, $(SOURCES))

# specific rules
all: $(OBJECTS)

clean:
	rm -f $(OBJECTS) .depend

.helloworld: helloworld.o $(OBJECTS)
	@$(CXX) $(CXXFLAGS) -o helloworld $^
helloworld: .helloworld
	@./helloworld

# generate depencies with relative paths to targets
.depend:
	@rm -f .depend
	@for name in $(BASENAMES); do \
		$(CXX) $(CXXFLAGS) -MM $$name.cpp -MT $$name.o >> .depend ;\
	done

-include .depend

.DEFAULT: all
.PHONY: .depend