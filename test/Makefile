# stackdb test makefile
CXX = g++
CXXFLAGS = -Wall -g -I../src
MAKEFLAGS = --warn-undefined-variables
# suppress warnings for these
CPPFLAGS=
LDFLAGS=
TARGET_ARCH=
LOADLIBES=
LDLIBS=

SOURCES = $(shell find . -name '*.cpp')
OBJECTS = $(patsubst ./%.cpp, %.o, $(SOURCES))
EXECUTE = $(patsubst ./%.cpp, %, $(SOURCES))

# specific rules
test: stackdb $(OBJECTS) $(EXECUTE)
	@for test in $(EXECUTE); do \
		echo "\nRunning tests in $$test:" ;\
		./$$test ;\
		echo "finished ...ok\n" ;\
	done

stackdb:
	cd ../src && $(MAKE)

clean:
	rm -f $(OBJECTS) $(EXECUTE) .depend

# general rules
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< 
%: %.o
	$(CXX) $(CXXFLAGS) -o $@ $< $(shell find ../src \( -name '*.o' ! -name '*helloworld.o' \))

# generate depencies to headers
.depend:
	$(CXX) $(CXXFLAGS) -MM $(SOURCES) > .depend
-include .depend

.PHONY: .depend