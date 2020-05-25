# stackdb test makefile
CXX = g++
CXXFLAGS = -Wall -g -I../src -I../include
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
		echo "Running $$test:" ;\
		./$$test ;\
		echo "----------------" ;\
	done

stackdb:
	@cd ../src && $(MAKE) --silent

clean:
	rm -f $(OBJECTS) $(EXECUTE) .depend

# general rules
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< 

%: %.o $(shell find ../src \( -name '*.o' ! -name '*helloworld.o' \))
	$(CXX) $(CXXFLAGS) -o $@ $^ 

# generate depencies to headers
.depend:
	@$(CXX) $(CXXFLAGS) -MM $(SOURCES) > .depend
-include .depend

.PHONY: .depend