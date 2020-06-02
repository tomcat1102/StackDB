# stackdb test makefile
ifndef PREFIX
 $(error Please use outermost makefile in stackdb directory)
endif

# feature flags
export DEFINES = -D HAVE_FDATASYNC \
				 -D HAVE_O_CLOEXEC
				 
# compiler and make flags
CXX = g++
export CXXFLAGS = -Wall -g -I$(PREFIX)/src -I../include $(DEFINES)
MAKEFLAGS = --warn-undefined-variables
# suppress warnings for these
CPPFLAGS=
LDFLAGS=
TARGET_ARCH=
LOADLIBES=
LDLIBS=

TEST_SOURCES = $(shell find . -name '*_test.cpp')
TEST_OBJECTS = $(patsubst ./%.cpp, %.o, $(TEST_SOURCES))
TEST_EXECUTE = $(patsubst ./%.cpp, %, $(TEST_SOURCES))

STACKDB_SOURCES = $(shell find ../src \( -name '*.cpp' ! -name '*helloworld.cpp' \))
STACKDB_OBJECTS = $(patsubst %.cpp, %.o, $(STACKDB_SOURCES))

test: stackdb $(TEST_EXECUTE)
	@echo "\ntests generated. try 'make runtest'"
# sh don't understand equality opertor == as in bash, so use = instead
runtest: stackdb $(TEST_EXECUTE)
	@for test in $(TEST_EXECUTE); do \
		echo -n "Running $$test: " ;\
		./$$test ; \
		if [ $$? = 0 ] ; then \
			echo "OK" ; \
		else \
			echo "FAILED!!!!!!!!" ; \
		fi \
	done

stackdb:
	@cd ../src && $(MAKE) --silent

clean:
	rm -f $(TEST_OBJECTS) $(TEST_EXECUTE) .depend

# general rules
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< 

%: %.cpp $(STACKDB_OBJECTS) test_util.o
	$(CXX) $(CXXFLAGS) -o $@ $^ 

%: %.o $(STACKDB_OBJECTS) test_util.o
	$(CXX) $(CXXFLAGS) -o $@ $^ 

# generate depencies to headers
.depend:
	@$(CXX) $(CXXFLAGS) -MM $(TEST_SOURCES) > .depend
-include .depend

.PHONY: .depend

