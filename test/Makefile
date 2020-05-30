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

SOURCES = $(shell find . -name '*_test.cpp')
OBJECTS = $(patsubst ./%.cpp, %.o, $(SOURCES))
EXECUTE = $(patsubst ./%.cpp, %, $(SOURCES))

test: stackdb $(OBJECTS) $(EXECUTE)
	@echo "\ntests generated. try 'make runtest'"
# sh don't understand equality opertor == as in bash, so use = instead
runtest: stackdb $(OBJECTS) $(EXECUTE)
	@for test in $(EXECUTE); do \
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
	rm -f $(OBJECTS) $(EXECUTE) .depend

# general rules
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< 

%: %.o $(shell find ../src \( -name '*.o' ! -name '*helloworld.o' \)) test_util.o
	$(CXX) $(CXXFLAGS) -o $@ $^ 

# generate depencies to headers
.depend:
	@$(CXX) $(CXXFLAGS) -MM $(SOURCES) > .depend
-include .depend

.PHONY: .depend