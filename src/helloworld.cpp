#include <iostream>
using namespace std;

// TODO: include headers here for checking
#include "stackdb/slice.h"
#include "stackdb/iterator.h"
#include "stackdb/status.h"
using namespace stackdb;

class TestIterator: public Iterator {
    void register_cleanup(cleanup_func func, void *arg1, void *arg2) {
        
    }
};

int main() {

    cout << "hello world" << endl;
}