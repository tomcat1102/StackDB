#include <iostream>
using namespace std;

// TODO: include headers here for checking
#include "stackdb/slice.h"
#include "stackdb/iterator.h"
#include "stackdb/status.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include "util/random.h"
using namespace stackdb;

class A {
public:
    A(int a): a(a) {}
    int a;
};

int main() {
    A a(10);
    cout << "hello world" << endl;
    cout << "a: " << a.a << endl;
}