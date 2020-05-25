#include "stackdb/iterator.h"
using namespace stackdb;

Iterator::~Iterator() {
    if (cleanup_head.is_empty()) return;

    // traverse and run each clenaup function from head in the list
    cleanup_node *node = &cleanup_head, *next;
    while (node != nullptr) {
        node->run();
        next = node->next;
        delete node;
        node = next;
    }
}

void Iterator::register_cleanup(cleanup_func func, void *arg1, void *arg2) {
    assert(func != nullptr);
    // use head node if it's empty
    cleanup_node *node;
    if (cleanup_head.is_empty()) {
        node = &cleanup_head;
    } else {
        node = new cleanup_node();
        node->next = cleanup_head.next;
        cleanup_head.next = node;
    }
}


