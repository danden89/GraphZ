#ifndef CONTEXT_HPP
#define CONTEXT_HPP

#include <stxxl/queue>

#include "GraphProperty.hpp"
#include "OpManager.hpp"
#include "VertexManager.hpp"

#define QUEUE_LEN 10000

class Context {
public:
    int nr_thread;
    int cur_iter;

    stxxl::queue<vertex_id> **even_active_vertices;
    stxxl::queue<vertex_id> **odd_active_vertices;

    ring_queue<adjlst *, QUEUE_LEN> **adjs;

    counter task_id_num;
    counter active_num;
    
    GraphProperty *gp;
    OpManager *op;
    VertexManager *vm;
    
    volatile boost::atomic<counter> finished_num;

    static alloc_mem_error() {
        std::cerr << "allocate memory error in Context" << std::endl;
        exit(-3);
    }

    Context(GraphProperty *_gp, int _nr_thread) {
        gp = _gp;
        nr_thread = _nr_thread;
        even_active_vertices = malloc(sizeof (stxxl::queue<vertex_id> *) * gp->par_num);
        odd_active_vertices = malloc(sizeof (stxxl::queue<vertex_id> *) * gp->par_num);
        if (even_active_vertices == 0 || odd_active_vertices == 0) {
            alloc_mem_error();
        }
        for (int i = 0; i < gp->par_num; i++) {
            even_active_vertices[i] = new stxxl::queue<vertex_id> ();
            odd_active_vertices[i] = new stxxl::queue<vertex_id> ();
            if (even_active_vertices[i] == 0 || odd_active_vertices[i] == 0) {
                alloc_mem_error();
            }
        }

        adjs = malloc(sizeof (ring_queue<adjlst *, QUEUE_LEN> *) * nr_thread);
        if (adjs == 0) {
            alloc_mem_error();
        }

        for (int i = 0; i < nr_thread; i++) {
            adjs[i] = new stxxl::queue < ring_queue<adjlst *, QUEUE_LEN> ();
            if (adjs[i] == 0) {
                alloc_mem_error();
            }
        }
    }

    virtual ~Context() {
        for (int i = 0; i < gp->par_num; i++) {
            free(even_active_vertices[i]);
            free(odd_active_vertices[i]);
        }

        for (int i = 0; i < nr_thread; i++) {
            free(adjs[i]);
        }

        free(even_active_vertices);
        free(odd_active_vertices);
        free(adjs);
    }



}

#endif