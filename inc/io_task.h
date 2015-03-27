#ifndef IO_TASK_H
#define IO_TASK_H

#include "graphtypes.h"
#include "boost/thread/mutex.hpp"


namespace graphzx {
#define QD 64



    //    struct io_task {
    //        int ids[QD];
    //        short num; //for some ocasions, the queue is not full.
    //    };

    template<typename edge_t>
    struct adjlst {
        vertex_id fid;
        counter num; //the number of out edges
        edge_t *edges;

    public:
        static boost::mutex *malloc_mutex ;
        static boost::mutex *free_mutex ;

        static struct adjlst<edge_t>* get_adjlst(vertex_id fid, counter num) {
            //malloc_mutex->lock();
            struct adjlst<edge_t> *adjl = (struct adjlst<edge_t> *)malloc(sizeof ( struct adjlst<edge_t>));
            if (!adjl) {
                //malloclock = 0;
                //malloc_mutex->unlock();
                return NULL;
            }
            adjl->edges = (edge_t *) malloc(sizeof (edge_t) * num);
            if (!(adjl->edges)) {
                free(adjl);
                //malloclock = 0;
                //malloc_mutex->unlock();
                return NULL;
            }
            //malloc_mutex->unlock();
            adjl->fid = fid;
            adjl->num = num;
            return adjl;
        }

        static struct adjlst<edge_t> free_adjlst(struct adjlst<edge_t>* adjl) {
            //free_mutex->lock();
            free(adjl->edges);
            free(adjl);
            //free_mutex->unlock();
        }
    };

    template<typename edge_t>
    boost::mutex * adjlst<edge_t>::malloc_mutex = new boost::mutex();
    
    template<typename edge_t>
    boost::mutex * adjlst<edge_t>::free_mutex = new boost::mutex();

    struct pos {
        counter idx;
        offset ofst;
    };

    template<typename edge_t>
    struct adjlst<edge_t> * prepare_adjlst(vertex_id fid, counter num);



    template<typename edge_t>
    void free_adjlst(struct adjlst<edge_t> * adjl);
}
#endif

