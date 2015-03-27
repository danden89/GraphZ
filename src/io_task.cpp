#include <io_task.h>
#include<stdlib.h>
#include<malloc.h>
#include<atomic.h>

#include <boost/pool/object_pool.hpp>

namespace graphzx {

    //boost::object_pool<struct adjlst> *adjlst_pool= new boost::object_pool<struct adjlst>();
    //boost::object_pool<struct adjlst> *adjlst_pool= new boost::object_pool<struct adjlst>();
    
    boost::mutex malloc_mutex;
    boost::mutex free_mutex;
    
    template<typename edge_t>
    struct adjlst<edge_t> * prepare_adjlst(vertex_id fid, counter num) {
        malloc_mutex.lock();
        struct adjlst<edge_t> *adjl = ( struct adjlst<edge_t> *)malloc(sizeof( struct adjlst<edge_t>));
        if (!adjl)
        {
            //malloclock = 0;
            malloc_mutex.unlock();
            return 0;
        }
        adjl->edges = (edge_t *) malloc(sizeof (edge_t) * num);
        if (!(adjl->edges)) {
            free(adjl);
            //malloclock = 0;
            malloc_mutex.unlock();
            return 0;
        }
        malloc_mutex.unlock();
        adjl->fid = fid;
        adjl->num = num;
        return adjl;
    }

    template<typename edge_t>
    void free_adjlst(struct adjlst<edge_t> * adjl) {
        free_mutex.lock();
        free(adjl->edges);        
        free(adjl);
        free_mutex.unlock();
    }
}