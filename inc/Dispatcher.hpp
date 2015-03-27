#ifndef DISPATCHER_HPP
#define	DISPATCHER_HPP

#include "tasks/PTaskBlockMgrWithScheduler.hpp"
#include "tasks/task.h"
#include "io_task.h"

namespace graphzx {

    template<typename edge_t, size_t BUFF_SIZE, size_t TASK_SIZE, size_t worker_num>
    class Dispatcher {
        PTaskBlockMgrWithScheduler< struct io_buffer <edge_t, BUFF_SIZE> > *in_iobuffer;
        PTaskBlockMgrWithScheduler< struct TaskBlock<struct adjlst<edge_t> *, TASK_SIZE> > **vertex_buffers;

    public:

        RdstcTimer timer;
        
        Dispatcher(PTaskBlockMgrWithScheduler< struct io_buffer <edge_t, BUFF_SIZE> > *_in_iobuffer, \
     PTaskBlockMgrWithScheduler< struct TaskBlock<struct adjlst<edge_t> *, TASK_SIZE> > **_vertex_buffers) {
            in_iobuffer = _in_iobuffer;
            vertex_buffers = _vertex_buffers;
        }

        //when the in coming buffers are all consumer and receive a block for the  
        void the_end() {
            for (int i = 0; i < worker_num; i++) {
                struct TaskBlock<struct adjlst<edge_t> *, TASK_SIZE> *ataskblock = vertex_buffers[i]->get_free_block();
                ataskblock->size = INVALID_SIZE;
                vertex_buffers[i]->add_task_block(ataskblock);
            }
        }


        void dispatch() {
            int id = 0; //index of worker
            struct TaskBlock<struct adjlst<edge_t> *, TASK_SIZE> *ataskblock = vertex_buffers[id]->get_free_block();
            ataskblock->size = 0;
            
            
            logstream(LOG_INFO) << "enter dispatch" << std::endl;
            
            while (true) {
                struct io_buffer <edge_t, BUFF_SIZE> *abuf = in_iobuffer->get_task_block();
//                logstream(LOG_DEBUG) << "abuf->bufsiz: " << abuf->bufsiz << std::endl;
//                logstream(LOG_DEBUG) << "abuf->startid: " << abuf->startid << std::endl;                
//                logstream(LOG_DEBUG) << "abuf->endid: " << abuf->endid << std::endl;
                //logstream(LOG_INFO) << "get a task blcok" << std::endl;
                if (abuf->bufsiz == INVALID_SIZE) {
                    
                    abuf->bufsiz = 0;
                    in_iobuffer->add_free_block(abuf);
                    if (ataskblock->size == 0) {
                        vertex_buffers[id]->add_free_block(ataskblock);
                    } else {
                        //blocks++;
                        vertex_buffers[id]->add_task_block(ataskblock);
                    }
//                    logstream(LOG_DEBUG) << "end_now " << std::endl;
                    the_end();
                    break;
                }
               // counter degree = abuf->bufsiz / (abuf->endid - abuf->startid) / sizeof (edge_t);                
                counter degree = abuf->bufsiz / (abuf->endid - abuf->startid) ;
                
                int nbytes = degree * sizeof (edge_t); //nbytes is the number bytes consumed by a vertex's out edges

//                logstream(LOG_DEBUG) << "degree" << degree << std::endl;
                size_t index = 0;
                
                for (vertex_id i = abuf->startid; i < abuf->endid; i++) {
                    timer.start();
                    // vc ++;
                    struct adjlst<edge_t> * padj = adjlst<edge_t>::get_adjlst(i, degree);
                    
                    //logstream(LOG_INFO) << "adding adjs" << std::endl;
                    if (padj == 0) {
                        perror("run out of memory");
                        exit(-3);
                    }
                    memcpy(padj->edges, abuf->buf + index, nbytes);
                    index += degree;

                    timer.stop();
                    if (!ataskblock->add_element(padj)) {
                        //logstream(LOG_DEBUG) << "size: " << ataskblock->size << std::endl;
                        vertex_buffers[id]->add_task_block(ataskblock);
                        //blocks++;
//                        logstream(LOG_DEBUG) << "distribute one: " << std::endl;
                        //logstream(LOG_DEBUG) << "co a adj b" << std::endl;
                        if (++id >= worker_num) {
                            id = 0;
                        }
                        ataskblock = vertex_buffers[id]->get_free_block();
                        
                        ataskblock->clear();
                        ataskblock->add_element(padj);
                    }
                }
                
                in_iobuffer->add_free_block(abuf);
                //logstream(LOG_DEBUG) << "get a free task blcok" << std::endl;
            }
//            logstream(LOG_DEBUG) << "vc in dispatcher: " << vc << std::endl;
//            logstream(LOG_DEBUG) << "blocks in dispatcher: " << blocks << std::endl;
        }

        static void run(Dispatcher<edge_t, BUFF_SIZE, TASK_SIZE, worker_num>  *dispatcher) {
//            cpu_set_t mask;
//            CPU_ZERO( &mask );
//            CPU_SET( 1, &mask );
//            assert( sched_setaffinity( 0, sizeof(mask),
//                             &mask) ==0);
            dispatcher->dispatch();
            logstream(LOG_INFO) << "dispatcher end: "<< std::endl;
        }
    };
}

#endif	/* DISPATCHER_HPP */

