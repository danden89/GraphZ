#ifndef WORKER_HPP
#define	WORKER_HPP

#include "tasks/PTaskBlockMgrWithScheduler.hpp"
#include "logger.hpp"
#include "io_task.h"

#include "RdstcTimer.hpp"
#include "IWorker.hpp"
#include "IApplication.hpp"
#include "tasks/task.h"
#include "Vertex_Tracker.hpp"
#include <strstream>
#include <stdlib.h>
#include "tasks/task.h"

//#define WORKER_TIMER
namespace graphzx {

    class Id_Allocator {
    public:
        static int id;
        static boost::mutex *id_mutex;

    public:
        static void reset() {
            id = 0;
        }

        static int alloc_id() {
            int rid;
            id_mutex->lock();
            rid = id;
            id++;
            id_mutex->unlock();
            return rid;
        }
    };

    
    
    template<typename edge_t, typename vertex_val_t, typename op_val_t, \
            size_t TASK_SIZE, size_t OP_TASK_SIZE, size_t worker_num, typename Application >
    class Worker : public IWorker<vertex_val_t, op_val_t> {
    private:

        Application *app;

    public:
        static GraphProperty *gp;
        int worker_id;

    public:
        static PTaskBlockMgrWithScheduler< struct TaskBlock<struct adjlst<edge_t>*, TASK_SIZE> > **vertex_buffers;
        static PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> > **op_handle_buffers;
        static PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> > *output_op_buffer;

        static vertex_val_t *vals;

        static int iter;
        static int nth_par;

    public:

        static void init_shared_objects(GraphProperty *_gp,
                PTaskBlockMgrWithScheduler< struct TaskBlock<struct adjlst<edge_t>*, TASK_SIZE> > **_vertex_buffers,
                PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> > **_op_handle_buffers,
                PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> > *_output_op_buffer
                ) {
            gp = _gp;
            vertex_buffers = _vertex_buffers;
            op_handle_buffers = _op_handle_buffers;
            output_op_buffer = _output_op_buffer;
            
            Vertex_Tracker::gp = gp;
            
            Id_Allocator::reset();
            
            //init_vals();
        }

//        static void init_vals(int _nth_par) {
//            nth_par = _nth_par;
//            if (vals == NULL) {
//                vals = new vertex_val_t[gp->nvertices_per_partition];
//            }
//            std::string valspath = gp->get_vals_path(nth_par);
//            FILE* fstrm = fopen(valspath.c_str(), "r");
//            size_t rdb = fread(vals, sizeof (vertex_val_t), gp->nvertices_per_partition, fstrm);
//            fclose(fstrm);
//        }

        static void store_vals(int nth_par) {
            std::string valspath = gp->get_vals_path(nth_par);
            FILE* fstrm = fopen(valspath.c_str(), "w");
            size_t rdb = fwrite(vals, sizeof (vertex_val_t), gp->nvertices_per_partition, fstrm);
            fclose(fstrm);
        }

    private:
        bool all_tasks_received;
        int op_end_msg;

        struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> **op_blocks;
        struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> **out_op_blocks;
    public:

        RdstcTimer local_timer;
        RdstcTimer delegate_timer;

        Worker(vertex_val_t *_vals) {
            worker_id = Id_Allocator::alloc_id();
            
            vals = _vals;
            all_tasks_received = false;
            op_end_msg = 0;
           
            op_blocks = new struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> *[worker_num];
            out_op_blocks = new struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> *[gp->par_num];
            
            app = new Application(this);
        }

        void init_op_blocks() {
            for (int i = 0; i < worker_num; i++) {
                if (i == worker_id)
                    op_blocks[i] = NULL;
                else
                    op_blocks[i] = op_handle_buffers[i]->get_free_block();
            }

            for (int i = 0; i < gp->par_num; i++) {
                out_op_blocks[i] = output_op_buffer->get_free_block();
            }

        }
        
        inline void handle_msg(vertex_id vid, op_val_t val) {
#ifdef WORKER_TIMER
            local_timer.start();
#endif
            int par = Vertex_Tracker::get_partition(vid);

#ifdef WORKER_TIMER
            local_timer.stop();
#endif
            if (par == nth_par) {
#ifdef WORKER_TIMER
                local_timer.start();
#endif
                int owner_id = Vertex_Tracker::get_worker_id(vid);
#ifdef WORKER_TIMER
                local_timer.stop();
#endif

                if (owner_id == worker_id) {
                    update_vertex(vid, val);
                } else {
                    if (vid == 342330) {
//                        logstream(LOG_DEBUG) << "add 342330 here!!" << owner_id << std::endl;
                    }
                    if (!op_blocks[owner_id]->add_element(AOP<op_val_t>(vid, val))) {
                        op_handle_buffers[owner_id]->add_task_block(op_blocks[owner_id]);
                        op_blocks[owner_id] = op_handle_buffers[owner_id]->get_free_block();
                        op_blocks[owner_id]->add_element(AOP<op_val_t>(vid, val));
                    }
                }
            } else {
                if (!out_op_blocks[par]->add_element(AOP<op_val_t>(vid, val))) {
                    output_op_buffer->add_task_block(out_op_blocks[par]);
                    out_op_blocks[par] = output_op_buffer->get_free_block();
                    out_op_blocks[par]->add_element(AOP<op_val_t>(vid, val));
                }
            }
        }

        bool is_the_end() {
            //get all the tasks and all delegated messages
            if (all_tasks_received && op_end_msg == (worker_num - 1))
                return true;
            return false;
        }

        //after getting all tasks from the dispatcher, send end massage to all other worker, 
        // so that they can know when to stop 
        // we still need to notify the op manager of the total ending later
        void broadcast_end() {
            for (int i = 0; i < worker_num; i++) {
                if (i != worker_id) {
                    op_handle_buffers[i]->add_task_block(op_blocks[i]);
                    op_blocks[i] = op_handle_buffers[i]->get_free_block();
                    op_blocks[i]->size = INVALID_SIZE;
                    op_handle_buffers[i]->add_task_block(op_blocks[i]);
                }
            }
        }

        inline void update_vertex(vertex_id vid, op_val_t val) {
#ifdef WORKER_TIMER
            local_timer.start();
#endif
            vertex_id index = vid % gp->nvertices_per_partition;
            vertex_val_t old_val = vals[index];
            vals[index] = app->operate(vals[index], val);
            if (fabs(vals[index].vval - old_val.vval) > 0.01)
                gp->converged = false;
            
#ifdef WORKER_TIMER
            local_timer.stop();
#endif
        }

        void delegate() {
            if (op_blocks[worker_id]->size == INVALID_SIZE) {
                op_end_msg++;
            } else {
                for (int i = 0; i < op_blocks[worker_id]->size; i++) {
                    AOP<op_val_t> aop = op_blocks[worker_id]->elements[i];
                    update_vertex(aop.vid, aop.val);
                }
            }
        }

        inline vertex_val_t& get_value(vertex_id vid) {
            //logstream(LOG_INFO) << "vid::" << vid << std::endl;
            //logstream(LOG_INFO) << "gp->nvertices_per_partition::" << gp->nvertices_per_partition << std::endl;
            return vals[vid % gp->nvertices_per_partition];
        }

        inline void set_value(vertex_id vid, vertex_val_t vval) {
            vals[vid % gp->nvertices_per_partition] = vval;
        }
        
        inline void update_notify() {
            gp->converged = false;
        }

       
        void *start_work(int _iter, int _nth_par) {
            iter = _iter;
            nth_par = _nth_par;
            all_tasks_received = false;

            op_end_msg = 0;
            
            init_op_blocks();
            
           

            logstream(LOG_INFO) << "worker_id: " << worker_id << std::endl;
            logstream(LOG_INFO) << "enter worker of local_iter: " << iter << std::endl;
            logstream(LOG_INFO) << "enter worker of partition nth_par: " << nth_par << std::endl;
          

            struct TaskBlock<struct adjlst<edge_t>*, TASK_SIZE> *ataskblock = \
                vertex_buffers[worker_id]->get_task_block();
            struct adjlst<edge_t> *padj;

            while (true) {
                if (!all_tasks_received) {
                    if (ataskblock->size != INVALID_SIZE) {
                        for (int i = 0; i < ataskblock->size; i++) {
                            padj = ataskblock->elements[i];
                            app->update(padj, iter);
                            adjlst<edge_t>::free_adjlst(padj);
                        }
                        vertex_buffers[worker_id]->add_free_block(ataskblock);
                        ataskblock = vertex_buffers[worker_id]->get_task_block();
                    } else {
                        vertex_buffers[worker_id]->add_free_block(ataskblock);
                        all_tasks_received = true;
                        
                        broadcast_end();
                    }
                    
                } else if (is_the_end() && NULL == (op_blocks[worker_id] = op_handle_buffers[worker_id]->try_get_task_block())) {
                    for (int i = 0; i < gp->par_num; i++){
                        //if (out_op_blocks[i]->size != 0) {
                            output_op_buffer->add_task_block(out_op_blocks[i]);
                        //}
                    }
                    out_op_blocks[0] = output_op_buffer->get_free_block();
                    out_op_blocks[0]->size = INVALID_SIZE;
                    output_op_buffer->add_task_block(out_op_blocks[0]);
                    
                    break;
                }
                //there should be some smarter ways to write this. Anyway, this one must work
                if (NULL == op_blocks[worker_id]) {
                    op_blocks[worker_id] = op_handle_buffers[worker_id]->try_get_task_block();
                }
                while (NULL != op_blocks[worker_id]) {
                    delegate_timer.start();
                    delegate();
                    delegate_timer.stop();
                    op_handle_buffers[worker_id]->add_free_block(op_blocks[worker_id]);
                    op_blocks[worker_id] = op_handle_buffers[worker_id]->try_get_task_block();
                }
            }
        }

        static void *run(Worker<edge_t, vertex_val_t, op_val_t, TASK_SIZE, OP_TASK_SIZE, worker_num, Application > *worker,  int _iter, int _nth_par) {
//            cpu_set_t mask;
//            CPU_ZERO( &mask );
//            CPU_SET( 1, &mask );
//            assert( sched_setaffinity( 0, sizeof(mask),
//                             &mask) ==0);
            worker->start_work(_iter, _nth_par);
            logstream(LOG_INFO) << "worker end: "<< std::endl;
        }

    };

#define TEMPLATE_FOR_INIT_WORKER     template<typename edge_t, typename vertex_val_t, typename op_val_t, \
            size_t TASK_SIZE, size_t OP_TASK_SIZE, size_t worker_num, typename Application >

#define WORKER_WITH_TEMPLATE     Worker<edge_t, vertex_val_t, op_val_t,\
                TASK_SIZE, OP_TASK_SIZE, worker_num, Application >

    TEMPLATE_FOR_INIT_WORKER
    GraphProperty *WORKER_WITH_TEMPLATE::gp = NULL;


    TEMPLATE_FOR_INIT_WORKER
    PTaskBlockMgrWithScheduler< struct TaskBlock<struct adjlst<edge_t>*, TASK_SIZE> >\
 **WORKER_WITH_TEMPLATE::vertex_buffers = NULL;

    TEMPLATE_FOR_INIT_WORKER
    PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> >\
 **WORKER_WITH_TEMPLATE::op_handle_buffers = NULL;

    TEMPLATE_FOR_INIT_WORKER
    PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> >\
 *WORKER_WITH_TEMPLATE::output_op_buffer = NULL;

    TEMPLATE_FOR_INIT_WORKER
    vertex_val_t *WORKER_WITH_TEMPLATE::vals = NULL;

    TEMPLATE_FOR_INIT_WORKER
            int WORKER_WITH_TEMPLATE::iter = 0;

    TEMPLATE_FOR_INIT_WORKER
            int WORKER_WITH_TEMPLATE::nth_par = 0;

}
#endif	/* WORKER_HPP */