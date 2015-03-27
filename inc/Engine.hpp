#ifndef ENGINE_HPP
#define	ENGINE_HPP


#include <boost/thread/barrier.hpp>
#include "OpManager.hpp"
#include "Sio.hpp"
#include "threadpool.hpp"
#include "tasks/PTaskBlockMgr.hpp"
#include "tasks/PTaskBlockMgrWithScheduler.hpp"
#include "Dispatcher.hpp"
#include "io_task.h"
#include "tasks/task.h"
#include "Worker.hpp"


const size_t BUFF_SIZE = 2000000; //should can store one vertex at least
const size_t SIZE = 40;
const size_t TASK_SIZE = 2000;
const size_t OP_TASK_SIZE = 3000000;
const size_t worker_num = 1;

namespace graphzx {

    template<typename edge_t, typename vertex_val_t, typename op_val_t, typename Application>
    class Engine {
    private:
        unsigned int max_iter_num;


    private:
        GraphProperty *gp;
        boost::threadpool::pool *tp;

        //buffers
    private:
        PTaskBlockMgrWithScheduler< struct io_buffer <edge_t, BUFF_SIZE> > *in_iobuffer;
        PTaskBlockMgrWithScheduler< struct TaskBlock<struct graphzx::adjlst<edge_t> *, TASK_SIZE> > **vertex_buffers;
        PTaskBlockMgrWithScheduler<TaskBlock<AOP<op_val_t>, OP_TASK_SIZE> >** op_handle_buffers;
        PTaskBlockMgrWithScheduler<TaskBlock<AOP<op_val_t>, OP_TASK_SIZE> >* output_op_buffer;


        vertex_val_t *vals;

    private:
        graphzx::Sio<edge_t, BUFF_SIZE> *sio;
        graphzx::Dispatcher<edge_t, BUFF_SIZE, TASK_SIZE, worker_num> *dispatcher;
        Worker< edge_t, vertex_val_t, op_val_t, \
             TASK_SIZE, OP_TASK_SIZE, worker_num,\
            Application > **workers;

        OpManager<vertex_val_t, op_val_t, OP_TASK_SIZE,\
            Application > *opmgr;

    private:


    private:

        void check_malloc(void *p) {
            if (p == NULL) {
                cerr << "allocate memory error in Engine" << endl;
                exit(-1);
            }
        }

    public:

        Engine(unsigned int _max_iter_num, GraphProperty *_gp, unsigned int _worker_num) \
    : max_iter_num(_max_iter_num), gp(_gp) {
            //gp = new GraphProperty(string("/home/ubu/Downloads/com-lj.ungraph.txt"));
            in_iobuffer = new PTaskBlockMgrWithScheduler< struct io_buffer <edge_t, BUFF_SIZE> >(SIZE);
            vertex_buffers = new PTaskBlockMgrWithScheduler< struct TaskBlock<struct adjlst<edge_t> *, TASK_SIZE> > *[worker_num];
            op_handle_buffers = new PTaskBlockMgrWithScheduler<TaskBlock<AOP<op_val_t>, OP_TASK_SIZE> >*[worker_num];

            for (int i = 0; i < worker_num; i++) {
                vertex_buffers[i] = new PTaskBlockMgrWithScheduler< struct TaskBlock<struct adjlst<edge_t> *, TASK_SIZE> >(SIZE);
                op_handle_buffers[i] = new PTaskBlockMgrWithScheduler<TaskBlock<AOP<op_val_t>, OP_TASK_SIZE> >(SIZE);
            }
            output_op_buffer = new PTaskBlockMgrWithScheduler<TaskBlock<AOP<op_val_t>, OP_TASK_SIZE> >(SIZE);

            sio = new graphzx::Sio<edge_t, BUFF_SIZE> (gp, in_iobuffer);
            dispatcher = new graphzx::Dispatcher<edge_t, BUFF_SIZE, TASK_SIZE, worker_num>(in_iobuffer, vertex_buffers);

            vals = new vertex_val_t[gp->nvertices_per_partition];
            
            workers = new Worker< edge_t, vertex_val_t, op_val_t, \
             TASK_SIZE, OP_TASK_SIZE, worker_num,\
  Application > *[worker_num];

            Worker<edge_t, vertex_val_t, op_val_t, \
             TASK_SIZE, OP_TASK_SIZE, worker_num,\
  Application >\
::init_shared_objects(gp, vertex_buffers, op_handle_buffers, output_op_buffer);

            for (int i = 0; i < worker_num; i++) {
                workers[i] = new Worker<edge_t, vertex_val_t, op_val_t, \
             TASK_SIZE, OP_TASK_SIZE, worker_num,\
  Application > (vals);
            }

            Vertex_Tracker::init(gp, worker_num, TASK_SIZE);


            opmgr = new OpManager<vertex_val_t, op_val_t, OP_TASK_SIZE,\
    Application > (gp, vals, output_op_buffer, worker_num);

            tp = new boost::threadpool::pool(worker_num+10);
        }

        virtual ~Engine() {
            
            delete tp;

            delete in_iobuffer;

            for (int i = 0; i < worker_num; i++) {
                delete vertex_buffers[i];
            }
            delete vertex_buffers;

            for (int i = 0; i < worker_num; i++) {
                delete op_handle_buffers[i];
            }
            delete op_handle_buffers;
            delete output_op_buffer;

            delete vals;

            delete sio;
            delete dispatcher;
            for (int i = 0; i < worker_num; i++) {
                delete workers[i];
            }
            delete workers;
        }
       
        int start() {
            for (int iter = 0; iter < max_iter_num; iter++) {
                gp->converged = true;
                for (int nth = 0; nth < gp-> par_num; nth++) {
                    logstream(LOG_INFO) << "iter: " << iter << std::endl; 
                    logstream(LOG_INFO) << "nth_par: " << nth << std::endl;
                    
//                    Worker< edge_t, vertex_val_t, op_val_t, \
//                        TASK_SIZE, OP_TASK_SIZE, worker_num,\
//                        Application >::init_vals(nth);

                    
                    //update stored messaged to those vertices' values
                    Vertex_Tracker::nth_par = nth;
                    Vertex_Tracker::tell_self();
                    //reload partitions

                    opmgr->set_partition(nth);


                    tp->schedule(boost::bind(graphzx::Sio<edge_t, BUFF_SIZE>::start_retrive, sio, iter, nth));
                    //tp->schedule(boost::bind(graphzx::Sio<edge_t, BUFF_SIZE>::start_wait, sio, iter, nth));
                    tp->schedule(boost::bind(graphzx::Dispatcher<edge_t, BUFF_SIZE, TASK_SIZE, worker_num>::run, dispatcher));

                    for (int i = 0; i < worker_num; i++) {
                        tp->schedule(boost::bind(Worker<edge_t, vertex_val_t, op_val_t, \
                            TASK_SIZE, OP_TASK_SIZE, worker_num,\
                                Application >::run, workers[i], iter, nth));
                    }

                    tp->schedule(boost::bind(OpManager<vertex_val_t, op_val_t, OP_TASK_SIZE,\
                        Application > ::run, opmgr, nth));


                    tp->wait();
                    
//                logstream(LOG_DEBUG) << "updatec: " << Application::updatec << std::endl; 
                }

                logstream(LOG_INFO) << "Sio times: " << std::endl;
                sio->timer.tellself();

                logstream(LOG_INFO) << "dispatcher times: "  << std::endl;
                dispatcher->timer.tellself();

                logstream(LOG_INFO) << "opmanager times: " << std::endl;
                opmgr->refresh_timer.tellself();
                opmgr->write_timer.tellself();

                if (iter != 0 && gp->converged)
                    return 0;
            }
            
        }

    };
}
#endif	/* ENGINE_HPP */

