#ifndef OPMANAGER_HPP
#define	OPMANAGER_HPP
#include <iostream>
#include "graphtypes.h"
#include "logger.hpp"
#include "Lock_pool.hpp"
#include "GraphProperty.hpp"
#include "RdstcTimer.hpp"
#include "tasks/PTaskBlockMgrWithScheduler.hpp"
#include "tasks/task.h"
#include "Vertex_Tracker.hpp"

#define BUFSIZE 1000000
#define TIMER
using namespace std;

//#define HTM
//#define TIMER

template<typename vertex_val_t, typename op_val_t, size_t OP_TASK_SIZE, typename Application >

class OpManager {
    int set_partition_calls;

    unsigned int cur_par;
    unsigned int par_num;
    unsigned int worker_num;

    unsigned long long nvertices_per_partition;
    unsigned long long nvertices_last_partition;
    unsigned long long nvertices_this_partition;

    std::string root_dir;
    std::string valname;
    std::string filepath;
    std::string opfilepath;

    fstream **valstreams;
    fstream **opstreams;

    FILE **val_fds;
    FILE **ops_fds;

    char **valbuf;
    char **opsbuf;

    AOP<op_val_t> opbuf[BUFSIZE];

    Lock_pool *lp;

    GraphProperty *gp;

    Application app;

    PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> > *output_op_buffer;

public:
    vertex_val_t *vals;
    //bool converged;
    //unsigned int cur_iter;

    RdstcTimer refresh_timer;
    RdstcTimer write_timer;

    vertex_id base_id;
private:

    void newfile(string f) {
        string cmd("touch ");
        cmd += f;
        system(cmd.c_str());
    }

public:

    OpManager(GraphProperty *_gp, vertex_val_t *_vals, \
            PTaskBlockMgrWithScheduler< struct TaskBlock<struct AOP<op_val_t>, OP_TASK_SIZE> > *_output_op_buffer, \
            int _worker_num) {
        worker_num = _worker_num;
        set_partition_calls = 0;
        gp = _gp;
        vals = _vals;
        output_op_buffer = _output_op_buffer;
        par_num = gp->par_num;
        valname = string("val");
        lp = new Lock_pool(10000);
        root_dir = gp->root_dir;
        //        logstream(LOG_DEBUG) << "root_dir " << root_dir << std::endl;
        nvertices_per_partition = gp->nvertices_per_partition;
        nvertices_last_partition = gp->vertices_num % gp->nvertices_per_partition;
        if (nvertices_last_partition == 0) {
            nvertices_last_partition = nvertices_per_partition;
        }

        cur_par = 0;
        init_partitions();
    }

    ~OpManager() {
        free(vals);
    }

    inline vertex_val_t get_val(vertex_id vid) {
        return vals[vid % nvertices_per_partition];
    }

    void set_val(vertex_id vid, vertex_val_t new_val) {
#ifdef HTM
        __transaction_atomic{
#else
        lp->lock(vid);
#endif
            vals[vid % nvertices_per_partition] = new_val;
#ifdef HTM
        }
#else
            lp->unlock(vid);
#endif 
    }

    vertex_val_t get_val_reset(vertex_id vid, vertex_val_t _init_val = 0) {
        vertex_val_t rts;
#ifdef HTM
        __transaction_atomic{
#else
        lp->lock(vid);
#endif
            rts = get_val(vid);
            vals[vid % nvertices_per_partition] = _init_val;
#ifdef HTM
        }
#else
            lp->unlock(vid);
#endif     

        return rts;
    }

    void init_partitions() {
        val_fds = (FILE **) malloc(par_num * sizeof (FILE*));
        ops_fds = (FILE **) malloc(par_num * sizeof (FILE*));

        //valbuf = (char **) malloc(par_num * sizeof (char *));
        opsbuf = (char **) malloc(par_num * sizeof (char *));

        for (unsigned int i = 0; i < par_num; i++) {
            init_partition(i);
        }
    }

    void init_partition(unsigned int nth_partition) {
        //        logstream(LOG_DEBUG) << "nth_partition: " << nth_partition << std::endl;
        string filepath = gp->get_vals_path(nth_partition);
        string opfilepath = filepath + string(".ops");

        newfile(filepath);
        newfile(opfilepath);
        logstream(LOG_DEBUG) << "filepath: " << filepath << std::endl;
        logstream(LOG_DEBUG) << "opfilepath: " << opfilepath << std::endl;

        val_fds [nth_partition ] = fopen(filepath.c_str(), "w+");
        ops_fds[nth_partition] = fopen(opfilepath.c_str(), "w+");

        assert(val_fds [nth_partition ] > 0);
        assert(ops_fds [nth_partition ] > 0);
        int trun_rs = ftruncate(fileno(ops_fds [nth_partition ]), 0);
        assert(trun_rs == 0);

        //valbuf[nth_partition ] = (char *) malloc(BUFSIZE);
        opsbuf[nth_partition ] = (char *) malloc(BUFSIZE);

        if (opsbuf[nth_partition] == 0) {
            alloc_mem_err();
        }
        //setbuffer(val_fds [nth_partition ], valbuf[nth_partition ], BUFSIZE);
        setbuffer(ops_fds [nth_partition ], opsbuf[nth_partition ], BUFSIZE);
    }

    void init_vals(vertex_val_t val) {
        nvertices_this_partition = nvertices_per_partition;
        for (int i = 0; i < par_num; i++) {
            if (i == par_num - 1)
                nvertices_this_partition = nvertices_last_partition;
            for (unsigned long long j = 0; j < nvertices_this_partition; j++) {
                vals[j] = val;
            }
            fseek(val_fds[i], 0, SEEK_SET);
            fwrite((char*) vals, 1, sizeof (vertex_val_t) * nvertices_this_partition, val_fds[i]);
        }
        cur_par = par_num - 1;
    }

    void init_vals_id() {
        vertex_id vid = 0;
        unsigned long long nvertices_this_partition = nvertices_per_partition;
        for (unsigned int i = 0; i < par_num; i++) {
            //            logstream(LOG_DEBUG) << "i: " << i << std::endl;
            if (i == par_num - 1)
                nvertices_this_partition = nvertices_last_partition;
            for (unsigned long long j = 0; j < nvertices_this_partition; j++) {
                vals[j] = vid++;
            }
            fseek(val_fds[i], 0, SEEK_SET);

            fwrite((char*) vals, 1, sizeof (vertex_val_t) * nvertices_this_partition, val_fds[i]);
        }
        cur_par = par_num - 1;
    }

     void int_vals() {
        vertex_val_t seed = vertex_val_t();
#pragma omp parallel for num_threads(4)
        for (int i = 0; i < gp->nvertices_per_partition; i++) {
            vals[i] = seed;
        }
    }
    
    void set_cur_par(unsigned int new_cur_par) {
        cur_par = new_cur_par;
        set_nvertices_this_partition();
    }

    void set_nvertices_this_partition() {
        if (cur_par == par_num - 1) {
            nvertices_this_partition = nvertices_last_partition;
        } else
            nvertices_this_partition = nvertices_per_partition;
    }

    void save_cur_partition() {
        //store data on disk

        set_partition_calls++;
        //        logstream(LOG_DEBUG) << "set_partition_calls: " << set_partition_calls << std::endl;
        fseek(val_fds[cur_par], 0, SEEK_SET);
        fwrite((char *) vals, 1, sizeof (vertex_val_t) * nvertices_this_partition, val_fds[cur_par]);
    }

    //refresh vals by from disk storage
    int counter = 0;
    
    void refresh_vals(int nth_par) {

        fseek(val_fds[nth_par], 0, SEEK_SET);
        fread((char *) vals, 1, sizeof (vertex_val_t) * nvertices_this_partition, val_fds[nth_par]);
        //logstream(LOG_DEBUG) << "counter: " << counter << std::endl;
        if(counter < gp->par_num){
            counter++;
            int_vals();
        }
        
        fseek(ops_fds[nth_par], 0, SEEK_SET);
        unsigned long long c;
        //        logstream(LOG_DEBUG) << "wait here? " << std::endl;
        int size = sizeof (AOP<op_val_t>) * BUFSIZE;
#ifdef TIMER
        refresh_timer.start();
#endif
        while ((c = fread((char *) opbuf, 1, size, ops_fds[nth_par])) != 0) {
            //            logstream(LOG_DEBUG) << "still reading: " << std::endl;
            c = c / sizeof (AOP<op_val_t>); //convert read bytes to number of operations
#pragma omp parallel for num_threads(8)
            for (int i = 0; i < c; i++) {
                // logstream(LOG_DEBUG) << "opbuf[i].vid: " <<opbuf[i].vid << std::endl;

                vertex_id target_vid = opbuf[i].vid;
                vertex_id ofst_vid = target_vid - base_id;

                if (opbuf[i].vid > 4000000) {
                    //                    logstream(LOG_DEBUG) << "opbuf[i].vid: " << opbuf[i].vid << std::endl;
                }
                //                logstream(LOG_DEBUG) << "target_vid: " << target_vid << std::endl;
                //                logstream(LOG_DEBUG) << "ofst_vid: " << ofst_vid << std::endl;
                //                logstream(LOG_DEBUG) << "base_id: " << base_id << std::endl;
                //vertex_val_t old_val = vals[ofst_vid];
                update_local(ofst_vid, opbuf[i].val);
                //if (fabs(vals[ofst_vid ].vval - old_val.vval) > 0.01)
                //    gp->converged = false;
            }
        }
#ifdef TIMER
        refresh_timer.stop();
#endif
        fseek(ops_fds[nth_par], 0, SEEK_SET);
        int trun_rs = ftruncate(fileno(ops_fds[nth_par]), 0);
        assert(trun_rs == 0);
    }

    void init_set_partition(unsigned int new_cur_par) {
        //store data on disk
        save_cur_partition();
        set_cur_par(new_cur_par);
        base_id = new_cur_par * gp->nvertices_per_partition;
        //refresh_vals(new_cur_par);
    }
    
    void set_partition(unsigned int new_cur_par) {
        //store data on disk
        save_cur_partition();
        set_cur_par(new_cur_par);
        base_id = new_cur_par * gp->nvertices_per_partition;
        refresh_vals(new_cur_par);
    }

    //After the final iteration, update all those vertices values in the "ops" file

    void final_updates() {
        for (int i = 0; i < gp->par_num - 1; i++) {
            set_partition(i);
        }
        save_cur_partition();
    }

    inline void update_local(vertex_id vid, op_val_t val) {
        vertex_val_t old_val = vals[vid];


#ifdef HTM
        __transaction_atomic{
#else
        lp->lock(vid);
#endif

            vals[vid] = app.operate(vals[vid], val);
#ifdef HTM
        }
#else
            lp->unlock(vid);
#endif


        if (fabs(vals[vid ].vval - old_val.vval) > 0.01)
            gp->converged = false;
    }

    void update(vertex_id vid, op_val_t val) {
        if (vid >= nvertices_per_partition * cur_par && vid < nvertices_per_partition * (cur_par + 1)) {
            update_local(vid, val);
        } else {
            AOP<op_val_t> aop;
            aop.vid = vid;
            aop.val = val;

#ifdef TIMER
            write_timer.start();
#endif
            //            logstream(LOG_DEBUG) << "nvertices_per_partition: " << nvertices_per_partition<< std::endl;
            //            logstream(LOG_DEBUG) << "vid: " << vid<< std::endl;
            fwrite((char*) & aop, 1, sizeof (AOP<op_val_t>), ops_fds[vid / nvertices_per_partition]);

#ifdef TIMER
            write_timer.stop();
#endif
        }
    }

    // aborted and it's moved to Engine

    //    bool end() {
    //        if (cur_iter == ITER_NUM - 1 || converged)
    //            //        if(cur_iter == ITER_NUM-1)
    //            return true;
    //        return false;
    //    }

    void alloc_mem_err() {
        std::cout << "allocate memory error in Opmanager" << endl;
        exit(-3);
    }

    void start_service(int nth_par) {

        int end_msgs = 0;
        struct TaskBlock <struct AOP<op_val_t>, OP_TASK_SIZE> *out_op_block;

        //refresh_vals();
        while (true) {
            out_op_block = output_op_buffer->get_task_block();
            if (out_op_block->size == INVALID_SIZE) {
                end_msgs++;
                if (end_msgs == worker_num){
                    output_op_buffer->add_free_block(out_op_block);
                    break;
                }
            } else {
                int par = Vertex_Tracker::get_partition(out_op_block->elements[0].vid);
#ifdef TIMER
                write_timer.start();
#endif
                fwrite(out_op_block->elements, sizeof (AOP<op_val_t>), out_op_block->size, ops_fds[par]);
#ifdef TIMER
                write_timer.stop();
#endif
            }
            output_op_buffer->add_free_block(out_op_block);
        }
    }

    static void run(OpManager<vertex_val_t, op_val_t, OP_TASK_SIZE, Application > *opManager, int nth_par) {
        opManager->start_service(nth_par);
        logstream(LOG_INFO) << "opmanager end: "<< std::endl;
    }
};


#endif	/* OPMANAGER_HPP */

