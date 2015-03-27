#ifndef	SIO_HPP
#define	SIO_HPP

#include<iostream>
#include<fstream>
#include<fcntl.h>
//#include<linux/aio_abi.h>
#include<stdlib.h>
#include <string.h>
#include<stdint.h>
#include<libaio.h>
#include<errno.h>
#include<unistd.h>
#include<queue>

//#include "common.h"
#include<io_task.h>

#include "threadpool.hpp"
#include <unistd.h>
#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <time.h>
#include <string.h>

#include "logger.hpp"
#include "atomic.h"
#include "threadpool/pool.hpp"

#include "RdstcTimer.hpp"
#include "OpManager.hpp"
#include "ring_queue.hpp"
#include "GraphProperty.hpp"
#include "tasks/PTaskBlockMgrWithScheduler.hpp"
#include "tasks/task.h"
#define _GNU_SOURCE   
#include <sched.h>
#include <sys/types.h>
#include <unistd.h>

namespace graphzx {
#define MAX_QD 200000

    using namespace std;

    const unsigned int big_num = 4;
    const unsigned int big_buffer = 1024 * 1024 * 6;
    
    template<typename edge_t, size_t BUFF_SIZE>
    class Sio {
        FILE *edge_fd;
        struct iocb *io[MAX_QD];
        int io_idx;
        int idbuffer[MAX_QD];
        struct io_event es[2 * QD];
        struct timespec timeout;

        edge_t buf0[BUFF_SIZE];
        degree_offset bases[200000];
        degree_id idbases[200000];
        
        vertex_id vertices_num;
        vertex_id active_vertices_num;

        counter largest; //the largest degreee
        unsigned long long ndegs;
        vertex_id nvertices_per_partition;

        string ofpath;
        string base_path;
        string idbase_path;

       

        GraphProperty *gp;
        
        PTaskBlockMgrWithScheduler< struct io_buffer <edge_t, BUFF_SIZE> > *in_iobuffer;

    public:
        RdstcTimer timer;
    public:

        Sio(GraphProperty *_gp, PTaskBlockMgrWithScheduler< struct io_buffer <edge_t, BUFF_SIZE> > *_in_iobuffer) {
            gp = _gp;
            in_iobuffer = _in_iobuffer;
            nvertices_per_partition = gp->nvertices_per_partition;
            vertices_num = gp->vertices_num;
            active_vertices_num = gp->active_vertices_num;
            edge_fd = NULL;
        }

        ~Sio() {
        }

        int get_vertices_num() {
            return vertices_num;
        }

        long long get_none0_num() {
            exit(-1);
        }

        bool is_0vertice(int id) {
            if (id > gp->active_vertices_num)
                return true;
            return false;
        }

        void the_end() {
            struct io_buffer <edge_t, BUFF_SIZE> *abuf = in_iobuffer->get_free_block();
            abuf->bufsiz = INVALID_SIZE;
//            logstream(LOG_DEBUG) << "end_sio"  << std::endl;
            in_iobuffer->add_task_block(abuf);
        }
        
        void read_send( unsigned long long nbytes, vertex_id startid, vertex_id endid){
            
            if(nbytes == 0)
                return;
            //logstream(LOG_DEBUG) << "mark 2: "  << std::endl;
            struct io_buffer <edge_t, BUFF_SIZE> *abuf = in_iobuffer->get_free_block();
            //logstream(LOG_INFO) << "get 1" << std::endl;
            timer.start();
            unsigned long long rdb = fread(abuf->buf, 1, nbytes, edge_fd);
            timer.stop();
            //memset(abuf->buf, 0,BUFF_SIZE*sizeof(edge_t)); 
            abuf->bufsiz = rdb/sizeof(edge_t);
            abuf->startid = startid;
            abuf->endid = endid;
            in_iobuffer->add_task_block(abuf);

            
            //logstream(LOG_INFO) << "add 1" << std::endl;
            
        }
        
        void read_assign( unsigned long long nbytes, vertex_id startid, vertex_id endid) {
            if ((nbytes % (endid - startid)) % sizeof (edge_t) != 0) {
//                logstream(LOG_DEBUG) << "nbytes::  " << nbytes << std::endl;
//                logstream(LOG_DEBUG) << "startid::  " << startid << std::endl;
//                logstream(LOG_DEBUG) << "endid::  " << endid << std::endl;
                exit(-1);
            }
//            logstream(LOG_DEBUG) << "nbytes::  " << nbytes << std::endl;
//            logstream(LOG_DEBUG) << "startid::  " << startid << std::endl;
//            logstream(LOG_DEBUG) << "endid::  " << endid << std::endl;
            counter degree = nbytes / (endid - startid) / sizeof (edge_t);
//            logstream(LOG_DEBUG) << "degree::  " << degree << std::endl;
            unsigned long read_id_num_per_round = BUFF_SIZE  / degree ;
            unsigned long max_bytes_per_round = read_id_num_per_round * degree* sizeof (edge_t);
           //unsigned long max_bytes_per_round = BUFF_SIZE * sizeof (edge_t);
            unsigned long long rdb;
//            logstream(LOG_DEBUG) << "in_iobuffer->get_free_num(): " << in_iobuffer->get_free_num() << std::endl;
//            logstream(LOG_DEBUG) << "in_iobuffer->get_task_num(): " << in_iobuffer->get_task_num() << std::endl;
            while (nbytes >= max_bytes_per_round) {
//                logstream(LOG_DEBUG) << "mark 1: "  << std::endl;
//                logstream(LOG_DEBUG) << "nbytes::  " << nbytes << std::endl;
//                logstream(LOG_DEBUG) << "read_id_num_per_round::  " << read_id_num_per_round << std::endl;
                read_send(max_bytes_per_round, startid,  startid + read_id_num_per_round);
                startid += read_id_num_per_round;
                nbytes -= max_bytes_per_round;
            }
//             logstream(LOG_DEBUG) << "mark 101: "  << std::endl;
            read_send(nbytes, startid, endid);
//            logstream(LOG_DEBUG) << "nbytes:  " << nbytes << std::endl;
//            logstream(LOG_DEBUG) << "startid:  " << startid << std::endl;
//            logstream(LOG_DEBUG) << "endid:  " << endid << std::endl;
            //job_adder(buf0, rdb, startid, endid);
        }
        
        void keep_retrive(int iter, int nth_par) {
            init_streams(nth_par);
            fseek(edge_fd, 0, SEEK_SET);
            if (nth_par < gp->active_par_num) {
                for (counter i = 0; i < ndegs; i++) {
                    offset nbytes;
                    if (i == ndegs - 1) {
                        if (active_vertices_num >= (nth_par + 1) * nvertices_per_partition) {
                            nbytes = ((nth_par + 1) * nvertices_per_partition - idbases[i].vid) * sizeof (edge_t) * idbases[i].deg;
//                            logstream(LOG_DEBUG) << "nbytes:  " << nbytes << std::endl;
                            read_assign(nbytes, idbases[i].vid, (nth_par + 1) * nvertices_per_partition);
                        } else {
                            nbytes = (active_vertices_num - idbases[i].vid) * sizeof (edge_t) * idbases[i].deg;
                            read_assign( nbytes, idbases[i].vid, active_vertices_num);
                            //for vertices with 0 degrees
                            struct io_buffer <edge_t, BUFF_SIZE> *zerobuf = in_iobuffer->get_free_block();
                            zerobuf->bufsiz = 0;
                            zerobuf->startid = active_vertices_num;
                            zerobuf->endid = (nth_par + 1) * nvertices_per_partition;
                            in_iobuffer->add_task_block(zerobuf);
                        }
                    } else {
                        nbytes = bases[i + 1].ofst - bases[i].ofst;
//                        logstream(LOG_DEBUG) << "nbytes:  " << nbytes << std::endl;
                        read_assign( nbytes, idbases[i].vid, idbases[i + 1].vid);
                    }

                }
            }
            else{
                struct io_buffer <edge_t, BUFF_SIZE> *zerobuf = in_iobuffer->get_free_block();
                zerobuf->bufsiz = 0;
                zerobuf->startid = nth_par * nvertices_per_partition;
                (nth_par + 1) * nvertices_per_partition;
                if (gp->vertices_num >= (nth_par + 1) * nvertices_per_partition)
                    zerobuf->endid = (nth_par + 1) * nvertices_per_partition;
                else
                    zerobuf->endid = gp->vertices_num;

                in_iobuffer->add_task_block(zerobuf);
            }
            
            the_end();
        }

        void newfile(string f) {
            string cmd("touch ");
            cmd += f;
            system(cmd.c_str());
        }

        void init_pathes(unsigned int nth_partition) {
            char tmp[10];
            sprintf(tmp, "%d", nth_partition);
            string nth(tmp);
            ofpath = gp->root_dir + nth + string("/output.graph");
            base_path = gp->root_dir + nth + string("/bases.idx");
            idbase_path = gp->root_dir + nth + string("/idbases.idx");

        }

        void cleanup_per_partition() {
            if (edge_fd > 0) {
                fseek(edge_fd, 0, SEEK_SET);
                fclose(edge_fd);
            }
        }

        bool init_streams(unsigned int nth_partition) {
            cleanup_per_partition();           
            
            init_pathes(nth_partition);
            logstream(LOG_INFO) << "ofpath: " << ofpath << std::endl;
            
            std::ifstream *fin = new std::ifstream();
            //            logstream(LOG_DEBUG) << "base_path: " << base_path << std::endl;
            fin->open(base_path.c_str());
            if (!fin->good()) {
//                logstream(LOG_DEBUG) << "open error..." << std::endl;
            }
            fin->seekg(0, std::ios::end);
            ndegs = fin->tellg() / sizeof (degree_offset);
            
            fin->seekg(0, std::ios::beg);
            fin->read((char *) bases, ndegs * sizeof (degree_offset));
            largest = bases[ndegs - 1].deg;
            
            logstream(LOG_INFO) << "ndegs: " << ndegs << std::endl;
            logstream(LOG_INFO) << "largest degree is: " << largest << std::endl;
            fin->close();
            fin->clear();
            fin->open(idbase_path.c_str());
            if (!fin->good()) {
                std::cout << "open error..." << std::endl;
            }
            //idbases = (degree_id *) malloc(ndegs * sizeof (degree_id));
            fin->read((char*) idbases, ndegs * sizeof (degree_id));
            fin->close();
            fin->clear();
            delete fin;
            
            if ((edge_fd = fopen(ofpath.c_str(), "rw")) == 0) {
                perror("open error");
                return false;
            }
            logstream(LOG_INFO) << "ofpath: " << ofpath << std::endl;

        }

        bool init() {
            pthread_t ntid;
            pthread_attr_t thread_attr;
            int thread_policy;
            struct sched_param thread_param;
            pthread_attr_init(&thread_attr);
            pthread_attr_setschedpolicy(&thread_attr, SCHED_RR);
            thread_param.__sched_priority = 99;
            pthread_attr_setschedparam(&thread_attr, &thread_param);
            int status, rr_min_priority, rr_max_priority;
            pthread_create(&ntid, &thread_attr, start_retrive, this);
            return true;
        }

        static void* start_retrive(void * object, int iter, int nth_par) {
            Sio<edge_t, BUFF_SIZE> *asio = (Sio<edge_t, BUFF_SIZE> *) object;
//            cpu_set_t mask;
//            CPU_ZERO( &mask );
//            CPU_SET( 0, &mask );
//            assert( sched_setaffinity(0, sizeof(mask),
//                             &mask) ==0);
            asio->keep_retrive(iter, nth_par);
             logstream(LOG_INFO) << "sio end: "<< std::endl;
            return 0;
        }

    };
}
#endif	/* SIO_HPP */

