/* 
 * File:   Vertex_Tracker.hpp
 * Author: ubu
 *
 * Created on September 28, 2014, 7:09 PM
 */

#ifndef VERTEX_TRACKER_HPP
#define	VERTEX_TRACKER_HPP

#include "GraphProperty.hpp"
#include "graphtypes.h"
//#include "logger.hpp"

class Vertex_Tracker{
public:
    static GraphProperty *gp;
    static unsigned nth_par;
    static unsigned int worker_num;
    static unsigned int TASK_SIZE;
    static unsigned long long timeofup2; 
    
    //determine the  partition number of a vertex
    static unsigned int get_partition(vertex_id vid){
        return vid/gp->nvertices_per_partition;
    }
    
    //determine the owner of a vertex id
    static unsigned int get_worker_id(vertex_id vid){
        return (vid%gp->nvertices_per_partition)%(timeofup2)/TASK_SIZE;
    }
    
    static void init(GraphProperty *_gp, int _worker_num, int _TASK_SIZE) {
        gp= _gp;
        worker_num = _worker_num;
        TASK_SIZE = _TASK_SIZE;
        timeofup2 = worker_num * TASK_SIZE;
    }
    
    static void tell_self(){
//        logstream(LOG_DEBUG) << "nth_par: " << nth_par << std::endl;
//        logstream(LOG_DEBUG) << "worker_num:" << worker_num << std::endl;
//        logstream(LOG_DEBUG) << "TASK_SIZE:" << TASK_SIZE << std::endl;
//        logstream(LOG_DEBUG) << "timeofup2:" << timeofup2 << std::endl;
    }
};

#endif	/* VERTEX_TRACKER_HPP */

