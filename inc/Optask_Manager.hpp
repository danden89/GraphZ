

#ifndef OPTASK_MANAGER_HPP
#define	OPTASK_MANAGER_HPP

#include "threadpool.hpp"
#include <queue>
#include "io_task.h"

#define VERTICES_PER_TASK 10000
#define QUEUE_NUM 10000

class Optask_Manager{
    boost::threadpool::pool *tp;
    struct adjlst<edge_t>* tasks[QUEUE_NUM][VERTICES_PER_TASK];
    
    int cur_queue_idx = 0;
    int cur_cell_idx = 0;
    
    inline bool insert_adjlst(struct adjlst<edge_t>*){
        
    }
};

#endif	/* OPTASK_MANAGER_HPP */

