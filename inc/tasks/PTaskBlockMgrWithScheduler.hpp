#ifndef PTASKBLOCKMGRWITHSCHEDULER_HPP
#define	PTASKBLOCKMGRWITHSCHEDULER_HPP

#include <list>
#include "boost/thread/mutex.hpp"
#include "boost/thread/condition_variable.hpp"
#include "PTaskBlockMgr.hpp"
#include "../logger.hpp"

template<typename T>
class PTaskBlockMgrWithScheduler : public PTaskBlockMgr<T> {
private:
    //used for schedule the list users
    pthread_mutex_t *free_lock;
    pthread_mutex_t *task_lock;

    //conditions for users to wait
    pthread_cond_t free_cond;
    pthread_cond_t task_cond;


public:

    PTaskBlockMgrWithScheduler(int block_num) : PTaskBlockMgr<T>::PTaskBlockMgr(block_num) {
        free_lock = new pthread_mutex_t();
        task_lock = new pthread_mutex_t();
        pthread_mutex_init(free_lock, NULL);
        pthread_mutex_init(task_lock, NULL);
        free_cond = PTHREAD_COND_INITIALIZER;
        task_cond = PTHREAD_COND_INITIALIZER;
    }

    virtual ~PTaskBlockMgrWithScheduler() {
    }


    inline T * get_free_block() {
        T *tmp;
        pthread_mutex_lock(free_lock);
        while (true) {
            tmp = PTaskBlockMgr<T>::get_free_block();
            if (tmp != NULL) {
                break;
            }
 
            pthread_cond_wait(&free_cond, free_lock);
        }
        pthread_mutex_unlock(free_lock);
        return tmp;
    }

    inline T * try_get_free_block() {
        return PTaskBlockMgr<T>::get_free_block();
    }

    inline T * get_task_block() {
        T *tmp;

        pthread_mutex_lock(task_lock);
        while (true) {
            tmp = PTaskBlockMgr<T>::get_task_block();
            if (tmp != NULL) {
                break;
            }
            pthread_cond_wait(&task_cond, task_lock);
        }
        pthread_mutex_unlock(task_lock);


        return tmp;
    }

    inline T * try_get_task_block() {
        return PTaskBlockMgr<T>::get_task_block();
    }

    inline void add_free_block(T * pblock) {
        PTaskBlockMgr<T>::add_free_block(pblock);
        pthread_mutex_lock(free_lock);
        pthread_cond_signal(&free_cond);
        pthread_mutex_unlock(free_lock);
    }

    inline void add_task_block(T * pblock) {
        PTaskBlockMgr<T>::add_task_block(pblock);
        pthread_mutex_lock(task_lock);
        pthread_cond_signal(&task_cond);
        pthread_mutex_unlock(task_lock);
    }
    
    void tell_self(){
        logstream(LOG_DEBUG) << "get_free_num: " << PTaskBlockMgr<T> ::get_free_num() << std::endl;
        logstream(LOG_DEBUG) << "get_task_num: " << PTaskBlockMgr<T> ::get_task_num() << std::endl;
    }
};

#endif	/* TASKBLOCKMGRWITHSCHEDULER_HPP */

