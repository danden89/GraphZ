/* 
 * File:   Lock_pool.hpp
 * Author: ubu
 *
 * Provide fine-grained lock
 */

#ifndef LOCK_POOL_HPP
#define	LOCK_POOL_HPP

#include<pthread.h>

class Lock_pool {
private:
    // boost::mutex **mutexes;
    pthread_mutex_t * mutexes;
    int lock_num;

    inline int hash(long long id) {
        return id & 8191;
    }

public:

    void lock(long long id) {
        pthread_mutex_lock(mutexes + hash(id));
    }

    void unlock(long long id) {
        //mutexes[hash(id)]->unlock();
        pthread_mutex_unlock(mutexes + hash(id));
    }

    Lock_pool(int num) {
        lock_num = num;
        mutexes = (pthread_mutex_t *) malloc(sizeof (pthread_mutex_t) * 8192);
        if (mutexes == 0) {
            std::cerr << "malloc error in Lock_pool" << std::endl;
            exit(-5);
        }
        for (int i = 0; i < 8192; i++) {
            pthread_mutex_init(mutexes + i, NULL);
        }
    }

    ~Lock_pool() {
        for (int i = 0; i < 8192; i++) {
            pthread_mutex_destroy(mutexes + i);
        }
        free(mutexes);
    }

};


#endif	/* LOCK_POOL_HPP */

