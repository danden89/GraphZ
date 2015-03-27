#ifndef PTASKBLOCKMGR_HPP
#define	PTASKBLOCKMGR_HPP

#include <iostream>
#include <list>
#include "boost/thread/mutex.hpp"

template<typename T>
class PTaskBlockMgr {
private:
    std::list< T*> *freelist;
    std::list< T*> *tasklist;

    //protect the use of the lists
    boost::mutex freelist_lock;
    boost::mutex tasklist_lock;

public:

    int block_num;
    
    PTaskBlockMgr(int _block_num) {
        block_num = _block_num;
        freelist = new std::list< T*>();
        tasklist = new std::list< T*>();
        T* tmp;
        for (int i = 0; i < block_num; i++) {
            tmp = new T();
            freelist->push_back(tmp);
        }
    }

    virtual ~PTaskBlockMgr() {
        while (!freelist->empty()) {
            delete freelist->front();
            freelist->pop_front();
        }
        while (!tasklist->empty()) {
            delete tasklist->front();
            tasklist->pop_front();
        }
        delete freelist;
        delete tasklist;
    }

    inline T* get_free_block() {
        T* tmp;
        if (freelist->size() == 0) {
            return NULL;
        }
        freelist_lock.lock();
        tmp = freelist->front();
        freelist->pop_front();
        freelist_lock.unlock();
        return tmp;
    }

    inline T* get_task_block() {
        T* tmp;
        if (tasklist->size() == 0)
            return NULL;
        tasklist_lock.lock();
        tmp = tasklist->front();
        tasklist->pop_front();
        tasklist_lock.unlock();
        return tmp;
    }

    inline void add_free_block(T* pblock) {
        pblock->clear();
        freelist_lock.lock();
        freelist->push_back(pblock);
        freelist_lock.unlock();
    }

    inline void add_task_block(T* pblock) {
        tasklist_lock.lock();
        tasklist->push_back(pblock);
        tasklist_lock.unlock();
    }
    
    inline int get_free_num(){
        return freelist->size();
    }
    
    inline int get_task_num(){
        return tasklist->size();
    }
};

#endif	/* PTASKBLOCKMGR_HPP */

