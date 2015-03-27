#ifndef TASK_H
#define	TASK_H

#include <stdlib.h>
#include <cstddef>
#include "graphtypes.h"
#include<libaio.h>

#define INVALID_SIZE ((size_t)(-1))

template<typename T, size_t BLOCK_SIZE>
struct TaskBlock {
public:
    T elements[BLOCK_SIZE];
    size_t size; // also used as the index of the TaskBlock

    TaskBlock() {
        size = 0;
    }

    ~TaskBlock() {
    }

    inline void clear() {
        size = 0;
    }

    inline bool full(){
    	if(size < BLOCK_SIZE){
    		return false;
    	}
        return true;
    }

    inline bool add_element(T obj) {
        if (!full()) {
            elements[size] = obj;
            size++;
            return true;
        }
        return false;
    }
};

template<typename edge_t, size_t BUFF_SIZE>
struct io_buffer {
public:
    vertex_id startid;
    vertex_id endid;
    size_t bufsiz;
    edge_t buf[BUFF_SIZE];
    struct iocb *io;
    
    inline void clear() {
        bufsiz = 0;
    }
        
    io_buffer() {
        io = (iocb*)new struct iocb[1];
    }

    ~io_buffer() {
        delete io;
    }
};


#include <unistd.h>
#include <fcntl.h>
#include <libaio.h>
#include <errno.h>
#include <cassert>
#include <cstdlib>
#include <cstdio>

#define FATAL(...)\
  do {\
    fprintf(stderr, __VA_ARGS__);\
    fprintf(stderr, "\n");\
    assert(0);\
    exit(-1);\
  } while (0)

static const void handle_error(int err) {
#define DECL_ERR(X) case -X: FATAL("Error "#X"\n"); break;
  switch (err) {
    DECL_ERR(EFAULT);
    DECL_ERR(EINVAL);
    DECL_ERR(ENOSYS);
    DECL_ERR(EAGAIN);
  };
  if (err < 0) FATAL("Unknown error");
#undef DECL_ERR
}

#define IO_RUN(F, ...)\
  do {\
    int err = F(__VA_ARGS__);\
    handle_error(err);\
  } while (0)

#endif	/* TASK_H */

