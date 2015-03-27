#ifndef ATOMIC_H
#define	ATOMIC_H

#include "atomic.h"
#include <stdlib.h>
namespace graphzx {

    
    inline int CAS(unsigned int *mem, unsigned int newval, unsigned int oldval) {
        __typeof(*mem) ret;
        __asm __volatile("lock; cmpxchgl %2, %1; sete %1; "
                : "=a" (ret), "=m" (*mem)
                : "r" (newval), "m" (*mem), "a" (oldval));
        return (int) ret;
    }
    
    inline int TAS(volatile int *s) {
        int r;
        __asm__ __volatile__(
                "xchgl %0, %1;"
                : "=r"(r), "=m"(*s)
                : "0"(1), "m"(*s)
                : "memory");

        return r;
    }
}
#endif	/* ATOMIC_H */

