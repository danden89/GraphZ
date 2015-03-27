#ifndef TIMER_HPP
#define	TIMER_HPP
#include <ctime>
#include "logger.hpp"

#define TICKS_PER_SEC (3.4*1000000000)
typedef unsigned long long __u64;
typedef unsigned long __u32;

class RdstcTimer
{
private:
     unsigned long long begin, end;
     

public:
     double costTime; 
     unsigned long long call_times;

public:
    RdstcTimer()
    {
        costTime = 0;
        call_times = 0;
    }

    void start()            
    {
        begin=rdtsc();
        call_times++;
    }

    void stop()               
    {
        end=rdtsc();
        costTime += 1.0 * (end-begin)/TICKS_PER_SEC;
    }

    void reset()         
    {
        costTime = 0;
    }
    
    void tellself() {
        logstream(LOG_INFO) << "\tcall times: " << call_times << std::endl;
        logstream(LOG_INFO) << "\tcostTime: " << costTime << std::endl;
    }
    
private:
    inline unsigned long long rdtsc() {
        __u32 lo, hi;
        __asm__ __volatile__
                (
                "rdtsc" : "=a"(lo), "=d"(hi)
                );
        return (__u64) hi << 32 | lo;
    }
};


#endif	/* TIMER_HPP */

