/* 
 * File:   Timer.hpp
 * Author: ubu
 *
 * Created on August 4, 2014, 11:21 AM
 */

#ifndef TIMER_HPP
#define	TIMER_HPP
#include <ctime>

class Timer
{
private:
    clock_t begin,end;

public:
    double costTime;            

public:
    Timer()
    {
        costTime = 0.0;
    }

    void start()            
    {
        begin=clock();
    }

    void stop()               
    {
        end=clock();
        costTime += 1.0*(end-begin)/CLOCKS_PER_SEC;
    }

    void reset()         
    {
        costTime = 0;
    }
};


#endif	/* TIMER_HPP */

