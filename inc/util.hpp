/* 
 * File:   util.hpp
 * Author: ubu
 *
 * Created on August 3, 2014, 7:47 PM
 */

#ifndef UTIL_HPP
#define	UTIL_HPP
#include<iostream>
using namespace std;

std::stringstream strm;
template<class T>
static void to_str(string& str, T &i){
    strm.clear();
    strm<<i;
    return strm.str();
}



#endif	/* UTIL_HPP */

