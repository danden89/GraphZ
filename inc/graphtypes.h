/* 
 * File:   graphtypes.h
 * Author: ubu
 *
 * Created on July 5, 2014, 8:33 PM
 */

#ifndef GRAPHTYPES_H
#define	GRAPHTYPES_H

#define TOPO_ONLY
//#define __cplusplus

#ifndef	__cplusplus
extern "C" {
#endif
   
    typedef float edge_value;
    typedef unsigned int vertex_id; 
    
    //as a sign to show a vertex's id does not exist
    #define INVALIDATE_VERTEX_ID (~(vertex_id)0)  
    //typedef unsigned long long offset;
    typedef unsigned int offset;
    typedef unsigned int counter;
    
    typedef struct {
        counter deg;
        vertex_id vid;
    } degree_id;
    
    typedef struct {
        counter deg;
        offset ofst;
    } degree_offset;

    template<typename op_val_t>
    struct AOP {
        vertex_id vid;
        op_val_t val;

        AOP() {
        }
        
        AOP(vertex_id _vid, op_val_t _val) {
            vid = _vid;
            val = _val;
        }
    };
    
#ifndef TOPO_ONLY
    
    typedef struct{
        VERTEX_ID vid;
        EDGE_VALUE val;
    } edge_t;
    
#else
    typedef struct{
        vertex_id vid;
    } edge_t;
    
#endif
        
#ifndef	__cplusplus
}
#endif

#endif	/* GRAPHTYPES_H */

