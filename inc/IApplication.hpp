#ifndef IAPPLICATION_HPP
#define	IAPPLICATION_HPP

#include "IWorker.hpp"
#include "io_task.h"

namespace graphzx {

    template<typename vertex_val_t, typename op_val_t, typename edge_t>
    class IApplication {
    public:
        IWorker<vertex_val_t, op_val_t> *pworker;

    public:

        IApplication< vertex_val_t, op_val_t, edge_t>() { }
        
        IApplication< vertex_val_t, op_val_t, edge_t>(IWorker<vertex_val_t, op_val_t> *_pworker) {
            pworker = _pworker;
        }

    public:

        inline vertex_val_t& get_val(vertex_id vid) {
            return pworker->get_value(vid);
        }

        inline void set_value(vertex_id vid, vertex_val_t vval) {
            pworker->set_value(vid, vval);
        }

        //to notify that the graph is not converged
        inline void update_notify(){
            pworker -> update_notify();
        }
        
        // decide what messages to be sent to its neighbors
        virtual void update(struct adjlst<edge_t> *padj, unsigned int iter) = 0;

        // how to update itself based on received messages
        virtual vertex_val_t operate(vertex_val_t val1, op_val_t val2) = 0;

 


    public:

        //send a message to the worker

        inline void send_msg(vertex_id vid, op_val_t val) {
            pworker->handle_msg(vid, val);
        }


    };
}
#endif	/* IAPPLICATION_HPP_ */

