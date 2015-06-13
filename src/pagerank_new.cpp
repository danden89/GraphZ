#include <graphz.h>
#include <cmath>

struct vertex_val_t{
    float vval;
    float agg_msg;
    
    vertex_val_t(){
        vval= 1.0 ;
        agg_msg=0;
    }
};

using namespace graphzx;
//typedef unsigned int edge_t;
typedef float op_val_t;
//typedef unsigned int vertex_val_t;

template<typename vertex_val_t, typename op_val_t, typename edge_t>
class pagerank_app : public IApplication<vertex_val_t, op_val_t, edge_t> {
public:
    
    // decide what messages to be sent to its neighbors
    
    inline void update(struct adjlst<edge_t> *padj, unsigned int iter) {
        //updatec++;
        counter num = padj-> num;

        
        vertex_val_t& val = IApplication<vertex_val_t, op_val_t, edge_t>::get_val(padj->fid);
        
        if(padj->fid == 2325295 )
        {
//            logstream(LOG_DEBUG) << "2325295: " << val.vval<<" , "<< val.agg_msg << std::endl; 
//            logstream(LOG_DEBUG) << "iter: " << iter << std::endl; 
//            logstream(LOG_DEBUG) << "num: " << num << std::endl; 
//            for (unsigned int i = 0; i < num; i++) {
//                vertex_id oid = padj->edges[i].vid;
//                logstream(LOG_DEBUG) << "oid: " << oid << std::endl; 
//                //send_msgc++;
//            }
        }

//        if (padj->fid == 2325262) {
//            logstream(LOG_DEBUG) << "2325262: " << val.vval << " , " << val.agg_msg << std::endl;
//            logstream(LOG_DEBUG) << "num: " << num << std::endl;
//        }

//        if (padj->fid >= 3997960) {
//            logstream(LOG_DEBUG) << "padj->fid: " << padj->fid << std::endl;
//            logstream(LOG_DEBUG) << "too large: " << num << std::endl;
//        }
        
        float new_val = 0.15 + 0.85 * val.agg_msg; 
        IApplication<vertex_val_t, op_val_t, edge_t>::add_diff( abs(new_val - val.vval) );
        val.vval = new_val; 
        val.agg_msg = 0;
        
        if (num > 0) {
            float vote;
            if (iter == 0) {
                vote = 1 / num;
            } else {
                vote = val.vval / num;
            }


            for (unsigned int i = 0; i < num; i++) {
                vertex_id oid = padj->edges[i].vid;
                IApplication<vertex_val_t, op_val_t, edge_t>::send_msg(oid, vote);
                //send_msgc++;
//                logstream(LOG_DEBUG) << "fid: " << padj->fid << std::endl;
//                    logstream(LOG_DEBUG) << "num: " << padj->num << std::endl;
//                    logstream(LOG_DEBUG) << "oid: " << oid << std::endl;
            }
        }
        
        IApplication<vertex_val_t, op_val_t, edge_t>::update_notify();
    }

    // how to update itself based on received messages

//    static int msgc;
//    static int updatec;
//    static int send_msgc;
    inline vertex_val_t operate(vertex_val_t val1, op_val_t val2) {
        val1.agg_msg += val2;
        //msgc++;
        return val1;
    }

    pagerank_app< vertex_val_t, op_val_t, edge_t>() {

    }

    pagerank_app< vertex_val_t, op_val_t, edge_t>(IWorker<vertex_val_t, op_val_t> *_pworker) : \
IApplication< vertex_val_t, op_val_t, edge_t>(_pworker) {
    }


};


//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int pagerank_app< vertex_val_t, op_val_t, edge_t >::msgc = 0;
//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int pagerank_app< vertex_val_t, op_val_t, edge_t >::send_msgc = 0;
//
//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int pagerank_app< vertex_val_t, op_val_t, edge_t >::updatec = 0;

int main(int argc, char *argv[]) {
    //GraphProperty *gp = new GraphProperty(string("/home/ubu/Downloads/com-lj.ungraph.txt"));
    //GraphProperty *gp = new GraphProperty(string("/home/xu/Downloads/friend/com-friendster.ungraph.txt"));
    
    GraphProperty *gp = new GraphProperty(string(argv[1]));

    Engine<edge_t, vertex_val_t, op_val_t, \
            pagerank_app< vertex_val_t, op_val_t, edge_t>> *engine = \
            new Engine<edge_t, vertex_val_t, op_val_t, \
            pagerank_app< vertex_val_t, op_val_t, edge_t >> (6, gp, 1);

    
    engine->start();
    //logstream(LOG_DEBUG) << "msgc: " << pagerank_app< vertex_val_t, op_val_t, edge_t >::msgc << std::endl; 
    delete engine;
}

