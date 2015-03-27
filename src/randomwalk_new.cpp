#include <graphz.h>

struct vertex_val_t{
    int vval;
    int agg_msg;
    
    vertex_val_t(){
        vval= 0 ;
        agg_msg=0;
    }
};

using namespace graphzx;
//typedef unsigned int edge_t;
typedef float op_val_t;
//typedef unsigned int vertex_val_t;

int walks_per_source() {
    return 100;
}

bool is_source(vertex_id v) {
    return (v % 50 == 0);
}

template<typename vertex_val_t, typename op_val_t, typename edge_t>
class randomwalk_app : public IApplication<vertex_val_t, op_val_t, edge_t> {
public:
    
    // decide what messages to be sent to its neighbors
    
    void update(struct adjlst<edge_t> *padj, unsigned int iter) {
        //updatec++;
        counter num = padj-> num;

        
        vertex_val_t& val = IApplication<vertex_val_t, op_val_t, edge_t>::get_val(padj->fid);

        if (iter == 0) {
            if (is_source(padj->fid) && num > 0)
                for (unsigned int i = 0; i < walks_per_source(); i++) {
                    vertex_id oid = padj->edges[ rand() % num ].vid;
                    //logstream(LOG_DEBUG) << "rand(): " << rand() << std::endl;
                    IApplication<vertex_val_t, op_val_t, edge_t>::send_msg(oid, 1);
                }
        } else {
            val.vval += val.agg_msg;
            if (num > 0)
                for (unsigned int i = 0; i < val.agg_msg; i++) {
                    vertex_id oid = padj->edges[ rand() % num].vid;
                    IApplication<vertex_val_t, op_val_t, edge_t>::send_msg(oid, 1);
                }
            val.agg_msg = 0;
        }
        IApplication<vertex_val_t, op_val_t, edge_t>::update_notify();
    }

    // how to update itself based on received messages

//    static int msgc;
//    static int updatec;
//    static int send_msgc;
    vertex_val_t operate(vertex_val_t val1, op_val_t val2) {
        val1.agg_msg += val2;
        return val1;
    }

    randomwalk_app< vertex_val_t, op_val_t, edge_t>() {

    }

    randomwalk_app< vertex_val_t, op_val_t, edge_t>(IWorker<vertex_val_t, op_val_t> *_pworker) : \
IApplication< vertex_val_t, op_val_t, edge_t>(_pworker) {
    }


};


//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int randomwalk_app< vertex_val_t, op_val_t, edge_t >::msgc = 0;
//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int randomwalk_app< vertex_val_t, op_val_t, edge_t >::send_msgc = 0;
//
//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int randomwalk_app< vertex_val_t, op_val_t, edge_t >::updatec = 0;

int main(int argc, char *argv[]) {
    //GraphProperty *gp = new GraphProperty(string("/home/ubu/Downloads/com-lj.ungraph.txt"));
    //GraphProperty *gp = new GraphProperty(string("/home/xu/Downloads/friend/com-friendster.ungraph.txt"));
    
    GraphProperty *gp = new GraphProperty(string(argv[1]));

    Engine<edge_t, vertex_val_t, op_val_t, \
            randomwalk_app< vertex_val_t, op_val_t, edge_t>> *engine = \
            new Engine<edge_t, vertex_val_t, op_val_t, \
            randomwalk_app< vertex_val_t, op_val_t, edge_t >> (6, gp, 1);

    
    engine->start();
    //logstream(LOG_DEBUG) << "msgc: " << randomwalk_app< vertex_val_t, op_val_t, edge_t >::msgc << std::endl; 
    delete engine;
}

