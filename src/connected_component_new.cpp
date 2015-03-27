#include <graphz.h>
using namespace graphzx;
//typedef unsigned int edge_t;
typedef unsigned int vertex_t;
typedef unsigned int op_val_t;
//typedef unsigned int vertex_val_t;

struct vertex_val_t{
    unsigned int vval;
    
    vertex_val_t(){
    }
};

template<typename vertex_val_t, typename op_val_t, typename edge_t>
class connect_componet_app : public IApplication<vertex_val_t, op_val_t, edge_t> {
public:
    // decide what messages to be sent to its neighbors

    void update(struct adjlst<edge_t> *padj, unsigned int iter) {
        counter num = padj-> num;
        vertex_val_t& val = IApplication<vertex_val_t, op_val_t, edge_t>::get_val(padj->fid);
        if(iter == 0){
            val.vval = padj->fid;
        }
        for (unsigned int i = 0; i < num; i++) {
            vertex_id oid = padj->edges[i].vid;
            IApplication<vertex_val_t, op_val_t, edge_t>::send_msg(oid, val.vval);
        }
    }

    // how to update itself based on received messages

    vertex_val_t operate(vertex_val_t val1, op_val_t val2) {
        val1.vval = std::min(val1.vval, val2);
        return val1;
    }

    connect_componet_app< vertex_val_t, op_val_t, edge_t>() {

    }

    connect_componet_app< vertex_val_t, op_val_t, edge_t>(IWorker<vertex_val_t, op_val_t> *_pworker) : \
IApplication< vertex_val_t, op_val_t, edge_t>(_pworker) {
    }


};

int main(int argc, char *argv[]) {
    //GraphProperty *gp = new GraphProperty(string("/home/ubu/Downloads/com-lj.ungraph.txt"));
    //GraphProperty *gp = new GraphProperty(string("/home/xu/Downloads/friend/com-friendster.ungraph.txt"));
    
    GraphProperty *gp = new GraphProperty(string(argv[1]));

    Engine<edge_t, vertex_val_t, op_val_t, \
            connect_componet_app< vertex_val_t, op_val_t, edge_t>> *engine = \
            new Engine<edge_t, vertex_val_t, op_val_t, \
            connect_componet_app< vertex_val_t, op_val_t, edge_t >> (500, gp, 1);

    engine->start();
    delete engine;
}

