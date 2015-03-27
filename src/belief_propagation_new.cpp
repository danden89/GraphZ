#include <graphz.h>

struct vertex_val_t {
    int vval;
    float p0;
    float msg0;
    float p1;
    float msg1;

    vertex_val_t() {
        float msg0 = 1;
        float msg1 = 1;
    }
};

struct op_val_t {
    float op0;
    float op1;
};


using namespace graphzx;
//typedef unsigned int edge_t;
//typedef float op_val_t;
//typedef unsigned int vertex_val_t;
float theta = 0.5;

int get_label() {
    return rand()%2;
}

float fi(int a, int b){
    if( a == b )
        return 1+theta;
    else
        return 1-theta;
}

template<typename vertex_val_t, typename op_val_t, typename edge_t>
class bp_app : public IApplication<vertex_val_t, op_val_t, edge_t> {
public:
    
    // decide what messages to be sent to its neighbors    
    void update(struct adjlst<edge_t> *padj, unsigned int iter) {
        //updatec++;
        counter num = padj-> num;
        
        vertex_val_t& val = IApplication<vertex_val_t, op_val_t, edge_t>::get_val(padj->fid);

        if (iter == 0) {
            val.vval = get_label();
            val.p0 = val.msg0;
            val.p1 = val.msg1;
            val.msg0 = 1;
            val.msg1 = 1;
            op_val_t op;
            op.op0 = fi(val.vval, 0);
            op.op1 = fi(val.vval, 1);
            for (unsigned int i = 0; i < num; i++) {                
                vertex_id oid = padj->edges[i].vid;
                IApplication<vertex_val_t, op_val_t, edge_t>::send_msg(oid, op);
            }
        } else {
            val.p0 = val.msg0;
            val.p1 = val.msg1;
            val.msg0 = 1;
            val.msg1 = 1;
            val.p0 *= fi(val.vval, 0);
            val.p1 *= fi(val.vval, 1);
            op_val_t op;
            op.op0 = fi(0, 0) * val.p0 + fi(1, 0) * val.p1;
            op.op1 = fi(0, 1) * val.p0 + fi(1, 1) * val.p1;

            for (unsigned int i = 0; i < num; i++) {
                vertex_id oid = padj->edges[i].vid;
                IApplication<vertex_val_t, op_val_t, edge_t>::send_msg(oid, op);
            }
        }
        IApplication<vertex_val_t, op_val_t, edge_t>::update_notify();
    }

    // how to update itself based on received messages

//    static int msgc;
//    static int updatec;
//    static int send_msgc;
    vertex_val_t operate(vertex_val_t val1, op_val_t val2) {
        val1.msg0 *= val2.op0;
        val1.msg1 *= val2.op1;
        return val1;
    }

    bp_app< vertex_val_t, op_val_t, edge_t>() {

    }

    bp_app< vertex_val_t, op_val_t, edge_t>(IWorker<vertex_val_t, op_val_t> *_pworker) : \
IApplication< vertex_val_t, op_val_t, edge_t>(_pworker) {
    }


};


//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int bp_app< vertex_val_t, op_val_t, edge_t >::msgc = 0;
//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int bp_app< vertex_val_t, op_val_t, edge_t >::send_msgc = 0;
//
//template<typename vertex_val_t, typename op_val_t, typename edge_t>
//int bp_app< vertex_val_t, op_val_t, edge_t >::updatec = 0;

int main(int argc, char *argv[]) {
    //GraphProperty *gp = new GraphProperty(string("/home/ubu/Downloads/com-lj.ungraph.txt"));
    //GraphProperty *gp = new GraphProperty(string("/home/xu/Downloads/friend/com-friendster.ungraph.txt"));
    
    GraphProperty *gp = new GraphProperty(string(argv[1]));

    Engine<edge_t, vertex_val_t, op_val_t, \
            bp_app< vertex_val_t, op_val_t, edge_t>> *engine = \
            new Engine<edge_t, vertex_val_t, op_val_t, \
            bp_app< vertex_val_t, op_val_t, edge_t >> (6, gp, 1);

    
    engine->start();
    //logstream(LOG_DEBUG) << "msgc: " << bp_app< vertex_val_t, op_val_t, edge_t >::msgc << std::endl; 
    delete engine;
}

