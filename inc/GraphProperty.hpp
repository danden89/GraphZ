#ifndef GRAPHPROPERTY_HPP
#define	GRAPHPROPERTY_HPP

#include<iostream>
#include<libconfig.h++>
#include <sstream>

using namespace libconfig;
using namespace std;

const string val_suffix = "val";
const string ops_suffix = "ops";

class GraphProperty {
public:
    libconfig::Config cfg;
    string config_path;
    string root_dir;
    string fpath;
    unsigned long long active_vertices_num; //the number of vertices that has a out-edge
    unsigned long long vertices_num; //number of all vertices
    unsigned long long nvertices_per_partition;
    unsigned long long par_num; //the number of partitions
    unsigned long long active_par_num; //the number of partitions that have a vertex whose degree > 1
    bool converged;

    
    GraphProperty(string _fpath){
        fpath = _fpath;
        root_dir = fpath+".dir/";
        config_path = root_dir+"properties.cfg";
        init();
        active_par_num = (active_vertices_num-1)/nvertices_per_partition +1;
    }
    
    virtual ~GraphProperty(){}
    
private:
    void init() {
        cfg.readFile(config_path.c_str());
        const Setting& root = cfg.getRoot();
        const Setting& graph = root["graph"];
        graph.lookupValue("active_vertices_num", active_vertices_num);
        graph.lookupValue("vertices_num", vertices_num);
        graph.lookupValue("partition_num", par_num);
        graph.lookupValue("nvertices_per_partition", nvertices_per_partition);
    }

public:

    string get_vals_path(int nth_par) {
       stringstream ss;
        ss << nth_par;
        string valspath;
        ss >> valspath;
        valspath = root_dir + valspath + std::string("/") + val_suffix;
        return valspath;
    }

    string get_ops_path(int nth_par) {
       stringstream ss;
        ss << nth_par;
        string opspath;
        ss >> opspath;
        opspath = root_dir + opspath + std::string("/") + ops_suffix;
        return opspath;
    }
    
    size_t get_vertices_num(int nth_par){
        if(nth_par < par_num-1)
            return nvertices_per_partition;
        else
            return vertices_num % nvertices_per_partition;
    }
};

//int main(){
//    GraphProperty gp(string("/home/ubu/Downloads/com-lj.ungraph.txt.1000"));
//    cout<<gp.vertices_num<<endl;
//    
//}

#endif	/* GRAPHPROPERTY_HPP */

