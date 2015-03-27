#include<iostream>
#include<fstream>
#include <string.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<queue>
#include"../inc/Bitmap.hpp"
#include "../inc/graphtypes.h"
#include "stxxl/sorter"
#include "stxxl/queue"
//#include <stxxl/vector>
#include "stxxl.h"

#include "../inc/pre_process.h"
#include "../inc/logger.hpp"
#include <libconfig.h++> 

#define NODE_NUM 5997962
#define MAX_EDGES 20000
#define BUF_SIZ 100000000
namespace pre {
    using namespace std;
    using namespace libconfig;

    //stxxl::vector<fod_edge_t> od_edges;
    //stxxl::vector<fod_edge_t, 1, stxxl::lru_pager<16>, (10*1024*1024), stxxl::RC, stxxl::uint64> od_edges;

    typedef stxxl::sorter<fedge_t, fedge_comparator, 2 * 1024 * 1024> fedge_sorter_t;
    typedef stxxl::sorter<fod_edge_t, fod_edge_degree_comparator, 2 * 1024 * 1024> fod_edge_degree_sorter_t;
    typedef stxxl::sorter<fod_edge_t, fod_edge_fid_comparator, 2 * 1024 * 1024> fod_edge_fid_sorter_t;
    typedef stxxl::sorter<fod_edge_t, fod_edge_tid_comparator, 2 * 1024 * 1024> fod_edge_tid_sorter_t;
    
    fod_edge_fid_sorter_t od_edges_init(fod_edge_fid_comparator(), 640*1024*1024); //use for reading
 //   fod_edge_fid_sorter_t od_edges_init_degrees(fod_edge_fid_comparator(), 64*1024*1024); //use for get degrees
    stxxl::queue<fod_edge_t> od_edges_init_degrees;
 //   std::queue<fod_edge_degree_sorter_t *> partitions_relabel_fids; //use for partitioning and relabel fids
    fod_edge_tid_sorter_t od_edges_relabel_tids(fod_edge_tid_comparator(), 640*1024*1024); //use for sorting and then relabel tids
    fod_edge_fid_sorter_t od_edges_relabeled(fod_edge_fid_comparator(), 640*1024*1024); //use to store edges that are relabled completely
    
    unsigned long long nvertices_per_partition;

 //  char *ifpath = "/home/ubu/Downloads/com-lj.ungraph.txt.100000";
 //   char *ifpath = "/home/ubu/Downloads/com-lj.ungraph.txt";
 //   char *ifpath = "/run/media/xu/graph/ydata-sum.txt";
    //char *ifpath = "/home/xu/Downloads/friend/com-friendster.ungraph.txt";
    char *ifpath = "/mnt/ssd/friend/com-friendster.ungraph.txt";

    bool directed = true;
    unsigned long long active_vertices_num = 0; //the number of vertices that has a out-edge
    unsigned long long vertices_num; //number of all vertices

    char abs_path[200]={0};
    string root_dir;
    char line[200];

    string ofpath;
    string base_path;
    string idbase_path;
    string lookup_path;
    string lookup_pathr;
    string config_path;

    ifstream *fin = NULL;
    fstream *fout = NULL;
    ofstream *foutn = NULL;
    ofstream *fbase_out = NULL;
    ofstream *fidbase_out = NULL;
    fstream *flookup_out = NULL;
    fstream *flookup_outr = NULL;

    unsigned long long edges_idx = 0;
    //use the global variable root_dir and realpath

    vertex_id ididx = 0;
    vertex_id newid0 = 0;
    offset base = 0;
    unsigned int partition_num; //the number of partitions
    
    graphzx::Bitmap *bmp;

    vertex_id label_buf[BUF_SIZ];

    void newdir(string dir) {
        int status;
        status = mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }

    void newfile(string f) {
        string cmd("touch ");
        cmd += f;
        system(cmd.c_str());
    }

    void cleanup_per_partition() {
        fout->close();
        fbase_out->close();
        fidbase_out->close();

        delete fout;
        delete fbase_out;
        delete fidbase_out;
    }

    void init_pathes(unsigned int nth_partition) {
        char tmp[10];
        sprintf(tmp, "%d", nth_partition);
        string nth(tmp);
        newdir(root_dir + nth + string("/"));
        ofpath = root_dir + nth + string("/output.graph");
        base_path = root_dir + nth + string("/bases.idx");
        idbase_path = root_dir + nth + string("/idbases.idx");
    }

    void init_global() {
        lookup_path = root_dir + string("lookup.map");
        lookup_pathr = root_dir + string("lookup.mapr");
        newfile(lookup_path);
        newfile(lookup_pathr);
        flookup_out = new fstream(lookup_path.c_str(), ios::in | ios::out | ios::binary | ios::trunc);
        flookup_outr = new fstream(lookup_pathr.c_str(), ios::in | ios::out | ios::binary | ios::trunc);
        config_path = root_dir + string("/properties.cfg");
    }

    void initstreams() {
        fout = new fstream(ofpath.c_str(), ios::out | ios::binary | ios::trunc);
        fbase_out = new ofstream(base_path.c_str(), ios::out | ios::binary | ios::trunc);
        fidbase_out = new ofstream(idbase_path.c_str(), ios::out | ios::binary | ios::trunc);
    }

    bool init_partition(unsigned int nth_partition) {
        init_pathes(nth_partition);
        initstreams();
    }

    void init_root_dir() {
        string tmp(abs_path);
        root_dir = tmp + string(".dir/");
        newdir(root_dir);
    }

    void reverse_map() {
        fedge_sorter_t *fedge_reverser = new fedge_sorter_t(fedge_comparator(), 64 * 1024 * 1024);
        flookup_outr->seekg(0, ios::beg);
        vertex_id *buf = (vertex_id *) malloc(1000000 * sizeof (vertex_id));
        if (buf == 0)
            std::cerr << "error when malloc in reverse_map" << std::endl;

        fedge_t aedge(0, 0);
        while (!flookup_outr->eof()) {            
            flookup_outr->read((char *) buf, 1000000 * sizeof (vertex_id));
            unsigned long long c = flookup_outr->gcount() / sizeof (vertex_id);
            for (unsigned long long i = 0; i < c; i++) {
                aedge.fid = buf[i];
                fedge_reverser->push(aedge);
                aedge.tid += 1;
            }
        }
        fedge_reverser->sort();

        flookup_out->seekp(ios::beg);
        vertex_id prev = -1;
        while (!fedge_reverser->empty()) {
            vertex_id atid = INVALIDATE_VERTEX_ID;
            for (unsigned long long i = 0; i < (*fedge_reverser)->fid - prev - 1; i++) {
                flookup_out->write((char *) &atid, sizeof (vertex_id));
            }
            atid = (*fedge_reverser)->tid;
            flookup_out->write((char *) &atid, sizeof (vertex_id));
            prev = (*fedge_reverser)->fid;
            ++(*fedge_reverser);
        }
        fedge_reverser->clear();
        delete fedge_reverser;
        free(buf);
    }

    void read_edges() {
        char *p;
        while (!fin->eof()) {
            fin->getline(line, 200);
            if (line[0] >= '0' && line[0] <= '9') {
                fod_edge_t aedge;
                p = strtok(line, " \t\r");
                aedge.fid = atoll(p);
                p = strtok(NULL, " \t\r");
                aedge.tid = atoll(p);
                od_edges_init.push(aedge);
                if (!directed) {
                    fod_edge_t aredge;
                    aredge.fid = aedge.tid;
                    aredge.tid = aedge.fid;
                    od_edges_init.push(aredge);
                }
            }
        }
    }

    void init_degrees() {
        //sort on the id of the out vertices
        od_edges_init.sort();
        std::queue<fod_edge_t> tmp_edges;

        //get the degree of each vertex
        while (!od_edges_init.empty()) {            
            active_vertices_num++;
            vertex_id oldfid = od_edges_init->fid;
            
            while (!od_edges_init.empty() && od_edges_init->fid == oldfid) {
                fod_edge_t aedge = (*od_edges_init);
                tmp_edges.push(aedge);
                ++od_edges_init;
            }
            unsigned int c = tmp_edges.size();
            while (!tmp_edges.empty()) {
                fod_edge_t aedge = tmp_edges.front();
                tmp_edges.pop();
                aedge.degree = c;   
                od_edges_init_degrees.push(aedge);
                if(aedge.fid == newid0){
//                    logstream(LOG_DEBUG) << aedge.fid<<"," \
                            << aedge.tid<<"," << aedge.degree << std::endl;
                }
            }            
        }
        
        od_edges_init.clear(); 
     
   
        logstream(LOG_INFO) << "active_vertices_num: " << active_vertices_num << std::endl;
    }

    bool init(char *ifpath) {
        //realpath(ifpath, abs_path);
        memcpy(abs_path, ifpath, strlen(ifpath)+1);
        init_root_dir();
        init_global();
        fin = new ifstream();
        fin->open(ifpath);
        cout << "abs_path: " << abs_path << endl;
        cout << "root_dir: " << root_dir << endl;

        read_edges();
        init_degrees();
        return true;
    }

    bool relabel_fids(unsigned nth_partition) {
        fod_edge_degree_sorter_t degree_sorter(fod_edge_degree_comparator(), 64 * 1024 * 1024);
        vertex_id c = 0;
        while (!od_edges_init_degrees.empty() &&  c < nvertices_per_partition) {
            vertex_id oldfid = od_edges_init_degrees.front().fid;
            c++;
            while (!od_edges_init_degrees.empty() && od_edges_init_degrees.front().fid == oldfid) {
                fod_edge_t aedge = od_edges_init_degrees.front();
                degree_sorter.push(aedge);
                od_edges_init_degrees.pop();
            }
        }

        
        degree_sorter.sort();
     
        
        
        vertex_id newid = nth_partition * nvertices_per_partition;
        while (!degree_sorter.empty()) {
            vertex_id oldfid = degree_sorter->fid;
            flookup_outr->write((char *) &(oldfid), sizeof (vertex_id));
            while (!degree_sorter.empty() && degree_sorter->fid == oldfid) {
                fod_edge_t aedge = (*degree_sorter);
                if(aedge.fid == newid0){
                    newid0=newid;
                }
                if(aedge.fid == 2823833){
//                    logstream(LOG_DEBUG) << aedge.fid<<"," \
                            << aedge.tid<<"," << aedge.degree << std::endl;
//                    logstream(LOG_DEBUG) << newid<<"," \
                            << aedge.tid<<"," << aedge.degree << std::endl;
                }
                aedge.fid = newid;
                 
                od_edges_relabel_tids.push(aedge);
                ++degree_sorter;
            }
            newid++;
        }

       
        
        degree_sorter.clear();
  //      partitions_relabel_fids.push(pdegree_sorter);
        
        if (od_edges_init_degrees.empty()) {
            return true;
        }
        return false;
    }

    void relabel_tids() {
//        od_edges_relabel_tids
//        <fod_edge_degree_sorter_t> partitions_relabel_fids
//        while(!partitions_relabel_fids.empty()){
//            fod_edge_degree_sorter_t sorter = *(partitions_relabel_fids.front());
//            partitions_relabel_fids.pop();
//            while(!sorter.empty()){
//                fod_edge_t aedge = (*sorter);
//                od_edges_relabel_tids.push(aedge);
//                ++sorter;
//            }
//            sorter.clear();
//        }
        //store those tids in sequence to be relabeled in a in-memory way
        od_edges_relabel_tids.sort();

//         while(!od_edges_relabel_tids.empty()){
//            logstream(LOG_DEBUG) << od_edges_relabel_tids->fid\
//                    <<","<<od_edges_relabel_tids->tid\
//                    <<","<<od_edges_relabel_tids->degree<< std::endl;
//            ++od_edges_relabel_tids;
//        }
//        exit(0);
        
        flookup_outr->clear();
        flookup_outr->seekp(0, ios::end);

        flookup_out->clear();
        flookup_out->seekg(0, ios::beg);


        unsigned long long idx = 0;
        unsigned long long nround = 0;
        while (true) {
            flookup_out->seekg(nround * sizeof (vertex_id) * BUF_SIZ, ios::beg);
            flookup_out->read((char *) label_buf, sizeof (vertex_id) * BUF_SIZ);
            unsigned int n = flookup_out->gcount() / sizeof (vertex_id);
            flookup_out->clear();
            unsigned long long offset = BUF_SIZ * nround;
            while ( !od_edges_relabel_tids.empty()  && \
                    od_edges_relabel_tids->tid - offset < n ) {
                fod_edge_t aedge = (*od_edges_relabel_tids);
                if (label_buf[aedge.tid - offset] == INVALIDATE_VERTEX_ID) {
                    label_buf[aedge.tid - offset] = flookup_outr->tellp() / sizeof (vertex_id);
                    flookup_out->seekp(aedge.tid  * sizeof (vertex_id), ios::beg);
                    flookup_out->write((char*) &(label_buf[aedge.tid - offset]), sizeof (vertex_id));
                    flookup_outr->write((char *) &(aedge.tid), sizeof (vertex_id));
                }
                if(aedge.fid == newid0){
//                    logstream(LOG_DEBUG) << aedge.fid<<"," \
                            << aedge.tid<<"," << aedge.degree << std::endl;
                }
                aedge.tid = label_buf[aedge.tid - offset];
                if(aedge.fid == newid0){
//                    logstream(LOG_DEBUG) << aedge.fid<<"," \
                            << aedge.tid<<"," << aedge.degree << std::endl;
                }
                
                od_edges_relabeled.push(aedge);
                ++od_edges_relabel_tids;
            }

            if (n < BUF_SIZ)
                break;
            nround++;
        }

//        logstream(LOG_DEBUG) << "flookup_out->good() : " << flookup_out->good() << std::endl;
        flookup_out->clear();
//        logstream(LOG_DEBUG) << "flookup_out->good() : " << flookup_out->good() << std::endl;
        flookup_out->seekp(0, ios::end);
//        logstream(LOG_DEBUG) << "flookup_out->tellp()  : " << flookup_out->tellp() << std::endl;


        while (!od_edges_relabel_tids.empty()) {
            vertex_id oldtid = od_edges_relabel_tids->tid;
            vertex_id newtid = flookup_outr->tellp() / sizeof (vertex_id);
            flookup_outr->write((char *) &oldtid, sizeof (vertex_id));
//            logstream(LOG_DEBUG) << "oldtid: " << oldtid << std::endl;
//            logstream(LOG_DEBUG) << "newtid: " << newtid << std::endl;

            unsigned long long invalidate_num = oldtid - \
flookup_out->tellp() / sizeof (vertex_id);
//            logstream(LOG_DEBUG) << "invalidate_num: " << invalidate_num << std::endl;
            vertex_id atid = INVALIDATE_VERTEX_ID;
            for (unsigned long long i = 0; i < invalidate_num; i++) {
                
                flookup_out->write((char *) &atid, sizeof (vertex_id));
            }
            flookup_out->write((char *) &newtid, sizeof (vertex_id));
            for (; !od_edges_relabel_tids.empty(); ++od_edges_relabel_tids) {
                fod_edge_t aedge = (*od_edges_relabel_tids);
                if (aedge.tid == oldtid) {
                    if (aedge.fid == newid0) {
//                        logstream(LOG_DEBUG) << aedge.fid << "," \
                            << aedge.tid << "," << aedge.degree << std::endl;
                    }
                    aedge.tid = newtid;
                    if (aedge.fid == newid0) {
//                        logstream(LOG_DEBUG) << aedge.fid << "," \
                            << aedge.tid << "," << aedge.degree << std::endl;
                    }
                    od_edges_relabeled.push(aedge);
                } else
                    break;
            }
        }
        od_edges_relabel_tids.clear();
        od_edges_relabeled.sort();
    }

    void get_vertices_num() {
        vertices_num = flookup_outr->tellp() / sizeof (vertex_id);
        logstream(LOG_INFO) << "vertices_num: " << vertices_num << std::endl;
    }

    bool process_partition(unsigned int nth_partition) {
        init_partition(nth_partition);
        fod_edge_degree_sorter_t sorter(fod_edge_degree_comparator(), 64*1024*1024);
        vertex_id c = 0;
//logstream(LOG_DEBUG) << "mark 1: " << std::endl;
        while (c < nvertices_per_partition && !od_edges_relabeled.empty()) {
            vertex_id oldid = od_edges_relabeled->fid;
            c++;
            while (!od_edges_relabeled.empty() && od_edges_relabeled->fid == oldid) {
                fod_edge_t aedge = (*od_edges_relabeled);
                sorter.push(aedge);
                ++od_edges_relabeled;
            }
        }
// logstream(LOG_DEBUG) << "mark 2: " << std::endl;
        sorter.sort();
            
        unsigned int old_degree = 0;
        while (!sorter.empty()) {
            fod_edge_t aedge = (*sorter);
            degree_id did;
            degree_offset dof;
            if (aedge.degree > old_degree) {
                old_degree = aedge.degree;
                did.deg = aedge.degree;
                did.vid = aedge.fid;
                fidbase_out->write((char *) &did, sizeof (degree_id));

                dof.deg = aedge.degree;
                dof.ofst = fout->tellp();
                fbase_out->write((char *) &dof, sizeof (degree_offset));
            }
            if(aedge.fid == newid0) {
//                logstream(LOG_DEBUG) << aedge.fid << "," \
                            << aedge.tid << "," << aedge.degree << std::endl;
            }
            fout->write((char *) &(aedge.tid), sizeof (vertex_id));
            ++sorter;
        }
//logstream(LOG_DEBUG) << "mark 3: " << std::endl;
        cleanup_per_partition();
        sorter.clear();
        if ((nth_partition + 1) * nvertices_per_partition >= vertices_num) {
            od_edges_relabeled.clear();
            return true;
        }

        return false;
    }

    void save_configs() {
        libconfig::Config cfg;
        //cfg.readFile(config_path);
        libconfig::Setting& root = cfg.getRoot();
        root.add("graph", Setting::TypeGroup);
        libconfig::Setting& graph = root["graph"];
        graph.add("active_vertices_num", Setting::TypeInt64) = (const long long )active_vertices_num;
        graph.add("vertices_num", Setting::TypeInt64) = (const long long )vertices_num;
        graph.add("partition_num", Setting::TypeInt64) =(const long long ) partition_num;
        graph.add("nvertices_per_partition", Setting::TypeInt64) =(const long long ) nvertices_per_partition;
        cfg.writeFile(config_path.c_str());
    }

    void preprocess(char *ifpath, unsigned long long partition_size, bool _directed) {
        nvertices_per_partition = partition_size;
        directed = _directed;
        init(ifpath);

        int i=0;
        while(!relabel_fids(i++)){}; //return true at the final partition
        
        reverse_map();
        relabel_tids();
        get_vertices_num();
        
        partition_num = 0;
        while (!process_partition(partition_num)) {
            partition_num++;
        }
        partition_num++;
        save_configs();
        flookup_out->close();
        flookup_outr->close();
        delete flookup_out;
        delete flookup_outr;
        
        
 
        std::cout << "parition_num: " << partition_num << std::endl;
    }
};

using namespace pre;

int main(int argc, char *argv[]) {
    //system("rm -rf /Downloads/com-lj.ungraph.txt.1000.dir/");
    system("rm /var/tmp/stxxl");
     ifpath = argv[1]; 
     unsigned long vertices_per_num = atol(argv[2]);
    //preprocess(ifpath, 1000000, true);
  //  preprocess(ifpath, 2000000, true);
 //   preprocess(ifpath, 300000000, true);
//      preprocess(ifpath, 10000000, true);
      preprocess(ifpath, vertices_per_num, true);
    //preprocess(ifpath, 1000000);

    return 0;
}
