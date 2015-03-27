#ifndef PRE_PROCESS_H
#define	PRE_PROCESS_H

#include "graphtypes.h"

typedef struct fedge_t {
    vertex_id fid;
    vertex_id tid;

    fedge_t() {
    }

    fedge_t(vertex_id _fid, vertex_id _tid) : fid(_fid), tid(_tid) {
    };

} fedge_t; //a full edge type that include the two vertices that compose the edge

typedef struct partition_interval {
//    unsigned int nth_partition;
    unsigned long long start_idx;
    unsigned long long end_idx;
} partition_interval;

struct fedge_comparator {

    bool operator()(const fedge_t& a, const fedge_t& b) const {
        if (a.fid != b.fid)
            return a.fid < b.fid;
        else
            return a.tid < b.tid;
    }

    fedge_t min_value() const {
        return fedge_t(std::numeric_limits<vertex_id >::min(), std::numeric_limits<vertex_id >::min());
    }

    fedge_t max_value() const {
        return fedge_t(std::numeric_limits<vertex_id >::max(), std::numeric_limits<vertex_id >::max());
    }
};

typedef struct fod_edge_t {
    unsigned int degree;
    vertex_id fid;
    vertex_id tid;

    fod_edge_t() {
    }

    fod_edge_t(unsigned int _degree, vertex_id _fid, vertex_id _tid) : degree(_degree), fid(_fid), tid(_tid) {
    };

} fod_edge_t; //a full edge type with the out-degree of the fid

struct fod_edge_degree_comparator {

    bool operator()(const fod_edge_t& a, const fod_edge_t& b) const {
        if (a.degree != b.degree)
            return a.degree < b.degree;
        else if (a.fid != b.fid)
            return a.fid < b.fid;
        else{
            return a.tid < b.tid;
        }
    }

    fod_edge_t min_value() const {
        return fod_edge_t(std::numeric_limits<unsigned int >::min(), std::numeric_limits<vertex_id >::min(), std::numeric_limits<vertex_id >::min());
    }

    fod_edge_t max_value() const {
        return fod_edge_t(std::numeric_limits<unsigned int >::max(), std::numeric_limits<vertex_id>::max(), std::numeric_limits<vertex_id >::max());
    }
};

struct fod_edge_fid_comparator {

    bool operator()(const fod_edge_t& a, const fod_edge_t& b) const {
        if (a.fid != b.fid)
            return a.fid < b.fid;
        else if (a.tid != b.tid)
            return a.tid < b.tid;
        else
            return a.degree < b.degree;
    }

    fod_edge_t min_value() const {
        return fod_edge_t(std::numeric_limits<unsigned int >::min(), std::numeric_limits<vertex_id >::min(), std::numeric_limits<vertex_id >::min());
    }

    fod_edge_t max_value() const {
        return fod_edge_t(std::numeric_limits<unsigned int >::max(), std::numeric_limits<vertex_id>::max(), std::numeric_limits<vertex_id >::max());
    }
};

struct fod_edge_tid_comparator {

    bool operator()(const fod_edge_t& a, const fod_edge_t& b) const {
        if (a.tid != b.tid)
            return a.tid < b.tid;
        else if (a.fid != b.fid)
            return a.fid < b.fid;
        else
            return a.degree < b.degree;
    }

    fod_edge_t min_value() const {
        return fod_edge_t(std::numeric_limits<unsigned int >::min(), std::numeric_limits<vertex_id >::min(), std::numeric_limits<vertex_id >::min());
    }

    fod_edge_t max_value() const {
        return fod_edge_t(std::numeric_limits<unsigned int >::max(), std::numeric_limits<vertex_id>::max(), std::numeric_limits<vertex_id >::max());
    }
};

#endif	/* PRE_PROCESS_H */

