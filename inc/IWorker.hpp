
#ifndef IWORKER_HPP
#define	IWORKER_HPP


namespace graphzx {

    template<typename vertex_val_t, typename op_val_t>
    class IWorker {
    public:
        virtual void handle_msg(vertex_id vid, op_val_t val) = 0;
        virtual vertex_val_t& get_value(vertex_id vid) =0;
        virtual void set_value(vertex_id vid, vertex_val_t vval) = 0;
        virtual void update_notify() = 0;
    };
}

#endif	/* IWORKER_HPP */