#include"Worker.hpp"
namespace graphzx {
    
    int Id_Allocator::id = 0;
    boost::mutex *Id_Allocator::id_mutex = new boost::mutex();
    
    

}