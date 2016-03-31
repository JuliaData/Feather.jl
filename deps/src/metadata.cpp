#include <iostream>
#include <feather/api.h>

struct metadata {
    int64_t rows;
    int64_t cols;
    bool hasdescription;
    char *path;
};

extern "C" {
    int16_t feather_openFeatherTable(metadata *pt) {
         std::unique_ptr<feather::TableReader> table;
         std::string fullpath = pt->path;
         std::cout << fullpath << std::endl;
         feather::Status st = feather::TableReader::OpenFile(fullpath, &table);
         if (!st.ok()) {
             std::cout << st.ToString() << std::endl;
             return st.posix_code();
         }
         pt->rows = table->num_rows();
         pt->cols = table->num_columns();
         pt->hasdescription = table->HasDescription();
         std::cout << pt->rows << ", " << pt->cols << ", " << pt->hasdescription << std::endl;
         return 0;
    }
}
