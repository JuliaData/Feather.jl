#include <cxx_wrap.hpp>
#include <array.hpp>

#include <algorithm>
#include <sstream>
#include <feather/api.h>

using namespace feather;

JULIA_CPP_MODULE_BEGIN(registry)

  cxx_wrap::Module& featherjl = registry.create_module("Feather");

  featherjl.add_type<Status>("Status")
    .method("ok", &Status::ok)
    .method("ToString", &Status::ToString);

  featherjl.add_type<ColumnType>("ColumnType");

  featherjl.method("openFeatherTable", [](const std::string& path)
  {
      std::unique_ptr<TableReader> table;
      Status st = TableReader::OpenFile(path, &table);
      if (!st.ok()) {
          std::cout << st.ToString() << std::endl;
      }
      return table;
  });

  featherjl.export_symbols("openFeatherTable");

//  featherjl.add_type<ArrayMetadata>("ArrayMetadata");

//  featherjl.add_type<metadata::Column>("Column")
//    .method("name", &metadata::Column::name)
//    .method("type", &metadata::Column::type)
//    .method("user_metadata", &metadata::Column::user_metadata)
//    .method("values", &metadata::Column::values);

//  featherjl.add_type<TableReader>("TableReader");

JULIA_CPP_MODULE_END
