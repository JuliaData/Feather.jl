#include <cxx_wrap.hpp>
#include <array.hpp>

#include <algorithm>
#include <sstream>
#include <feather/api.h>

using namespace feather;

JULIA_CPP_MODULE_BEGIN(registry)

  cxx_wrap::Module& feathercxx = registry.create_module("Feather");

  feathercxx.add_type<Status>("Status")
    .method("ok", &Status::ok)
    .method("ToString", &Status::ToString);

//  feathercxx.add_type<PrimitiveArray>("PrimitiveArray");

  feathercxx.add_type<Column>("Column")
    .method("name", &Column::name)
//     .method("values", &Column::values)
     ;

  feathercxx.add_type<CategoryColumn>("CategoryColumn")
    .method("ordered", &CategoryColumn::ordered);

  feathercxx.add_type<TableReader>("TableReader")
    .method("num_columns", &TableReader::num_columns)
    .method("num_rows", &TableReader::num_rows)
    .method("HasDescription", &TableReader::HasDescription)
    .method("version", &TableReader::version)
    .method("GetDescription", &TableReader::GetDescription);

  feathercxx.method("openFeatherTable", [](const std::string& path)
  {
      std::unique_ptr<TableReader> table;
      Status st = TableReader::OpenFile(path, &table);
      if (!st.ok()) {
          std::cout << st.ToString() << std::endl;
      }
      return table;
  });

  feathercxx.method("getcolumn", [](const TableReader rdr, int i)
  {
      std::unique_ptr<Column> col;
      Status st = rdr.GetColumn(i, &col);
      if (!st.ok()) {
          std::cout << st.ToString() << std::endl;
      }
      return col;
  });

  feathercxx.method("columntype", [](const Column col)
  {
      return static_cast<int>(col.type());
  });

  feathercxx.method("datatype", [](const Column col)
  {
      return static_cast<int>(col.values().type);
  });
  
//  feathercxx.export_symbols("openFeatherTable");

JULIA_CPP_MODULE_END
