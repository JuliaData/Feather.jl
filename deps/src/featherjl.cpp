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

  featherjl.add_type<PrimitiveArray>("PrimitiveArray");

  featherjl.add_type<Column>("Column")
     .method("name", &Column::name)
//     .method("values", &Column::values)
     ;

  featherjl.add_type<TableReader>("TableReader")
    .method("num_columns", &TableReader::num_columns)
    .method("num_rows", &TableReader::num_rows)
    .method("HasDescription", &TableReader::HasDescription)
    .method("version", &TableReader::version)
    .method("GetDescription", &TableReader::GetDescription);

  featherjl.method("openFeatherTable", [](const std::string& path)
  {
      std::unique_ptr<TableReader> table;
      Status st = TableReader::OpenFile(path, &table);
      if (!st.ok()) {
          std::cout << st.ToString() << std::endl;
      }
      return table;
  });

  featherjl.method("getcolumn", [](const TableReader rdr, int i)
  {
      std::unique_ptr<Column> col;
      Status st = rdr.GetColumn(i, &col);
      if (!st.ok()) {
          std::cout << st.ToString() << std::endl;
      }
      return col;
  });

  featherjl.method("columntype", [](const Column col)
  {
      return static_cast<int>(col.type());
  });

  featherjl.export_symbols("openFeatherTable");

//  featherjl.add_type<ArrayMetadata>("ArrayMetadata");

//  featherjl.add_type<metadata::Column>("Column")
//    .method("name", &metadata::Column::name)
//    .method("type", &metadata::Column::type)
//    .method("user_metadata", &metadata::Column::user_metadata)
//    .method("values", &metadata::Column::values);


JULIA_CPP_MODULE_END
