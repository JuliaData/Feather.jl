module Feather

using Cxx, DataArrays, DataFrames
import DataFrames: DataFrame, names, ncol, nrow

using Cxx: CppPtr, CxxQualType, CppBaseType

addHeaderDir(joinpath(dirname(@__FILE__), "..", "deps", "usr", "include"))
cxxinclude(  joinpath(dirname(@__FILE__), "..", "deps", "usr", "include", "feather", "metadata_generated.h"))

typealias CTablePt CppPtr{CxxQualType{CppBaseType{Symbol("feather::fbs::CTable")},(true,false,false)},(false,false,false)}
typealias ColumnPt CppPtr{CxxQualType{CppBaseType{Symbol("feather::fbs::Column")},(true,false,false)},(false,false,false)}
typealias PrimitivePt CppPtr{CxxQualType{CppBaseType{Symbol("feather::fbs::PrimitiveArray")},(true,false,false)},(false,false,false)}
typealias CategoryMetadataPt CppPtr{CxxQualType{CppBaseType{Symbol("feather::fbs::CategoryMetadata")},(true,false,false)},(false,false,false)}

export
    DataFrame,
    names,
    ncol,
    nrow

const magic = "FEA1"

include("column.jl")
include("reader.jl")

end
