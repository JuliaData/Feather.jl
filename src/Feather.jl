module Feather

using Cxx, DataArrays, DataFrames
import DataFrames: DataFrame, names, ncol, nrow

addHeaderDir(joinpath(dirname(@__FILE__), "..", "deps", "usr", "include"))
cxxinclude(  joinpath(dirname(@__FILE__), "..", "deps", "usr", "include", "feather", "metadata_generated.h"))

export
    DataFrame,
    names,
    ncol,
    nrow

const magic = "FEA1"

include("column.jl")
include("reader.jl")

end
