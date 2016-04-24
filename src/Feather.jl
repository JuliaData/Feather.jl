module Feather

using Cxx
import DataFrames: names, ncol, nrow

addHeaderDir(joinpath(dirname(@__FILE__), "..", "deps", "usr", "include"))
cxxinclude(  joinpath(dirname(@__FILE__), "..", "deps", "usr", "include", "feather", "metadata_generated.h"))

export
    names,
    ncol,
    nrow,
    nulls

const magic = "FEA1"

include("primitive.jl")
include("column.jl")
include("reader.jl")

end
