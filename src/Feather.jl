module Feather

using FlatBuffers
#using CxxWrap, DataArrays, DataFrames

#wrap_module(Libdl.find_library(["libfeatherjl.so"], [Pkg.dir("Feather", "deps", "usr", "lib")]))

export
    Reader

include("tables.jl")
include("reader.jl")

end # module
