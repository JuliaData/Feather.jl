module Feather

using DataFrames

## temporary definition
const libfeather = Libdl.find_library(["libfeather.so"])

@enum Status OK OOM KEY_ERROR INVALID IO_ERROR NOT_IMPLEMENTED=10 UNKNOWN=50

include("array.jl")
include("datetime.jl")
include("column.jl")
include("reader.jl")

end # module
