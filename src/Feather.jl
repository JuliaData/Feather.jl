module Feather


## temporary definition
const libfeather = Libdl.find_library(["libfeather.so"])

include("feather-h.jl")
include("reader.jl")

end # module
