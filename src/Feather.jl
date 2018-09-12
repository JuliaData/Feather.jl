module Feather

using Arrow, Mmap, Dates
using FlatBuffers, CategoricalArrays, DataFrames, Tables

const FEATHER_VERSION = 2
# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = Vector{UInt8}(codeunits("FEA1"))
const MIN_FILE_LENGTH = 12
const SHOULD_USE_MMAP = !Sys.iswindows()


include("metadata.jl")  # flatbuffer defintions
include("loadfile.jl")
include("source.jl")
include("sink.jl")


end # module
