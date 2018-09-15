VERSION < v"0.7.0-beta2.199" && __precompile__()
module Feather

using Arrow, Compat, Compat.Mmap
using FlatBuffers, CategoricalArrays, DataStreams, DataFrames

using Compat.Sys: iswindows


if Base.VERSION < v"0.7.0-DEV.2575"
    const Dates = Base.Dates
    using Missings
    using Compat: @warn, @error
else
    import Dates
end


const FEATHER_VERSION = 2
# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = Vector{UInt8}(codeunits("FEA1"))
const MIN_FILE_LENGTH = 12
const SHOULD_USE_MMAP = !iswindows()


include("metadata.jl")  # flatbuffer defintions
include("loadfile.jl")
include("source.jl")
include("sink.jl")


end # module
