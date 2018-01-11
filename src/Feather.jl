__precompile__(true)
module Feather

using FlatBuffers, Missings, WeakRefStrings, CategoricalArrays, DataStreams, DataFrames


if Base.VERSION < v"0.7.0-DEV.2575"
    const Dates = Base.Dates
else
    import Dates
end
if Base.VERSION >= v"0.7.0-DEV.2009"
    using Mmap
end
if Base.VERSION < v"0.7-DEV"
    iswindows = is_windows
else
    iswindows = Sys.iswindows
end


export Data


const FEATHER_VERSION = 2
# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = Vector{UInt8}("FEA1")
const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
const ALIGNMENT = 8
const SHOULD_USE_MMAP = !iswindows()


include("arrow/Arrow.jl")  # Arrow type definitions
include("metadata.jl")  # flatbuffer defintions
include("dataio.jl")
include("source.jl")
include("sink.jl")
include("fileio.jl")


end # module
