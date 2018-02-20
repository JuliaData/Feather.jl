__precompile__(true)
module Feather

using Arrow
using FlatBuffers, Missings, CategoricalArrays, DataStreams, DataFrames


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

# TODO padding, WTF?? doesn't specify any of this in FORMAT.md

# TODO for now we are not compatible with old feather files (that don't pad length)

export Data

import Base: length, size, read, write
import DataFrames: DataFrame
import Arrow.nullcount


const FEATHER_VERSION = 2
# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = Vector{UInt8}("FEA1")
const ALIGNMENT = 8
const SHOULD_USE_MMAP = !iswindows()


include("metadata.jl")  # flatbuffer defintions
include("utils.jl")
include("source.jl")
include("sink.jl")


end # module
