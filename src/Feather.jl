__precompile__(true)
module Feather

using Arrow, Compat
using FlatBuffers, CategoricalArrays, DataStreams, DataFrames

using Compat.Sys: iswindows


if Base.VERSION < v"0.7.0-DEV.2575"
    const Dates = Base.Dates
    using Missings
    using Compat: @warn
else
    import Dates
end
if Base.VERSION ≥ v"0.7.0-DEV.2009"
    using Mmap
end

export Data

import Base: length, size, read, write
import DataFrames: DataFrame
import Arrow.nullcount


const FEATHER_VERSION = 2
# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = Vector{UInt8}(codeunits("FEA1"))
const ALIGNMENT = 8
const MIN_FILE_LENGTH = 12
const SHOULD_USE_MMAP = !iswindows()


include("metadata.jl")  # flatbuffer defintions
include("utils.jl")
include("source.jl")
include("sink.jl")


end # module
