module Metadata

if Base.VERSION < v"0.7.0-DEV.2575"
    const Dates = Base.Dates
else
    import Dates
end

using FlatBuffers

@enum(DType, BOOL = 0, INT8 = 1, INT16 = 2, INT32 = 3, INT64 = 4,
  UINT8 = 5, UINT16 = 6, UINT32 = 7, UINT64 = 8,
  FLOAT = 9, DOUBLE = 10,  UTF8 = 11,  BINARY = 12,
  CATEGORY = 13, TIMESTAMP = 14, DATE = 15, TIME = 16)

@enum(Encoding, PLAIN = 0, DICTIONARY = 1)

@enum(TimeUnit, SECOND = 0, MILLISECOND = 1, MICROSECOND = 2, NANOSECOND = 3)

# FlatBuffers.enumsizeof(::Type{TimeUnit}) = UInt8

mutable struct PrimitiveArray
    dtype::DType
    encoding::Encoding
    offset::Int64
    length::Int64
    null_count::Int64
    total_bytes::Int64
end

# TODO why are these done this way rather with an abstract type???
mutable struct CategoryMetadata
    levels::PrimitiveArray
    ordered::Bool
end

@DEFAULT CategoryMetadata ordered=false

mutable struct TimestampMetadata
    unit::TimeUnit
    timezone::String
end

mutable struct DateMetadata
end

mutable struct TimeMetadata
    unit::TimeUnit
end

@UNION TypeMetadata (Void,CategoryMetadata,TimestampMetadata,DateMetadata,TimeMetadata)

mutable struct Column
    name::String
    values::PrimitiveArray
    metadata_type::Int8
    metadata::TypeMetadata
    user_metadata::String
end

function Column(name::String, values::PrimitiveArray, metadata::TypeMetadata=nothing,
                user_metadata::String="")
    Column(name, values, FlatBuffers.typeorder(TypeMetadata, typeof(metadata)),
           metadata, user_metadata)
end

mutable struct CTable
    description::String
    num_rows::Int64
    columns::Vector{Column}
    version::Int32
    metadata::String
end

end # module

# wesm/feather/cpp/src/metadata_generated.h
# wesm/feather/cpp/src/types.h
const JULIA_TYPE_DICT = Dict{Metadata.DType,DataType}(
    Metadata.BOOL      => Arrow.Bool,
    Metadata.INT8      => Int8,
    Metadata.INT16     => Int16,
    Metadata.INT32     => Int32,
    Metadata.INT64     => Int64,
    Metadata.UINT8     => UInt8,
    Metadata.UINT16    => UInt16,
    Metadata.UINT32    => UInt32,
    Metadata.UINT64    => UInt64,
    Metadata.FLOAT     => Float32,
    Metadata.DOUBLE    => Float64,
    Metadata.UTF8      => WeakRefString{UInt8},
    Metadata.BINARY    => Vector{UInt8},
    Metadata.CATEGORY  => Int64,
    Metadata.TIMESTAMP => Int64,
    Metadata.DATE      => Int64,
    Metadata.TIME      => Int64
)

const MDATA_TYPE_DICT = Dict{DataType,Metadata.DType}(
    Bool    => Metadata.BOOL,
    Int8    => Metadata.INT8,
    Int16   => Metadata.INT16,
    Int32   => Metadata.INT32,
    Int64   => Metadata.INT64,
    UInt8   => Metadata.UINT8,
    UInt16  => Metadata.UINT16,
    UInt32  => Metadata.UINT32,
    UInt64  => Metadata.UINT64,
    Float32 => Metadata.FLOAT,
    Float64 => Metadata.DOUBLE,
    String  => Metadata.UTF8,
    Vector{UInt8}   => Metadata.BINARY,
    Dates.DateTime   => Metadata.INT64,
    Dates.Date   => Metadata.INT32,
    WeakRefString{UInt8} => Metadata.UTF8
)

const NON_PRIMITIVE_TYPES = Set([Metadata.UTF8, Metadata.BINARY])

# TODO why is this a separate dict??
# const JULIA_TIME_DICT = Dict{Metadata.TimeUnit,DataType}(
#     Metadata.SECOND => Arrow.Second,
#     Metadata.MILLISECOND => Arrow.Millisecond,
#     Metadata.MICROSECOND => Arrow.Microsecond,
#     Metadata.NANOSECOND => Arrow.Nanosecond
# )
# const MDATA_TIME_DICT = Dict{DataType,Metadata.TimeUnit}(v=>k for (k,v) in JULIA_TIME_DICT)


# TODO this really doesn't seem like an ideal way to deal with this
isprimitivetype(t::Metadata.DType) = t ∉ NON_PRIMITIVE_TYPES


juliastoragetype(meta::Void, values_type::Metadata.DType) = JULIA_TYPE_DICT[values_type]
function juliastoragetype(meta::Metadata.CategoryMetadata, values_type::Metadata.DType)
    CategoricalString{JULIA_TYPE_DICT[values_type]}
end
function juliastoragetype(meta::Metadata.TimestampMetadata, values_type::Metadata.DType)
    throw(ErrorException("Not implemented."))
    # TODO define this!
end

# TODO finish implementing these
juliatype(::Type{T}) where T = T

# TODO these functions should be able to be combined with previous
function schematype(::Type{T}, nullcount::Integer, nullable::Bool, wrs::Bool) where T
    (nullcount == 0 && !nullable) ? T : Union{T,Missing}
end
function schematype(::Type{<:AbstractString}, nullcount::Integer, nullable::Bool, wrs::Bool)
    s = wrs ? WeakRefString{UInt8} : String
    (nullcount == 0 && !nullable) ? s : Union{s, Missing}
end
function schematype(::Type{CategoricalString{R}}, nullcount::Integer, nullable::Bool,
                    wrs::Bool) where R
    (nullcount == 0 && !nullable) ? CategoricalString{R} : Union{CategoricalString{R}, Missing}
end
