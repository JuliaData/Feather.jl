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
    Metadata.UTF8      => String,  # can also be WeakRefString{UInt8}
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

const JULIA_TIME_DICT = Dict{Metadata.TimeUnit,DataType}(
    Metadata.SECOND => Dates.Second,
    Metadata.MILLISECOND => Dates.Millisecond,
    Metadata.MICROSECOND => Dates.Microsecond,
    Metadata.NANOSECOND => Dates.Nanosecond
)
const MDATA_TIME_DICT = Dict{DataType,Metadata.TimeUnit}(v=>k for (k,v) in JULIA_TIME_DICT)


isprimitivetype(t::Metadata.DType) = t ∉ NON_PRIMITIVE_TYPES


juliatype(meta::Void, values_type::Metadata.DType) = JULIA_TYPE_DICT[values_type]
function juliatype(meta::Metadata.CategoryMetadata, values_type::Metadata.DType)
    JULIA_TYPE_DICT[meta.levels.dtype]
end
function juliatype(meta::Metadata.TimestampMetadata, values_type::Metadata.DType)
    Timestamp{JULIA_TIME_DICT[meta.unit]}
end
juliatype(meta::Metadata.DateMetadata, values_type::Metadata.DType) = Datestamp

function juliatype(col::Metadata.Column)
    T = juliatype(col.metadata, col.values.dtype)
    col.values.null_count == 0 ? T : Union{T,Missing}
end

feathertype(::Type{T}) where T = MDATA_TYPE_DICT[T]
feathertype(::Type{Union{T,Missing}}) where T = feathertype(T)
feathertype(::Type{Arrow.Datestamp}) = Metadata.INT32
feathertype(::Type{Arrow.Timestamp}) = Metadata.INT64

getmetadata(io::IO, ::Type{T}) where T = nothing
getmetadata(io::IO, ::Type{Union{T,Missing}}) where T = getmetadata(io, T)
getmetadata(io::IO, ::Type{Arrow.Datestamp}) = Metadata.DateMetadata()
getmetadata(io::IO, ::Type{Arrow.Timestamp{T}}) where T = Metadata.TimeMetadata(MDATA_TIME_DICT[T])
# TODO will need category metadata
