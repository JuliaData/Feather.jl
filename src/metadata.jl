module Metadata

if !isdefined(Core, :String)
    typealias String UTF8String
end

using FlatBuffers

@enum(Type_, BOOL = 0, INT8 = 1, INT16 = 2, INT32 = 3, INT64 = 4,
  UINT8 = 5, UINT16 = 6, UINT32 = 7, UINT64 = 8,
  FLOAT = 9, DOUBLE = 10,  UTF8 = 11,  BINARY = 12,
  CATEGORY = 13, TIMESTAMP = 14, DATE = 15, TIME = 16)

@enum(Encoding, PLAIN = 0, DICTIONARY = 1)

@enum(TimeUnit, SECOND = 0, MILLISECOND = 1, MICROSECOND = 2, NANOSECOND = 3)

# FlatBuffers.enumsizeof(::Type{TimeUnit}) = UInt8

type PrimitiveArray
    type_::Type_
    encoding::Encoding
    offset::Int64
    length::Int64
    null_count::Int64
    total_bytes::Int64
end

type CategoryMetadata
    levels::PrimitiveArray
    ordered::Bool
end

@default CategoryMetadata ordered=false

type TimestampMetadata
    unit::TimeUnit
    timezone::String
end

type DateMetadata
end

type TimeMetadata
    unit::TimeUnit
end

@union TypeMetadata Union{Void,CategoryMetadata,TimestampMetadata,DateMetadata,TimeMetadata}

type Column
    name::String
    values::PrimitiveArray
    metadata_type::Int8
    metadata::TypeMetadata
    user_metadata::String
end

function Column(name::String, values::PrimitiveArray, metadata::TypeMetadata=nothing, user_metadata::String="")
    return Column(name, values, FlatBuffers.typeorder(TypeMetadata, typeof(metadata)), metadata, user_metadata)
end

type CTable
    description::String
    num_rows::Int64
    columns::Vector{Column}
    version::Int32
    metadata::String
end

end # module

# wesm/feather/cpp/src/metadata_generated.h
# wesm/feather/cpp/src/types.h
const Type_2julia = Dict{Metadata.Type_,DataType}(
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

const julia2Type_ = Dict{DataType,Metadata.Type_}(
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
    DateTime   => Metadata.INT64,
    Date   => Metadata.INT32,
    WeakRefString{UInt8} => Metadata.UTF8
)

const NON_PRIMITIVE_TYPES = Set([Metadata.UTF8, Metadata.BINARY])

const TimeUnit2julia = Dict{Metadata.TimeUnit,DataType}(
    Metadata.SECOND => Arrow.Second,
    Metadata.MILLISECOND => Arrow.Millisecond,
    Metadata.MICROSECOND => Arrow.Microsecond,
    Metadata.NANOSECOND => Arrow.Nanosecond
)
const julia2TimeUnit = Dict{DataType,Metadata.TimeUnit}([(v, k) for (k,v) in TimeUnit2julia])
