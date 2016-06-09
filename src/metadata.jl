module Metadata

include(joinpath(Pkg.dir("FlatBuffers"), "src/header.jl"))

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
