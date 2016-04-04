@enum Status OK OOM KEY_ERROR INVALID IO_ERROR NOT_IMPLEMENTED=10 UNKNOWN=50

@enum Feather_Type BOOL INT8 INT16 INT32 INT64 UINT8 UINT16 UINT32 UINT64 FLOAT DOUBLE UTF8 BINARY

@enum Column_Type PRIMITIVE CATEGORY TIMESTAMP DATE TIME

@enum Unit SECOND MILLISECOND NANOSECOND

immutable Feather_Array
    typ::Feather_Type
    length::Int64
    null_count::Int64

    nulls::Ptr{Void}
    values::Ptr{Void}

    offsets::Ptr{Int32}
end

Feather_Array() = Feather_Array(BOOL, 0, 0, C_NULL, C_NULL, C_NULL)

immutable Category
    indices::Feather_Array
    levels::Feather_Array
    ordered::Cint
end

immutable Category_Data
    levels::Feather_Array
    ordered::Cint
end

immutable TimeStamp_Data
    timezone::Ptr{UInt8}
    unit::Unit
end

immutable Time_Data
    unit::Unit
end

immutable Column
    typ::Column_Type
    name::Ptr{UInt8}
    values::Feather_Array

    data::Ptr{Void}
    type_metadata::Ptr{Void}
end

Column() = Column(PRIMITIVE, C_NULL, Feather_Array(), C_NULL, C_NULL)
