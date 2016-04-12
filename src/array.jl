@enum Feather_Type BOOL INT8 INT16 INT32 INT64 UINT8 UINT16 UINT32 UINT64 FLOAT DOUBLE UTF8 BINARY

immutable Feather_Array
    typ::Feather_Type
    length::Int64
    null_count::Int64

    nulls::Ptr{Void}
    values::Ptr{Void}

    offsets::Ptr{Int32}
end


Feather_Array() = Feather_Array(BOOL, 0, 0, C_NULL, C_NULL, C_NULL)

jtype(a::Feather_Array) = jtypes[convert(Integer, a.typ) + 1]

Base.values(a::Feather_Array) = pointer_to_array(convert(Ptr{jtype(a)}, a.values), a.length)
