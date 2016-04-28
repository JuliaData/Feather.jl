type Column{T}
    values::AbstractVector{T}
    ptr::Cxx.CppPtr
    meta::ByteString
end

const _dtypes = Dict{ASCIIString, DataType}("BOOL" => Bool, "INT8" => Int8, "INT16" => Int16, "INT32" => Int32,
    "INT64" => Int64, "UINT8" => UInt8, "UINT16" => UInt16, "UINT32" => UInt32, "UINT64" => UInt64,
    "FLOAT" => Float32, "DOUBLE" => Float64, "UTF8" => UTF8String)
function _dtype(cpt::Cxx.CppPtr)
    _dtypes[pointer_to_string(icxx"feather::fbs::EnumNameType($cpt->type());")]
end

function Column(cpt::Cxx.CppPtr, i::Integer, bpt::Ptr{UInt8})
    1 ≤ i ≤ icxx"$cpt->size();" || throw(BoundsError("Column $i"))
    ptr = icxx"$cpt->Get($i - 1);"
    mtyp = pointer_to_string(icxx"feather::fbs::EnumNameTypeMetadata($ptr->metadata_type());")
    vpt = icxx"$ptr->values();"
    T, n, o = _dtype(vpt), icxx"$vpt->length();", icxx"$vpt->offset();"
    nc = icxx"$vpt->null_count();"
    nullbytes = icxx"$vpt->total_bytes();" - n * sizeof(T)
    v = pointer_to_array(reinterpret(Ptr{T}, bpt + o + nullbytes), n)
    if nc ≠ 0
        bitmask = BitArray{1}(n)
        Base.unsafe_copy!(convert(Ptr{UInt8}, pointer(bitmask.chunks)), bpt + o, nullbytes)
        v = DataVector{T}(v, !bitmask)
    end
    Column(v, ptr, mtyp)
end

name(col::Column) = pointer_to_string(icxx"$(col.ptr)->name()->c_str();")
Base.size(col::Column) = size(col.values)
Base.getindex(col::Column, i) = col.values[i]
