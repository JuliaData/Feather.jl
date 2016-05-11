type Column{T}
    values::AbstractVector{T}
    name::String
end

const _dtypes = Dict{String, DataType}("BOOL" => Bool, "INT8" => Int8, "INT16" => Int16, "INT32" => Int32,
    "INT64" => Int64, "UINT8" => UInt8, "UINT16" => UInt16, "UINT32" => UInt32, "UINT64" => UInt64,
    "FLOAT" => Float32, "DOUBLE" => Float64, "UTF8" => String)

dtype(ppt::PrimitivePt) = _dtypes[pointer_to_string(icxx"feather::fbs::EnumNameType($ppt->type());")]
Base.length(ppt::PrimitivePt) = icxx"$ppt->length();"
offset(ppt::PrimitivePt) = icxx"$ppt->offset();"
total_bytes(ppt::PrimitivePt) = icxx"$ppt->total_bytes();"

mtype(cpt::ColumnPt) = pointer_to_string(icxx"feather::fbs::EnumNameTypeMetadata($cpt->metadata_type());")
name(cpt::ColumnPt) = pointer_to_string(icxx"$cpt->name()->c_str();")
valpt(cpt::ColumnPt) = icxx"$cpt->values();"

function values(ppt::PrimitivePt, bpt::Ptr{UInt8})
    n, o, tb = icxx"$ppt->length();", icxx"$ppt->offset();", icxx"$ppt->total_bytes();"
    if (T = dtype(ppt)) <: Number
        v = pointer_to_array(reinterpret(Ptr{T}, bpt + o + tb - n * sizeof(T)), n)
        if 0 ≠ icxx"$ppt->null_count();"
            bitmask = BitArray{1}(n)
            Base.unsafe_copy!(convert(Ptr{UInt8}, pointer(bitmask.chunks)), bpt + o, nullbytes)
            v = DataVector{T}(v, bitmask)
        end
        return v
    end
    assert(T == String)
    offsets = pointer_to_array(reinterpret(Ptr{Int32}, bpt + o), n + 1)
    assert(offsets[1] == 0 && issorted(offsets))
    len = diff(offsets)
    spt = bpt + o + (n + 1) * sizeof(Int32)
    v = [pointer_to_string(spt + offsets[i], len[i]) for i in 1:n]
end

function Column(cpt::CppPtr, i::Integer, bpt::Ptr{UInt8})
    1 ≤ i ≤ icxx"$cpt->size();" || throw(BoundsError("Column $i"))
    cpt = icxx"$cpt->Get($(i - 1));"
    v, nm = values(valpt(cpt), bpt), name(cpt)
    mtyp = mtype(cpt)
    if mtyp == "NONE"
        return Column(v, nm)
    elseif mtyp == "CategoryMetadata"
        mdpt = icxx"static_cast<const feather::fbs::CategoryMetadata *>($cpt->metadata());"
        pool = values(icxx"$mdpt->levels();", bpt)
        return Column(PooledDataArray(DataArrays.RefArray(v .+ 1), pool), nm)
    else
        error("Unknown metadata type $mtyp")
    end
end

name(col::Column) = col.name
Base.size(col::Column) = size(col.values)
Base.getindex(col::Column, i) = col.values[i]
