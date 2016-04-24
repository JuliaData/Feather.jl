type Column
    values::Primitive
    ptr::Cxx.CppPtr
    meta
end

function Column(cpt::Cxx.CppPtr, i::Integer, bpt::Ptr{UInt8})
    1 ≤ i ≤ icxx"$cpt->size();" || throw(BoundsError("Column $i"))
    ptr = icxx"$cpt->Get($i - 1);"
    mtyp = pointer_to_string(icxx"feather::fbs::EnumNameTypeMetadata($ptr->metadata_type());")
    Column(Primitive(icxx"$ptr->values();", bpt), ptr, mtyp)
end

name(col::Column) = pointer_to_string(icxx"$(col.ptr)->name()->c_str();")
nulls(col::Column) = nulls(col.values)
