@enum Column_Type PRIMITIVE CATEGORY TIMESTAMP DATE TIME

immutable Column
    typ::Column_Type
    name::Ptr{UInt8}
    values::Feather_Array

    data::Ptr{Void}
    type_metadata::Ptr{Void}
end

Column() = Column(PRIMITIVE, C_NULL, Feather_Array(), C_NULL, C_NULL)

free(c::Column) = ccall((:feather_column_free, libfeather), Status, (Ref{Column}, ), Ref{c})

function jtype(c::Column)
    if c.typ ≠ PRIMITIVE
        error("Not yet implemented")
    end
    jtype(c.values)
end

function Base.values(c::Column)
    if c.typ ≠ PRIMITIVE
        error("Not yet implemented")
    end
    values(c.values)
end        
