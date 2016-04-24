type Primitive{T} <: DenseVector{T}
    values::Vector{T}
    nulls::Vector{Int64}
end

const _dtypes = Dict{ASCIIString, DataType}("BOOL" => Bool, "INT8" => Int8, "INT16" => Int16, "INT32" => Int32,
    "INT64" => Int64, "UINT8" => UInt8, "UINT16" => UInt16, "UINT32" => UInt32, "UINT64" => UInt64,
    "FLOAT" => Float32, "DOUBLE" => Float64, "UTF8" => UTF8String)
function _dtype(cpt::Cxx.CppPtr)
    _dtypes[pointer_to_string(icxx"feather::fbs::EnumNameType($cpt->type());")]
end

function Primitive(ppt::Cxx.CppPtr, bpt::Ptr{UInt8})
    T = _dtype(ppt)
    n = icxx"$ppt->length();"
    o = icxx"$ppt->offset();"
    v = pointer_to_array(reinterpret(Ptr{T}, bpt + o), n)
    o += n * sizeof(T)
    nc = icxx"$ppt->null_count();"
    Primitive{T}(v, nc == 0 ? Int32[] : pointer_to_array(reinterpret(Ptr{Int64}, bpt + o), nc))
end

Base.size(pr::Primitive) = size(pr.values)
Base.getindex(pr::Primitive, i) = pr.values[i]
Base.eltype{T}(pr::Primitive{T}) = T

nulls(pr::Primitive) = pr.nulls
