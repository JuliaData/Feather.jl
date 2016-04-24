type Primitive{T} <: DenseVector{T}
    values::Vector{T}
    bitmask::BitArray{1}
end

const _dtypes = Dict{ASCIIString, DataType}("BOOL" => Bool, "INT8" => Int8, "INT16" => Int16, "INT32" => Int32,
    "INT64" => Int64, "UINT8" => UInt8, "UINT16" => UInt16, "UINT32" => UInt32, "UINT64" => UInt64,
    "FLOAT" => Float32, "DOUBLE" => Float64, "UTF8" => UTF8String)
function _dtype(cpt::Cxx.CppPtr)
    _dtypes[pointer_to_string(icxx"feather::fbs::EnumNameType($cpt->type());")]
end

function Primitive(ppt::Cxx.CppPtr, bpt::Ptr{UInt8})
    T, n, o = _dtype(ppt), icxx"$ppt->length();", icxx"$ppt->offset();"
    bitmask = BitArray{1}()
        # check if there are missing values
    if (nc = icxx"$ppt->null_count();") > 0
        # missing values are marked with bitmask which is stored first in chuncks of 32 bits
        bitmask = BitArray{1}(n)
        nullbytes = 1
        while nullbytes << 3 < n
            nullbytes += 1
        end
        Base.unsafe_copy!(convert(Ptr{UInt8}, pointer(bitmask.chunks)), bpt + o, nullbytes)
        o += nullbytes
    end
    Primitive{T}(pointer_to_array(reinterpret(Ptr{T}, bpt + o), n), bitmask)
end

Base.size(pr::Primitive) = size(pr.values)
Base.getindex(pr::Primitive, i) = pr.values[i]
Base.eltype{T}(pr::Primitive{T}) = T

nulls(pr::Primitive) = find(!pr.bitmask)
