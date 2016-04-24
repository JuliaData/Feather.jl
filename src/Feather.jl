module Feather

using Cxx
import DataFrames: names, ncol, nrow

addHeaderDir(joinpath(dirname(@__FILE__), "..", "deps", "include"))
cxxinclude(  joinpath(dirname(@__FILE__), "..", "deps", "include", "feather", "metadata_generated.h"))

export
    names,
    ncol,
    nrow

const magic = "FEA1"

type Reader
    ptr::Cxx.CppPtr
    path::AbstractString
    buf::IO
end

function Reader(path::AbstractString)
    io = IOBuffer(Mmap.mmap(path))
    pos = position(skip(seekend(io), -8)) # last 8 bytes are [..., UInt32(metadata size), magic]
    pos -= read(io, Int32)                # start of the metadata
                                          # file should begin and end with "FEA1"
    if io.data[1:4] ≠ magic.data || read(io) ≠ magic.data
        throw(ArgumentError(string("File: ", path, " is not in feather format")))
    end
    ptr = icxx"feather::fbs::GetCTable($(pointer(io.data) + pos));"
    Reader(ptr, path, io)
end

nrow(tbl::Reader) = icxx"$(tbl.ptr)->num_rows();"
ncol(tbl::Reader) = Int(icxx"$(tbl.ptr)->columns()->size();")
name(tbl::Reader, i::Integer) = pointer_to_string(icxx"$(tbl.ptr)->columns()->Get($i - 1)->name()->c_str();")
names(tbl::Reader) = [name(tbl, i) for i in 1:ncol(tbl)]

type Column{T} <: DenseVector{T}
    r::Reader
    colnr::Int
    ptr::Ptr{T}   # keep a pointer to the data to vaoid reloading the pointer
    bitmask::Nullable{BitArray{1}}
end

_offset(tbl::Reader, i::Integer) = icxx"$(tbl.ptr)->columns()->Get($i - 1)->values()->offset();"
_null_count(tbl::Reader, i::Integer) = icxx"$(tbl.ptr)->columns()->Get($i - 1)->values()->null_count();"
function _type(tbl::Reader, i::Integer)
    t = icxx"$(tbl.ptr)->columns()->Get($i - 1)->values()->type();"
    if t == icxx"feather::fbs::Type_BOOL;"
        return Bool
    elseif t == icxx"feather::fbs::Type_INT8;"
        return Int8
    elseif t == icxx"feather::fbs::Type_INT16;"
        return Int16
    elseif t == icxx"feather::fbs::Type_INT32;"
        return Int32
    elseif t == icxx"feather::fbs::Type_INT64;"
        return Int64
    elseif t == icxx"feather::fbs::Type_UINT8;"
        return UInt8
    elseif t == icxx"feather::fbs::Type_UINT16;"
        return UInt16
    elseif t == icxx"feather::fbs::Type_UINT32;"
        return UInt32
    elseif t == icxx"feather::fbs::Type_UINT64;"
        return UInt64
    elseif t == icxx"feather::fbs::Type_FLOAT;"
        return Float32
    elseif t == icxx"feather::fbs::Type_DOUBLE;"
        return Float64
    elseif t == icxx"feather::fbs::Type_UTF8;"
        return UTF8String
    else
        error("type not handled yet!")
    end
end

function Column(r::Reader, i::Integer)
    if !(1 <= i <= ncol(r))
        error("illegal column index")
    end

    # fetch element type for column
    T = _type(r, i)

    # get pointer to beginning of data
    ptr = pointer(r.buf.data) + _offset(r, i)

    # check if there are missing values
    if _null_count(r, i) > 0
        # missing values are marked with bitmask which is stored first in steps of 32 bit
        n = nrow(r)
        ba = BitArray{1}(n)
        nullbytes = 1
        while nullbytes << 3 < n
            nullbytes += 1
        end
        Base.unsafe_copy!(convert(Ptr{UInt8}, pointer(ba.chunks)), ptr, nullbytes)
        bitmask = Nullable(ba)
    else
        bitmask = Nullable{BitArray{1}}()
    end

    return Column{T}(r, i, convert(Ptr{T}, ptr + nullbytes), bitmask)
end

Base.size(c::Column) = (nrow(c.r),)

function Base.getindex(c::Column, i::Integer)
    x = unsafe_load(c.ptr, i)
    if isnull(c.bitmask)
        return x
    else
        if get(c.bitmask)[i]
            return x
        else
            throw(UndefRefError())
        end
    end
end

Base.summary(c::Column) =
    string(Base.dims2string(size(c)), " ", typeof(c), "\n$(name(c.r, c.colnr))")

end
