module Feather

using Cxx

addHeaderDir("/usr/local/include/")
Libdl.dlopen(Libdl.find_library(["libfeather"]))
cxxinclude("feather/api.h")

checkStatus(x::Cxx.CppValue) = icxx"$x.ok();"

function getType(x::Cxx.CppEnum)
    if x == icxx"feather::PrimitiveType::BOOL;"
        return Bool
    elseif x == icxx"feather::PrimitiveType::INT8;"
        return Int8
    elseif x == icxx"feather::PrimitiveType::INT16;"
        return Int16
    elseif x == icxx"feather::PrimitiveType::INT32;"
        return Int32
    elseif x == icxx"feather::PrimitiveType::INT64;"
        return Int64
    elseif x == icxx"feather::PrimitiveType::UINT8;"
        return UInt8
    elseif x == icxx"feather::PrimitiveType::UINT16;"
        return UInt16
    elseif x == icxx"feather::PrimitiveType::UINT32;"
        return UInt32
    elseif x == icxx"feather::PrimitiveType::UINT64;"
        return UInt64
    elseif x == icxx"feather::PrimitiveType::FLOAT;"
        return Float32
    elseif x == icxx"feather::PrimitiveType::DOUBLE;"
        return Float64
    elseif x == icxx"feather::PrimitiveType::UTF8;"
        return UFT8String
    else
        error("don't know how to handle binary data yet")
    end
end

type TableReader
    ptr::Cxx.CppValue
end

function Base.show(io::IO, tr::TableReader)
    n = ncols(tr)
    println(io, Base.summary(tr))
    nms = [name(tr[j]) for j = 1:n]
    tps = [string(eltype(tr[j])) for j = 1:n]
    maxlt = max(mapreduce(length, max, nms), mapreduce(length, max, tps))
    for j in 1:n
        print(io, lpad(nms[j], maxlt + 1, ' '))
    end
    println(io)
    for j in 1:n
        col = tr[j]
        print(io, lpad(string(eltype(col)), maxlt + 1, ' '))
    end
    println(io)
end

ncols(tr::TableReader) = icxx"$(tr.ptr)->num_columns();"

function Base.getindex(tr::TableReader, i::Integer)
    n = ncols(tr)
    if !(1 <= i <= n)
        error("columns number out of range")
    end

    colPtr = icxx"std::unique_ptr<feather::Column>();"
    status = icxx"$(tr.ptr)->GetColumn($i - 1, &$colPtr);"

    if checkStatus(status)
        T = getType(icxx"$colPtr->values().type;")
        return Column{T}(colPtr)
    else
        error("something is not ok!")
    end
end

function readFeather(filename::AbstractString)
    table = icxx"std::unique_ptr<feather::TableReader>(new feather::TableReader);"
    status = icxx"feather::TableReader::OpenFile($filename, &$table);"
    if checkStatus(status)
        return TableReader(table)
    else
        error("something is not ok!")
    end
end

type Column{T}
    ptr::Cxx.CppValue
end

Base.eltype{T}(col::Column{T}) = T

name(col::Column) = pointer_to_string(icxx"$(col.ptr)->name().data();")

function Base.show(io::IO, col::Column)
    println(io, Base.summary(col))
    println(io, name(col))
    show(io, Vector(col))
end

Base.length(col::Column) = icxx"$(col.ptr)->values().length;"
Base.pointer{T}(col::Column{T}) = convert(Ptr{T}, icxx"$(col.ptr)->values().values;")

function Base.convert{T}(::Type{Vector}, col::Column{T})
    m = length(col)
    valPtr = pointer(col)
    return pointer_to_array(valPtr, m)
end

end
