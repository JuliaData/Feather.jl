type Reader
    path::AbstractString
    nrow::Int64
    ncol::Int64
    columns::Vector{CxxWrap.UniquePtr{Column}}
    names::Vector{UTF8String}
    ptr
end

function Reader(path::AbstractString)
    uptr = openFeatherTable(path)
    ptr = get(uptr)
    cols = [getcolumn(ptr, i - 1) for i in 1:num_columns(ptr)]
    Reader(path, num_rows(ptr), length(cols), cols, map(c -> utf8(name(get(c))), cols), uptr)
end

Base.size(r::Reader) = (r.nrow, r.ncol)

Base.size(r::Reader, i::Integer) = i == 1 ? r.nrow : (i == 2 ? r.ncol : 1)

const jtypes = [Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
     Float32, Float64, UTF8String, Ptr{Void}]

jtype(upt::CxxWrap.UniquePtr{Column}) = jtypes[datatype(get(upt)) + 1]

@enum Column_Type PRIMITIVE CATEGORY TIMESTAMP DATE TIME

coltype(upt::CxxWrap.UniquePtr{Column}) = Column_Type(columntype(get(upt)))

function Base.show(io::IO, r::Reader)
    println(io, string('[', r.nrow, " × ", r.ncol, "] @ ", r.path))
    mxnm = maximum(map(length, r.names)) + 2
    for i in eachindex(r.names)
        coli = r.columns[i]
        println(io, " ", rpad(r.names[i], mxnm), ": ", coltype(coli), "(", jtype(coli), ")")
    end
end

if false

function Base.Dict(r::Reader)
    nms, cols, res = r.names, r.columns, Dict{Symbol, Any}()
    for i in eachindex(nms)
        res[symbol(nms[i])] = values(cols[i])
    end
    res
end

DataFrames.DataFrame(r::Reader) = DataFrame(Dict(r))

end
