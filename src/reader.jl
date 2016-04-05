type Reader
    path::AbstractString
    ptr::Ptr{Void}
    nrow::Int64
    ncol::Int64
    columns::Vector{Column}
    names::Vector{UTF8String}
end

function free(r::Reader)
    for c in r.columns
        free(c)
    end
    ccall((:feather_reader_free, libfeather), Status, (Ptr{Void}, ), r.ptr)
end

function Reader(path::AbstractString)
    handle = [C_NULL]
    status = ccall((:feather_reader_open_file, libfeather), Status,
        (Ptr{UInt8}, Ptr{Ptr{Void}}), path, handle)
    if status ≠ OK
        error(string("feather_reader_open_file, path: ", path, ", ", status))
    end
    ptr = handle[1]
    rr = ccall((:feather_reader_num_rows, libfeather), Int64, (Ptr{Void}, ), ptr)
    cc = ccall((:feather_reader_num_columns, libfeather), Int64, (Ptr{Void}, ), ptr)
    cols = Column[]
    for i in 0:cc - 1
        columni = [Column()]
        status = ccall((:feather_reader_get_column, libfeather), Status,
            (Ptr{Void}, Cint, Ptr{Column}), ptr, i, columni)
        if status ≠ OK
            error(string("feather_reader_get_column, i: ", i, ", ", status))
        end
        append!(cols, columni)
    end
    res = Reader(path, ptr, rr, cc, cols, [utf8(bytestring(c.name)) for c in cols])
#    finalizer(res, free)
    res
end

Base.size(r::Reader) = (r.nrow, r.ncol)

Base.size(r::Reader, i::Integer) = i == 1 ? r.nrow : (i == 2 ? r.ncol : 1)

function Base.show(io::IO, r::Reader)
    println(io, string('[', r.nrow, " × ", r.ncol, "] @ ", r.path))
    mxnm = maximum(map(length, r.names)) + 2
    for i in eachindex(r.names)
        println(io, string(" ", rpad(r.names[i], mxnm), ": ", jtype(r.columns[i])))
    end
end

function Base.Dict(r::Reader)
    nms, cols, res = r.names, r.columns, Dict{Symbol, Any}()
    for i in eachindex(nms)
        res[symbol(nms[i])] = values(cols[i])
    end
    res
end

DataFrames.DataFrame(r::Reader) = DataFrame(Dict(r))
