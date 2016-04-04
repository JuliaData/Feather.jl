immutable Reader
    ptr::Ptr{Void}
    rows::Int64
    columns::Int64
end

function Reader(path::AbstractString)
    handle = Ptr{Void}[C_NULL]
    status = ccall((:feather_reader_open_file, libfeather), Status,
        (Ptr{UInt8}, Ptr{Ptr{Void}}), path, handle)
    if status ≠ OK
        error(string("feather_reader_open_file, path: ", path, ", ", status))
    end
    ptr = handle[1]
    rr = ccall((:feather_reader_num_rows, libfeather), Int64, (Ptr{Void}, ), ptr)
    cc = ccall((:feather_reader_num_columns, libfeather), Int64, (Ptr{Void}, ), ptr)
    Reader(ptr, rr, cc)
end

Base.size(r::Reader) = (r.rows, r.columns)

function Base.getindex(r::Reader, i::Integer)
    if !(0 < i ≤ r.columns)
        throw(BoundsError(r, i))
    end
    cols = [Column()]
    status = ccall((:feather_reader_get_column, libfeather), Status,
        (Ptr{Void}, Cint, Ptr{Column}), r.ptr, i - 1, cols)
    if status ≠ OK
        error(string("feather_reader_get_column:", status))
    end
    cols[1]
end
