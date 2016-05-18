type Reader
    path::AbstractString
    tpt::CTablePt
    buf::IO
    columns::Vector{Column}
end

columns(ct::CTablePt) = icxx"$ct->columns();"
description(ct::CTablePt) = pointer_to_string(icxx"$ct->description();")
nrow(ct::CTablePt) = icxx"$ct->num_rows();"
version(ct::CTablePt) = icxx"$ct->version();"

function Reader(path::AbstractString)
    io = IOBuffer(Mmap.mmap(path))
    bpt = pointer(io.data)
    pos = position(skip(seekend(io), -8)) # last 8 bytes are [..., UInt32(metadata size), magic]
    pos -= read(io, Int32)                # start of the metadata
                                          # file should begin and end with "FEA1"
    if io.data[1:4] ≠ magic.data || read(io) ≠ magic.data
        throw(ArgumentError(string("File: ", path, " is not in feather format")))
    end
    tpt = icxx"feather::fbs::GetCTable($(pointer(io.data) + pos));"
    cpt = columns(tpt)
    cols = [Column(cpt, i, bpt) for i in 1:icxx"$cpt->size();"]
    Reader(path, tpt, io, cols)
end

nrow(rdr::Reader) = icxx"$(rdr.tpt)->num_rows();"
ncol(rdr::Reader) = length(rdr.columns)
names(rdr::Reader) = map(name, rdr.columns)

Base.size(rdr::Reader) = (nrow(rdr), ncol(rdr))
Base.size(rdr::Reader, i::Integer) = i == 1 ? nrow(rdr) : i == 2 ? ncol(rdr) : 1
Base.getindex(rdr::Reader, i::Integer) = rdr.columns[i]

function Base.show(io::IO, r::Reader)
    println(io, string('[', nrow(r), " × ", ncol(r), "] @ ", r.path))
    nms = names(r)
    mxnm = maximum(map(length, nms)) + 2
    for coli in r.columns
        println(io, " ", rpad(coli.name, mxnm), ": ", typeof(coli.values))
    end
end

function DataFrames.DataFrame(rdr::Reader)
    DataFrame(convert(Vector{Any}, [c.values for c in rdr.columns]), map(Symbol, names(rdr)))
end
