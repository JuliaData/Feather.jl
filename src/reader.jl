type Reader
    path::AbstractString
    tpt::Cxx.CppPtr
    buf::IO
    columns::Vector{Column}
end

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
    cpt = icxx"$tpt->columns();"
    cols = [Column(cpt, i, bpt) for i in 1:icxx"$cpt->size();"]
    Reader(path, tpt, io, cols)
end

nrow(rdr::Reader) = icxx"$(rdr.tpt)->num_rows();"
ncol(rdr::Reader) = length(rdr.columns)
names(rdr::Reader) = map(name, rdr.columns)

Base.size(rdr::Reader) = (nrow(rdr), ncol(rdr))
Base.size(rdr::Reader, i::Integer) = i == 1 ? nrow(rdr) : i == 2 ? ncol(rdr) : 1
Base.getindex(rdr::Reader, i::Integer) = rdr.columns[i]
