const magic = "FEA1"

type Reader
    path::AbstractString
    io::IO
    metadata::CTable
end

function Reader(path::AbstractString)
    io = IOBuffer(Mmap.mmap(path))
    pos = position(skip(seekend(io), - 8)) # last 8 bytes are [..., UInt32(metadata size), magic]
    pos -= read(io, Int32)                 # start of the metadata
                                           # file should begin and end with "FEA1"
    if io.data[1:4] ≠ magic.data || readbytes(io) ≠ magic.data
        throw(ArgumentError(string("File: ", path, " is not in feather format")))
    end
    pos += read(seek(io, pos), Int32)      # flatbuffer offset to root table at beginning of buffer
    Reader(path, io, CTable(io, pos))
end

function Base.show(io::IO, r::Reader)
    println(io, string('[', r.metadata[:numRows], " × ", length(r.metadata[:columns]), "] @ ", r.path))
#    mxnm = maximum(map(length, r.names)) + 2
#    for i in eachindex(r.names)
#        coli = r.columns[i]
#        println(io, " ", rpad(r.names[i], mxnm), ": ", coltype(coli), "(", jtype(coli), ")")
#    end
end
