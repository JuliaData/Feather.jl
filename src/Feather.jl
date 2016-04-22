module Feather

using Cxx
import DataFrames: names, ncol, nrow

addHeaderDir(Pkg.dir("Feather", "deps", "usr", "include"))
cxxinclude("feather/metadata_generated.h")

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

end
