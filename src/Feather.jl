module Feather

using Cxx
import DataFrames: names, ncol, nrow

addHeaderDir(Pkg.dir("Feather", "deps", "usr", "include"))
cxxinclude("feather/metadata_generated.h")

export TableReader,
    names,
    ncol,
    nrow


type TableReader
    ptr::Cxx.CppPtr
    path::AbstractString
    buf::IO
end

function TableReader(path::AbstractString)
    buf = IOBuffer(Mmap.mmap(path))
    metasz = read(skip(seekend(buf), -8), Int32)
    mpt = pointer(buf.data) + position(skip(buf, -(metasz + sizeof(Int32))))
    ptr = icxx"feather::fbs::GetCTable($mpt);"
    TableReader(ptr, path, buf)
end

nrow(tbl::TableReader) = icxx"$(tbl.ptr)->num_rows();"
ncol(tbl::TableReader) = Int(icxx"$(tbl.ptr)->columns()->size();")
name(tbl::TableReader, i::Integer) = pointer_to_string(icxx"$(tbl.ptr)->columns()->Get($i - 1)->name()->c_str();")
names(tbl::TableReader) = [name(tbl, i) for i in 1:ncol(tbl)]

end
