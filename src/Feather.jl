module Feather

immutable metadata
    rows::Int64
    columns::Int64
    hasdescription::Bool
    pathpt::Ptr{UInt8}
    path::AbstractString
end

Libdl.dlopen("libfeather.so", Libdl.RTLD_GLOBAL)

const featherso = Pkg.dir("Feather","deps","src","metadata.so")

path = Pkg.dir("Feather", "test", "data", "mtcars.feather")

mdat = metadata(0, 0, false, pointer(path), path)

ccall((:feather_openFeatherTable, featherso), Int16, (Ref{metadata}, ), Ref(mdat))
# package code goes here

end # module
