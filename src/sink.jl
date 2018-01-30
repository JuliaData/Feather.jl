
mutable struct Sink <: Data.Sink
    path::String
    schema::Data.Schema
    ctable::Metadata.CTable
    io::IOBuffer
end

function Sink(filename::AbstractString, sch::Data.Schema=Data.Schema())
    ctable = Metadata.CTable("", 0, Metadata.Column[], FEATHER_VERSION, "")
    Sink(filename, sch, ctable, IOBuffer())
end
