
# TODO currently there is no appending or anything like that

mutable struct Sink <: Data.Sink
    path::String
    schema::Data.Schema
    ctable::Metadata.CTable
    io::IOBuffer
    description::String
    metadata::String
    columns::Vector{ArrowVector}
end

# TODO change default IO
function Sink(filename::AbstractString, sch::Data.Schema=Data.Schema();
              description::AbstractString="", metdata::AbstractString="")
    ctable = Metadata.CTable("", 0, Metadata.Column[], FEATHER_VERSION, "")
    io = open(filename, "w+")
    Sink(filename, sch, ctable, io, description, metadata, Vector{ArrowVector}(size(sch, 2)))
end
function Sink(filename::AbstractString, df::DataFrame; description::AbstractString="",
              metadata::AbstractString="")
    Sink(filename, Data.schema(df), description=description, metadata=metadata)
end

size(sink::Sink) = size(sink.schema)
size(sink::Sink, i::Integer) = size(sink.schema, i)


function Data.streamto!(sink::Sink, ::Type{Data.Column}, val::AbstractString{T}, row, col) where T
    cols[col] = arrowformat(val)
end


# TODO tentative
function Metadata.PrimitiveArray(A::Primitive{J}, off::Integer, nbytes::Integer) where J
    Metadata.PrimitiveArray(feathertype(J), Metadata.PLAIN, off, length(A), 0, nbytes)
end
function Metadata.PrimitiveArray(A::NullablePrimitive{J}, off::Integer, nbytes::Integer) where J
    Metadata.PrimitiveArray(feathertype(J), Metadata.PLAIN, off, length(A), nullcount(A), nbytes)
end


function writepadded(io::IO, x)
    bw = write(io, x)
    diff = padding(bw) - bw
    write(io, zeros(UInt8, diff))
    bw + diff
end


function writecolumn(io::IO, name::AbstractString, A::Primitive{J}) where J
    a = position(io)
    write(io, A, padding=padding)
    b = position(io)
    values = Metadata.PrimitiveArray(A, a, b-a)
    Metadata.Column(String(name), values, getmetadata(io, J), "")
end
function writecolumn(io::IO, name::AbstractString, A::NullablePrimitive{J}) where J
    a = position(io)
    write(io, bitmask(A), padding=padding)
    write(io, values(A), padding=padding)
    b = position(io)
    values = Metadata.PrimitiveArray(A, a, b-a)
    Metadata.Column(String(name), values, getmetadata(io, J), "")
end

function writecolumn(sink::Sink, col::String)
    writecolumn(sink.io, col, sink.columns[sink.schema[col]])
end
writecolumns(sink::Sink) = Metadata.Column[writecolumn(sink, n) for n ∈ sink.schema]


# also write size
function writemetadata(io::IO, ctable::Metadata.CTable)
    meta = FlatBuffers.build!(ctable)
    rng = (meta.head+1):length(meta.bytes)
    writepadded(io, view(meta.bytes, rng)) + write(io, Int32(length(rng)))
end
writemetadata(sink::Sink) = writemetadata(sink.io, sink.ctable)


function Data.close!(sink::Sink)
    write(io, FEATHER_MAGIC_BYTES)  # TODO possibly revert to using padding
    cols = writecolumns(sink)
    ctable = Metadata.CTable(sink.description, size(sink,1), cols, FEATHER_VERSION, sink.metadata)
    sink.ctable = ctable
    writemetadata(sink)
    write(io, FEATHER_MAGIC_BYTES)
    close(sink.io)
    sink
end

