
# TODO currently there is no appending or anything like that

mutable struct Sink <: Data.Sink
    filename::String
    schema::Data.Schema
    ctable::Metadata.CTable
    io::IO
    description::String
    metadata::String
    columns::Vector{ArrowVector}
end

function Sink(filename::AbstractString, sch::Data.Schema=Data.Schema(),
              cols::AbstractVector{<:ArrowVector}=Vector{ArrowVector}(undef, size(sch,2));
              description::AbstractString="", metadata::AbstractString="")
    ctable = Metadata.CTable(description, 0, Metadata.Column[], FEATHER_VERSION, metadata)
    io = open(filename, "w+")
    Sink(filename, sch, ctable, io, description, metadata, cols)
end
function Sink(filename::AbstractString, df::DataFrame; description::AbstractString="",
              metadata::AbstractString="")
    Sink(filename, Data.schema(df), description=description, metadata=metadata)
end

# required by DataStreams
function Sink(sch::Data.Schema, ::Type{Data.Column}, append::Bool, file::AbstractString;
              reference::Vector{UInt8}=UInt8[], kwargs...)
    Sink(file, sch)
end
function Sink(sink::Sink, sch::Data.Schema, ::Type{Data.Column}, append::Bool;
              reference::Vector{UInt8}=UInt8[])
    Sink(sink.filename, sch, sink.columns)
end

Data.streamtypes(::Type{Sink}) = [Data.Column]

Base.size(sink::Sink) = size(sink.schema)
Base.size(sink::Sink, i::Integer) = size(sink.schema, i)


"""
    write(filename::AbstractString, df::DataFrame; overwrite::Bool=false)

Write the dataframe `df` to the feather formatted file `filename`.

If the file `filename` already exists, an error will be thrown, unless `overwrite=true` in
which case the file will be deleted before writing.
"""
function write(filename::AbstractString, df::AbstractDataFrame; overwrite::Bool=false)
    if isfile(filename)
        if !overwrite
            throw(ArgumentError("File $filename already exists. Pass `overwrite=true` to overwrite."))
        else
            if Sys.iswindows()
                try
                    rm(filename)
                catch e
                    @error("Unable to delete file, is it a Feather file already being read from?")
                    throw(e)
                end
            else
                rm(filename)
            end
        end
    end
    sink = Feather.Sink(filename, df)
    Data.stream!(df, sink)
    Data.close!(sink)
end


function Data.streamto!(sink::Sink, ::Type{Data.Column}, val::AbstractVector{T}, row, col) where T
    sink.columns[col] = arrowformat(val)
end

# NOTE: the below is very inefficient, but we are forced to do it by the Feather format
function Data.streamto!(sink::Sink, ::Type{Data.Column}, val::AbstractVector{Union{T,Missing}},
                        row, col) where T
    hasmissing = Compat.findfirst(ismissing, val)
    sink.columns[col] = arrowformat(hasmissing == nothing ? convert(AbstractVector{T}, val) : val)
end


function Metadata.PrimitiveArray(A::ArrowVector{J}, off::Integer, nbytes::Integer) where J
    Metadata.PrimitiveArray(feathertype(J), Metadata.PLAIN, off, length(A), nullcount(A), nbytes)
end
function Metadata.PrimitiveArray(A::DictEncoding, off::Integer, nbytes::Integer)
    Metadata.PrimitiveArray(feathertype(eltype(references(A))), Metadata.PLAIN, off, length(A),
                            nullcount(A), nbytes)
end


writecontents(io::IO, A::Primitive) = writepadded(io, A)
writecontents(io::IO, A::NullablePrimitive) = writepadded(io, A, bitmask, values)
writecontents(io::IO, A::List) = writepadded(io, A, offsets, values)
writecontents(io::IO, A::NullableList) = writepadded(io, A, bitmask, offsets, values)
writecontents(io::IO, A::BitPrimitive) = writepadded(io, A, values)
writecontents(io::IO, A::NullableBitPrimitive) = writepadded(io, A, bitmask, values)
writecontents(io::IO, A::DictEncoding) = writecontents(io, references(A))
function writecontents(::Type{Metadata.PrimitiveArray}, io::IO, A::ArrowVector)
    a = position(io)
    writecontents(io, A)
    b = position(io)
    Metadata.PrimitiveArray(A, a, b-a)
end


function writecolumn(io::IO, name::AbstractString, A::ArrowVector{J}) where J
    vals = writecontents(Metadata.PrimitiveArray, io, A)
    Metadata.Column(String(name), vals, getmetadata(io, J, A), "")
end
function writecolumn(sink::Sink, col::String)
    writecolumn(sink.io, col, sink.columns[sink.schema[col]])
end
writecolumns(sink::Sink) = Metadata.Column[writecolumn(sink, n) for n ∈ Data.header(sink.schema)]


function writemetadata(io::IO, ctable::Metadata.CTable)
    meta = FlatBuffers.build!(ctable)
    rng = (meta.head+1):length(meta.bytes)
    writepadded(io, view(meta.bytes, rng))
    Int32(length(rng))
end
writemetadata(sink::Sink) = writemetadata(sink.io, sink.ctable)


function Data.close!(sink::Sink)
    writepadded(sink.io, FEATHER_MAGIC_BYTES)
    cols = writecolumns(sink)
    ctable = Metadata.CTable(sink.description, size(sink,1), cols, FEATHER_VERSION, sink.metadata)
    sink.ctable = ctable
    len = writemetadata(sink)
    Base.write(sink.io, Int32(len))  # these two writes combined are properly aligned
    Base.write(sink.io, FEATHER_MAGIC_BYTES)
    close(sink.io)
    sink
end

