
"DataStreams Sink implementation for feather-formatted binary files"
mutable struct Sink{T} <: Data.Sink
    ctable::Metadata.CTable
    file::String
    io::IOBuffer
    description::String
    metadata::String
    df::T
end

function Sink(file::AbstractString, schema::Data.Schema=Data.Schema(), ::Type{T}=Data.Column,
              existing=missing;
              description::AbstractString="", metadata::AbstractString="",
              append::Bool=false, reference::Vector{UInt8}=UInt8[]) where {T<:Data.StreamType}
    if !ismissing(existing)
        df = DataFrame(schema, T, append, existing; reference=reference)
    else
        if append
            df = Feather.read(file)
        else
            # again, this is actually a DataFrameStream
            df = DataFrame(schema, T, append; reference=reference)
        end
    end
    if append
        schema.rows += length(df.columns) > 0 ? length(df.columns[1]) : 0
    end
    io = IOBuffer()
    writepadded(io, FEATHER_MAGIC_BYTES)
    Sink(Metadata.CTable("", 0, Metadata.Column[], FEATHER_VERSION, ""), file, io,
         description, metadata, df)
end

# DataStreams interface
function Sink(sch::Data.Schema, ::Type{T}, append::Bool, file::AbstractString;
              reference::Vector{UInt8}=UInt8[], kwargs...) where T
    Sink(file, sch, T; append=append, reference=reference, kwargs...)
end

function Sink(sink, sch::Data.Schema, ::Type{T}, append::Bool; reference::Vector{UInt8}=UInt8[]) where T
    Sink(sink.file, sch, T, sink.df; append=append, reference=reference)
end

Data.streamtypes(::Type{Sink}) = [Data.Column, Data.Field]
Data.weakrefstrings(::Type{Sink}) = true

function Data.streamto!(sink::Feather.Sink, ::Type{Data.Field}, val::T, row, col,
                        kr::Type{Val{S}}) where {T, S}
    Data.streamto!(sink.df, Data.Field, val, row, col, kr)
end
function Data.streamto!(sink::Feather.Sink, ::Type{Data.Column}, column::T, row, col,
                        kr::Type{Val{S}}) where {T, S}
    Data.streamto!(sink.df, Data.Column, column, row, col, kr)
end


# TODO must clean up to make this more easily comprehensible
function Data.close!(sink::Feather.Sink)
    header = sink.df.header
    data = sink.df
    io = sink.io
    # write out arrays, building each array's metadata as we go
    rows = length(header) > 0 ? length(data.columns[1]) : 0
    columns = Metadata.Column[]
    for (i, name) in enumerate(header)
        arr = data.columns[i]
        total_bytes = 0
        offset = position(io)
        null_count = nullcount(arr)
        len = length(arr)
        total_bytes = writenulls(io, arr, null_count, len, total_bytes)
        # write out array values
        TT = Missings.T(eltype(arr))
        total_bytes += writecolumn(io, TT, arr)
        values = Metadata.PrimitiveArray(feathertype(TT), Metadata.PLAIN, offset, len, null_count,
                                         total_bytes)
        push!(columns, Metadata.Column(String(name), values, getmetadata(io, TT, arr), String("")))
    end
    # write out metadata
    ctable = Metadata.CTable(sink.description, rows, columns, FEATHER_VERSION, sink.metadata)
    meta = FlatBuffers.build!(ctable)
    rng = (meta.head + 1):length(meta.bytes)
    writepadded(io, view(meta.bytes, rng))
    # write out metadata size
    Base.write(io, Int32(length(rng)))
    # write out final magic bytes
    Base.write(io, FEATHER_MAGIC_BYTES)
    len = position(io)
    open(sink.file, "w") do f
        Base.write(f, view(io.data, 1:len))
    end
    sink.io = IOBuffer()
    sink.ctable = ctable
    sink
end

