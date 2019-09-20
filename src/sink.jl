"""
    Feather.write(file::String, tbl; description="", metadata="")
    Feather.write(io::IO, tbl; description="", metadata="")

Write any Tables.jl implementation as a feather-formatted file. Optionally, a `description`
and `metadata` can be provided as Strings.
"""
function write end

write(filename::AbstractString; kwargs...) = x->write(filename, x; kwargs...)
function write(io::IO, tbl; description::String="", metadata::String="")
    writepadded(io, FEATHER_MAGIC_BYTES)
    metacols = Metadata.Column[]
    columns = Tables.columns(tbl)
    names = propertynames(columns)
    len = 0
    for nm in names
        column = getarrow(getproperty(columns, nm))
        len = length(column)
        vals = writecontents(Metadata.PrimitiveArray, io, column)
        push!(metacols, Metadata.Column(String(nm), vals,
                                        getmetadata(io, eltype(column), column), ""))
    end
    ctable = Metadata.CTable(description, len, metacols, FEATHER_VERSION, metadata)
    metalen = writemetadata(io, ctable)
    Base.write(io, metalen)
    Base.write(io, FEATHER_MAGIC_BYTES)
    io
end
function write(filename::AbstractString, tbl; description::String="", metadata::String="")
    open(filename, "w+") do io
        write(io, tbl, description=description, metadata=metadata)
    end
    filename
end

getarrow(col::AbstractVector{T}) where {T} = arrowformat(col)
function getarrow(col::AbstractVector{Union{T, Missing}}) where {T}
    hasmissing = any(ismissing, col)
    return arrowformat(hasmissing ? col : convert(AbstractVector{T}, col))
end
function getarrow(col::AbstractVector{Missing})
    throw(ArgumentError("Feather format does not support writing `AbstractVector{Missing}`. "*
                        "Consider converting column to `AbstractVector{Union{T,Missing}}` where "*
                        "`T` is a supported type."))
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

function writemetadata(io::IO, ctable::Metadata.CTable)
    meta = FlatBuffers.build!(ctable)
    rng = (meta.head+1):length(meta.bytes)
    writepadded(io, view(meta.bytes, rng))
    Int32(length(rng))
end

