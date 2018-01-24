
mutable struct Source{S} <: Data.Source
    path::String
    schema::Data.Schema
    ctable::Metadata.CTable
    data::Vector{UInt8}
end


function Source(file::AbstractString, sch::Data.Schema{R,T}, ctable::Metadata.CTable,
                data::Vector{UInt8}) where {R,T}
    Source{T}(file, sch, ctable, data)
end
function Source(file::AbstractString; nullable::Bool=false, weakrefstrings::Bool=true)
    data = loadfile(file)
    ctable = getctable(data)
    sch = Data.schema(ctable, nullable=nullable, weakrefstrings=weakrefstrings)
    Source(file, sch, ctable, data)
end

Data.schema(s::Source) = s.schema
Data.header(s::Source) = Data.header(s.schema)
Data.types(s::Source{S}) where S = Tuple(S.parameters)

getcolumn(s::Source, col::Integer) = s.ctable.columns[col]

size(s::Source) = size(s.schema)
size(s::Source, i::Integer) = size(s.schema, i)

datapointer(s::Source) = pointer(s.data)

checkcolbounds(s::Source, col::Integer) = (1 ≤ col ≤ size(s, 2)) || throw(BoundsError(s, col))


# DataFrame constructor, using Arrow objects
function DataFrame(s::Source)
    DataFrame((Symbol(h)=>constructcolumn(s,i) for (i,h) ∈ enumerate(Data.header(s)))...)
end


#=====================================================================================================
    new column construction stuff
=====================================================================================================#
length(p::Metadata.PrimitiveArray) = p.length

startloc(p::Metadata.PrimitiveArray) = p.offset+1

nullcount(p::Metadata.PrimitiveArray) = p.null_count

function bitmasklength(p::Metadata.PrimitiveArray)
    nullcount(p) == 0 ? 0 : paddedlength(bytesforbits(length(p)))
end

function offsetslength(p::Metadata.PrimitiveArray)
    isprimitivetype(p.dtype) ? 0 : (length(p)+1)*sizeof(Int32)
end

# TODO check if this is correct!!!, padding???
datalength(p::Metadata.PrimitiveArray) = p.total_bytes - offsetslength(p) - bitmasklength(p)

dataloc(p::Metadata.PrimitiveArray) = startloc(p) + bitmasklength(p) + offsetslength(p)

# only makes sense for nullable arrays
bitmaskloc(p::Metadata.PrimitiveArray) = startloc(p)

function offsetsloc(p::Metadata.PrimitiveArray)
    if isprimitivetype(p.dtype)
        throw(ErrorException("Trying to obtain offset values for primitive array."))
    end
    startloc(p) + bitmasklength(p)
end


function Arrow.Primitive(::Type{T}, ptr::Ptr, p::Metadata.PrimitiveArray) where T
    Primitive{T}(ptr, dataloc(p), length(p))
end

function Arrow.NullablePrimitive(::Type{T}, ptr::Ptr, p::Metadata.PrimitiveArray) where T
    NullablePrimitive{T}(ptr, bitmaskloc(p), dataloc(p), length(p), nullcount(p))
end

function Arrow.List(::Type{T}, ptr::Ptr, p::Metadata.PrimitiveArray) where T<:AbstractString
    q = Primitive{UInt8}(ptr, dataloc(p), datalength(p))
    List{typeof(q),T}(ptr, offsetsloc(p), length(p), q)
end

function Arrow.NullableList(::Type{T}, ptr::Ptr, p::Metadata.PrimitiveArray) where T<:AbstractString
    q = Primitive{UInt8}(ptr, dataloc(p), datalength(p))
    NullableList{typeof(q),T}(ptr, bitmaskloc(p), offsetsloc(p), length(p), nullcount(p), q)
end


function constructcolumn(ptr::Ptr{UInt8}, ::Type{T}, col::Metadata.Column) where T
    Primitive(T, ptr, col.values)
end
function constructcolumn(ptr::Ptr{UInt8}, ::Type{Union{T,Missing}}, col::Metadata.Column) where T
    NullablePrimitive(T, ptr, col.values)
end
function constructcolumn(ptr::Ptr{UInt8}, ::Type{T}, col::Metadata.Column) where T<:AbstractString
    List(T, ptr, col.values)
end
function constructcolumn(ptr::Ptr{UInt8}, ::Type{Union{T,Missing}}, col::Metadata.Column
                        ) where T<:AbstractString
    NullableList(T, ptr, col.values)
end
function constructcolumn(s::Source, ::Type{T}, col::Integer) where T
    @boundscheck checkcolbounds(s, col)
    constructcolumn(datapointer(s), T, getcolumn(s, col))
end
constructcolumn(s::Source{S}, col::Integer) where S = constructcolumn(s, S.parameters[col], col)
