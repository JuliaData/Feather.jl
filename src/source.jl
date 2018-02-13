
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
function Source(file::AbstractString)
    data = loadfile(file)
    ctable = getctable(data)
    sch = Data.schema(ctable)
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
    nullcount(p) == 0 ? 0 : padding(bytesforbits(length(p)))
end

function offsetslength(p::Metadata.PrimitiveArray)
    isprimitivetype(p.dtype) ? 0 : padding((length(p)+1)*sizeof(Int32))
end

valueslength(p::Metadata.PrimitiveArray) = p.total_bytes - offsetslength(p) - bitmasklength(p)

valuesloc(p::Metadata.PrimitiveArray) = startloc(p) + bitmasklength(p) + offsetslength(p)

# only makes sense for nullable arrays
bitmaskloc(p::Metadata.PrimitiveArray) = startloc(p)

function offsetsloc(p::Metadata.PrimitiveArray)
    if isprimitivetype(p.dtype)
        throw(ErrorException("Trying to obtain offset values for primitive array."))
    end
    startloc(p) + bitmasklength(p)
end


function Arrow.Primitive(::Type{T}, data::Vector{UInt8}, p::Metadata.PrimitiveArray) where T
    Primitive{T}(data, valuesloc(p), length(p))
end
function Arrow.NullablePrimitive(::Type{T}, data::Vector{UInt8}, p::Metadata.PrimitiveArray) where T
    NullablePrimitive{T}(data, bitmaskloc(p), valuesloc(p), length(p))
end
function Arrow.List(::Type{T}, data::Vector{UInt8}, p::Metadata.PrimitiveArray) where T<:AbstractString
    q = Primitive{UInt8}(data, valuesloc(p), valueslength(p))
    List{T}(data, offsetsloc(p), length(p), q)
end
function Arrow.NullableList(::Type{T}, data::Vector{UInt8}, p::Metadata.PrimitiveArray
                           ) where T<:AbstractString
    q = Primitive{UInt8}(data, valuesloc(p), valueslength(p))
    NullableList{T}(data, bitmaskloc(p), offsetsloc(p), length(p), q)
end

arrowvector(::Type{T}, data::Vector{UInt8}, p::Metadata.PrimitiveArray) where T = Primitive(T, data, p)
function arrowvector(::Type{Union{T,Missing}}, data::Vector{UInt8}, p::Metadata.PrimitiveArray) where T
    NullablePrimitive(T, data, p)
end
function arrowvector(::Type{T}, data::Vector{UInt8}, p::Metadata.PrimitiveArray) where T<:AbstractString
    List(T, data, p)
end
function arrowvector(::Type{Union{T,Missing}}, data::Vector{UInt8}, p::Metadata.PrimitiveArray
                    ) where T<:AbstractString
    NullableList(T, data, p)
end


function Arrow.DictEncoding(::Type{T}, data::Vector{UInt8}, col::Metadata.Column) where T
    lvls = arrowvector(T, data, col.metadata.levels)
    DictEncoding{T}(data, valuesloc(col.values), length(col.values), lvls)
end


function constructcolumn(::Type{T}, data::Vector{UInt8}, meta::K, col::Metadata.Column) where {T,K}
    arrowvector(T, data, col.values)
end
function constructcolumn(::Type{T}, data::Vector{UInt8}, meta::Metadata.CategoryMetadata,
                         col::Metadata.Column) where T
    DictEncoding(T, data, col)
end
function constructcolumn(::Type{T}, data::Vector{UInt8}, col::Metadata.Column) where T
    constructcolumn(T, data, col.metadata, col)
end
function constructcolumn(s::Source, ::Type{T}, col::Integer) where T
    @boundscheck checkcolbounds(s, col)
    constructcolumn(T, s.data, getcolumn(s, col))
end
constructcolumn(s::Source{S}, col::Integer) where S = constructcolumn(s, S.parameters[col], col)
