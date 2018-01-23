
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
    constructing columns

    TODO: all this stuff assumes no offsets (i.e. primitive bits types)
=====================================================================================================#
nrows(col::Metadata.Column) = col.values.length

nullcount(col::Metadata.Column) = col.values.null_count

coloffset(col::Metadata.Column) = col.values.offset+1

# right now all bitmasks are the same length
function bitmaskbytes(col::Metadata.Column)
    nullcount(col) == 0 ? 0 : paddedlength(bytesforbits(col.values.length))
end

# length of the offsets buffer
function offsetslength(col::Metadata.Column)
    isprimitivetype(col.values.dtype) ? 0 : (nrows(col)+1)*sizeof(Int32)
end

coldatalocation(col::Metadata.Column) = coloffset(col) + bitmaskbytes(col) + offsetslength(col)

# only makes sense if has nulls
colbitmasklocation(col::Metadata.Column) = coloffset(col)

function coloffsetslocation(col::Metadata.Column)
    if isprimitivetype(col.values.dtype)
        throw(ErrorException("Trying to obtain offset values for primitive array."))
    end
    coloffset(col) + bitmaskbytes(col)
end

# doesn't include offsets
collocations(col::Metadata.Column) = (colbitmasklocation(col), coldatalocation(col))


function constructcolumn(ptr::Ptr{UInt8}, ::Type{T}, col::Metadata.Column) where T
    Primitive{T}(ptr, coldatalocation(col), nrows(col))
end
function constructcolumn(ptr::Ptr{UInt8}, ::Type{Union{T,Missing}}, col::Metadata.Column) where T
    off, dat = collocations(col)
    NullablePrimitive{T}(ptr, off, dat, nrows(col), nullcount(col))
end

function constructcolumn(ptr::Ptr{UInt8}, ::Type{T}, col::Metadata.Column) where T<:AbstractString
    data_loc = coldatalocation(col)
    offset_loc = coloffsetslocation(col)
    p = Primitive{UInt8}(ptr, data_loc, col.values.total_bytes)
    List{typeof(p),T}(ptr, offset_loc, nrows(col), p)
end
function constructcolumn(ptr::Ptr{UInt8}, ::Type{Union{T,Missing}}, col::Metadata.Column
                        ) where T<:AbstractString
    bmask_loc, data_loc = collocations(col)
    offset_loc = coloffsetslocation(col)
    p = Primitive{UInt8}(ptr, data_loc, col.values.total_bytes)
    NullableList{typeof(p),T}(ptr, bmask_loc, offset_loc, nrows(col), nullcount(col), p)
end

function constructcolumn(s::Source, ::Type{T}, col::Integer) where T
    @boundscheck checkcolbounds(s, col)
    constructcolumn(datapointer(s), T, getcolumn(s, col))
end
constructcolumn(s::Source{S}, col::Integer) where S = constructcolumn(s, S.parameters[col], col)



