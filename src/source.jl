
mutable struct Source{S} <: Data.Source
    path::String
    schema::Data.Schema
    ctable::Metadata.CTable
    data::Vector{UInt8}
    columns::Vector{ArrowVector}
end


function Source(file::AbstractString, sch::Data.Schema{R,T}, ctable::Metadata.CTable,
                data::Vector{UInt8}) where {R,T}
    s = Source{T}(file, sch, ctable, data, Vector{ArrowVector}(undef, 0))
    s.columns = constructall(s)
    s
end
function Source(file::AbstractString; use_mmap::Bool=SHOULD_USE_MMAP)
    data = loadfile(file, use_mmap=use_mmap)
    ctable = getctable(data)
    sch = Data.schema(ctable)
    Source(file, sch, ctable, data)
end

Data.schema(s::Source) = s.schema
Data.header(s::Source) = Data.header(s.schema)
Data.types(s::Source{S}) where S = Tuple(S.parameters)

getcolumn(s::Source, col::Integer) = s.ctable.columns[col]

colnumber(s::Source, col::Integer) = col
colnumber(s::Source, col::String) = s.schema[col]
colnumber(s::Source, col::Symbol) = colnumber(s, string(col))

colname(s::Source, col::Integer) = Symbol(s.schema.header[col])
colname(s::Source, col::String) = Symbol(col)
colname(s::Source, col::Symbol) = col

size(s::Source) = size(s.schema)
size(s::Source, i::Integer) = size(s.schema, i)

datapointer(s::Source) = pointer(s.data)

checkcolbounds(s::Source, col::Integer) = (1 ≤ col ≤ size(s, 2)) || throw(BoundsError(s, col))


# DataFrame constructor, using Arrow objects
DataFrame(s::Source) = DataFrame((colname(s,i)=>s.columns[i] for i ∈ 1:size(s,2))...)


"""
    Feather.read(file::AbstractString)

Create a `DataFrame` representing the Feather file `file`.  This data frame will use `ArrowVector`s to
refer to data within the feather file.  By default this is memory mapped and no data is actually read
from disk until a particular field of the dataframe is accessed.

To copy the entire file into memory, instead use `materialize`.
"""
read(file::AbstractString; use_mmap::Bool=SHOULD_USE_MMAP) = DataFrame(Source(file, use_mmap=use_mmap))


# TODO update docs
"""
    Feather.materialize(s::Feather.Source[, rows, cols])
    Feather.materialize(file::AbstractString[, rows, cols])

Read a feather file into memory and return it as a `DataFrame`.  Optionally one may only read in
particular rows or columns (these should be specified with `AbstractVector`s, columns can be either
integers or `Symbol`s).

For most purposes, it is recommended that you use `read` instead so that data is read off
disk only as necessary.
"""
materialize(x) = x
materialize(A::ArrowVector{T}) where T = convert(Vector{T}, A)
materialize(A::DictEncoding{T}) where T = Arrow.categorical(A)
materialize(A::ArrowVector, idx::AbstractVector{<:Integer}) = A[idx]
materialize(A::DictEncoding, idx::AbstractVector{<:Integer}) = Arrow.categorical(A, idx)

materialize(s::Source, col::Union{Symbol,<:Integer}) = materialize(s.columns[colnumber(s,col)])
function materialize(s::Source, rows::AbstractVector{<:Integer}, col::Union{Symbol,<:Integer})
    materialize(s.columns[colnumber(s,col)], rows)
end

function materialize(s::Source, rows::AbstractVector{<:Integer}, cols::AbstractVector{T}
                    ) where {T<:Union{Symbol,<:Integer}}
    DataFrame((colname(s,col)=>materialize(s,rows,col) for col ∈ cols)...)
end
function materialize(file::AbstractString, rows::AbstractVector{<:Integer}, cols::AbstractVector{T}
                    ) where {T<:Union{Symbol,<:Integer}}
    materialize(Source(file), rows, cols)
end
function materialize(s::Source, cols::AbstractVector{<:Union{Symbol,<:Integer}})
    materialize(s, 1:size(s,1), cols)
end

materialize(s::Source) = materialize(s, 1:size(s,1), 1:size(s,2))
materialize(file::AbstractString) = materialize(Source(file))

materialize(df::DataFrame) = DataFrame((n=>materialize(df[n]) for n ∈ names(df))...)
#=====================================================================================================
    DataStreams interface
=====================================================================================================#
Data.streamtype(::Type{Source}, ::Type{Data.Field}) = true
Data.streamtype(::Type{Source}, ::Type{Data.Column}) = true
Data.accesspattern(::Source) = Data.RandomAccess

Data.reference(s::Source) = s.data
function Data.isdone(s::Source, row::Integer, col::Integer, rows::Integer, cols::Integer)
    col > cols || row > rows
end
function Data.isdone(s::Source, row::Integer, col::Integer)
    rows, cols = size(s)
    Data.isdone(s, row, col, rows, cols)
end

function Data.streamfrom(s::Source, ::Type{Data.Field}, ::Type{T}, row::Integer, col::Integer) where T
    s.columns[col][row]
end
Data.streamfrom(s::Source, ::Type{Data.Column}, ::Type{T}, col::Integer) where T = s.columns[col][:]


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

function offsetsloc(p::Metadata.PrimitiveArray)
    if isprimitivetype(p.dtype)
        throw(ErrorException("Trying to obtain offset values for primitive array."))
    end
    startloc(p) + bitmasklength(p)
end

# override default offset type
Locate.Offsets(col::Metadata.PrimitiveArray) = Locate.Offsets{Int32}(offsetsloc(col))

Locate.length(col::Metadata.PrimitiveArray) = length(col)
Locate.values(col::Metadata.PrimitiveArray) = startloc(col) + bitmasklength(col) + offsetslength(col)
# this is only relevant for lists, values type must be UInt8
Locate.valueslength(col::Metadata.PrimitiveArray) = valueslength(col)
Locate.bitmask(col::Metadata.PrimitiveArray) = startloc(col)

function constructcolumn(::Type{T}, data::Vector{UInt8}, meta::Metadata.CategoryMetadata,
                         col::Metadata.Column) where T
    reftype = juliatype(col.values.dtype)
    DictEncoding{T}(locate(data, reftype, col.values), locate(data, T, col.metadata.levels))
end
function constructcolumn(::Type{Union{T,Missing}}, data::Vector{UInt8}, meta::Metadata.CategoryMetadata,
                         col::Metadata.Column) where T
    reftype = Union{juliatype(col.values.dtype),Missing}
    DictEncoding{Union{T,Missing}}(locate(data, reftype, col.values),
                                   locate(data, T, col.metadata.levels))
end

function constructcolumn(::Type{T}, data::Vector{UInt8}, meta, col::Metadata.Column) where T
    locate(data, T, col.values)
end

function constructcolumn(s::Source, ::Type{T}, col::Integer) where T
    @boundscheck checkcolbounds(s, col)
    col = getcolumn(s, col)
    constructcolumn(T, s.data, col.metadata, col)
end
constructcolumn(s::Source{S}, col::Integer) where S = constructcolumn(s, S.parameters[col], col)
constructcolumn(s::Source, col::AbstractString) = constructcolumn(s, s.schema[col])

constructall(s::Source) = ArrowVector[constructcolumn(s, i) for i ∈ 1:size(s,2)]
