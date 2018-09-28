
mutable struct Source{NT}
    path::String
    size::Tuple{Int64, Int64}
    ctable::Metadata.CTable
    data::Vector{UInt8}
    columns::Vector{ArrowVector}
end

function Source(file::AbstractString, ::Type{NT}, ctable::Metadata.CTable,
                data::Vector{UInt8}) where {NT <: NamedTuple}
    s = Source{NT}(file, (ctable.num_rows, length(ctable.columns)), ctable, data, Vector{ArrowVector}(undef, 0))
    s.columns = constructall(s)
    s
end
function Source(file::AbstractString; use_mmap::Bool=SHOULD_USE_MMAP)
    data = loadfile(file, use_mmap=use_mmap)
    ctable = getctable(data)
    sch = schema(ctable)
    Source(file, sch, ctable, data)
end

function schema(ctable::Metadata.CTable)
    ncols = length(ctable.columns)
    header = Vector{Symbol}(undef, ncols)
    types = Vector{Type}(undef, ncols)
    for (i, col) ∈ enumerate(ctable.columns)
        header[i] = Symbol(col.name)
        types[i] = juliatype(col)
    end
    return NamedTuple{Tuple(header), Tuple{types...}}
end

Tables.istable(::Type{<:Source}) = true
Tables.columnaccess(::Type{<:Source}) = true
Tables.schema(s::Source{NT}) where {NT} = Tables.Schema(NT)

Tables.columns(s::Source{NamedTuple{names, T}}) where {names, T} =
    NamedTuple{names}(Tuple(s.columns[i] for i = 1:length(names)))

getcolumn(s::Source, col::Integer) = s.ctable.columns[col]

colnumber(s::Source, col::Integer) = col
colnumber(s::Source{NamedTuple{names, T}}, col::String) where {names, T} = Tables.columnindex(names, Symbol(col))
colnumber(s::Source{NamedTuple{names, T}}, col::Symbol) where {names, T} = Tables.columnindex(names, col)

colname(s::Source{NamedTuple{names, T}}, col::Integer) where {names, T} = names[col]
colname(s::Source{NamedTuple{names, T}}, col::Symbol) where {names, T} = names[col]
colname(s::Source, col::String) = Symbol(col)
colname(s::Source, col::Symbol) = col

Base.size(s::Source) = s.size
Base.size(s::Source, i::Integer) = ifelse(i == 1, s.size[1], s.size[2])

datapointer(s::Source) = pointer(s.data)

checkcolbounds(s::Source, col::Integer) = (1 ≤ col ≤ size(s, 2)) || throw(BoundsError(s, col))

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
    new column construction stuff
=====================================================================================================#
Base.length(p::Metadata.PrimitiveArray) = p.length

startloc(p::Metadata.PrimitiveArray) = p.offset+1

Arrow.nullcount(p::Metadata.PrimitiveArray) = p.null_count

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
constructcolumn(s::Source{NamedTuple{names, T}}, col::Integer) where {names, T} = constructcolumn(s, fieldtype(T, col), col)
constructcolumn(s::Source{NamedTuple{names, T}}, col::AbstractString) where {names, T} = constructcolumn(s, Tables.columnindex(names, Symbol(col)))

constructall(s::Source) = ArrowVector[constructcolumn(s, i) for i ∈ 1:size(s,2)]
