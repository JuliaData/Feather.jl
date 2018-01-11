__precompile__(true)
module Feather

using FlatBuffers, Missings, WeakRefStrings, CategoricalArrays, DataStreams, DataFrames

if Base.VERSION < v"0.7.0-DEV.2575"
    const Dates = Base.Dates
else
    import Dates
end

if Base.VERSION >= v"0.7.0-DEV.2009"
    using Mmap
end

export Data

# named FEATHER_VERSION to avoid confusion with Julia Base.VERSION
const FEATHER_VERSION = 2

# Arrow type definitions
include("Arrow.jl")

# flatbuffer defintions
include("metadata.jl")

# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = Vector{UInt8}("FEA1")

bytes_for_bits(size::Integer) = div(((size + 7) & ~7), 8)
const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
getbit(byte::UInt8, i::Integer) = (byte & BITMASK[i]) == 0
const ALIGNMENT = 8
paddedlength(x::Integer) = div((x + ALIGNMENT - 1), ALIGNMENT) * ALIGNMENT
getoutputlength(version::Int32, x::Integer) = version < FEATHER_VERSION ? x : paddedlength(x)

function writepadded(io::IO, x)
    bw = Base.write(io, x)
    diff = paddedlength(bw) - bw
    Base.write(io, zeros(UInt8, diff))
    bw + diff
end
function writepadded(io::IO, x::Vector{String})
    bw = 0
    for str in x
        bw += Base.write(io, str)
    end
    diff = paddedlength(bw) - bw
    Base.write(io, zeros(UInt8, diff))
    bw + diff
end

juliastoragetype(meta::Void, values_type::Metadata.Type_) = Type_2julia[values_type]
function juliastoragetype(meta::Metadata.CategoryMetadata, values_type::Metadata.Type_)
    R = Type_2julia[values_type]
    CategoricalString{R}
end
function juliastoragetype(meta::Metadata.TimestampMetadata, values_type::Metadata.Type_)
    Arrow.Timestamp{TimeUnit2julia[meta.unit],meta.timezone == "" ? :UTC : Symbol(meta.timezone)}
end
juliastoragetype(meta::Metadata.DateMetadata, values_type::Metadata.Type_) = Arrow.Date
function juliastoragetype(meta::Metadata.TimeMetadata, values_type::Metadata.Type_)
    Arrow.Time{TimeUnit2julia[meta.unit]}
end

# these are Julia types corresponding to arrow types
juliatype(::Type{<:Arrow.Timestamp}) = Dates.DateTime
juliatype(::Type{<:Arrow.Date}) = Dates.Date
juliatype(::Type{T}) where {T} = T
juliatype(::Type{Arrow.Bool}) = Bool

# TODO figure out types for some of these things
function addlevels!(::Type{T}, levels::Dict, orders::Dict, i::Integer, meta::Metadata.TypeMetadata,
                    values_type::Metadata.Type_, data::AbstractVector{UInt8}, version::Int32) where {T}
end
function addlevels!(::Type{<:CategoricalString}, catlevels::Dict, orders::Dict, i::Integer,
                    meta::Metadata.TypeMetadata, values_type::Metadata.Type_,
                    data::AbstractVector{UInt8}, version::Int32)
    ptr = convert(Ptr{Int32}, pointer(data) + meta.levels.offset)
    offsets = [unsafe_load(ptr, j) for j ∈ 1:meta.levels.length + 1]
    ptr += getoutputlength(version, sizeof(offsets))
    ptr2 = convert(Ptr{UInt8}, ptr)
    catlevels[i] = map(x->unsafe_string(ptr2 + offsets[x], offsets[x+1] - offsets[x]),
                       1:meta.levels.length)
    orders[i] = meta.ordered
    nothing
end

function schematype(::Type{T}, nullcount::Integer, nullable::Bool, wrs::Bool) where {T}
    (nullcount == 0 && !nullable) ? T : Union{T, Missing}
end
function schematype(::Type{<:AbstractString}, nullcount::Integer, nullable::Bool, wrs::Bool)
    s = wrs ? WeakRefString{UInt8} : String
    (nullcount == 0 && ! nullable) ? s : Union{s, Missing}
end
function schematype(::Type{CategoricalString{R}}, nullcount::Integer, nullable::Bool,
                    wrs::Bool) where {R}
    (nullcount == 0 && !nullable) ? CategoricalString{R} : Union{CategoricalString{R}, Missing}
end

# DataStreams interface types
mutable struct Source{S, T} <: Data.Source
    path::String
    schema::Data.Schema
    ctable::Metadata.CTable
    data::Vector{UInt8}
    # ::S # separate from the types in schema, since we need to convert between feather storage types & julia types
    levels::Dict{Int,Vector{String}}
    orders::Dict{Int,Bool}
    columns::T # holds references to pre-fetched columns for Data.getfield
end

if Base.VERSION < v"0.7-DEV"
    iswindows = is_windows
else
    iswindows = Sys.iswindows
end

# reading feather files
if iswindows()
    const should_use_mmap = false
else
    const should_use_mmap = true
end


"""
    validfile(file::AbstractString, use_mmap::Bool)

Checks whether the file in location `file` may be a valid Feather file.

Returns file data.  Used by `Source`.
"""
function validfile(file::AbstractString, use_mmap::Bool)
    isfile(file) || throw(ArgumentError("'$file' is not a valid file."))
    m = use_mmap ? Mmap.mmap(file) : Base.read(file)
    if length(m) < 12
        throw(ArgumentError("'$file' is not in the feather format: total length of file: $(length(m))"))
    end
    if m[1:4] ≠ FEATHER_MAGIC_BYTES || m[end-3:end] ≠ FEATHER_MAGIC_BYTES
        throw(ArgumentError("'$file' is not in the feather format: header = $(m[1:4]),
                            footer = $(m[end-3:end])"))
    end
    m
end

function Source(file::AbstractString; nullable::Bool=false,
                weakrefstrings::Bool=true, use_mmap::Bool=should_use_mmap)
    # validity checks
    m = validfile(file, use_mmap)
    # read file metadata using FlatBuffers
    metalength = Base.read(IOBuffer(m[length(m)-7:length(m)-4]), Int32)
    metapos = length(m) - (metalength + 7)
    rootpos = Base.read(IOBuffer(m[metapos:metapos+4]), Int32)
    ctable = FlatBuffers.read(Metadata.CTable, m, metapos + rootpos - 1)
    # TODO again, comparison of Int32 with VersionNumber??
    if ctable.version < FEATHER_VERSION
        warn("This Feather file is old and may not be readable.")
    end
    header = String[]
    types = Type[]
    juliatypes = Type[]
    columns = ctable.columns
    levels = Dict{Int,Vector{String}}()
    orders = Dict{Int,Bool}()
    for (i, col) in enumerate(columns)
        push!(header, col.name)
        push!(types, juliastoragetype(col.metadata, col.values.type_))
        jl = juliatype(types[end])
        addlevels!(jl, levels, orders, i, col.metadata, col.values.type_, m, ctable.version)
        push!(juliatypes, schematype(jl, col.values.null_count, nullable, weakrefstrings))
    end
    sch = Data.Schema(juliatypes, header, ctable.num_rows)
    columns = DataFrame(sch, Data.Column, false)  # returns DataFrameStream, not DataFrame!!
    sch.rows = ctable.num_rows
    Source{Tuple{types...}, typeof(columns)}(file, sch, ctable, m, levels, orders, columns)
end

# DataStreams interface
Data.allocate(::Type{CategoricalString{R}}, rows, ref) where {R} = CategoricalArray{String, 1, R}(rows)
function Data.allocate(::Type{Union{CategoricalString{R}, Missing}}, rows, ref) where {R}
    CategoricalArray{Union{String, Missing}, 1, R}(rows)
end

Data.schema(source::Feather.Source) = source.schema
Data.reference(source::Feather.Source) = source.data
Data.isdone(io::Feather.Source, row, col, rows, cols) =  col > cols || row > rows
function Data.isdone(io::Source, row, col)
    rows, cols = size(Data.schema(io))
    return isdone(io, row, col, rows, cols)
end
Data.streamtype(::Type{<:Feather.Source}, ::Type{Data.Column}) = true
Data.streamtype(::Type{<:Feather.Source}, ::Type{Data.Field}) = true

@inline function Data.streamfrom(source::Source, ::Type{Data.Field}, ::Type{T}, row, col) where {T}
    if isempty(source.columns, col)
        append!(source.columns[col], Data.streamfrom(source, Data.Column, T, row, col))
    end
    source.columns[col][row]
end

"""
    nrows(s::Source)

Return the number of rows of the underlying data.
"""
nrows(s::Source) = s.ctable.num_rows

"""
    nnulls(s::Source, col)

Return the number of nulls in column number `col` according to metadata.
"""
nnulls(s::Source, col) = s.ctable.columns[col].values.null_count

"""
    coloffset(s::Source, col)

Return the offset of colum n number `col`.
"""
coloffset(s::Source, col) = s.ctable.columns[col].values.offset

function checknonull(s::Source, col)
    if nnulls(s, col) > 0
        throw(ErrorException("Column $col was expected to have no nulls but has $(nnulls(s, col))."))
    end
end
function getbools(s::Source, col)
    if nnulls(s, col) == 0
        zeros(Bool, nrows(s))
    else
        Bool[getbit(s.data[coloffset(s, col) + bytes_for_bits(x)], mod1(x,8)) for x ∈ 1:nrows(s)]
    end
end

@inline function unwrap(s::Source, ::Type{T}, col, rows, off::Integer=0) where {T}
    bitmask_bytes = if nnulls(s, col) > 0
        getoutputlength(s.ctable.version, bytes_for_bits(s.ctable.num_rows))
    else
        0
    end
    ptr = convert(Ptr{T}, pointer(s.data) + coloffset(s, col) + bitmask_bytes + off)
    [unsafe_load(ptr, i) for i = 1:rows]
end


"""
    getoffsets(s::Source, col)

Get offsets associated with a particular column.
"""
getoffsets(s::Source, col) = unwrap(s, Int32, col, nrows(s)+1)


"""
    getvalues(s::Source, col[, offsets::Vector{Int32}])

Get values associated with a column given offsets.  If not provided, offsets will be retrieved.
"""
function getvalues(s::Source, col, offsets::Vector{Int32})
    unwrap(s, UInt8, col, offsets[end], getoutputlength(s.ctable.version, sizeof(offsets)))
end
getvalues(s::Source, col) = getvalues(s, col, getoffsets(s, col))


"""
    transform!(::Type{T}, A::Vector, len::Integer)

**TODO**: These definitely require explanation.
"""
transform!(::Type{T}, A::Vector, len::Integer) where {T} = A
transform!(::Type{Dates.Date}, A, len::Integer) = map(x->Arrow.unix2date(x), A)
function transform!(::Type{Dates.DateTime}, A::Vector{Arrow.Timestamp{P,Z}}, len::Integer) where {P, Z}
    map(x->Arrow.unix2datetime(P, x), A)
end
transform!(::Type{CategoricalString{R}}, A::Vector, len::Integer) where {R} = map(x->x + R(1), A)
function transform!(::Type{Bool}, A::Vector, len::Integer)
    B = falses(len)
    Base.copy_chunks!(B.chunks, 1, map(x->x.value, A), 1, length(A) * 64)
    convert(Vector{Bool}, B)
end

@inline function Data.streamfrom(source::Source{S}, ::Type{Data.Column}, ::Type{T},
                                 row, col) where {S, T}
    checknonull(source, col)
    A = unwrap(source, S.parameters[col], col, source.ctable.num_rows)
    transform!(T, A, source.ctable.num_rows)
end
@inline function Data.streamfrom(source::Source{S}, ::Type{Data.Column}, ::Type{Union{T, Missing}},
                                 row, col) where {S, T}
    A = transform!(T, unwrap(source, S.parameters[col], col, source.ctable.num_rows), nrows(source))
    bools = getbools(source, col)
    V = Vector{Union{T, Missing}}(A)
    foreach(x->bools[x] && (V[x] = missing), 1:length(A))
    V
end
@inline function Data.streamfrom(source::Source{S}, ::Type{Data.Column}, ::Type{Bool}, row, col) where {S}
    checknonull(source, col)
    A = unwrap(source, S.parameters[col], col, max(1,div(bytes_for_bits(source.ctable.num_rows),8)))
    transform!(Bool, A, source.ctable.num_rows)::Vector{Bool}
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{T}, row, col
                                ) where {T <: AbstractString}
    checknonull(source, col)
    offsets = getoffsets(source, col)
    values = getvalues(source, col, offsets)
    T[unsafe_string(pointer(values, offsets[i]+1), Int(offsets[i+1] - offsets[i])) for i ∈ 1:nrows(s)]
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{Union{T, Missing}},
                                 row, col) where {T <: AbstractString}
    bools = getbools(source, col)
    offsets = getoffsets(source, col)
    values = getvalues(source, col, offsets)
    A = T[unsafe_string(pointer(values,offsets[i]+1), Int(offsets[i+1]-offsets[i])) for
                                                                        i ∈ 1:nrows(source)]
    V = Vector{Union{T, Missing}}(A)
    foreach(x->bools[x] && (V[x] = missing), 1:length(A))
    V
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{WeakRefString{UInt8}},
                                 row, col)
    checknonull(source, col)
    offsets = getoffsets(source, col)
    offset = coloffset(source, col)
    offset += if nnulls(source, col) > 0
        Feather.getoutputlength(source.ctable.version, Feather.bytes_for_bits(source.ctable.num_rows))
    else
        0
    end
    offset += getoutputlength(source.ctable.version, sizeof(offsets))
    values = getvalues(source, col, offsets)
    A = [WeakRefString(pointer(source.data, offset+offsets[i]+1), Int(offsets[i+1]-offsets[i])) for
                                                                        i ∈ 1:nrows(source)]
    WeakRefStringArray(source.data, A)
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column},
                                 ::Type{Union{WeakRefString{UInt8}, Missing}}, row, col)
    bools = getbools(source, col)
    offsets = getoffsets(source, col)
    offset = coloffset(source, col)
    offset += if nnulls(source, col) > 0
        Feather.getoutputlength(source.ctable.version, bytes_for_bits(nrows(source)))
    else
        0
    end
    offset += getoutputlength(source.ctable.version, sizeof(offsets))
    values = getvalues(source, col, offsets)
    A = Union{WeakRefString{UInt8},Missing}[WeakRefString(pointer(source.data,offset+offsets[i]+1), Int(offsets[i+1]-offsets[i])) for
                                             i ∈ 1:nrows(source)]
    foreach(x->bools[x] && (A[x] = missing), 1:length(A))
    WeakRefStringArray(source.data, A)
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{CategoricalString{R}},
                                 row, col) where {R}
    checknonull(source, col)
    refs = transform!(CategoricalString{R}, unwrap(source, R, col, nrows(source)), nrows(source))
    pool = CategoricalPool{String, R}(source.levels[col], source.orders[col])
    CategoricalArray{String,1}(refs, pool)
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column},
                                 ::Type{Union{CategoricalString{R}, Missing}}, row, col) where {R}
    refs = transform!(CategoricalString{R}, unwrap(source, R, col, nrows(source)), nrows(source))
    bools = getbools(source, col)
    refs = R[ifelse(bools[i], R(0), refs[i]) for i = 1:source.ctable.num_rows]
    pool = CategoricalPool{String, R}(source.levels[col], source.orders[col])
    CategoricalArray{Union{String, Missing},1}(refs, pool)
end


"""
`Feather.read{T <: Data.Sink}(file, sink_type::Type{T}, sink_args...; weakrefstrings::Bool=true)` => `T`

`Feather.read(file, sink::Data.Sink; weakrefstrings::Bool=true)` => `Data.Sink`

`Feather.read` takes a feather-formatted binary `file` argument and "streams" the data to the
provided `sink` argument, a `DataFrame` by default. A fully constructed `sink` can be provided as the 2nd argument (the 2nd method above),
or a Sink can be constructed "on the fly" by providing the type of Sink and any necessary positional arguments
(the 1st method above).

Keyword arguments:

  * `nullable::Bool=false`: will return columns as `NullableVector{T}` types by default, regarldess of # of missing values. When set to `false`, columns without missing values will be returned as regular `Vector{T}`
  * `weakrefstrings::Bool=true`: indicates whether string-type columns should be returned as `WeakRefString` (for efficiency) or regular `String` types
  * `use_mmap::Bool=true`: indicates whether to use system `mmap` capabilities when reading the feather file; on some systems or environments, mmap may not be available or reliable (virtualbox env using shared directories can be problematic)
  * `append::Bool=false`: indicates whether the feather file should be appended to the provided `sink` argument; note that column types between the feather file and existing sink must match to allow appending
  * `transforms`: a `Dict{Int,Function}` or `Dict{String,Function}` that provides transform functions to be applied to feather fields or columns as they are parsed from the feather file; note that feather files can be parsed field-by-field or entire columns at a time, so transform functions need to operate on scalars or vectors appropriately, depending on the `sink` argument's preferred streaming type; by default, a `Feather.Source` will stream entire columns at a time, so a transform function would take a single `NullableVector{T}` argument and return an equal-length `NullableVector`

Examples:

```julia
# default read method, returns a DataFrame
df = Feather.read("cool_feather_file.feather")

# read a feather file directly into a SQLite database table
db = SQLite.DB()
Feather.read("cool_feather_file.feather", SQLite.Sink, db, "cool_feather_table")
```
"""
function read end  # TODO why is this here?
function read(file::AbstractString, sink=DataFrame, args...;
              nullable::Bool=false, weakrefstrings::Bool=true, use_mmap::Bool=true,
              append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(Source(file; nullable=nullable, weakrefstrings=weakrefstrings, use_mmap=use_mmap),
                        sink, args...; append=append, transforms=transforms)
    Data.close!(sink)
end
function read(file::AbstractString, sink::T; nullable::Bool=false, weakrefstrings::Bool=true,
              use_mmap::Bool=true, append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(Source(file; nullable=nullable, weakrefstrings=weakrefstrings, use_mmap=use_mmap),
                        sink; append=append, transforms=transforms)
    Data.close!(sink)
end
function read(source::Feather.Source, sink=DataFrame, args...; append::Bool=false,
              transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(source, sink, args...; append=append, transforms=transforms)
    Data.close!(sink)
end
function read(source::Feather.Source, sink::T; append::Bool=false,
              transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(source, sink; append=append, transforms=transforms);
    Data.close!(sink)
end

# writing feather files
feathertype(::Type{T}) where {T} = Feather.julia2Type_[T]
feathertype(::Type{Union{T, Missing}}) where {T} = feathertype(T)
feathertype(::Type{CategoricalString{R}}) where {R} = julia2Type_[R]
feathertype(::Type{<:Arrow.Time}) = Metadata.INT64
feathertype(::Type{Dates.Date}) = Metadata.INT32
feathertype(::Type{Dates.DateTime}) = Metadata.INT64
feathertype(::Type{<:AbstractString}) = Metadata.UTF8

getmetadata(io::IO, ::Type{T}, A::AbstractVector) where T = nothing
getmetadata(io::IO, ::Type{Union{T, Missing}}, A::AbstractVector) where {T} = getmetadata(io, T, A)
getmetadata(io::IO, ::Type{Dates.Date}, A::AbstractVector) = Metadata.DateMetadata()
function getmetadata(io::IO, ::Type{Arrow.Time{T}}, A::AbstractVector) where {T}
    Metadata.TimeMetadata(julia2TimeUnit[T])
end
function getmetadata(io, ::Type{Dates.DateTime}, A::AbstractVector)
    Metadata.TimestampMetadata(julia2TimeUnit[Arrow.Millisecond], "")
end
function getmetadata(io, ::Type{CategoricalString{R}}, A::AbstractVector) where {R}
    lvls = CategoricalArrays.levels(A)
    len = length(lvls)
    offsets = zeros(Int32, len+1)
    offsets[1] = off = 0
    for (i,v) in enumerate(lvls)
        off += length(v)
        offsets[i + 1] = off
    end
    offset = position(io)
    total_bytes = writepadded(io, view(reinterpret(UInt8, offsets), 1:(sizeof(Int32) * (len + 1))))
    total_bytes += writepadded(io, lvls)
    Metadata.CategoryMetadata(Metadata.PrimitiveArray(Metadata.UTF8, Metadata.PLAIN, offset,
                                                      len, 0, total_bytes),
                              isordered(A))
end

values(A::Vector) = A
values(A::CategoricalArray{T,1,R}) where {T, R} = map(x-> x - R(1), A.refs)

nullcount(A::AbstractVector) = 0
nullcount(A::Vector{Union{T, Missing}}) where {T} = sum(ismissing(x) for x in A)
nullcount(A::CategoricalArray) = sum(A.refs .== 0)

# List types
valuelength(val::T) where {T} = sizeof(string(val))
valuelength(val::String) = sizeof(val)
valuelength(val::WeakRefString{UInt8}) = val.len
valuelength(val::Missing) = 0

writevalue(io::IO, val::T) where {T} = Base.write(io, string(val))
writevalue(io::IO, val::Missing) = 0
writevalue(io::IO, val::String) = Base.write(io, val)

# Bool
function writecolumn(io::IO, ::Type{Bool}, A::AbstractVector)
    writepadded(io, view(reinterpret(UInt8, convert(BitVector, A).chunks), 1:bytes_for_bits(length(A))))
end
# Category
function writecolumn(io::IO, ::Type{CategoricalString{R}}, A::AbstractVector) where {R}
    writepadded(io, view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(R))))
end
function writecolumn(io::IO, ::Type{Union{CategoricalString{R}, Missing}}, A::AbstractVector) where {R}
    writepadded(io, view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(R))))
end
# Date
writecolumn(io::IO, ::Type{Dates.Date}, A::AbstractVector) = writepadded(io, map(Arrow.date2unix, A))
# Timestamp
function writecolumn(io::IO, ::Type{Dates.DateTime}, A::AbstractVector)
    writepadded(io, map(Arrow.datetime2unix, A))
end
function writecolumn(io::IO, ::Type{DateTime}, A::Vector{Union{DateTime,Missing}})
    writecolumn(io, DateTime, map(x->ifelse(ismissing(x),Dates.unix2datetime(0),x), A))
end
function writecolumn(io::IO, ::Type{Date}, A::Vector{Union{DateTime,Missing}})
    zerodate = Date(Dates.unix2datetime(0))
    writecolumn(io, Date, map(x->ifelse(ismissing(x),zerodate,x), A))
end
function writecolumn(io::IO, ::Type{T},
                     arr::Union{WeakRefStringArray{WeakRefString{UInt8}}, Vector{T},
                                Vector{Union{Missing, T}}}
                    ) where {T <: Union{Vector{UInt8}, AbstractString}}
    len = length(arr)
    off = 0
    offsets = zeros(Int32, len + 1)
    for ind = 1:length(arr)
        v = arr[ind]
        off += Feather.valuelength(v)
        offsets[ind + 1] = off
    end
    total_bytes = writepadded(io, view(reinterpret(UInt8, offsets), 1:length(offsets) * sizeof(Int32)))
    total_bytes += offsets[len+1]
    for val in arr
        writevalue(io, val)
    end
    diff = paddedlength(offsets[len+1]) - offsets[len+1]
    if diff > 0
        Base.write(io, zeros(UInt8, diff))
        total_bytes += diff
    end
    total_bytes
end
# other primitive T
function writecolumn(io::IO, ::Type{T}, A::Vector{Union{Missing, T}}) where {T}
    writecolumn(io, T, map(x->ifelse(ismissing(x),zero(T),x), A))
end
writecolumn(io::IO, ::Type{T}, A::AbstractVector) where {T} = writepadded(io, A)
function writenulls(io::IO, A::AbstractVector, null_count::Integer, len::Integer, total_bytes::Integer)
    total_bytes
end


function writenulls(io::IO, A::Vector{Union{T, Missing}}, null_count::Integer, len::Integer,
                    total_bytes::Integer) where {T}
    # write out null bitmask
    if null_count > 0
        null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(Bool[!ismissing(x) for x in A])
        total_bytes = writepadded(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
    end
    total_bytes
end
function writenulls(io::IO, A::T, null_count::Integer, len::Integer, total_bytes::Integer
                   ) where {T <: CategoricalArray}
    # write out null bitmask
    if null_count > 0
        null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(map(!, A.refs .== 0))
        total_bytes = writepadded(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
    end
    total_bytes
end

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

"""
`Feather.write{T <: Data.Source}(io, source::Type{T}, source_args...)` => `Feather.Sink`

`Feather.write(io, source::Data.Source)` => `Feather.Sink`


Write a `Data.Source` out to disk as a feather-formatted binary file. The two methods allow the passing of a
fully constructed `Data.Source` (2nd method), or the type of Source and any necessary positional arguments (1st method).

Keyword arguments:

  * `append::Bool=false`: indicates whether the `source` argument should be appended to an existing feather file; note that column types between the `source` argument and feather file must match to allow appending
  * `transforms`: a `Dict{Int,Function}` or `Dict{String,Function}` that provides transform functions to be applied to source fields or columns as they are streamed to the feather file; note that feather sinks can be receive data field-by-field or entire columns at a time, so transform functions need to operate on scalars or vectors appropriately, depending on the `source` argument's allowed streaming types; by default, a `Feather.Sink` will stream entire columns at a time, so a transform function would take a single `NullableVector{T}` argument and return an equal-length `NullableVector`

Examples:

```julia
df = DataFrame(...)
Feather.write("shiny_new_feather_file.feather", df)

Feather.write("sqlite_query_result.feather", SQLite.Source, db, "select * from cool_table")
```
"""
function write end # TODO agian, why is this needed?
function write(io::AbstractString, ::Type{T}, args...;
               append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...) where {T}
    sink = Data.stream!(T(args...), Feather.Sink, io; append=append, transforms=transforms, kwargs...)
    Data.close!(sink)
end
function write(io::AbstractString, source; append::Bool=false,
               transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(source, Feather.Sink, io; append=append, transforms=transforms, kwargs...)
    Data.close!(sink)
end
function write(sink::Sink, ::Type{T}, args...; append::Bool=false,
               transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(T(args...), sink; append=append, transforms=transforms)
    Data.close!(sink)
end
function write(sink::Sink, source; append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(source, sink; append=append, transforms=transforms)
    Data.close!(sink)
end

end # module
