module Feather

using FlatBuffers, DataStreams, DataFrames, NullableArrays, CategoricalArrays, WeakRefStrings

using DataArrays

export Data, DataFrame

# because there's currently not a better place for this to live
import Base.==
=={T}(x::WeakRefString{T}, y::CategoricalArrays.CategoricalValue) = String(x) == String(y)
=={T}(y::CategoricalArrays.CategoricalValue, x::WeakRefString{T}) = String(x) == String(y)

# sync with feather
const VERSION = 2

# Arrow type definitions
include("Arrow.jl")

# flatbuffer defintions
include("metadata.jl")

# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = Vector{UInt8}("FEA1")

bytes_for_bits(size) = div(((size + 7) & ~7), 8)
const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
getbit(byte::UInt8, i) = (byte & BITMASK[i]) == 0
const ALIGNMENT = 8
paddedlength(x) = div((x + ALIGNMENT - 1), ALIGNMENT) * ALIGNMENT
getoutputlength(version, x) = version < VERSION ? x : paddedlength(x)

function writepadded(io, x)
    bw = Base.write(io, x)
    diff = paddedlength(bw) - bw
    Base.write(io, zeros(UInt8, diff))
    return bw + diff
end

juliastoragetype(meta::Void, values_type) = Type_2julia[values_type]
function juliastoragetype(meta::Metadata.CategoryMetadata, values_type)
    R = Type_2julia[values_type]
    return CategoricalValue{String,R}
end
juliastoragetype(meta::Metadata.TimestampMetadata, values_type) = Arrow.Timestamp{TimeUnit2julia[meta.unit],meta.timezone == "" ? :UTC : Symbol(meta.timezone)}
juliastoragetype(meta::Metadata.DateMetadata, values_type) = Arrow.Date
juliastoragetype(meta::Metadata.TimeMetadata, values_type) = Arrow.Time{TimeUnit2julia[meta.unit]}

juliatype{T<:Arrow.Timestamp}(::Type{T}) = DateTime
juliatype{T<:Arrow.Date}(::Type{T}) = Date
juliatype{T}(::Type{T}) = T
juliatype(::Type{Arrow.Bool}) = Bool

addlevels!{T}(::Type{T}, levels, orders, i, meta, values_type, data, version) = return
function addlevels!{T <: CategoricalValue}(::Type{T}, catlevels, orders, i, meta, values_type, data, version)
    ptr = pointer(data) + meta.levels.offset
    offsets = unsafe_wrap(Array, convert(Ptr{Int32}, ptr), meta.levels.length + 1)
    ptr += getoutputlength(version, sizeof(offsets))
    catlevels[i] = map(x->unsafe_string(ptr + offsets[x], offsets[x+1] - offsets[x]), 1:meta.levels.length)
    orders[i] = meta.ordered
    return
end

schematype{T}(::Type{T}, nullcount, nullable, wrs) = (nullcount == 0 && !nullable) ? Vector{T} : NullableVector{T}
schematype{T <: AbstractString}(::Type{T}, nullcount, nullable, wrs) = wrs ? NullableVector{WeakRefString{UInt8}} : NullableVector{String}
schematype{T, R}(::Type{CategoricalValue{T, R}}, nullcount, nullable, wrs) = (nullcount == 0 && !nullable) ? CategoricalVector{T, R} : NullableCategoricalVector{T, R}

# DataStreams interface types
type Source <: Data.Source
    path::String
    schema::Data.Schema
    ctable::Metadata.CTable
    data::Vector{UInt8}
    feathertypes::Vector{DataType} # separate from the types in schema, since we need to convert between feather storage types & julia types
    levels::Dict{Int,Vector{String}}
    orders::Dict{Int,Bool}
    columns::Vector{Any} # holds references to pre-fetched columns for Data.getfield
end

# reading feather files
if is_windows()
    const should_use_mmap = false
else
    const should_use_mmap = true
end

function Source(file::AbstractString; nullable::Bool=true, weakrefstrings::Bool=true, use_mmap::Bool=should_use_mmap)
    # validity checks
    isfile(file) || throw(ArgumentError("'$file' is not a valid file"))
    m = use_mmap ? Mmap.mmap(file) : Base.read(file)
    length(m) < 12 && throw(ArgumentError("'$file' is not in the feather format: total length of file = $(length(m))"))
    (m[1:4] == FEATHER_MAGIC_BYTES && m[end-3:end] == FEATHER_MAGIC_BYTES) ||
        throw(ArgumentError("'$file' is not in the feather format: header = $(m[1:4]), footer = $(m[end-3:end])"))
    # read file metadata using FlatBuffers
    metalength = Base.read(IOBuffer(m[length(m)-7:length(m)-4]), Int32)
    metapos = length(m) - (metalength + 7)
    rootpos = Base.read(IOBuffer(m[metapos:metapos+4]), Int32)
    ctable = FlatBuffers.read(Metadata.CTable, m, metapos + rootpos - 1)
    ctable.version < VERSION && warn("This Feather file is old and will not be readable beyond the 0.3.0 release")
    header = String[]
    types = DataType[]
    juliatypes = DataType[]
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
    # construct Data.Schema and Feather.Source
    return Source(file, Data.Schema(header, juliatypes, ctable.num_rows), ctable, m, types, levels, orders, Array{Any}(length(columns)))
end

# DataStreams interface
Data.schema(source::Feather.Source, ::Type{Data.Column}) = source.schema
Data.reference(source::Feather.Source) = source.data
function Data.isdone(io::Feather.Source, row, col)
    rows, cols = size(Data.schema(io))
    return col > cols || row > rows
end
Data.streamtype{T<:Feather.Source}(::Type{T}, ::Type{Data.Column}) = true
Data.streamtype{T<:Feather.Source}(::Type{T}, ::Type{Data.Field}) = true

vectortype{T}(::Type{T}) = Vector{T}
vectortype{T}(::Type{Nullable{T}}) = NullableVector{T}
vectortype{S, R}(::Type{CategoricalValue{S, R}}) = CategoricalVector{S, R}
vectortype{S, R}(::Type{Nullable{CategoricalValue{S, R}}}) = NullableCategoricalVector{S, R}

function Data.streamfrom{T}(source::Source, ::Type{Data.Field}, ::Type{T}, row, col)
    !isassigned(source.columns, col) && (source.columns[col] = Data.streamfrom(source, Data.Column, vectortype(T), col))
    return source.columns[col][row]::T
end

checknonull(source, col) = source.ctable.columns[col].values.null_count > 0 && throw(NullException)
getbools(s::Source, col) = s.ctable.columns[col].values.null_count == 0 ? zeros(Bool, s.ctable.num_rows) : Bool[getbit(s.data[s.ctable.columns[col].values.offset + bytes_for_bits(x)], mod1(x, 8)) for x = 1:s.ctable.num_rows]

function unwrap{T}(s::Source, ::Type{T}, col, rows, off=0)
    bitmask_bytes = s.ctable.columns[col].values.null_count > 0 ? Feather.getoutputlength(s.ctable.version, Feather.bytes_for_bits(s.ctable.num_rows)) : 0
    return unsafe_wrap(Array, convert(Ptr{T}, pointer(s.data) + s.ctable.columns[col].values.offset + bitmask_bytes + off), rows)::Vector{T}
end

transform!{T}(::Type{T}, A, len) = T[x for x in A]
transform!(::Type{Date}, A, len) = map(x->Arrow.unix2date(x), A)
transform!{P,Z}(::Type{DateTime}, A::Vector{Arrow.Timestamp{P,Z}}, len) = map(x->Arrow.unix2datetime(P, x), A)
transform!{T,R}(::Type{CategoricalValue{T,R}}, A, len) = map(x->x + R(1), A)
function transform!(::Type{Bool}, A, len)
    B = falses(len)
    Base.copy_chunks!(B.chunks, 1, map(x->x.value, A), 1, length(A) * 64)
    return convert(Vector{Bool}, B)
end

function Data.streamfrom{T}(source::Source, ::Type{Data.Column}, ::Type{T}, col)
    checknonull(source, col)
    A = unwrap(source, source.feathertypes[col], col, source.ctable.num_rows)
    return transform!(eltype(T), A, source.ctable.num_rows)::T
end
function Data.streamfrom{T}(source::Source, ::Type{Data.Column}, ::Type{NullableVector{T}}, col)
    A = transform!(T, unwrap(source, source.feathertypes[col], col, source.ctable.num_rows), source.ctable.num_rows)::Vector{T}
    bools = getbools(source, col)
    return NullableArray{T,1}(A, bools, source.data)
end
function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{Vector{Bool}}, col)
    checknonull(source, col)
    A = unwrap(source, source.feathertypes[col], col, max(1,div(bytes_for_bits(source.ctable.num_rows),8)))
    return transform!(Bool, A, source.ctable.num_rows)::Vector{Bool}
end
function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{NullableVector{Bool}}, col)
    bools = getbools(source, col)
    A = transform!(Bool, unwrap(source, source.feathertypes[col], col, max(1,div(bytes_for_bits(source.ctable.num_rows),8))), source.ctable.num_rows)
    return NullableArray{Bool, 1}(A, bools)
end
function Data.streamfrom{T <: AbstractString}(source::Source, ::Type{Data.Column}, ::Type{Vector{T}}, col)
    checknonull(source, col)
    offsets = unwrap(source, Int32, col, source.ctable.num_rows + 1)
    values = unwrap(source, UInt8, col, offsets[end], getoutputlength(source.ctable.version, sizeof(offsets)))
    return T[unsafe_string(pointer(values, offsets[i]+1), Int(offsets[i+1] - offsets[i])) for i = 1:source.ctable.num_rows]
end
function Data.streamfrom{T <: AbstractString}(source::Source, ::Type{Data.Column}, ::Type{NullableVector{T}}, col)
    bools = getbools(source, col)
    offsets = unwrap(source, Int32, col, source.ctable.num_rows + 1)
    values = unwrap(source, UInt8, col, offsets[end], getoutputlength(source.ctable.version, sizeof(offsets)))
    A = T[unsafe_string(pointer(values, offsets[i]+1), Int(offsets[i+1] - offsets[i])) for i = 1:source.ctable.num_rows]
    return NullableArray{T, 1}(A, bools)
end
function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{NullableVector{WeakRefString{UInt8}}}, col)
    offsets = Feather.unwrap(source, Int32, col, source.ctable.num_rows + 1)
    offset = source.ctable.columns[col].values.offset +
             (source.ctable.columns[col].values.null_count > 0 ? Feather.getoutputlength(source.ctable.version, Feather.bytes_for_bits(source.ctable.num_rows)) : 0) +
             getoutputlength(source.ctable.version, sizeof(offsets))
    A = [WeakRefString(pointer(source.data, offset + offsets[i]+1), Int(offsets[i+1] - offsets[i]), Int(offset + offsets[i]+1)) for i = 1:source.ctable.num_rows]
    bools = getbools(source, col)
    return NullableArray{WeakRefString{UInt8},1}(A, bools, source.data)
end
function Data.streamfrom{T,R}(source::Source, ::Type{Data.Column}, ::Type{CategoricalVector{T,R}}, col)
    checknonull(source, col)
    refs = transform!(CategoricalValue{T,R}, unwrap(source, R, col, source.ctable.num_rows), source.ctable.num_rows)
    pool = CategoricalPool{String, R}(source.levels[col], source.orders[col])
    return CategoricalArray{String,1,R}(refs, pool)
end
function Data.streamfrom{T,R}(source::Source, ::Type{Data.Column}, ::Type{NullableCategoricalVector{T,R}}, col)
    refs = transform!(CategoricalValue{T,R}, unwrap(source, R, col, source.ctable.num_rows), source.ctable.num_rows)
    bools = getbools(source, col)
    refs = R[ifelse(bools[i], R(0), refs[i]) for i = 1:source.ctable.num_rows]
    pool = CategoricalPool{String, R}(source.levels[col], source.orders[col])
    return NullableCategoricalArray{String,1,R}(refs, pool)
end

"""
`Feather.read{T <: Data.Sink}(file, sink_type::Type{T}, sink_args...; weakrefstrings::Bool=true)` => `T`

`Feather.read(file, sink::Data.Sink; weakrefstrings::Bool=true)` => `Data.Sink`

`Feather.read` takes a feather-formatted binary `file` argument and "streams" the data to the
provided `sink` argument, a `DataFrame` by default. A fully constructed `sink` can be provided as the 2nd argument (the 2nd method above),
or a Sink can be constructed "on the fly" by providing the type of Sink and any necessary positional arguments
(the 1st method above).

Keyword arguments:

  * `nullable::Bool=true`: will return columns as `NullableVector{T}` types by default, regarldess of # of null values. When set to `false`, columns without null values will be returned as regular `Vector{T}`
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
function read end

function read(file::AbstractString, sink=DataFrame, args...; nullable::Bool=true, weakrefstrings::Bool=true, use_mmap::Bool=true, append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(Source(file; nullable=nullable, weakrefstrings=weakrefstrings, use_mmap=use_mmap), sink, append, transforms, args...)
    Data.close!(sink)
    return sink
end

function read{T}(file::AbstractString, sink::T; nullable::Bool=true, weakrefstrings::Bool=true, use_mmap::Bool=true, append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(Source(file; nullable=nullable, weakrefstrings=weakrefstrings, use_mmap=use_mmap), sink, append, transforms)
    Data.close!(sink)
    return sink
end

read(source::Feather.Source, sink=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms, args...); Data.close!(sink); return sink)
read{T}(source::Feather.Source, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms); Data.close!(sink); return sink)

# writing feather files
feathertype{T}(::Type{T}) = Feather.julia2Type_[T]
feathertype{S,R}(::Type{CategoricalValue{S,R}}) = julia2Type_[R]
feathertype{P}(::Type{Arrow.Time{P}}) = Metadata.INT64
feathertype(::Type{Date}) = Metadata.INT32
feathertype(::Type{DateTime}) = Metadata.INT64
feathertype{T<:AbstractString}(::Type{T}) = Metadata.UTF8

getmetadata{T}(io, ::Type{T}, A) = nothing
getmetadata(io, ::Type{Date}, A) = Metadata.DateMetadata()
getmetadata{T}(io, ::Type{Arrow.Time{T}}, A) = Metadata.TimeMetadata(julia2TimeUnit[T])
getmetadata(io, ::Type{DateTime}, A) = Metadata.TimestampMetadata(julia2TimeUnit[Arrow.Millisecond], "")
function getmetadata{S,R}(io, T::Type{CategoricalValue{S,R}}, A)
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
    return Metadata.CategoryMetadata(Metadata.PrimitiveArray(Metadata.UTF8, Metadata.PLAIN, offset, len, 0, total_bytes), isordered(A))
end

values(A::Vector) = A
values(A::NullableVector) = A.values
values{S,R}(A::Union{CategoricalArray{S,1,R},NullableCategoricalArray{S,1,R}}) = map(x-> x - R(1), A.refs)
values(A::DataArray) = A.data

nullcount(A::NullableVector) = sum(A.isnull)
nullcount(A::Vector) = 0
nullcount(A::CategoricalArray) = 0
nullcount(A::NullableCategoricalArray) = sum(A.refs .== 0)
nullcount(A::DataArray) = sum(A.na)

# Bool
function writecolumn(io, ::Type{Bool}, A)
    return writepadded(io, view(reinterpret(UInt8, convert(BitVector, values(A)).chunks), 1:bytes_for_bits(length(A))))
end
# Category
function writecolumn{S,R}(io, ::Type{CategoricalValue{S,R}}, A)
    return writepadded(io, view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(R))))
end
# Date
function writecolumn(io, ::Type{Date}, A)
    return writepadded(io, view(reinterpret(UInt8, map(Arrow.date2unix, values(A))), 1:(length(A) * sizeof(Int32))))
end
# Timestamp
function writecolumn(io, ::Type{DateTime}, A)
    return writepadded(io, view(reinterpret(UInt8, map(Arrow.datetime2unix, values(A))), 1:(length(A) * sizeof(Int64))))
end
# other primitive T
function writecolumn{T}(io, ::Type{T}, A)
    return writepadded(io, view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(T))))
end
# List types
valuelength{T}(val::T) = length(String(val))
valuelength{T}(val::Nullable{T}) = isnull(val) ? 0 : length(get(val))

writevalue{T}(io, val::T) = Base.write(io, Vector{UInt8}(String(val)))
writevalue{T}(io, val::Nullable{T}) = isnull(val) ? 0 : Base.write(io, Vector{UInt8}(String(get(val))))

function writecolumn{T<:Union{Vector{UInt8},AbstractString}}(io, ::Type{T}, arr)
    len = length(arr)
    off = 0
    offsets = zeros(Int32, len + 1)
    for (ind, v) in enumerate(arr)
        off += Feather.valuelength(v)
        offsets[ind + 1] = off
    end
    total_bytes = Feather.writepadded(io, view(reinterpret(UInt8, offsets), 1:length(offsets) * sizeof(Int32)))
    total_bytes += offsets[len+1]
    for val in arr
        Feather.writevalue(io, val)
    end
    diff = Feather.paddedlength(offsets[len+1]) - offsets[len+1]
    if diff > 0
        Base.write(io, zeros(UInt8, diff))
        total_bytes += diff
    end
    return total_bytes
end

writenulls(io, A, null_count, len, total_bytes) = return total_bytes
function writenulls(io, A::NullableVector, null_count, len, total_bytes)
    # write out null bitmask
    if null_count > 0
        null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(map(!, A.isnull))
        total_bytes = writepadded(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
    end
    return total_bytes
end
function writenulls{T <: NullableCategoricalArray}(io, A::T, null_count, len, total_bytes)
    # write out null bitmask
    if null_count > 0
        null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(map(!, A.refs .== 0))
        total_bytes = writepadded(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
    end
    return total_bytes
end
function writenulls(io, A::DataArray, null_count, len, total_bytes)
    # write out null bitmask
    if null_count > 0
        null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(!A.na)
        total_bytes = writepadded(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
    end
    return total_bytes
end

"DataStreams Sink implementation for feather-formatted binary files"
type Sink <: Data.Sink
    ctable::Metadata.CTable
    file::String
    io::IOBuffer
    description::String
    metadata::String
    df::DataFrame
end

function renullify(A::NullableVector{WeakRefString{UInt8}})
    if !isempty(A.parent)
        parent = copy(A.parent)
        B = [WeakRefString(pointer(parent, x.ind), x.len, x.ind) for x in A.values]
        return NullableArray{WeakRefString{UInt8}, 1}(B, A.isnull, parent)
    else
        return A
    end
end

function Sink{T<:Data.StreamType}(file::AbstractString, schema::Data.Schema=Data.Schema(), ::Type{T}=Data.Column;
              description::AbstractString="", metadata::AbstractString="", append::Bool=false)
    if append && isfile(file)
        df = Feather.read(file)
        for i = 1:size(df, 2)
            if eltype(df.columns[i]) <: Nullable{WeakRefString{UInt8}}
                df.columns[i] = renullify(df.columns[i])
            end
        end
        schema.rows += size(df, 1)
    else
        df = DataFrame(schema, T)
    end
    io = IOBuffer()
    Feather.writepadded(io, FEATHER_MAGIC_BYTES)
    return Sink(Metadata.CTable("", 0, Metadata.Column[], VERSION, ""), file, io, description, metadata, df)
end

# DataStreams interface
function Sink{T}(sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8}, file::AbstractString; kwargs...)
    sink = Sink(file, sch, T; append=append, kwargs...)
    return sink
end

function Sink{T}(sink, sch::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8})
    if !append
        for col in sink.df.columns
            empty!(col)
        end
        sink.df = DataFrame(sch, T)
    else
        sch.rows += size(sink.df, 1)
    end
    return sink
end

Data.streamtypes{T<:Feather.Sink}(::Type{T}) = [Data.Column, Data.Field]

Data.streamto!{T}(sink::Feather.Sink, ::Type{Data.Field}, val::T, row, col, sch) = Data.streamto!(sink.df, Data.Field, val, row, col, sch)
Data.streamto!{T}(sink::Feather.Sink, ::Type{Data.Column}, column::T, row, col, sch) = Data.streamto!(sink.df, Data.Column, column, row, col, sch)

function Data.close!(sink::Feather.Sink)
    sch = Data.schema(sink.df)
    header = Data.header(sch)
    data = sink.df
    io = sink.io
    # write out arrays, building each array's metadata as we go
    rows = size(sch, 1)
    columns = Feather.Metadata.Column[]
    for (i, name) in enumerate(header)
        arr = data[i]
        total_bytes = 0
        offset = position(io)
        null_count = Feather.nullcount(arr)
        len = length(arr)
        total_bytes = Feather.writenulls(io, arr, null_count, len, total_bytes)
        # write out array values
        TT = eltype(arr) <: Nullable ? eltype(eltype(arr)) : eltype(arr)
        total_bytes += Feather.writecolumn(io, TT, arr)
        values = Feather.Metadata.PrimitiveArray(Feather.feathertype(TT), Feather.Metadata.PLAIN, offset, len, null_count, total_bytes)
        push!(columns, Feather.Metadata.Column(String(name), values, Feather.getmetadata(io, TT, arr), String("")))
    end
    # write out metadata
    ctable = Metadata.CTable(sink.description, rows, columns, VERSION, sink.metadata)
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
    return sink
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
function write end

function write{T}(io::AbstractString, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(T(args...), Feather.Sink, append, transforms, io; kwargs...)
    Data.close!(sink)
    return sink
end
function write(io::AbstractString, source; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(source, Feather.Sink, append, transforms, io; kwargs...)
    Data.close!(sink)
    return sink
end

write{T}(sink::Sink, ::Type{T}, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(T(args...), sink, append, transforms); Data.close!(sink); return sink)
write(sink::Sink, source; append::Bool=false, transforms::Dict=Dict{Int,Function}()) = (sink = Data.stream!(source, sink, append, transforms); Data.close!(sink); return sink)

end # module
