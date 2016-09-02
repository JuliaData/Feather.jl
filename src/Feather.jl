module Feather

if !isdefined(Core, :String)
    typealias String UTF8String
end
if !isdefined(Base, :view)
    view = sub
end
if Base.VERSION < v"0.5.0-dev+4631"
    unsafe_wrap{A<:Array}(::Type{A}, ptr, len) = pointer_to_array(ptr, len)
    unsafe_string(ptr, len) = utf8(ptr, len)
end

using FlatBuffers, DataStreams, DataFrames, NullableArrays, CategoricalArrays, WeakRefStrings

export Data, DataFrame

# sync with feather
const VERSION = 1

# Arrow type definitions
include("Arrow.jl")

# flatbuffer defintions
include("metadata.jl")

# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = "FEA1".data

ceil_byte(size) = (size + 7) & ~7
bytes_for_bits(size) = div(((size + 7) & ~7), 8)
const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
getbit(byte::UInt8, i) = (byte & BITMASK[i]) == 0

juliastoragetype(meta::Void, values_type) = Type_2julia[values_type]
function juliastoragetype(meta::Metadata.CategoryMetadata, values_type)
    R = Type_2julia[values_type]
    return meta.ordered ? OrdinalValue{String,R} : NominalValue{String,R}
end
juliastoragetype(meta::Metadata.TimestampMetadata, values_type) = Arrow.Timestamp{TimeUnit2julia[meta.unit],meta.timezone == "" ? :UTC : Symbol(meta.timezone)}
juliastoragetype(meta::Metadata.DateMetadata, values_type) = Arrow.Date
juliastoragetype(meta::Metadata.TimeMetadata, values_type) = Arrow.Time{TimeUnit2julia[meta.unit]}

juliatype{T<:Arrow.Timestamp}(::Type{T}) = DateTime
juliatype{T<:Arrow.Date}(::Type{T}) = Date
juliatype{T}(::Type{T}) = T

# get an array from a pointer, performing any necessary transformation from feather => julia
unwrap(ptr, rows, copy) = (A = unsafe_wrap(Array, ptr, rows); return copy ? [x for x in A] : A)
unwrap{T<:Arrow.Date}(ptr::Ptr{T}, rows, copy) = map(x->Arrow.unix2date(x), unsafe_wrap(Array, ptr, rows))
unwrap{P,Z}(ptr::Ptr{Arrow.Timestamp{P,Z}}, rows, copy) = map(x->Arrow.unix2datetime(P, x), unsafe_wrap(Array, ptr, rows))
unwrap{R}(ptr::Union{Ptr{NominalValue{String,R}},Ptr{OrdinalValue{String,R}}}, rows, copy) = map(x->x + R(1), unsafe_wrap(Array, convert(Ptr{R}, ptr), rows))

addlevels!{T}(::Type{T}, levels, i, meta, values_type, data) = return
function addlevels!{T <: CategoricalArrays.CategoricalValue}(::Type{T}, catlevels, i, meta, values_type, data)
    ptr = pointer(data) + meta.levels.offset
    offsets = unsafe_wrap(Array, convert(Ptr{Int32}, ptr), meta.levels.length + 1)
    ptr += sizeof(offsets)
    catlevels[i] = map(x->unsafe_string(ptr + offsets[x], offsets[x+1] - offsets[x]), 1:meta.levels.length)
    return
end

function factorize!{R}(::Type{NominalValue{String,R}}, ptr, rows, levels, bools)
    refs = unwrap(ptr, rows, true)
    pool = NominalPool{String, R}(levels)
    return NominalArray{String,1,R}(refs, pool)
end
function factorize!{R}(::Type{OrdinalValue{String,R}}, ptr, rows, levels, bools)
    refs = unwrap(ptr, rows, true)
    pool = OrdinalPool{String, R}(levels)
    return OrdinalArray{String,1,R}(refs, pool)
end
function factorize!{R}(::Type{Nullable{NominalValue{String,R}}}, ptr, rows, levels, bools)
    refs = unwrap(ptr, rows, true)
    refs = R[ifelse(bools[i], R(0), refs[i]) for i = 1:rows]
    pool = NominalPool{String, R}(levels)
    return NullableNominalArray{String,1,R}(refs, pool)
end
function factorize!{R}(::Type{Nullable{OrdinalValue{String,R}}}, ptr, rows, levels, bools)
    refs = unwrap(ptr, rows, true)
    refs = R[ifelse(bools[i], R(0), refs[i]) for i = 1:rows]
    pool = OrdinalPool{String, R}(levels)
    return NullableOrdinalArray{String,1,R}(refs, pool)
end

# DataStreams interface types
type Source <: Data.Source
    schema::Data.Schema
    ctable::Metadata.CTable
    data::Vector{UInt8}
    feathertypes::Vector{DataType} # separate from the types in schema, since we need to convert between feather storage types & julia types
    levels::Dict{Int,Vector{String}}
    columns::Vector{Any} # holds references to pre-fetched columns for Data.getfield
end

# reading feather files
function Source(file::AbstractString)
    # validity checks
    isfile(file) || throw(ArgumentError("'$file' is not a valid file"))
    m = Mmap.mmap(file)
    length(m) < 12 && throw(ArgumentError("'$file' is not in the feather format"))
    (m[1:4] == FEATHER_MAGIC_BYTES && m[end-3:end] == FEATHER_MAGIC_BYTES) ||
        throw(ArgumentError("'$file' is not in the feather format"))
    # read file metadata using FlatBuffers
    metalength = Base.read(IOBuffer(m[length(m)-7:length(m)-4]), Int32)
    metapos = length(m) - (metalength + 7)
    rootpos = Base.read(IOBuffer(m[metapos:metapos+4]), Int32)
    ctable = FlatBuffers.read(Metadata.CTable, m, metapos + rootpos - 1)
    header = String[]
    types = DataType[]
    juliatypes = DataType[]
    columns = ctable.columns
    levels = Dict{Int,Vector{String}}()
    for (i, col) in enumerate(columns)
        push!(header, col.name)
        push!(types, juliastoragetype(col.metadata, col.values.type_))
        jl = juliatype(types[end])
        addlevels!(jl, levels, i, col.metadata, col.values.type_, m)
        push!(juliatypes, col.values.null_count == 0 ? jl : Nullable{jl})
    end
    # construct Data.Schema and Feather.Source
    return Source(Data.Schema(header, juliatypes, ctable.num_rows), ctable, m, types, levels, Array{Any}(length(columns)))
end

# DataStreams interface
function Data.isdone(io::Feather.Source, row, col)
    rows, cols = size(Data.schema(io))
    return col > cols || row > rows
end
Data.streamtype{T<:Feather.Source}(::Type{T}, ::Type{Data.Column}) = true
Data.streamtype{T<:Feather.Source}(::Type{T}, ::Type{Data.Field}) = true

function Data.getfield{T}(source::Feather.Source, ::Type{T}, row, col)
    if !isdefined(source.columns, col)
        source.columns[col] = Data.getcolumn(source, T, col)
    end
    return source.columns[col][row]
end

function Data.getcolumn{T}(source::Source, ::Type{T}, i)
    rows = Int32(source.ctable.num_rows)
    m = source.data
    # create a corresponding Julia Vector for the feather array
    col = source.ctable.columns[i]
    values = col.values
    typ = source.feathertypes[i]
    values.null_count > 0 && throw(NullException)
    bitmask_bytes = 0
    if T <: CategoricalArrays.CategoricalValue
        column = Feather.factorize!(T, convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes), rows, source.levels[i], Bool[])
    elseif Feather.isprimitive(values.type_)
        # for primitive types, we can just "wrap" the feather pointer
        column = Feather.unwrap(convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes), rows, true)
    else
        # for string types, we need to manually construct based on each elements length
        ptr = pointer(m) + values.offset + bitmask_bytes
        offsets = unsafe_wrap(Array, convert(Ptr{Int32}, ptr), rows+1)
        values = unsafe_wrap(Array, ptr + sizeof(offsets), offsets[end])
        arr = WeakRefString{UInt8}[WeakRefString(pointer(values,offsets[i]+1), Int(offsets[i+1] - offsets[i])) for i = 1:rows]
        column = [string(x) for x in arr]
    end
    return column
end
function Data.getcolumn{T}(source::Source, ::Type{Nullable{T}}, i)
    rows = Int32(source.ctable.num_rows)
    m = source.data
    # create a corresponding Julia NullableVector for the feather array
    col = source.ctable.columns[i]
    typ = source.feathertypes[i]
    values = col.values
    if values.null_count > 0
        # read feather null bitarray
        bitmask_bytes = Feather.bytes_for_bits(rows)
        nulls = zeros(Bool, rows)
        for x = 1:rows
            nulls[x] = Feather.getbit(m[values.offset + Feather.bytes_for_bits(x)], mod1(x, 8))
        end
    else
        bitmask_bytes = 0
        nulls = zeros(Bool, rows)
    end
    if T <: CategoricalArrays.CategoricalValue
        column = Feather.factorize!(Nullable{T}, convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes), rows, source.levels[i], nulls)
    elseif Feather.isprimitive(values.type_)
        # for primitive types, we can just "wrap" the feather pointer
        arr = Feather.unwrap(convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes), rows, false)
        column = NullableArray{eltype(arr),1}(arr, nulls, m)
    else
        # for string types, we need to manually construct based on each elements length
        ptr = pointer(m) + values.offset + bitmask_bytes
        offsets = unsafe_wrap(Array, convert(Ptr{Int32}, ptr), rows+1)
        values = unsafe_wrap(Array, ptr + sizeof(offsets), offsets[end])
        arr = WeakRefString{UInt8}[WeakRefString(pointer(values, offsets[i] + 1), Int(offsets[i + 1] - offsets[i])) for i = 1:rows]
        column = NullableArray{WeakRefString{UInt8},1}(arr, nulls, parent)
    end
    return column
end

"""
`Feather.read{T <: Data.Sink}(file, sink_type::Type{T}, sink_args...)` => `T`
`Feather.read(file, sink::Data.Sink)` => `Data.Sink`

`Feather.read` takes a feather-formatted binary `file` argument and "streams" the data to the
provided `sink` argument. A fully constructed `sink` can be provided as the 2nd argument (the 2nd method above),
or a Sink can be constructed "on the fly" by providing the type of Sink and any necessary positional arguments
(the 1st method above). By default, a `DataFrame` is returned.

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

function read(file::AbstractString, sink=DataFrame, args...; append::Bool=false)
    source = Source(file)
    return Data.stream!(source, sink, append, args...)
end

function read{T}(file::AbstractString, sink::T; append::Bool=false)
    source = Source(file)
    return Data.stream!(source, sink, append)
end

read(source::Feather.Source, sink=DataFrame, args...; append::Bool=false) = Data.stream!(source, sink, append, args...)
read{T}(source::Feather.Source, sink::T; append::Bool=false) = Data.stream!(source, sink, append)

# writing feather files
feathertype{T}(::Type{T}) = Feather.julia2Type_[T]
feathertype{S,R}(::Union{Type{NominalValue{S,R}},Type{OrdinalValue{S,R}}}) = julia2Type_[R]
feathertype{P}(::Type{Arrow.Time{P}}) = Metadata.INT64
feathertype(::Type{Date}) = Metadata.INT32
feathertype(::Type{DateTime}) = Metadata.INT64
feathertype{T<:AbstractString}(::Type{T}) = Metadata.UTF8

getmetadata{T}(io, ::Type{T}, A) = nothing
getmetadata(io, ::Type{Date}, A) = Metadata.DateMetadata()
getmetadata{T}(io, ::Type{Arrow.Time{T}}, A) = Metadata.TimeMetadata(julia2TimeUnit[T])
getmetadata(io, ::Type{DateTime}, A) = Metadata.TimestampMetadata(julia2TimeUnit[Arrow.Millisecond], "")
function getmetadata{S,R}(io, T::Union{Type{NominalValue{S,R}},Type{OrdinalValue{S,R}}}, A)
    lvls = CategoricalArrays.levels(A)
    len = length(lvls)
    offsets = zeros(Int32, len+1)
    offsets[1] = off = 0
    for (i,v) in enumerate(lvls)
        off += length(v)
        offsets[i + 1] = off
    end
    offset = position(io)
    total_bytes = Base.write(io, view(reinterpret(UInt8, offsets), 1:(sizeof(Int32) * (len + 1))))
    total_bytes += Base.write(io, lvls)
    return Metadata.CategoryMetadata(Metadata.PrimitiveArray(julia2Type_[R], Metadata.PLAIN, offset, len, 0, total_bytes), T <: OrdinalValue)
end

values(A::Vector) = A
values(A::NullableVector) = A.values
values{S,R}(A::Union{NominalArray{S,1,R},OrdinalArray{S,1,R},NullableNominalArray{S,1,R},NullableOrdinalArray{S,1,R}}) = map(x-> x - R(1), A.refs)

# Category
function writecolumn{S,R}(io, ::Union{Type{NominalValue{S,R}},Type{OrdinalValue{S,R}}}, o, b, A)
    return Base.write(io, view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(R))))
end
# Date
function writecolumn(io, ::Type{Date}, o, b, A)
    return Base.write(io, view(reinterpret(UInt8, map(Arrow.date2unix, values(A))), 1:(length(A) * sizeof(Int32))))
end
# Timestamp
function writecolumn(io, ::Type{DateTime}, o, b, A)
    return Base.write(io, view(reinterpret(UInt8, map(Arrow.datetime2unix, values(A))), 1:(length(A) * sizeof(Int64))))
end
# Date, Timestamp, Time and other primitive T
function writecolumn{T}(io, ::Type{T}, o, b, A)
    return Base.write(io, view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(T))))
end
# List types
valuelength{T}(val::T) = length(String(val))
valuelength{T}(val::Nullable{T}) = isnull(val) ? 0 : length(get(val))

writevalue{T}(io, val::T) = Base.write(io, String(val).data)
writevalue{T}(io, val::Nullable{T}) = isnull(val) ? 0 : Base.write(io, String(get(val)).data)

function writecolumn{T<:Union{Vector{UInt8},AbstractString}}(io, ::Type{T}, offsets, writeoffset, arr)
    len = length(arr)
    off = isempty(offsets) ? 0 : offsets[end]
    ind = isempty(offsets) ? 1 : length(offsets)
    total_bytes = sizeof(Int32) * (isempty(offsets) ? len + 1 : len)
    append!(offsets, zeros(Int32, isempty(offsets) ? len + 1 : len))
    offsets[1] = isempty(offsets) ? 0 : offsets[1]
    for v in arr
        off += valuelength(v)
        offsets[ind + 1] = off
        ind += 1
    end
    writeoffset && Base.write(io, view(reinterpret(UInt8, offsets), 1:length(offsets) * sizeof(Int32)))
    total_bytes += offsets[len+1]
    for val in arr
        writevalue(io, val)
    end
    return total_bytes
end

writenulls(io, A, null_count, len, total_bytes) = return total_bytes
function writenulls(io, A::NullableVector, null_count, len, total_bytes)
    # write out null bitmask
    if null_count > 0
        total_bytes += null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(!A.isnull)
        Base.write(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
    end
    return total_bytes
end
function writenulls{T <: Union{NullableNominalArray,NullableOrdinalArray}}(io, A::T, null_count, len, total_bytes)
    # write out null bitmask
    if null_count > 0
        total_bytes += null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(!(A.refs .== 0))
        Base.write(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
    end
    return total_bytes
end

"DataStreams Sink implementation for feather-formatted binary files"
type Sink <: Data.Sink
    schema::Data.Schema
    ctable::Metadata.CTable
    io::IO
    description::String
    metadata::String
end

function Sink(file::Union{IO,AbstractString}; description::AbstractString=String(""), metadata::AbstractString=String(""))
    io = isa(file, AbstractString) ? open(file, "w") : file
    Base.write(io, FEATHER_MAGIC_BYTES)
    return Sink(Data.EMPTYSCHEMA, Metadata.CTable("", 0, Metadata.Column[], VERSION, ""), io, description, metadata)
end

Base.close(s::Sink) = (applicable(close, s.io) && close(s.io); return nothing)
Base.flush(s::Sink) = (applicable(flush, s.io) && flush(s.io); return nothing)

# DataStreams interface
function Sink{T}(source, ::Type{T}, append::Bool, file::AbstractString)
    sink = Sink(file)
    # currently doesn't support appending to existing Sink
    return sink
end
function Sink{T}(sink, source, ::Type{T}, append::Bool)
    # currently doesn't support appending to existing Sink
    return sink
end

Data.streamtypes{T<:Feather.Sink}(::Type{T}) = [Data.Column]

function Data.stream!(source, ::Type{Data.Field}, sink::Feather.Sink)
    df = Data.stream!(source, DataFrame)
    return Data.stream!(df, sink)
end

function Data.stream!(source, ::Type{Data.Column}, sink::Feather.Sink, append::Bool=false)
    sch = Data.schema(source)
    header = Data.header(sch)
    types = Data.types(sch)
    # data = df.columns
    io = sink.io
    # write out arrays, building each array's metadata as we go
    rows = size(sch, 1)
    columns = Metadata.Column[]
    for (i, name) in enumerate(header)
        @inbounds T = types[i]
        arr = Data.getcolumn(source, T, i)
        total_bytes = 0
        offset = position(io)
        null_count = Data.nullcount(arr)
        len = length(arr)
        total_bytes = writenulls(io, arr, null_count, len, total_bytes)
        # write out array values
        TT = eltype(arr) <: Nullable ? eltype(eltype(arr)) : eltype(arr)
        total_bytes += writecolumn(io, TT, Int32[], true, arr)
        values = Metadata.PrimitiveArray(feathertype(TT), Metadata.PLAIN, offset, len, null_count, total_bytes)
        push!(columns, Metadata.Column(String(name), values, getmetadata(io, TT, arr), String("")))
    end
    # write out metadata
    ctable = Metadata.CTable(sink.description, rows, columns, VERSION, sink.metadata)
    meta = FlatBuffers.build!(ctable)
    rng = (meta.head + 1):length(meta.bytes)
    Base.write(io, view(meta.bytes, rng))
    # write out metadata size
    Base.write(io, Int32(length(rng)))
    # write out final magic bytes
    Base.write(io, FEATHER_MAGIC_BYTES)
    flush(io)
    sink.schema = sch
    sink.ctable = ctable
    return sink
end

"""
`Feather.write{T <: Data.Source}(io, source::Type{T}, source_args...)` => `Feather.Sink`
`Feather.write(io, source::Data.Source)` => `Feather.Sink`

Write a `Data.Source` out to disk as a feather-formatted binary file. The two methods allow the passing of a
fully constructed `Data.Source` (2nd method), or the type of Source and any necessary positional arguments (1st method).

Examples:

```julia
df = DataFrame(...)
Feather.write("shiny_new_feather_file.feather", df)

Feather.write("sqlite_query_result.feather", SQLite.Source, "select * from cool_table")
```
"""
function write end

function write{T}(io::Union{AbstractString,IO}, ::Type{T}, args...; # append::Bool=false,
                    description::AbstractString=String(""), metadata::AbstractString=String(""))
    source = T(args...)
    sink = Sink(io; description=description, metadata=metadata)
    Data.stream!(source, sink, false) # append)
    close(sink)
    return sink
end
function write(io::Union{AbstractString,IO}, source; # append::Bool=false,
                    description::AbstractString=String(""), metadata::AbstractString=String(""))
    sink = Sink(io; description=description, metadata=metadata)
    Data.stream!(source, sink, false) # append)
    close(sink)
    return sink
end

write{T}(sink::Sink, ::Type{T}, args...; append::Bool=false) = Data.stream!(T(args...), sink, append)
write(sink::Sink, source; append::Bool=false) = Data.stream!(source, sink, append)

end # module
