"""
package for reading/writing [feather-formatted binary files](https://github.com/wesm/feather).

As noted on the official feather homepage, the feather format is still considered "beta" and should not be relied on for
long-term storage/productions needs.

"""
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

using FlatBuffers, DataStreams, DataFrames, NullableArrays, WeakRefStrings

export Data, DataFrame

# sync with feather
const VERSION = 1

# flatbuffer defintions
include("metadata.jl")

# Arrow type definitions
include("Arrow.jl")

# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = "FEA1".data

ceil_byte(size) = (size + 7) & ~7
bytes_for_bits(size) = div(((size + 7) & ~7), 8)
const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
getbit(byte::UInt8, i) = (byte & BITMASK[i]) == 0

# wesm/feather/cpp/src/metadata_generated.h
# wesm/feather/cpp/src/types.h
"maps our Arrow/Feather types enum to final Arrow Julia eltypes"
const Type_2julia = Dict{Metadata.Type_,DataType}(
    Metadata.BOOL      => Bool,
    Metadata.INT8      => Int8,
    Metadata.INT16     => Int16,
    Metadata.INT32     => Int32,
    Metadata.INT64     => Int64,
    Metadata.UINT8     => UInt8,
    Metadata.UINT16    => UInt16,
    Metadata.UINT32    => UInt32,
    Metadata.UINT64    => UInt64,
    Metadata.FLOAT     => Float32,
    Metadata.DOUBLE    => Float64,
    Metadata.UTF8      => WeakRefString{UInt8},
    Metadata.BINARY    => Vector{UInt8},
    Metadata.CATEGORY  => Int64,
    Metadata.TIMESTAMP => Int64,
    Metadata.DATE      => Int64,
    Metadata.TIME      => Int64
)

# "maps Julia types to Arrow/Feather type enum values"
const julia2Type_ = Dict{DataType,Metadata.Type_}(
    Bool    => Metadata.BOOL,
    Int8    => Metadata.INT8,
    Int16   => Metadata.INT16,
    Int32   => Metadata.INT32,
    Int64   => Metadata.INT64,
    UInt8   => Metadata.UINT8,
    UInt16  => Metadata.UINT16,
    UInt32  => Metadata.UINT32,
    UInt64  => Metadata.UINT64,
    Float32 => Metadata.FLOAT,
    Float64 => Metadata.DOUBLE,
    String  => Metadata.UTF8,
    Vector{UInt8}   => Metadata.BINARY,
    DateTime   => Metadata.INT64,
    Date   => Metadata.INT32,
    WeakRefString{UInt8} => Metadata.UTF8
)

const NON_PRIMITIVE_TYPES = Set([Metadata.UTF8,Metadata.BINARY])
"whether an Arrow/Feather type is primitive or not (i.e. not represented by a List{UInt8})"
isprimitive(x::Metadata.Type_) = x in NON_PRIMITIVE_TYPES ? false : true

const TimeUnit2julia = Dict{Metadata.TimeUnit,DataType}(
    Metadata.SECOND => Arrow.Second,
    Metadata.MILLISECOND => Arrow.Millisecond,
    Metadata.MICROSECOND => Arrow.Microsecond,
    Metadata.NANOSECOND => Arrow.Nanosecond
)
const julia2TimeUnit = Dict{DataType,Metadata.TimeUnit}([(v, k) for (k,v) in TimeUnit2julia])

"""
Given a `meta` and `Metadata.Type_`, returns the storage Julia type
"""
function juliastoragetype end

juliastoragetype(meta::Void, values_type::Metadata.Type_, data) = Type_2julia[values_type]
function juliastoragetype(meta::Metadata.CategoryMetadata, values_type::Metadata.Type_, data)
    levelinfo = meta.levels
    len = levelinfo.length
    ptr = pointer(data) + levelinfo.offset
    offsets = unsafe_wrap(Array, convert(Ptr{Int32}, ptr), len+1)
    ptr += sizeof(offsets)
    levels = tuple(map(x->Symbol(unsafe_string(ptr + offsets[x], offsets[x+1] - offsets[x])), 1:len)...)
    return Arrow.Category{meta.ordered,Type_2julia[values_type],levels}
end
juliastoragetype(meta::Metadata.TimestampMetadata, values_type::Metadata.Type_, data) = Arrow.Timestamp{TimeUnit2julia[meta.unit],meta.timezone == "" ? :UTC : Symbol(meta.timezone)}
juliastoragetype(meta::Metadata.DateMetadata, values_type::Metadata.Type_, data) = Arrow.Date
juliastoragetype(meta::Metadata.TimeMetadata, values_type::Metadata.Type_, data) = Arrow.Time{TimeUnit2julia[meta.unit]}

juliatype{T<:Arrow.Timestamp}(::Type{T}) = DateTime
juliatype{T<:Arrow.Date}(::Type{T}) = Date
juliatype{T}(::Type{T}) = T

"""
`unwrap` creates a Julia array from a feather file column; performing any necessary conversions
"""
function unwrap end

unwrap(ptr, rows) = unsafe_wrap(Array, ptr, rows)
unwrap{T<:Arrow.Date}(ptr::Ptr{T}, rows) = map(x->Arrow.unix2date(x), unsafe_wrap(Array, ptr, rows))
unwrap{P,Z}(ptr::Ptr{Arrow.Timestamp{P,Z}}, rows) = map(x->Arrow.unix2datetime(P, x), unsafe_wrap(Array, ptr, rows))

# DataStreams interface types
"""
A `Feather.Source` implements the `DataStreams` interface for a feather-formatted binary file.
"""
type Source <: Data.Source
    schema::Data.Schema
    ctable::Metadata.CTable
    data::Vector{UInt8}
    feathertypes::Vector{DataType} # separate from the types in schema, since we need to convert between feather storage types & julia types
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
    for col in columns
        push!(header, col.name)
        push!(types, juliastoragetype(col.metadata, col.values.type_, m))
        jl = juliatype(types[end])
        push!(juliatypes, col.values.null_count == 0 ? jl : Nullable{jl})
    end
    # construct Data.Schema and Feather.Source
    return Source(Data.Schema(header, juliatypes, ctable.num_rows), ctable, m, types, Array{Any}(length(columns)))
end

# DataStreams interface
Data.reset!(io::Feather.Source) = nothing
function Data.isdone(io::Feather.Source, row, col)
    rows, cols = size(Data.schema(io))
    return col > cols && row > rows
end
Data.streamtype{T<:Feather.Source}(::Type{T}, ::Type{Data.Column}) = true
Data.streamtype{T<:Feather.Source}(::Type{T}, ::Type{Data.Field}) = true

function Data.getfield{T}(source::Feather.Source, ::Type{T}, row, col)
    if !isdefined(source.columns, col)
        source.columns[col] = Data.getcolumn(source, T, col)
    end
    return Date.getfield(source.columns[col], T, row)
end

function Data.getcolumn{T}(source::Source, ::Type{T}, i)
    rows = Int32(source.ctable.num_rows)
    columns = source.ctable.columns
    types = source.feathertypes
    m = source.data
    parent = source.data
    # create a corresponding Julia Vector for the feather array
    col = columns[i]
    typ = types[i]
    values = col.values
    values.null_count > 0 && throw(NullException)
    bitmask_bytes = 0
    if Feather.isprimitive(values.type_)
        # for primitive types, we can just "wrap" the feather pointer
        column = Feather.unwrap(convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes), rows)
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
    columns = source.ctable.columns
    types = source.feathertypes
    m = source.data
    parent = source.data # schema.metadata["parent"]
    # create a corresponding Julia NullableVector for each feather array
    col = columns[i]
    typ = types[i]
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
    if Feather.isprimitive(values.type_)
        # for primitive types, we can just "wrap" the feather pointer
        arr = Feather.unwrap(convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes), rows)
        column = NullableArray{eltype(arr),1}(arr, nulls, parent)
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
"get the Arrow/Feather enum type from an AbstractColumn"
function feathertype end

feathertype{T}(::Type{T}) = Feather.julia2Type_[T]
feathertype{O,I,T}(::Type{Arrow.Category{O,I,T}}) = julia2Type_[I]
feathertype{P}(::Type{Arrow.Time{P}}) = Metadata.INT64
feathertype(::Type{Date}) = Metadata.INT32
feathertype(::Type{DateTime}) = Metadata.INT64
feathertype{T<:AbstractString}(::Type{T}) = Metadata.UTF8

getmetadata{T}(io, ::Type{T}) = nothing
getmetadata(io, ::Type{Date}) = Metadata.DateMetadata()
getmetadata{T}(io, ::Type{Arrow.Time{T}}) = Metadata.TimeMetadata(julia2TimeUnit[T])
getmetadata(io, ::Type{DateTime}) = Metadata.TimestampMetadata(julia2TimeUnit[Arrow.Millisecond], "")
function getmetadata{O,I,T}(io, ::Type{Arrow.Category{O,I,T}})
    len = length(T)
    offsets = zeros(Int32, len+1)
    values = map(string, T)
    offsets[1] = off = 0
    for (i,v) in enumerate(values)
        off += length(v)
        offsets[i + 1] = off
    end
    offset = position(io)
    total_bytes = Base.write(io, view(reinterpret(UInt8, offsets), 1:(sizeof(Int32) * (len + 1))))
    total_bytes += Base.write(io, collect(values))
    return Metadata.CategoryMetadata(Metadata.PrimitiveArray(julia2Type_[I], Metadata.PLAIN, offset, len, 0, total_bytes), O)
end

values(A::Vector) = A
values(A::NullableVector) = A.values

# Category
function writecolumn{O,I,T}(io, ::Type{Arrow.Category{O,I,T}}, o, b, A)
    return Base.write(io, Base.view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(I))))
end
# Date
function writecolumn(io, ::Type{Date}, o, b, A)
    return Base.write(io, Base.view(reinterpret(UInt8, map(Arrow.date2unix, values(A))), 1:(length(A) * sizeof(Int32))))
end
# Timestamp
function writecolumn(io, ::Type{DateTime}, o, b, A)
    return Base.write(io, Base.view(reinterpret(UInt8, map(Arrow.datetime2unix, values(A))), 1:(length(A) * sizeof(Int64))))
end
# Date, Timestamp, Time and other primitive T
function writecolumn{T}(io, ::Type{T}, o, b, A)
    return Base.write(io, Base.view(reinterpret(UInt8, values(A)), 1:(length(A) * sizeof(T))))
end
# List types
valuelength{T}(val::T) = length(val)
valuelength{T}(val::Nullable{T}) = isnull(val) ? 0 : length(get(val))
writevalue{T}(io, val::T) = Base.write(io, string(val).data)
writevalue{T}(io, val::Nullable{T}) = isnull(val) ? 0 : Base.write(io, string(val).data)

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
    writeoffset && Base.write(io, Base.view(reinterpret(UInt8, offsets), 1:length(offsets) * sizeof(Int32)))
    total_bytes += offsets[len+1]
    for val in arr
        writevalue(io, val)
    end
    return total_bytes
end

nullcount(A::NullableVector) = sum(A.isnull)
nullcount(A::Vector) = 0
writenulls(io, A::Vector, null_count, len, total_bytes) = return total_bytes
function writenulls(io, A::NullableVector, null_count, len, total_bytes)
    # write out null bitmask
    if null_count > 0
        total_bytes += null_bytes = Feather.bytes_for_bits(len)
        bytes = BitArray(!A.isnull)
        Base.write(io, Base.view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
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
        null_count = nullcount(arr)
        len = length(arr)
        total_bytes = writenulls(io, arr, null_count, len, total_bytes)
        # write out array values
        TT = eltype(arr) <: Nullable ? eltype(eltype(arr)) : eltype(arr)
        total_bytes += writecolumn(io, TT, Int32[], true, arr)
        values = Metadata.PrimitiveArray(feathertype(TT), Metadata.PLAIN, offset, len, null_count, total_bytes)
        push!(columns, Metadata.Column(String(name), values, getmetadata(io, TT), String("")))
    end
    # write out metadata
    ctable = Metadata.CTable(sink.description, rows, columns, VERSION, sink.metadata)
    meta = FlatBuffers.build!(ctable)
    rng = (meta.head + 1):length(meta.bytes)
    Base.write(io, Base.view(meta.bytes, rng))
    # write out metadata size
    Base.write(io, Int32(length(rng)))
    # write out final magic bytes
    Base.write(io, FEATHER_MAGIC_BYTES)
    flush(io)
    sink.schema = sch
    sink.ctable = ctable
    return sink
end

function Data.stream!(dfs::Vector{DataFrame}, sink::Sink; uniontype="includeall")
    if isa(uniontype, DataFrame)
        header = [(a, b) for (a,b) in zip(Data.header(uniontype), Data.types(uniontype))]
    elseif uniontype == "includeall"
        header = Pair{String,DataType}[]
        for df in dfs
            header = union(header, [(a, b) for (a,b) in zip(Data.header(df), Data.types(df))])
        end
    elseif uniontype == "includematches"
        header = [(a, b) for (a,b) in zip(Data.header(dfs[1]), Data.types(dfs[1]))]
        for i = 2:length(dfs)
            header = intersect(header, [(a, b) for (a,b) in zip(Data.header(dfs[i]), Data.types(dfs[i]))])
        end
    end
    io = sink.io
    # write out arrays, building each array's metadata as we go
    columns = Feather.Metadata.Column[]
    rows = 0
    for (name,T) in header
        offset = position(io)
        nulls = BitVector()
        offsets = Int32[]
        buf = IOBuffer()
        rows = 0
        for df in dfs
            rows += size(df, 1)
            if haskey(df, Symbol(name))
                arr = df[Symbol(name)]
                append!(nulls, BitArray(!arr.isnull))
                Feather.writecolumn(buf, offsets, false, arr)
            else
                len = size(df, 1)
                append!(nulls, falses(len))
                Feather.writecolumn(buf, offsets, false, NullableArray(T, len))
            end
        end
        null_count = sum(nulls)
        total_bytes = 0
        total_bytes += null_bytes = Feather.bytes_for_bits(rows)
        Base.write(io, Base.view(reinterpret(UInt8, nulls.chunks), 1:null_bytes))
        if !isempty(offsets)
            total_bytes += Base.write(io, offsets)
        end
        total_bytes += Base.write(io, takebuf_array(buf))
        values = Metadata.PrimitiveArray(feathertype(T), Metadata.PLAIN, offset, rows, null_count, total_bytes)
        push!(columns, Metadata.Column(String(name), values, getmetadata(io, T), String("")))
    end
    # write out metadata
    ctable = Metadata.CTable(sink.description, rows, columns, VERSION, sink.metadata)
    meta = FlatBuffers.build!(ctable)
    rng = (meta.head + 1):length(meta.bytes)
    Base.write(io, Base.view(meta.bytes, rng))
    # write out metadata size
    Base.write(io, Int32(length(rng)))
    # write out final magic bytes
    Base.write(io, FEATHER_MAGIC_BYTES)
    flush(io)
    sink.schema = Data.Schema(String[x[1] for x in header], DataType[x[2] for x in header], rows)
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
