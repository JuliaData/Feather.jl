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
    Metadata.UTF8      => WeakRefString{UInt8}, # List
    Metadata.BINARY    => Vector{UInt8}, # List
    Metadata.CATEGORY  => Int64,
    Metadata.TIMESTAMP => Int64,
    Metadata.DATE      => Int64,
    Metadata.TIME      => Int64
)

# "maps Julia types to Arrow/Feather type enum values"
# const julia2Type_ = Dict{DataType,Metadata.Type_}([v=>k for (k,v) in Type_2julia])
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
    String  => Metadata.UTF8, # List
    Vector{UInt8}   => Metadata.BINARY, # List
    DateTime   => Metadata.INT64,
    Date   => Metadata.INT32,
)

const NON_PRIMITIVE_TYPES = Set(Metadata.Type_[Metadata.UTF8,Metadata.BINARY])
"whether a Arrow/Feather type is primitive or not (i.e. not represented by a List{UInt8})"
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
`unwrap` creates a Julia array from a feather file; performing any necessary conversions
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
        push!(juliatypes, juliatype(types[end]))
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
    return source.columns[col][row]
end

function Data.getcolumn{T}(source::Source, ::Type{T}, i)
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
        arr = WeakRefString{UInt8}[WeakRefString(pointer(values,offsets[i]+1), Int(offsets[i+1] - offsets[i])) for i = 1:rows]
        column = NullableArray{WeakRefString{UInt8},1}(arr, nulls, parent)
    end
    return column
end

"read a feather-formatted binary file into a Julia DataFrame"
read(file::AbstractString, sink=DataFrame) = Data.stream!(Source(file), sink)

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

# Category
function writecolumn{O,I,T}(io, o, b, A::AbstractVector{Nullable{Arrow.Category{O,I,T}}})
    return Base.write(io, Base.view(reinterpret(UInt8, A.values), 1:(length(A) * sizeof(I))))
end
# Date
function writecolumn(io, o, b, A::AbstractVector{Nullable{Date}})
    return Base.write(io, Base.view(reinterpret(UInt8, map(Arrow.date2unix, A.values)), 1:(length(A) * sizeof(Int32))))
end
# Timestamp
function writecolumn(io, o, b, A::AbstractVector{Nullable{DateTime}})
    return Base.write(io, Base.view(reinterpret(UInt8, map(Arrow.datetime2unix, A.values)), 1:(length(A) * sizeof(Int64))))
end
# Date, Timestamp, Time and other primitive T
function writecolumn{T}(io, o, b, A::AbstractVector{Nullable{T}})
    return Base.write(io, Base.view(reinterpret(UInt8, A.values), 1:(length(A) * sizeof(T))))
end
# List types
function writecolumn{T<:Union{Vector{UInt8},AbstractString}}(io, offsets, writeoffset, arr::AbstractVector{Nullable{T}})
    len = length(arr)
    off = isempty(offsets) ? 0 : offsets[end]
    ind = isempty(offsets) ? 1 : length(offsets)
    total_bytes = sizeof(Int32) * (isempty(offsets) ? len + 1 : len)
    append!(offsets, zeros(Int32, isempty(offsets) ? len + 1 : len))
    offsets[1] = isempty(offsets) ? 0 : offsets[1]
    for v in arr
        off += isnull(v) ? 0 : length(get(v))
        offsets[ind + 1] = off
        ind += 1
    end
    writeoffset && Base.write(io, Base.view(reinterpret(UInt8, offsets), 1:length(offsets) * sizeof(Int32)))
    total_bytes += offsets[len+1]
    for val in arr
        isnull(val) && continue
        # might be String, WeakRefString
        v = convert(String, get(val))
        Base.write(io, v.data)
    end
    return total_bytes
end

"DataStreams Sink implementation for feather-formatted binary files"
type Sink{I<:IO} <: Data.Sink
    schema::Data.Schema
    ctable::Metadata.CTable
    io::I
    description::String
    metadata::String
end

function Sink(file::AbstractString; description::AbstractString=String(""), metadata::AbstractString=String(""))
    io = open(file, "w")
    Base.write(io, FEATHER_MAGIC_BYTES)
    return Sink(Data.EMPTYSCHEMA, Metadata.CTable("",0,Metadata.Column[],VERSION,""), io, description, metadata)
end

# DataStreams interface
Data.streamtypes{T<:Feather.Sink}(::Type{T}) = [Data.Column]

function Data.stream!(source, ::Type{Data.Field}, sink::Feather.Sink)
    df = Data.stream!(source, DataFrame)
    return Data.stream!(df, sink)
end

function Data.stream!(source, ::Type{Data.Column}, sink::Feather.Sink)
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
        null_count = sum(arr.isnull)
        len = length(arr)
        # write out null bitmask
        if null_count > 0
            total_bytes += null_bytes = Feather.bytes_for_bits(len)
            bytes = BitArray(!arr.isnull)
            Base.write(io, Base.view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
        end
        # write out array values
        total_bytes += writecolumn(io, Int32[], true, arr)
        TT = eltype(eltype(arr))
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
    close(io)
    sink.schema = sch
    sink.ctable = ctable
    return sink
end

function Data.stream!(dfs::Vector{DataFrame}, sink::Sink; uniontype="includeall")
    if isa(uniontype, DataFrame)
        header = [(a, b) for (a,b) in zip(Data.header(uniontype),Data.types(uniontype))]
    elseif uniontype == "includeall"
        header = Pair{String,DataType}[]
        for df in dfs
            header = union(header, [(a, b) for (a,b) in zip(Data.header(df),Data.types(df))])
        end
    elseif uniontype == "includematches"
        header = [(a, b) for (a,b) in zip(Data.header(dfs[1]),Data.types(dfs[1]))]
        for i = 2:length(dfs)
            header = intersect(header, [(a, b) for (a,b) in zip(Data.header(dfs[i]),Data.types(dfs[i]))])
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
    close(io)
    sink.schema = Data.Schema(String[x[1] for x in header], DataType[x[2] for x in header], rows)
    sink.ctable = ctable
    return sink
end

"write a Julia DataFrame to a feather-formatted binary file"
function write(file::AbstractString, df::DataFrame; description::AbstractString=String(""), metadata::AbstractString=String(""))
    sink = Sink(file; description=description, metadata=metadata)
    return Data.stream!(df, sink)
end

function write(file::AbstractString, dfs::DataFrame...; uniontype="includeall", description::AbstractString=String(""), metadata::AbstractString=String(""))
    sink = Sink(file; description=description, metadata=metadata)
    return Data.stream!(collect(dfs), sink; uniontype=uniontype)
end

end # module
