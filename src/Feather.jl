"""
package for reading/writing [feather-formatted binary files](https://github.com/wesm/feather) and loading into a Julia DataFrame.

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

export Data

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
    Metadata.UTF8      => UInt8, # List
    Metadata.BINARY    => UInt8, # List
    Metadata.CATEGORY  => Int64,
    Metadata.TIMESTAMP => Int64,
    Metadata.DATE      => Int64,
    Metadata.TIME      => Int64
)

const NON_PRIMITIVE_TYPES = Set(Metadata.Type_[Metadata.UTF8,Metadata.BINARY])
"whether a Arrow/Feather type is primitive or not (i.e. not represented by a List{UInt8})"
isprimitive(x::Metadata.Type_) = x in NON_PRIMITIVE_TYPES ? false : true

"maps Julia types to Arrow/Feather type enum values"
const julia2Type_ = Dict{DataType,Metadata.Type_}([v=>k for (k,v) in Type_2julia])

const TimeUnit2julia = Dict{Metadata.TimeUnit,DataType}(
    Metadata.SECOND => Arrow.Second,
    Metadata.MILLISECOND => Arrow.Millisecond,
    Metadata.MICROSECOND => Arrow.Microsecond,
    Metadata.NANOSECOND => Arrow.Nanosecond
)
const julia2TimeUnit = Dict{DataType,Metadata.TimeUnit}([v=>k for (k,v) in TimeUnit2julia])

"""
Given a `meta` and `Metadata.Type_`, returns the storage Julia type
"""
function juliatype end

juliatype(meta::Void, values_type::Metadata.Type_, data) = Type_2julia[values_type]
function juliatype(meta::Metadata.CategoryMetadata, values_type::Metadata.Type_, data)
    levelinfo = meta.levels
    len = levelinfo.length
    ptr = pointer(data) + levelinfo.offset
    offsets = unsafe_wrap(Array, convert(Ptr{Int32}, ptr), len+1)
    ptr += sizeof(offsets)
    levels = tuple(map(x->Symbol(unsafe_string(ptr + offsets[x], offsets[x+1] - offsets[x])), 1:len)...)
    return Arrow.Category{meta.ordered,Type_2julia[values_type],levels}
end
juliatype(meta::Metadata.TimestampMetadata, values_type::Metadata.Type_, data) = Arrow.Timestamp{TimeUnit2julia[meta.unit],meta.timezone == "" ? :UTC : Symbol(meta.timezone)}
juliatype(meta::Metadata.DateMetadata, values_type::Metadata.Type_, data) = Arrow.Date
juliatype(meta::Metadata.TimeMetadata, values_type::Metadata.Type_, data) = Arrow.Time{TimeUnit2julia[meta.unit]}

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
    columns = ctable.columns
    for col in columns
        push!(header, col.name)
        push!(types, juliatype(col.metadata, col.values.type_, m))
    end
    # construct Data.Schema and Feather.Source
    return Source(Data.Schema(header, types, ctable.num_rows, Dict("parent"=>m)), ctable, m)
end

function Data.stream!(source::Source, ::Type{DataFrame})
    data = []
    rows = Int32(source.ctable.num_rows)
    columns = source.ctable.columns
    types = source.schema.types
    m = source.data
    parent = source.schema.metadata["parent"]
    # create a corresponding Julia NullableVector for each feather array
    for i = 1:length(columns)
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
        if isprimitive(values.type_)
            # for primitive types, we can just "wrap" the feather pointer
            arr = unwrap(convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes), rows)
            column = NullableArray{eltype(arr),1}(arr, nulls, parent)
        else
            # for string types, we need to manually construct based on each elements length
            ptr = pointer(m) + values.offset + bitmask_bytes
            offsets = unsafe_wrap(Array, convert(Ptr{Int32}, ptr), rows+1)
            values = unsafe_wrap(Array, ptr + sizeof(offsets), offsets[end])
            arr = WeakRefString{UInt8}[WeakRefString(pointer(values,offsets[i]+1), Int(offsets[i+1] - offsets[i])) for i = 1:rows]
            column = NullableArray{WeakRefString{UInt8},1}(arr, nulls, parent)
        end
        push!(data, column)
    end
    return DataFrame(data, map(Symbol, source.schema.header))
end

"read a feather-formatted binary file into a Julia DataFrame"
read(file::AbstractString) = Data.stream!(Source(file), DataFrame)

# writing feather files
"get the Arrow/Feather enum type from an AbstractColumn"
function feathertype end

feathertype{T}(::AbstractVector{Nullable{T}}) = julia2Type_[T]
feathertype{O,I,T}(::AbstractVector{Nullable{Arrow.Category{O,I,T}}}) = julia2Type_[I]
feathertype(::AbstractVector{Nullable{DateTime}}) = Metadata.INT64
feathertype(::AbstractVector{Nullable{Date}}) = Metadata.INT32
feathertype{P}(::AbstractVector{Nullable{Arrow.Time{P}}}) = Metadata.INT64
feathertype(::AbstractVector{Nullable{Vector{UInt8}}}) = Metadata.BINARY
feathertype{T<:AbstractString}(::AbstractVector{Nullable{T}}) = Metadata.UTF8

getmetadata{T}(io, a::AbstractVector{T}) = nothing
getmetadata(io, ::AbstractVector{Nullable{Date}}) = Metadata.DateMetadata()
getmetadata{T}(io, ::AbstractVector{Nullable{Arrow.Time{T}}}) = Metadata.TimeMetadata(julia2TimeUnit[T])
getmetadata(io, ::AbstractVector{Nullable{DateTime}}) = Metadata.TimestampMetadata(julia2TimeUnit[Arrow.Millisecond], "")
function getmetadata{O,I,T}(io, ::AbstractVector{Nullable{Arrow.Category{O,I,T}}})
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
function writecolumn{O,I,T}(io, A::AbstractVector{Nullable{Arrow.Category{O,I,T}}})
    return Base.write(io, view(reinterpret(UInt8, A.values), 1:(length(A) * sizeof(I))))
end
# Date
function writecolumn(io, A::AbstractVector{Nullable{Date}})
    return Base.write(io, view(reinterpret(UInt8, map(Arrow.date2unix, A.values)), 1:(length(A) * sizeof(Int32))))
end
# Timestamp
function writecolumn(io, A::AbstractVector{Nullable{DateTime}})
    return Base.write(io, view(reinterpret(UInt8, map(Arrow.datetime2unix, A.values)), 1:(length(A) * sizeof(Int64))))
end
# Date, Timestamp, Time and other primitive T
function writecolumn{T}(io, A::AbstractVector{Nullable{T}})
    return Base.write(io, view(reinterpret(UInt8, A.values), 1:(length(A) * sizeof(T))))
end
# List types
function writecolumn{T<:Union{Vector{UInt8},AbstractString}}(io, arr::AbstractVector{Nullable{T}})
    len = length(arr)
    total_bytes = sizeof(Int32) * (len+1)
    offsets = zeros(Int32, len+1)
    offsets[1] = off = 0
    for (i,v) in enumerate(arr.values)
        off += length(v)
        offsets[i + 1] = off
    end
    Base.write(io, view(reinterpret(UInt8, offsets), 1:total_bytes))
    total_bytes += values_bytes = offsets[len+1]
    for val in arr
        isnull(val) && continue
        # might be String, Vector{UInt8}, WeakRefString
        v = convert(String, get(val))
        Base.write(io, v.data)
    end
    return total_bytes
end

# "write a Arrow dataframe out to a feather file"
"DataStreams Sink implementation for feather-formatted binary files"
type Sink{I<:IO} <: Data.Sink
    schema::Data.Schema
    ctable::Metadata.CTable
    io::I
    description::String
    metadata::String
end

function Sink(file::AbstractString ;description::AbstractString=String(""), metadata::AbstractString=String(""))
    io = open(file, "w")
    Base.write(io, FEATHER_MAGIC_BYTES)
    return Sink(Data.EMPTYSCHEMA, Metadata.CTable("",0,Metadata.Column[],VERSION,""), io, description, metadata)
end

function Data.stream!(df::DataFrame, sink::Sink)
    header = map(string, names(df))
    data = df.columns
    io = sink.io
    # write out arrays, building each array's metadata as we go
    rows = length(data) > 0 ? length(data[1]) : 0
    columns = Metadata.Column[]
    for (name,arr) in zip(header,data)
        total_bytes = 0
        offset = position(io)
        null_count = sum(arr.isnull)
        len = length(arr)
        # write out null bitmask
        if null_count > 0
            total_bytes += null_bytes = Feather.bytes_for_bits(len)
            bytes = BitArray(!arr.isnull)
            Base.write(io, view(reinterpret(UInt8, bytes.chunks), 1:null_bytes))
        end
        # write out array values
        total_bytes += writecolumn(io, arr)
        values = Metadata.PrimitiveArray(feathertype(arr), Metadata.PLAIN, offset, len, null_count, total_bytes)
        push!(columns, Metadata.Column(String(name), values, getmetadata(io, arr), String("")))
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
    close(io)
    sink.schema = Data.schema(df)
    sink.ctable = ctable
    return sink
end

"write a Julia DataFrame to a feather-formatted binary file"
function write(file::AbstractString, df::DataFrame;description::AbstractString="", metadata::AbstractString="")
    sink = Sink(file; description=description, metadata=metadata)
    return Data.stream!(df, sink)
end

end # module
