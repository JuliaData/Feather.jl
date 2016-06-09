module Feather

using Arrow, FlatBuffers

# sync with feather version
const VERSION = 1

# flatbuffer defintions
include(joinpath(Pkg.dir("Feather"), "src/metadata.jl"))

# wesm/feather/cpp/src/common.h
const FEATHER_MAGIC_BYTES = "FEA1".data
ceil_byte(size) = (size + 7) & ~7
bytes_for_bits(size) = div(((size + 7) & ~7), 8)

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
const julia2Type_ = Dict{DataType,Metadata.Type_}(filter(x->!(x[2] in NON_PRIMITIVE_TYPES),v=>k for (k,v) in Type_2julia))

const TimeUnit2julia = Dict{Metadata.TimeUnit,DataType}(
    Metadata.SECOND => Arrow.Second,
    Metadata.MILLISECOND => Arrow.Millisecond,
    Metadata.MICROSECOND => Arrow.Microsecond,
    Metadata.NANOSECOND => Arrow.Nanosecond
)
const julia2TimeUnit = Dict{DataType,Metadata.TimeUnit}(v=>k for (k,v) in TimeUnit2julia)

juliatype(meta::Void, values_type::Metadata.Type_, data) = Type_2julia[values_type]
function juliatype(meta::Metadata.CategoryMetadata, values_type::Metadata.Type_, data)
    levelinfo = meta.levels
    len = levelinfo.length
    ptr = pointer(data) + levelinfo.offset
    offsets = pointer_to_array(convert(Ptr{Int32}, ptr), len+1)
    ptr += sizeof(offsets)
    levels = tuple(map(x->Symbol(String(ptr + offsets[x], offsets[x+1] - offsets[x])), 1:len)...)
    return Arrow.Category{meta.ordered,Type_2julia[values_type],levels}
end
juliatype(meta::Metadata.TimestampMetadata, values_type::Metadata.Type_, data) = Arrow.Timestamp{TimeUnit2julia[meta.unit],meta.timezone == "" ? :UTC : Symbol(meta.timezone)}
juliatype(meta::Metadata.DateMetadata, values_type::Metadata.Type_, data) = Arrow.Date
juliatype(meta::Metadata.TimeMetadata, values_type::Metadata.Type_, data) = Arrow.Time{TimeUnit2julia[meta.unit]}

# read
"read a feather file"
function read(file::AbstractString)
    # validity checks
    isfile(file) || throw(ArgumentError("'$file' is not a valid file"))
    m = Mmap.mmap(file)
    length(m) < 12 && throw(ArgumentError("'$file' is not in the feather format"))
    (m[1:4] == FEATHER_MAGIC_BYTES && m[end-3:end] == FEATHER_MAGIC_BYTES) ||
        throw(ArgumentError("'$file' is not in the feather format"))
    # read file metadata
    metalength = Base.read(IOBuffer(m[length(m)-7:length(m)-4]), Int32)
    metapos = length(m) - (metalength + 7)
    rootpos = Base.read(IOBuffer(m[metapos:metapos+4]), Int32)
    ctable = FlatBuffers.read(FlatBuffers.Table{Metadata.CTable}(m, metapos + rootpos - 1))
    header = String[]
    types = DataType[]
    columns = ctable.columns
    for col in columns
        push!(header, col.name)
        push!(types, juliatype(col.metadata, col.values.type_, m))
    end
    # read the actual data
    data = Arrow.AbstractColumn[]
    rows = Int32(ctable.num_rows)
    for i = 1:length(columns)
        col = columns[i]
        typ = types[i]
        values = col.values
        null_count = Int32(values.null_count)
        if null_count > 0
            nulls = BitArray(rows)
            if rows >= 64
                # this is safe because we keep a reference to mmap in our Arrow.Column
                chunks = pointer_to_array(pointer(m) + values.offset, bytes_for_bits(rows))
            else
                # need to pad our Vector{UInt8} to be big enough to hold a full UInt64
                chunks = m[(values.offset + 1):(values.offset + bytes_for_bits(rows))]
                while length(chunks) < 8
                    push!(chunks, 0x00)
                end
            end
            nulls.chunks = reinterpret(UInt64,chunks)
        else
            nulls = trues(rows)
        end
        bitmask_bytes = null_count == 0 ? 0 : bytes_for_bits(rows)
        feather_type = values.type_
        if isprimitive(feather_type)
            ptr = convert(Ptr{typ}, pointer(m) + values.offset + bitmask_bytes)
            column = Arrow.Column(m, rows, null_count, nulls, pointer_to_array(ptr, rows))
        else # list types
            ptr = pointer(m) + values.offset + bitmask_bytes
            offsets = pointer_to_array(convert(Ptr{Int32}, ptr), rows+1)
            values = pointer_to_array(ptr + sizeof(offsets), offsets[end])
            column = Arrow.List{feather_type,typ}(m, rows, null_count, nulls, offsets, values)
        end
        #TODO: to fully comply with Arrow format, we'll need to pad nulls/values to 64-byte alignments
        push!(data, column)
    end
    return (header, data, ctable)
end

"get the Arrow/Feather enum type from an AbstractColumn"
function feathertype end

feathertype{T}(::Arrow.Column{T}) = julia2Type_[T]
feathertype{O,I,T}(::Arrow.Column{Arrow.Category{O,I,T}}) = julia2Type_[I]
feathertype{P,Z}(::Arrow.Column{Arrow.Timestamp{P,Z}}) = Metadata.INT64
feathertype(::Arrow.Column{Arrow.Date}) = Metadata.INT32
feathertype{P}(::Arrow.Column{Arrow.Time{P}}) = Metadata.INT64
feathertype{A,T}(::Arrow.List{A,T}) = A

getmetadata{T}(io, a::Arrow.AbstractColumn{T}) = nothing
getmetadata(io, ::Arrow.Column{Arrow.Date}) = Metadata.DateMetadata()
getmetadata{T}(io, ::Arrow.Column{Arrow.Time{T}}) = Metadata.TimeMetadata(julia2TimeUnit[T])
getmetadata{T,TZ}(io, ::Arrow.Column{Arrow.Timestamp{T,TZ}}) = Metadata.TimestampMetadata(julia2TimeUnit[T], TZ == :UTC ? "" : string(TZ))
function getmetadata{O,I,T}(io, ::Arrow.Column{Arrow.Category{O,I,T}})
    len = length(T)
    offsets = zeros(Int32, len+1)
    values = map(string, T)
    offsets[1] = off = 0
    for (i,v) in enumerate(values)
        off += length(v)
        offsets[i + 1] = off
    end
    offset = position(io)
    total_bytes = Base.write(io, sub(reinterpret(UInt8, offsets), 1:(sizeof(Int32) * (len + 1))))
    total_bytes += Base.write(io, collect(values))
    return Metadata.CategoryMetadata(Metadata.PrimitiveArray(julia2Type_[I], Metadata.PLAIN, offset, len, 0, total_bytes), O)
end

# Category
function writecolumn{O,I,T}(io, A::Arrow.Column{Arrow.Category{O,I,T}})
    return Base.write(io, sub(reinterpret(UInt8, A.values), 1:(A.length * sizeof(I))))
end
# Date, Timestamp, Time, and other primitive T
function writecolumn{T}(io, A::Arrow.Column{T})
    return Base.write(io, sub(reinterpret(UInt8, A.values), 1:(A.length * sizeof(eltype(A)))))
end
# List types
function writecolumn{A,T}(io, arr::Arrow.List{A,T})
    total_bytes = sizeof(Int32) * (arr.length+1)
    Base.write(io, sub(reinterpret(UInt8, arr.offsets), 1:total_bytes))
    total_bytes += values_bytes = arr.offsets[arr.length+1]
    Base.write(io, sub(reinterpret(UInt8, arr.values), 1:values_bytes))
    return total_bytes
end

"write a Arrow dataframe out to a feather file"
function write(header, data::Vector{Arrow.AbstractColumn}, file::AbstractString, desc::String="", metadata::String="")
    io = open(file, "w")
    Base.write(io, FEATHER_MAGIC_BYTES)
    # write out arrays, building each array's metadata as we go
    rows = length(data) > 0 ? length(data[1]) : 0
    columns = Metadata.Column[]
    for (name,arr) in zip(header,data)
        total_bytes = 0
        offset = position(io)
        null_count = arr.null_count
        len = length(arr)
        # write out null bitmask
        if null_count > 0
            total_bytes += null_bytes = bytes_for_bits(arr.length)
            bytes = reinterpret(UInt8, arr.nulls.chunks)
            nulls = append!(bytes, zeros(UInt8,max(0,null_bytes - length(bytes))))
            Base.write(io, sub(nulls, 1:null_bytes))
        end
        # write out array values
        total_bytes += writecolumn(io, arr)
        values = Metadata.PrimitiveArray(feathertype(arr), Metadata.PLAIN, offset, len, null_count, total_bytes)
        push!(columns, Metadata.Column(name, values, getmetadata(io, arr), ""))
    end
    # write out metadata
    meta = FlatBuffers.Builder(Metadata.CTable)
    FlatBuffers.build!(meta, Metadata.CTable(desc, rows, columns, VERSION, metadata))
    rng = (meta.head + 1):length(meta.bytes)
    Base.write(io, sub(meta.bytes, rng))
    # write out metadata size
    Base.write(io, Int32(length(rng)))
    # write out final magic bytes
    Base.write(io, FEATHER_MAGIC_BYTES)
    close(io)
    return file
end

end # module

# Define conversions between Data.Table and DataFrame is the latter is defined
if isdefined(:DataFrames)
    DataFrames.DataFrame(dt::DataStreams.Data.Table) = DataFrame(convert(Vector{Any},DataArray[DataArray(x.values,x.isnull) for x in dt.data]),Symbol[symbol(x) for x in DataStreams.Data.header(dt)])
    function DataStreams.Data.Table(df::DataFrames.DataFrame)
        rows, cols = size(df)
        schema = DataStreams.Data.Schema(String[string(c) for c in names(df)],DataType[eltype(i) for i in df.columns],rows)
        data = NullableArrays.NullableVector[NullableArrays.NullableArray(x.data,convert(Vector{Bool},x.na)) for x in df.columns]
        return DataStreams.Data.Table(schema,data,0)
    end
end
