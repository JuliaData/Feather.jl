#=====================================================================================================
    dataio.jl
        This file contains functions for reading/writing data from a buffer.
=====================================================================================================#

bytes_for_bits(size::Integer) = div(((size + 7) & ~7), 8)
getbit(byte::UInt8, i::Integer) = (byte & BITMASK[i]) == 0
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

