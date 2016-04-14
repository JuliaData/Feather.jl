using FlatBuffers

@enum(TimeUnit, Second, Millisecond, Nanosecond)

type PrimitiveArray <: FlatBuffers.Table
    io::IO
    pos::Integer
    members::Dict{Symbol, Tuple{Int16, DataType, Any}}
    vtbloff::Int32
    vtblsz::Int16
end

function PrimitiveArray(io::IO, pos::Integer)
    vtbloff = read(seek(io, pos), Int32)
    vtblsz = read(seek(io, pos - vtbloff), Int16)
    PrimitiveArray(io, pos, Dict{Symbol, Tuple{Int16, DataType, Any}}(
        :type => (Int16(4), UInt8, zero(UInt8)),
        :encoding => (Int16(6), UInt8, zero(UInt8)),
        :offset => (Int16(8), Int64, zero(Int64)),
        :length => (Int16(10), Int64, zero(Int64)),
        :null_count => (Int16(12), Int64, zero(Int64)),
        :total_bytes => (Int16(14), Int64, zero(Int64))
    ), vtbloff, vtblsz)
end

type Column <: FlatBuffers.Table
    io::IO
    pos::Integer
    members::Dict{Symbol, Tuple{Int16, DataType, Any}}
    vtbloff::Int32
    vtblsz::Int16
end

function Column(io::IO, pos::Integer)
    vtbloff = read(seek(io, pos), Int32)
    vtblsz = read(seek(io, pos - vtbloff), Int16)
    Column(io, pos, Dict{Symbol, Tuple{Int16, DataType, Any}}(
        :name => (Int16(4), UTF8String, utf8("")),
        :values => (Int16(6), PrimitiveArray, PrimitiveArray(IOBuffer([0x00,0x00,0x00,0x00]), 0))
    ), vtbloff, vtblsz)
end

type CTable <: FlatBuffers.Table
    io::IO
    pos::Integer
    members::Dict{Symbol, Tuple{Int16, DataType, Any}}
    vtbloff::Int32
    vtblsz::Int16
end

function CTable(io::IO, pos::Integer)
    vtbloff = read(seek(io, pos), Int32)
    vtblsz = read(seek(io, pos - vtbloff), Int16)
    CTable(io, pos, Dict{Symbol, Tuple{Int16, DataType, Any}}(
        :description => (Int16(4), UTF8String, utf8("")),
        :numRows => (Int16(6), Int64, zero(Int64)),
        :columns => (Int16(8), Vector{Column}, Column[]),
        :version => (Int16(10), Int32, zero(Int32)),
        :metadata => (Int16(12), UTF8String, utf8(""))
    ), vtbloff, vtblsz)
end
