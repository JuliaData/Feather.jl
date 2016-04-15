using FlatBuffers

@enum(Type_, BOOL = 0, INT8 = 1, INT16 = 2, INT32 = 3, INT64 = 4,
  UINT8 = 5, UINT16 = 6, UINT32 = 7, UINT64 = 8,
  FLOAT = 9, DOUBLE = 10,  UTF8 = 11,  BINARY = 12,
  CATEGORY = 13, TIMESTAMP = 14, DATE = 15, TIME = 16)

@enum(Encoding_, PLAIN = 0, DICTIONARY = 1)

@enum(TimeUnit_, SECOND = 0, MILLISECOND = 1, NANOSECOND = 2)

for nm in [:PrimitiveArray, :CategoryMetadata, :TimestampMetadata, :DateMetadata, :TimeMetadata, :Column, :CTable]
    @eval begin
        type $nm <: FlatBuffers.Table
            io::FlatBuffers.TableIO
            memb::FlatBuffers.Membrs
        end

        $nm(io::IO, pos) = $nm(FlatBuffers.TableIO(io, pos), $(symbol(string(nm,"_members"))))
    end
end

typealias TypeMetadata Union{CategoryMetadata, TimestampMetadata, DateMetadata, TimeMetadata}

const PrimitiveArray_members = FlatBuffers.Membrs(
    :type => (4, UInt8, 0),
    :encoding => (6, UInt8, 0),
    :offset => (8, Int64, 0),
    :length => (10, Int64, 0),
    :null_count => (12, Int64, 0),
    :total_bytes => (14, Int64, 0)
)

const CategoryMetadata_members = FlatBuffers.Membrs(
    :levels => (4, PrimitiveArray, PrimitiveArray(IOBuffer("\0\0\0\0"), 0)),
    :ordered => (6, Bool, false)
)

const TimestampMetadata_members = FlatBuffers.Membrs(
    :unit => (4, UInt8, 0),
    :timezone => (6, UTF8String, "")
)

const DateMetadata_members = FlatBuffers.Membrs()

const TimeMetadata_members = FlatBuffers.Membrs(
    :unit => (4, UInt8, 0)
)

const Column_members = FlatBuffers.Membrs(
    :name => (4, UTF8String, ""),
    :values => (6, PrimitiveArray, PrimitiveArray(IOBuffer("\0\0\0\0"), 0)),
    :metadata => (8, TypeMetadata, nothing),
    :user_metadata => (10, UTF8String, "")
)

const CTable_members = FlatBuffers.Membrs(
    :description => (Int16(4), UTF8String, ""),
    :numRows => (Int16(6), Int64, 0),
    :columns => (Int16(8), Vector{Column}, Column[]),
    :version => (Int16(10), Int32, 0),
    :metadata => (Int16(12), UTF8String, ""),
)
