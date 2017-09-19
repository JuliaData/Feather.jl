module Arrow

struct Bool
    value::UInt64
end

# Time types
struct Second end
struct Millisecond end
struct Microsecond end
struct Nanosecond end

# Timestamp type with time unit `P` and timezone `Z`
struct Timestamp{P,Z}
    value::Int64
end

const UNIXEPOCH_TS = Dates.value(DateTime(1970)) #Rata Die milliseconds for 1970-01-01T00:00:00

scale(::Type{Second}, x) = 1000 * x.value
scale(::Type{Millisecond}, x) = x.value
scale(::Type{Microsecond}, x) = div(x.value,1000)
scale(::Type{Nanosecond}, x) =  div(x.value,1000000)

function unix2datetime(::Type{P}, x) where P
    rata = UNIXEPOCH_TS + scale(P, x)
    return DateTime(Dates.UTM(rata))
end
datetime2unix(x::DateTime) = Dates.value(x) - UNIXEPOCH_TS

Base.convert(::Type{DateTime}, x::Timestamp{P,Z}) where {P, Z} = unix2datetime(P, x)
Base.show(io::IO, x::Timestamp) = show(io, convert(DateTime,x))

struct Date
    value::Int32
end

const UNIXEPOCH_DT = Dates.value(Dates.Date(1970))
function unix2date(x)
    rata = UNIXEPOCH_DT + x.value
    return Dates.Date(Dates.UTD(rata))
end
date2unix(x::Dates.Date) = (Dates.value(x) - UNIXEPOCH_DT) % Int32

Base.convert(::Type{Dates.Date}, x::Arrow.Date) = unix2date(x.value)
Base.show(io::IO, x::Arrow.Date) = show(io, convert(Dates.Date,x))

# Exact Time type with time unit `P`
struct Time{P}
    value::Int64
end

end # module
