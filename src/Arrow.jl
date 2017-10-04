module Arrow

struct Bool
    value::UInt64
end

# Time types
struct Second end
struct Millisecond end
struct Microsecond end
struct Nanosecond end

abstract type ArrowTimeType end

value(x) = x
value(x::Dates.TimeType) = Dates.value(x)
value(x::ArrowTimeType) = x.value

# Timestamp type with time unit `P` and timezone `Z`
struct Timestamp{P,Z} <: ArrowTimeType
    value::Int64
end

const UNIXEPOCH_TS = Dates.value(DateTime(1970)) #Rata Die milliseconds for 1970-01-01T00:00:00

scale(::Type{Second}, x) = 1000 * value(x)
scale(::Type{Millisecond}, x) = value(x)
scale(::Type{Microsecond}, x) = div(value(x),1000)
scale(::Type{Nanosecond}, x) =  div(value(x),1000000)

function unix2datetime(::Type{P}, x) where P
    rata = UNIXEPOCH_TS + scale(P, x)
    return DateTime(Dates.UTM(rata))
end
datetime2unix(x::DateTime) = value(x) - UNIXEPOCH_TS

Base.convert(::Type{DateTime}, x::Timestamp{P,Z}) where {P, Z} = unix2datetime(P, x)
Base.show(io::IO, x::Timestamp) = show(io, convert(DateTime,x))

struct Date <: ArrowTimeType
    value::Int32
end

const UNIXEPOCH_DT = Dates.value(Dates.Date(1970))
function unix2date(x)
    rata = UNIXEPOCH_DT + value(x)
    return Dates.Date(Dates.UTD(rata))
end
date2unix(x::Dates.Date) = (value(x) - UNIXEPOCH_DT) % Int32

Base.convert(::Type{Dates.Date}, x::Arrow.Date) = unix2date(x.value)
Base.show(io::IO, x::Arrow.Date) = show(io, convert(Dates.Date,x))

# Exact Time type with time unit `P`
struct Time{P} <: ArrowTimeType
    value::Int64
end

end # module
