module Arrow

# Core type definitions for Arrow format that don't currently exist in julia proper
# Category type
 # `O` is a Bool indicating whether the categories are ordered
 # `I` is the size of internal storage
 # `T` is a Tuple containing the levels a symbols
immutable Category{O,I,T}
    value::I
end

Base.show{O,I,T}(io::IO, ::Type{Category{O,I,T}}) = print(io, "Arrow.Category{ordered=$(O),$(I)}")
Base.show{O,I,T}(io::IO, x::Category{O,I,T}) = print(io, "$(T[x.value+1])")

# Time types
immutable Second end
immutable Millisecond end
immutable Microsecond end
immutable Nanosecond end

# Timestamp type with time unit `P` and timezone `Z`
immutable Timestamp{P,Z}
    value::Int64
end

const UNIXEPOCH_TS = Dates.value(DateTime(1970)) #Rata Die milliseconds for 1970-01-01T00:00:00

scale(::Type{Second}, x) = 1000 * x.value
scale(::Type{Millisecond}, x) = x.value
scale(::Type{Microsecond}, x) = div(x.value,1000)
scale(::Type{Nanosecond}, x) =  div(x.value,1000000)

function unix2datetime{P}(::Type{P}, x)
    rata = UNIXEPOCH_TS + scale(P, x)
    return DateTime(Dates.UTM(rata))
end
function datetime2unix(x::DateTime)
    return Dates.value(x) - UNIXEPOCH_TS
end

Base.convert{P,Z}(::Type{DateTime}, x::Timestamp{P,Z}) = unix2datetime(P, x.value)
Base.show(io::IO, x::Timestamp) = show(io, convert(DateTime,x))

immutable Date
    value::Int32
end

const UNIXEPOCH_DT = Dates.value(Dates.Date(1970))
function unix2date(x)
    rata = UNIXEPOCH_DT + x.value
    return Dates.Date(Dates.UTD(rata))
end
date2unix(x::Dates.Date) = Int32(Dates.value(x) - UNIXEPOCH_DT)

Base.convert(::Type{Dates.Date}, x::Arrow.Date) = unix2date(x.value)
Base.show(io::IO, x::Arrow.Date) = show(io, convert(Dates.Date,x))

# Exact Time type with time unit `P`
immutable Time{P}
    value::Int64
end

end # module
