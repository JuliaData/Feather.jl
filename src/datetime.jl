@enum Unit SECOND MILLISECOND NANOSECOND

immutable Category
    indices::Feather_Array
    levels::Feather_Array
    ordered::Cint
end

immutable Category_Data
    levels::Feather_Array
    ordered::Cint
end

immutable TimeStamp_Data
    timezone::Ptr{UInt8}
    unit::Unit
end

immutable Time_Data
    unit::Unit
end
