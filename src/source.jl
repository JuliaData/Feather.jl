
# DataStreams interface types
mutable struct Source{S, T} <: Data.Source
    path::String
    schema::Data.Schema
    ctable::Metadata.CTable
    data::Vector{UInt8}
    # ::S # separate from the types in schema, since we need to convert between feather storage types & julia types
    levels::Dict{Int,Vector{String}}
    orders::Dict{Int,Bool}
    columns::T # holds references to pre-fetched columns for Data.getfield
end


"""
    validfile(file::AbstractString, use_mmap::Bool)

Checks whether the file in location `file` may be a valid Feather file.

Returns file data.  Used by `Source`.
"""
function validfile(file::AbstractString, use_mmap::Bool)
    isfile(file) || throw(ArgumentError("'$file' is not a valid file."))
    m = use_mmap ? Mmap.mmap(file) : Base.read(file)
    if length(m) < 12
        throw(ArgumentError("'$file' is not in the feather format: total length of file: $(length(m))"))
    end
    if m[1:4] ≠ FEATHER_MAGIC_BYTES || m[end-3:end] ≠ FEATHER_MAGIC_BYTES
        throw(ArgumentError("'$file' is not in the feather format: header = $(m[1:4]),
                            footer = $(m[end-3:end])"))
    end
    m
end

function Source(file::AbstractString; nullable::Bool=false,
                weakrefstrings::Bool=true, use_mmap::Bool=SHOULD_USE_MMAP)
    # validity checks
    m = validfile(file, use_mmap)
    # read file metadata using FlatBuffers
    metalength = Base.read(IOBuffer(m[length(m)-7:length(m)-4]), Int32)
    metapos = length(m) - (metalength + 7)
    rootpos = Base.read(IOBuffer(m[metapos:metapos+4]), Int32)
    ctable = FlatBuffers.read(Metadata.CTable, m, metapos + rootpos - 1)
    # TODO again, comparison of Int32 with VersionNumber??
    if ctable.version < FEATHER_VERSION
        warn("This Feather file is old and may not be readable.")
    end
    header = String[]
    types = Type[]
    juliatypes = Type[]
    columns = ctable.columns
    levels = Dict{Int,Vector{String}}()
    orders = Dict{Int,Bool}()
    for (i, col) in enumerate(columns)
        push!(header, col.name)
        push!(types, juliastoragetype(col.metadata, col.values.type_))
        jl = juliatype(types[end])
        addlevels!(jl, levels, orders, i, col.metadata, col.values.type_, m, ctable.version)
        push!(juliatypes, schematype(jl, col.values.null_count, nullable, weakrefstrings))
    end
    sch = Data.Schema(juliatypes, header, ctable.num_rows)
    columns = DataFrame(sch, Data.Column, false)  # returns DataFrameStream, not DataFrame!!
    sch.rows = ctable.num_rows
    Source{Tuple{types...}, typeof(columns)}(file, sch, ctable, m, levels, orders, columns)
end

# DataStreams interface
Data.allocate(::Type{CategoricalString{R}}, rows, ref) where {R} = CategoricalArray{String, 1, R}(rows)
function Data.allocate(::Type{Union{CategoricalString{R}, Missing}}, rows, ref) where {R}
    CategoricalArray{Union{String, Missing}, 1, R}(rows)
end

Data.schema(source::Feather.Source) = source.schema
Data.reference(source::Feather.Source) = source.data
Data.isdone(io::Feather.Source, row, col, rows, cols) =  col > cols || row > rows
function Data.isdone(io::Source, row, col)
    rows, cols = size(Data.schema(io))
    return isdone(io, row, col, rows, cols)
end
Data.streamtype(::Type{<:Feather.Source}, ::Type{Data.Column}) = true
Data.streamtype(::Type{<:Feather.Source}, ::Type{Data.Field}) = true

@inline function Data.streamfrom(source::Source, ::Type{Data.Field}, ::Type{T}, row, col) where {T}
    if isempty(source.columns, col)
        append!(source.columns[col], Data.streamfrom(source, Data.Column, T, row, col))
    end
    source.columns[col][row]
end

"""
    nrows(s::Source)

Return the number of rows of the underlying data.
"""
nrows(s::Source) = s.ctable.num_rows

"""
    nnulls(s::Source, col)

Return the number of nulls in column number `col` according to metadata.
"""
nnulls(s::Source, col) = s.ctable.columns[col].values.null_count

"""
    coloffset(s::Source, col)

Return the offset of colum n number `col`.
"""
coloffset(s::Source, col) = s.ctable.columns[col].values.offset

function checknonull(s::Source, col)
    if nnulls(s, col) > 0
        throw(ErrorException("Column $col was expected to have no nulls but has $(nnulls(s, col))."))
    end
end
function getbools(s::Source, col)
    if nnulls(s, col) == 0
        zeros(Bool, nrows(s))
    else
        Bool[getbit(s.data[coloffset(s, col) + bytes_for_bits(x)], mod1(x,8)) for x ∈ 1:nrows(s)]
    end
end

@inline function unwrap(s::Source, ::Type{T}, col, rows, off::Integer=0) where {T}
    bitmask_bytes = if nnulls(s, col) > 0
        getoutputlength(s.ctable.version, bytes_for_bits(s.ctable.num_rows))
    else
        0
    end
    ptr = convert(Ptr{T}, pointer(s.data) + coloffset(s, col) + bitmask_bytes + off)
    [unsafe_load(ptr, i) for i = 1:rows]
end


"""
    getoffsets(s::Source, col)

Get offsets associated with a particular column.
"""
getoffsets(s::Source, col) = unwrap(s, Int32, col, nrows(s)+1)


"""
    getvalues(s::Source, col[, offsets::Vector{Int32}])

Get values associated with a column given offsets.  If not provided, offsets will be retrieved.
"""
function getvalues(s::Source, col, offsets::Vector{Int32})
    unwrap(s, UInt8, col, offsets[end], getoutputlength(s.ctable.version, sizeof(offsets)))
end
getvalues(s::Source, col) = getvalues(s, col, getoffsets(s, col))


"""
    transform!(::Type{T}, A::Vector, len::Integer)

**TODO**: These definitely require explanation.
"""
transform!(::Type{T}, A::Vector, len::Integer) where {T} = A
transform!(::Type{Dates.Date}, A, len::Integer) = map(x->Arrow.unix2date(x), A)
function transform!(::Type{Dates.DateTime}, A::Vector{Arrow.Timestamp{P,Z}}, len::Integer) where {P, Z}
    map(x->Arrow.unix2datetime(P, x), A)
end
transform!(::Type{CategoricalString{R}}, A::Vector, len::Integer) where {R} = map(x->x + R(1), A)
function transform!(::Type{Bool}, A::Vector, len::Integer)
    B = falses(len)
    Base.copy_chunks!(B.chunks, 1, map(x->x.value, A), 1, length(A) * 64)
    convert(Vector{Bool}, B)
end


@inline function Data.streamfrom(source::Source{S}, ::Type{Data.Column}, ::Type{T},
                                 row, col) where {S, T}
    checknonull(source, col)
    A = unwrap(source, S.parameters[col], col, source.ctable.num_rows)
    transform!(T, A, source.ctable.num_rows)
end
@inline function Data.streamfrom(source::Source{S}, ::Type{Data.Column}, ::Type{Union{T, Missing}},
                                 row, col) where {S, T}
    A = transform!(T, unwrap(source, S.parameters[col], col, source.ctable.num_rows), nrows(source))
    bools = getbools(source, col)
    V = Vector{Union{T, Missing}}(A)
    foreach(x->bools[x] && (V[x] = missing), 1:length(A))
    V
end
@inline function Data.streamfrom(source::Source{S}, ::Type{Data.Column}, ::Type{Bool}, row, col) where {S}
    checknonull(source, col)
    A = unwrap(source, S.parameters[col], col, max(1,div(bytes_for_bits(source.ctable.num_rows),8)))
    transform!(Bool, A, source.ctable.num_rows)::Vector{Bool}
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{T}, row, col
                                ) where {T <: AbstractString}
    checknonull(source, col)
    offsets = getoffsets(source, col)
    values = getvalues(source, col, offsets)
    T[unsafe_string(pointer(values, offsets[i]+1), Int(offsets[i+1] - offsets[i])) for i ∈ 1:nrows(s)]
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{Union{T, Missing}},
                                 row, col) where {T <: AbstractString}
    bools = getbools(source, col)
    offsets = getoffsets(source, col)
    values = getvalues(source, col, offsets)
    A = T[unsafe_string(pointer(values,offsets[i]+1), Int(offsets[i+1]-offsets[i])) for
                                                                        i ∈ 1:nrows(source)]
    V = Vector{Union{T, Missing}}(A)
    foreach(x->bools[x] && (V[x] = missing), 1:length(A))
    V
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{WeakRefString{UInt8}},
                                 row, col)
    checknonull(source, col)
    offsets = getoffsets(source, col)
    offset = coloffset(source, col)
    offset += if nnulls(source, col) > 0
        Feather.getoutputlength(source.ctable.version, Feather.bytes_for_bits(source.ctable.num_rows))
    else
        0
    end
    offset += getoutputlength(source.ctable.version, sizeof(offsets))
    values = getvalues(source, col, offsets)
    A = [WeakRefString(pointer(source.data, offset+offsets[i]+1), Int(offsets[i+1]-offsets[i])) for
                                                                        i ∈ 1:nrows(source)]
    WeakRefStringArray(source.data, A)
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column},
                                 ::Type{Union{WeakRefString{UInt8}, Missing}}, row, col)
    bools = getbools(source, col)
    offsets = getoffsets(source, col)
    offset = coloffset(source, col)
    offset += if nnulls(source, col) > 0
        Feather.getoutputlength(source.ctable.version, bytes_for_bits(nrows(source)))
    else
        0
    end
    offset += getoutputlength(source.ctable.version, sizeof(offsets))
    values = getvalues(source, col, offsets)
    A = Union{WeakRefString{UInt8},Missing}[WeakRefString(pointer(source.data,offset+offsets[i]+1), Int(offsets[i+1]-offsets[i])) for
                                             i ∈ 1:nrows(source)]
    foreach(x->bools[x] && (A[x] = missing), 1:length(A))
    WeakRefStringArray(source.data, A)
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column}, ::Type{CategoricalString{R}},
                                 row, col) where {R}
    checknonull(source, col)
    refs = transform!(CategoricalString{R}, unwrap(source, R, col, nrows(source)), nrows(source))
    pool = CategoricalPool{String, R}(source.levels[col], source.orders[col])
    CategoricalArray{String,1}(refs, pool)
end
@inline function Data.streamfrom(source::Source, ::Type{Data.Column},
                                 ::Type{Union{CategoricalString{R}, Missing}}, row, col) where {R}
    refs = transform!(CategoricalString{R}, unwrap(source, R, col, nrows(source)), nrows(source))
    bools = getbools(source, col)
    refs = R[ifelse(bools[i], R(0), refs[i]) for i = 1:source.ctable.num_rows]
    pool = CategoricalPool{String, R}(source.levels[col], source.orders[col])
    CategoricalArray{Union{String, Missing},1}(refs, pool)
end

