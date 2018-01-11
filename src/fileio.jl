#====================================================================================================
    fileio.jl
        Here we keep functions that involve reading and writing whole files.
====================================================================================================#
"""
`Feather.read{T <: Data.Sink}(file, sink_type::Type{T}, sink_args...; weakrefstrings::Bool=true)` => `T`

`Feather.read(file, sink::Data.Sink; weakrefstrings::Bool=true)` => `Data.Sink`

`Feather.read` takes a feather-formatted binary `file` argument and "streams" the data to the
provided `sink` argument, a `DataFrame` by default. A fully constructed `sink` can be provided as the 2nd argument (the 2nd method above),
or a Sink can be constructed "on the fly" by providing the type of Sink and any necessary positional arguments
(the 1st method above).

Keyword arguments:

  * `nullable::Bool=false`: will return columns as `NullableVector{T}` types by default, regarldess of # of missing values. When set to `false`, columns without missing values will be returned as regular `Vector{T}`
  * `weakrefstrings::Bool=true`: indicates whether string-type columns should be returned as `WeakRefString` (for efficiency) or regular `String` types
  * `use_mmap::Bool=true`: indicates whether to use system `mmap` capabilities when reading the feather file; on some systems or environments, mmap may not be available or reliable (virtualbox env using shared directories can be problematic)
  * `append::Bool=false`: indicates whether the feather file should be appended to the provided `sink` argument; note that column types between the feather file and existing sink must match to allow appending
  * `transforms`: a `Dict{Int,Function}` or `Dict{String,Function}` that provides transform functions to be applied to feather fields or columns as they are parsed from the feather file; note that feather files can be parsed field-by-field or entire columns at a time, so transform functions need to operate on scalars or vectors appropriately, depending on the `sink` argument's preferred streaming type; by default, a `Feather.Source` will stream entire columns at a time, so a transform function would take a single `NullableVector{T}` argument and return an equal-length `NullableVector`

Examples:

```julia
# default read method, returns a DataFrame
df = Feather.read("cool_feather_file.feather")

# read a feather file directly into a SQLite database table
db = SQLite.DB()
Feather.read("cool_feather_file.feather", SQLite.Sink, db, "cool_feather_table")
```
"""
function read end  # TODO why is this here?
function read(file::AbstractString, sink=DataFrame, args...;
              nullable::Bool=false, weakrefstrings::Bool=true, use_mmap::Bool=true,
              append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(Source(file; nullable=nullable, weakrefstrings=weakrefstrings, use_mmap=use_mmap),
                        sink, args...; append=append, transforms=transforms)
    Data.close!(sink)
end
function read(file::AbstractString, sink::T; nullable::Bool=false, weakrefstrings::Bool=true,
              use_mmap::Bool=true, append::Bool=false, transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(Source(file; nullable=nullable, weakrefstrings=weakrefstrings, use_mmap=use_mmap),
                        sink; append=append, transforms=transforms)
    Data.close!(sink)
end
function read(source::Feather.Source, sink=DataFrame, args...; append::Bool=false,
              transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(source, sink, args...; append=append, transforms=transforms)
    Data.close!(sink)
end
function read(source::Feather.Source, sink::T; append::Bool=false,
              transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(source, sink; append=append, transforms=transforms);
    Data.close!(sink)
end




"""
`Feather.write{T <: Data.Source}(io, source::Type{T}, source_args...)` => `Feather.Sink`

`Feather.write(io, source::Data.Source)` => `Feather.Sink`


Write a `Data.Source` out to disk as a feather-formatted binary file. The two methods allow the passing of a
fully constructed `Data.Source` (2nd method), or the type of Source and any necessary positional arguments (1st method).

Keyword arguments:

  * `append::Bool=false`: indicates whether the `source` argument should be appended to an existing feather file; note that column types between the `source` argument and feather file must match to allow appending
  * `transforms`: a `Dict{Int,Function}` or `Dict{String,Function}` that provides transform functions to be applied to source fields or columns as they are streamed to the feather file; note that feather sinks can be receive data field-by-field or entire columns at a time, so transform functions need to operate on scalars or vectors appropriately, depending on the `source` argument's allowed streaming types; by default, a `Feather.Sink` will stream entire columns at a time, so a transform function would take a single `NullableVector{T}` argument and return an equal-length `NullableVector`

Examples:

```julia
df = DataFrame(...)
Feather.write("shiny_new_feather_file.feather", df)

Feather.write("sqlite_query_result.feather", SQLite.Source, db, "select * from cool_table")
```
"""
function write end # TODO agian, why is this needed?
function write(io::AbstractString, ::Type{T}, args...;
               append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...) where {T}
    sink = Data.stream!(T(args...), Feather.Sink, io; append=append, transforms=transforms, kwargs...)
    Data.close!(sink)
end
function write(io::AbstractString, source; append::Bool=false,
               transforms::Dict=Dict{Int,Function}(), kwargs...)
    sink = Data.stream!(source, Feather.Sink, io; append=append, transforms=transforms, kwargs...)
    Data.close!(sink)
end
function write(sink::Sink, ::Type{T}, args...; append::Bool=false,
               transforms::Dict=Dict{Int,Function}()) where {T}
    sink = Data.stream!(T(args...), sink; append=append, transforms=transforms)
    Data.close!(sink)
end
function write(sink::Sink, source; append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sink = Data.stream!(source, sink; append=append, transforms=transforms)
    Data.close!(sink)
end

