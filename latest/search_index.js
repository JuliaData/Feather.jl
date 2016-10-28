var documenterSearchIndex = {"docs": [

{
    "location": "index.html#",
    "page": "Home",
    "title": "Home",
    "category": "page",
    "text": ""
},

{
    "location": "index.html#Feather.jl-Documentation-1",
    "page": "Home",
    "title": "Feather.jl Documentation",
    "category": "section",
    "text": "Feather.jl provides a pure Julia library for reading and writing feather-formatted binary files, an efficient on-disk representation of a DataFrame.For more info on the feather and related Arrow projects see the links below:feather: https://github.com/wesm/feather\nArrow: https://arrow.apache.org/"
},

{
    "location": "index.html#Feather.read",
    "page": "Home",
    "title": "Feather.read",
    "category": "Function",
    "text": "Feather.read{T <: Data.Sink}(file, sink_type::Type{T}, sink_args...; weakrefstrings::Bool=true) => T\n\nFeather.read(file, sink::Data.Sink; weakrefstrings::Bool=true) => Data.Sink\n\nFeather.read takes a feather-formatted binary file argument and \"streams\" the data to the provided sink argument, a DataFrame by default. A fully constructed sink can be provided as the 2nd argument (the 2nd method above), or a Sink can be constructed \"on the fly\" by providing the type of Sink and any necessary positional arguments (the 1st method above).\n\nKeyword arguments:\n\nnullable::Bool=true: will return columns as NullableVector{T} types by default, regarldess of # of null values. When set to false, columns without null values will be returned as regular Vector{T}\nweakrefstrings::Bool=true: indicates whether string-type columns should be returned as WeakRefString (for efficiency) or regular String types\nuse_mmap::Bool=true: indicates whether to use system mmap capabilities when reading the feather file; on some systems or environments, mmap may not be available or reliable (virtualbox env using shared directories can be problematic)\nappend::Bool=false: indicates whether the feather file should be appended to the provided sink argument; note that column types between the feather file and existing sink must match to allow appending\ntransforms: a Dict{Int,Function} or Dict{String,Function} that provides transform functions to be applied to feather fields or columns as they are parsed from the feather file; note that feather files can be parsed field-by-field or entire columns at a time, so transform functions need to operate on scalars or vectors appropriately, depending on the sink argument's preferred streaming type; by default, a Feather.Source will stream entire columns at a time, so a transform function would take a single NullableVector{T} argument and return an equal-length NullableVector\n\nExamples:\n\n# default read method, returns a DataFrame\ndf = Feather.read(\"cool_feather_file.feather\")\n\n# read a feather file directly into a SQLite database table\ndb = SQLite.DB()\nFeather.read(\"cool_feather_file.feather\", SQLite.Sink, db, \"cool_feather_table\")\n\n\n\n"
},

{
    "location": "index.html#Feather.write",
    "page": "Home",
    "title": "Feather.write",
    "category": "Function",
    "text": "Feather.write{T <: Data.Source}(io, source::Type{T}, source_args...) => Feather.Sink\n\nFeather.write(io, source::Data.Source) => Feather.Sink\n\nWrite a Data.Source out to disk as a feather-formatted binary file. The two methods allow the passing of a fully constructed Data.Source (2nd method), or the type of Source and any necessary positional arguments (1st method).\n\nKeyword arguments:\n\nappend::Bool=false: indicates whether the source argument should be appended to an existing feather file; note that column types between the source argument and feather file must match to allow appending\ntransforms: a Dict{Int,Function} or Dict{String,Function} that provides transform functions to be applied to source fields or columns as they are streamed to the feather file; note that feather sinks can be receive data field-by-field or entire columns at a time, so transform functions need to operate on scalars or vectors appropriately, depending on the source argument's allowed streaming types; by default, a Feather.Sink will stream entire columns at a time, so a transform function would take a single NullableVector{T} argument and return an equal-length NullableVector\n\nExamples:\n\ndf = DataFrame(...)\nFeather.write(\"shiny_new_feather_file.feather\", df)\n\nFeather.write(\"sqlite_query_result.feather\", SQLite.Source, db, \"select * from cool_table\")\n\n\n\n"
},

{
    "location": "index.html#High-level-interface-1",
    "page": "Home",
    "title": "High-level interface",
    "category": "section",
    "text": "Feather.read\nFeather.write"
},

]}
