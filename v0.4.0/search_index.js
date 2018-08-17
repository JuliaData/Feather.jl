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
    "text": "Feather.jl provides a pure Julia library for reading and writing feather-formatted binary files, an efficient on-disk representation of a DataFrame.For more info on the feather and related Arrow projects see the links below:feather\narrowThe back-end for Feather.jl is provided by Arrow.jl."
},

{
    "location": "index.html#Reading-Feather-Data-1",
    "page": "Home",
    "title": "Reading Feather Data",
    "category": "section",
    "text": "Typical usage of Feather.jl will involve calls like Feather.read(\"filename.feather\").  By default this will return a Julia DataFrame uses memory mapping to lazily reference data in the file.  The initial call of Feather.read will only read the metadata that is needed to appropriately format the DataFrame, all other data remains untouched until requested.  For example,df = Feather.read(\"particledata.feather\")  # only metadata is read\n\nsize(df); names(df)  # you can use DataFrames functions to access properties of your feather\n\nhead(df, 10)  # the first 10 rows are read in to return the head to you.\nhead(huge_df, 10)  # even if your feather is 5TB, only the first 10 rows will be loaded\n                   # therefore it should happen almost as quickly on a 5TB table as a 5MB one\n\nsort(df[:some_column])  # calls like this will only read data from a single column\n                        # so, if you do this on a 5TB feather, it might take a while, but it won\'t\n                        # waste time reading all the other columns\n\nnew_df = join(df[[:key_col, :A]], other_df, on=:key_col)  # you can even do complex operations like joins\n# in the above, the join will only read the data from the feather that it needs\n# so, if you\'ve only selected two columns, only those columns will be read from disk\n# no matter how big the feather isThe DataFrame returned from Feather.read works just like a normal dataframe, even though it only lazily loads data from disk.  Therefore, you can apply any tools to the DataFrame and it will only load data as needed.  The only requirement is that the tool works on generic AbstractVector objects, as the DataFrame columns are special Arrow.jl objects that allow for this lazy loading.For, example, you can use DataFramesMeta.jl or Query.jl to query the DataFrame, providing you with sort of a makeshift database.In order to ensure an entire table or selected rows and columns are read from disk and copied completely into memory one can use Feather.materialize, but in most cases this should not be necessary."
},

{
    "location": "index.html#Writing-Feather-Data-1",
    "page": "Home",
    "title": "Writing Feather Data",
    "category": "section",
    "text": "A typical use case for writing data will involve a call like Feather.write(\"fileanme.feather\", df) where df is a DataFrame.  Note that columns must have elements of the appropriate Julia types which correspond to formats that are described by the Feather standard.  These areInteger types\nFloat32, Float64\nString\nDate, DateTime, TimeIn addition, a CategoricalArray{T} where T is any of the above types will be saved as an Arrow formatted categorical array.  It is strongly recommended that the element types of a DataFrame being written to a feather file are concrete to ensure proper serialization.  For example, a column containing all integers should be an AbstractVector{Int64} rather than an AbstractVector{Integer} since feather does not support the serialization of columns of mixed integer types.In principle it is possible to store other Julia bits-types in feather files, but this is not officially supported by the feather standard and you do so at your own risk."
},

{
    "location": "index.html#Feather.read",
    "page": "Home",
    "title": "Feather.read",
    "category": "function",
    "text": "Feather.read(file::AbstractString)\n\nCreate a DataFrame representing the Feather file file.  This data frame will use ArrowVectors to refer to data within the feather file.  By default this is memory mapped and no data is actually read from disk until a particular field of the dataframe is accessed.\n\nTo copy the entire file into memory, instead use materialize.\n\n\n\n\n\n"
},

{
    "location": "index.html#Feather.materialize",
    "page": "Home",
    "title": "Feather.materialize",
    "category": "function",
    "text": "Feather.materialize(s::Feather.Source[, rows, cols])\nFeather.materialize(file::AbstractString[, rows, cols])\n\nRead a feather file into memory and return it as a DataFrame.  Optionally one may only read in particular rows or columns (these should be specified with AbstractVectors, columns can be either integers or Symbols).\n\nFor most purposes, it is recommended that you use read instead so that data is read off disk only as necessary.\n\n\n\n\n\n"
},

{
    "location": "index.html#Feather.write",
    "page": "Home",
    "title": "Feather.write",
    "category": "function",
    "text": "write(filename::AbstractString, df::DataFrame)\n\nWrite the dataframe df to the feather formatted file filename.\n\n\n\n\n\n"
},

{
    "location": "index.html#High-level-interface-1",
    "page": "Home",
    "title": "High-level interface",
    "category": "section",
    "text": "Feather.read\nFeather.materialize\nFeather.write"
},

]}
