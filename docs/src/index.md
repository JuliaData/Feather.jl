# Feather.jl Documentation

`Feather.jl` provides a pure Julia library for reading and writing feather-formatted binary files, an efficient on-disk representation of a `DataFrame`.

For more info on the feather and related Arrow projects see the links below:

* [feather](https://github.com/wesm/feather)
* [arrow](https://arrow.apache.org/)

The back-end for Feather.jl is provided by [Arrow.jl](https://github.com/ExpandingMan/Arrow.jl).

```@contents
```

## Reading Feather Data
Typical usage of Feather.jl will involve calls like `Feather.read("filename.feather")`.  By default this will return a Julia `DataFrame` uses memory mapping to
lazily reference data in the file.  The initial call of `Feather.read` will *only* read the metadata that is needed to appropriately format the `DataFrame`, all
other data remains untouched until requested.  For example,
```julia
df = Feather.read("particledata.feather")  # only metadata is read

size(df); names(df)  # you can use DataFrames functions to access properties of your feather

head(df, 10)  # the first 10 rows are read in to return the head to you.
head(huge_df, 10)  # even if your feather is 5TB, only the first 10 rows will be loaded
                   # therefore it should happen almost as quickly on a 5TB table as a 5MB one

sort(df[:some_column])  # calls like this will only read data from a single column
                        # so, if you do this on a 5TB feather, it might take a while, but it won't
                        # waste time reading all the other columns

new_df = join(df[[:key_col, :A]], other_df, on=:key_col)  # you can even do complex operations like joins
# in the above, the join will only read the data from the feather that it needs
# so, if you've only selected two columns, only those columns will be read from disk
# no matter how big the feather is
```
The `DataFrame` returned from `Feather.read` works just like a normal dataframe, even though it only lazily loads data from disk.  Therefore, you can apply any
tools to the `DataFrame` and it will only load data as needed.  The only requirement is that the tool works on generic `AbstractVector` objects, as the
`DataFrame` columns are special Arrow.jl objects that allow for this lazy loading.

For, example, you can use [DataFramesMeta.jl](https://github.com/JuliaStats/DataFramesMeta.jl) or [Query.jl](https://github.com/davidanthoff/Query.jl) to query
the `DataFrame`, providing you with sort of a makeshift database.

In order to ensure an entire table or selected rows and columns are read from disk and copied completely into memory one can use `Feather.materialize`, but in
most cases this should not be necessary.


## Writing Feather Data
A typical use case for writing data will involve a call like `Feather.write("fileanme.feather", df)` where `df` is a `DataFrame`.  Note that columns must have
elements of the appropriate Julia types which correspond to formats that are described by the Feather standard.  These are
- `Integer` types
- `Float32`, `Float64`
- `String`
- `Date`, `DateTime`, `Time`
In addition, a `CategoricalArray{T}` where `T` is any of the above types will be saved as an Arrow formatted categorical array.  It is strongly recommended that
the element types of a `DataFrame` being written to a feather file are concrete to ensure proper serialization.  For example, a column containing all integers
should be an `AbstractVector{Int64}` rather than an `AbstractVector{Integer}` since feather does not support the serialization of columns of mixed integer
types.

In principle it is possible to store other Julia bits-types in feather files, but this is not officially supported by the feather standard and you do so at your
own risk.


## High-level interface
```@docs
Feather.read
Feather.materialize
Feather.write
```
