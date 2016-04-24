# Feather

[![Build Status](https://travis-ci.org/dmbates/Feather.jl.svg?branch=master)](https://travis-ci.org/JuliaStats/Feather.jl)

[Feather](http://github.com/wesm/feather) is a format for saving and retrieving data frames.  That repository provides the sources for _libfeather_, a C++ library to read and write the feather format,
and feather packages for `R` and for `Python/pandas`.

This repository provides a Julia package to read the feather format.  A writer will be added soon.

This version requires Julia 0.5- and the [Cxx package](https://github.com/Keno/Cxx.jl)

```julia
julia> using Feather

julia> iris = Feather.Reader(Pkg.dir("Feather", "test", "data", "iris.feather"));

julia> nrow(iris)
150

julia> ncol(iris)
150

julia> names(iris)
5-element Array{ByteString,1}:
 "Sepal.Length"
 "Sepal.Width"
 "Petal.Length"
 "Petal.Width"
 "Species"     

julia> iris[5].values
150-element Feather.Primitive{Int32}:
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 0
 ⋮
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2
 2

julia> iris[5].meta
"CategoryMetadata"

```
