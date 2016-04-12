# Feather

[![Build Status](https://travis-ci.org/dmbates/Feather.jl.svg?branch=master)](https://travis-ci.org/JuliaStats/Feather.jl)

[Feather](http://github.com/wesm/feather) is a format for saving and retrieving data frames.  That repository provides the sources for _libfeather_, a C++ library to read and write the feather format,
and feather packages for `R` and for `Python/pandas`.

This repository provides a Julia package to read the feather format.  A writer will be added soon.

Using the package requires that "libfeather.so" be on the LD_LIBRARY_PATH.

An example using the reader is

```julia
julia> using Feather

julia> Reader(Pkg.dir("Feather", "test", "data", "mtcars.feather"))
[32 × 11] @ /home/bates/.julia/v0.4/Feather/test/data/mtcars.feather
 mpg   : PRIMITIVE(Float64)
 cyl   : PRIMITIVE(Float64)
 disp  : PRIMITIVE(Float64)
 hp    : PRIMITIVE(Float64)
 drat  : PRIMITIVE(Float64)
 wt    : PRIMITIVE(Float64)
 qsec  : PRIMITIVE(Float64)
 vs    : PRIMITIVE(Float64)
 am    : PRIMITIVE(Float64)
 gear  : PRIMITIVE(Float64)
 carb  : PRIMITIVE(Float64)

 julia> rr = Reader(Pkg.dir("Feather", "test", "data", "iris.feather"));

 julia> rr
 [150 × 5] @ /home/bates/.julia/v0.4/Feather/test/data/iris.feather
  Sepal.Length  : PRIMITIVE(Float64)
  Sepal.Width   : PRIMITIVE(Float64)
  Petal.Length  : PRIMITIVE(Float64)
  Petal.Width   : PRIMITIVE(Float64)
  Species       : CATEGORY(Int32)

```
