# Feather

[![Build Status](https://travis-ci.org/dmbates/Feather.jl.svg?branch=master)](https://travis-ci.org/JuliaStats/Feather.jl)

[Feather](http://github.com/wesm/feather) is a format for saving and retrieving data frames.  
The repository provides the sources for _libfeather_, a C++ library to read and write the feather format,
and feather packages for `R` and for `Python/pandas`.

A Julia package to read and write the feather format will be built in this repository.

A reader for the feather format has been added.  Using the package requires that "libfeather.so" be on the LD_LIBRARY_PATH.

An example of the reader is

```julia
julia> using Feather

julia> rr = Feather.Reader(Pkg.dir("Feather", "test", "data", "mtcars.feather"));

julia> cols = [rr[i] for i in 1:rr.columns]
11-element Array{Any,1}:
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x00000000026ab448,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301004,Ptr{Int32} @0x00007f15fe301e58),Ptr{Void} @0x00000000026ab430,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x0000000002659478,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301108,Ptr{Int32} @0x00007f15fe301df8),Ptr{Void} @0x0000000002659460,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x00000000024b0b68,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe30120c,Ptr{Int32} @0x00007f15fe301db4),Ptr{Void} @0x00000000024b0b50,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x0000000002691e28,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301310,Ptr{Int32} @0x00007f15fe301d60),Ptr{Void} @0x0000000002691e10,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x0000000002693e08,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301414,Ptr{Int32} @0x00007f15fe301d1c),Ptr{Void} @0x0000000002693df0,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x0000000002692478,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301518,Ptr{Int32} @0x00007f15fe301cd8),Ptr{Void} @0x0000000002692460,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x0000000002657038,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe30161c,Ptr{Int32} @0x00007f15fe301c94),Ptr{Void} @0x0000000002657020,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x00000000026abd48,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301720,Ptr{Int32} @0x00007f15fe301c50),Ptr{Void} @0x00000000026abd30,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x00000000026acfd8,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301824,Ptr{Int32} @0x00007f15fe301c10),Ptr{Void} @0x00000000026acfc0,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x000000000271e848,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301928,Ptr{Int32} @0x00007f15fe301bcc),Ptr{Void} @0x000000000271e830,Ptr{Void} @0x0000000000000000)
 Feather.Column(PRIMITIVE::Feather.Column_Type,Ptr{UInt8} @0x0000000002669a18,Feather.Feather_Array(DOUBLE::Feather.Feather_Type,32,0,Ptr{Void} @0x0000000000000000,Ptr{Void} @0x00007f15fe301a2c,Ptr{Int32} @0x00007f15fe301b84),Ptr{Void} @0x0000000002669a00,Ptr{Void} @0x0000000000000000)

julia> names = [bytestring(c.name) for c in cols]
11-element Array{Any,1}:
 "mpg"
 "cyl"
 "disp"
 "hp"  
 "drat"
 "wt"  
 "qsec"
 "vs"  
 "am"  
 "gear"
 "carb"
```
