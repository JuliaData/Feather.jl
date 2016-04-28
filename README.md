# Feather

[![Build Status](https://travis-ci.org/dmbates/Feather.jl.svg?branch=master)](https://travis-ci.org/JuliaStats/Feather.jl)

[Feather](http://github.com/wesm/feather) is a format for saving and retrieving data frames with implementations for
[R](http://r-project.org) and for [Python/Pandas](http://pandas.pydata.org).

This package provides a reader for the feather format.
It requires Julia 0.5- and the [Cxx package](https://github.com/Keno/Cxx.jl)

```jl
julia> using Feather

julia> rr = Feather.Reader(Pkg.dir("Feather", "test", "data", "mtcars.feather"))
[32 × 11] @ /home/bates/.julia/v0.5/Feather/test/data/mtcars.feather
 mpg   : Float64
 cyl   : Float64
 disp  : Float64
 hp    : Float64
 drat  : Float64
 wt    : Float64
 qsec  : Float64
 vs    : Float64
 am    : Float64
 gear  : Float64
 carb  : Float64

julia> DataFrame(rr)
32x11 DataFrames.DataFrame
│ Row │ mpg  │ cyl │ disp  │ hp    │ drat │ wt    │ qsec  │ vs  │ am  │ gear │ carb │
┝━━━━━┿━━━━━━┿━━━━━┿━━━━━━━┿━━━━━━━┿━━━━━━┿━━━━━━━┿━━━━━━━┿━━━━━┿━━━━━┿━━━━━━┿━━━━━━┥
│ 1   │ 21.0 │ 6.0 │ 160.0 │ 110.0 │ 3.9  │ 2.62  │ 16.46 │ 0.0 │ 1.0 │ 4.0  │ 4.0  │
│ 2   │ 21.0 │ 6.0 │ 160.0 │ 110.0 │ 3.9  │ 2.875 │ 17.02 │ 0.0 │ 1.0 │ 4.0  │ 4.0  │
│ 3   │ 22.8 │ 4.0 │ 108.0 │ 93.0  │ 3.85 │ 2.32  │ 18.61 │ 1.0 │ 1.0 │ 4.0  │ 1.0  │
│ 4   │ 21.4 │ 6.0 │ 258.0 │ 110.0 │ 3.08 │ 3.215 │ 19.44 │ 1.0 │ 0.0 │ 3.0  │ 1.0  │
│ 5   │ 18.7 │ 8.0 │ 360.0 │ 175.0 │ 3.15 │ 3.44  │ 17.02 │ 0.0 │ 0.0 │ 3.0  │ 2.0  │
│ 6   │ 18.1 │ 6.0 │ 225.0 │ 105.0 │ 2.76 │ 3.46  │ 20.22 │ 1.0 │ 0.0 │ 3.0  │ 1.0  │
│ 7   │ 14.3 │ 8.0 │ 360.0 │ 245.0 │ 3.21 │ 3.57  │ 15.84 │ 0.0 │ 0.0 │ 3.0  │ 4.0  │
│ 8   │ 24.4 │ 4.0 │ 146.7 │ 62.0  │ 3.69 │ 3.19  │ 20.0  │ 1.0 │ 0.0 │ 4.0  │ 2.0  │
│ 9   │ 22.8 │ 4.0 │ 140.8 │ 95.0  │ 3.92 │ 3.15  │ 22.9  │ 1.0 │ 0.0 │ 4.0  │ 2.0  │
│ 10  │ 19.2 │ 6.0 │ 167.6 │ 123.0 │ 3.92 │ 3.44  │ 18.3  │ 1.0 │ 0.0 │ 4.0  │ 4.0  │
│ 11  │ 17.8 │ 6.0 │ 167.6 │ 123.0 │ 3.92 │ 3.44  │ 18.9  │ 1.0 │ 0.0 │ 4.0  │ 4.0  │
│ 12  │ 16.4 │ 8.0 │ 275.8 │ 180.0 │ 3.07 │ 4.07  │ 17.4  │ 0.0 │ 0.0 │ 3.0  │ 3.0  │
│ 13  │ 17.3 │ 8.0 │ 275.8 │ 180.0 │ 3.07 │ 3.73  │ 17.6  │ 0.0 │ 0.0 │ 3.0  │ 3.0  │
│ 14  │ 15.2 │ 8.0 │ 275.8 │ 180.0 │ 3.07 │ 3.78  │ 18.0  │ 0.0 │ 0.0 │ 3.0  │ 3.0  │
│ 15  │ 10.4 │ 8.0 │ 472.0 │ 205.0 │ 2.93 │ 5.25  │ 17.98 │ 0.0 │ 0.0 │ 3.0  │ 4.0  │
│ 16  │ 10.4 │ 8.0 │ 460.0 │ 215.0 │ 3.0  │ 5.424 │ 17.82 │ 0.0 │ 0.0 │ 3.0  │ 4.0  │
│ 17  │ 14.7 │ 8.0 │ 440.0 │ 230.0 │ 3.23 │ 5.345 │ 17.42 │ 0.0 │ 0.0 │ 3.0  │ 4.0  │
│ 18  │ 32.4 │ 4.0 │ 78.7  │ 66.0  │ 4.08 │ 2.2   │ 19.47 │ 1.0 │ 1.0 │ 4.0  │ 1.0  │
│ 19  │ 30.4 │ 4.0 │ 75.7  │ 52.0  │ 4.93 │ 1.615 │ 18.52 │ 1.0 │ 1.0 │ 4.0  │ 2.0  │
│ 20  │ 33.9 │ 4.0 │ 71.1  │ 65.0  │ 4.22 │ 1.835 │ 19.9  │ 1.0 │ 1.0 │ 4.0  │ 1.0  │
│ 21  │ 21.5 │ 4.0 │ 120.1 │ 97.0  │ 3.7  │ 2.465 │ 20.01 │ 1.0 │ 0.0 │ 3.0  │ 1.0  │
│ 22  │ 15.5 │ 8.0 │ 318.0 │ 150.0 │ 2.76 │ 3.52  │ 16.87 │ 0.0 │ 0.0 │ 3.0  │ 2.0  │
│ 23  │ 15.2 │ 8.0 │ 304.0 │ 150.0 │ 3.15 │ 3.435 │ 17.3  │ 0.0 │ 0.0 │ 3.0  │ 2.0  │
│ 24  │ 13.3 │ 8.0 │ 350.0 │ 245.0 │ 3.73 │ 3.84  │ 15.41 │ 0.0 │ 0.0 │ 3.0  │ 4.0  │
│ 25  │ 19.2 │ 8.0 │ 400.0 │ 175.0 │ 3.08 │ 3.845 │ 17.05 │ 0.0 │ 0.0 │ 3.0  │ 2.0  │
│ 26  │ 27.3 │ 4.0 │ 79.0  │ 66.0  │ 4.08 │ 1.935 │ 18.9  │ 1.0 │ 1.0 │ 4.0  │ 1.0  │
│ 27  │ 26.0 │ 4.0 │ 120.3 │ 91.0  │ 4.43 │ 2.14  │ 16.7  │ 0.0 │ 1.0 │ 5.0  │ 2.0  │
│ 28  │ 30.4 │ 4.0 │ 95.1  │ 113.0 │ 3.77 │ 1.513 │ 16.9  │ 1.0 │ 1.0 │ 5.0  │ 2.0  │
│ 29  │ 15.8 │ 8.0 │ 351.0 │ 264.0 │ 4.22 │ 3.17  │ 14.5  │ 0.0 │ 1.0 │ 5.0  │ 4.0  │
│ 30  │ 19.7 │ 6.0 │ 145.0 │ 175.0 │ 3.62 │ 2.77  │ 15.5  │ 0.0 │ 1.0 │ 5.0  │ 6.0  │
│ 31  │ 15.0 │ 8.0 │ 301.0 │ 335.0 │ 3.54 │ 3.57  │ 14.6  │ 0.0 │ 1.0 │ 5.0  │ 8.0  │
│ 32  │ 21.4 │ 4.0 │ 121.0 │ 109.0 │ 4.11 │ 2.78  │ 18.6  │ 1.0 │ 1.0 │ 4.0  │ 2.0  │
```
Although it may be tempting to elide these two steps, don't do it.
The feather file is memory mapped as a byte array and the contents of the columns
are simply subarrays of this byte array.
If the `Reader` goes out of scope and gets garbage-collected, the byte array is lost
and hence the contents of the columns are lost.

A safe way to create a DataFrame without saving the `Reader` object is
```jl
julia> BOD = deepcopy(DataFrame(Feather.Reader(Pkg.dir("Feather", "test", "data", "BOD.feather"))))
6x2 DataFrames.DataFrame
│ Row │ Time │ demand │
┝━━━━━┿━━━━━━┿━━━━━━━━┥
│ 1   │ 1.0  │ 8.3    │
│ 2   │ 2.0  │ 10.3   │
│ 3   │ 3.0  │ 19.0   │
│ 4   │ 4.0  │ 16.0   │
│ 5   │ 5.0  │ 15.6   │
│ 6   │ 7.0  │ 19.8   │
```

At present categorical columns provide only the (0-based) references into the pool but
not the pool of values itself.

A feather format writer will be added in the fullness of time.
