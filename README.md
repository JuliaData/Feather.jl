
# Feather

*Julia library for working with feather-formatted files*

| **Documentation**                                                               | **Build Status**                                                                                |
|:-------------------------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------:|
| [![][docs-stable-img]][docs-stable-url] [![][docs-latest-img]][docs-latest-url] | [![][travis-img]][travis-url] [![][appveyor-img]][appveyor-url] [![][codecov-img]][codecov-url] |


## ⚠ Project Status
Feather.jl reads an older feather format now known as "Feather v1".  The current standard,
Feather v2, is simply the [apache arrow](https://arrow.apache.org/) format written to
disk.  As such, you're probably looking for
[Arrow.jl](https://github.com/JuliaData/Arrow.jl) which will allow you to read and write
Feather v2, but not feather v1.  We suggest that you upgrade any data you might have in
the v1 format to v2 using this package together with Arrow.jl, or with
[`pyarrow`](https://pypi.org/project/pyarrow/) which maintains support for the legacy
feather format.

Please note that the maintainers of this package have moved on to the new format, so it is
unlikely to receive updates and there may not be anyone available to review PR's.

## Installation

The package is registered in the `General` Registry and so can be installed with `Pkg.add`.

```julia
julia> using Pkg

julia> Pkg.add("Feather")
```

or from the Pkg REPL (accessed by typing `]` from the main REPL prompt/

```
] add Feather
```

## Documentation

- [**STABLE**][docs-stable-url] &mdash; **most recently tagged version of the documentation.**
- [**LATEST**][docs-latest-url] &mdash; *in-development version of the documentation.*


## Contributing and Questions

Contributions are very welcome, as are feature requests and suggestions. Please open an
[issue][issues-url] if you encounter any problems or would just like to ask a question.


[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: https://juliadata.github.io/Feather.jl/latest

[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: https://juliadata.github.io/Feather.jl/stable

[travis-img]: https://travis-ci.org/JuliaData/Feather.jl.svg?branch=master
[travis-url]: https://travis-ci.org/JuliaData/Feather.jl

[appveyor-img]: https://ci.appveyor.com/api/projects/status/nyybu2t2ofln4rn6/branch/master?svg=true
[appveyor-url]: https://ci.appveyor.com/project/quinnj/feather-jl-xxi09

[codecov-img]: https://codecov.io/gh/JuliaData/Feather.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/JuliaData/Feather.jl

[issues-url]: https://github.com/JuliaData/Feather.jl/issues

[pkg-0.4-img]: http://pkg.julialang.org/badges/Feather_0.4.svg
[pkg-0.4-url]: http://pkg.julialang.org/?pkg=Feather
[pkg-0.5-img]: http://pkg.julialang.org/badges/Feather_0.5.svg
[pkg-0.5-url]: http://pkg.julialang.org/?pkg=Feather
[pkg-0.6-img]: http://pkg.julialang.org/badges/Feather_0.6.svg
[pkg-0.6-url]: http://pkg.julialang.org/?pkg=Feather
