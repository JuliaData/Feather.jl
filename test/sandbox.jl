using DataFrames
using DataStreams
using Feather


s = Feather.Source("basictest.feather", weakrefstrings=false)

# df = DataFrame(s)

