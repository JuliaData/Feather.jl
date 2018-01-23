using DataFrames
using DataStreams
using Feather


s = Feather.Source("datestest.feather", weakrefstrings=false)

# df = DataFrame(s)

