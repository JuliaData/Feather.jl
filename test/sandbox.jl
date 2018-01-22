using DataFrames
using DataStreams
using Feather


s = Feather.Source("stringtest1.feather")

v = Feather.constructcolumn(s, 1)

