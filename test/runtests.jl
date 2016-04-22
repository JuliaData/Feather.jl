using Feather
using Base.Test

# write your own tests here
rdr = TableReader(Pkg.dir("Feather", "test", "data", "mtcars.feather"))
