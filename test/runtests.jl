using Feather
using Base.Test

# write your own tests here
testdir = dirname(@__FILE__)
mtcars = Feather.Reader(joinpath(testdir, "data", "mtcars.feather"))
@test nrow(mtcars) == 32
@test ncol(mtcars) == 11
@test names(mtcars) == ["mpg","cyl","disp","hp","drat","wt","qsec","vs","am","gear","carb"]
