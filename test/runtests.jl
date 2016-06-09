using Feather
using Base.Test

testdir = joinpath(Pkg.dir("Feather"),"test/data/")
files = map(x->testdir * x, readdir(testdir))
for f in files
    dt = Feather.read(file)
    temp = tempname()
    Feather.write(dt[1], dt[2], temp)
    dt2 = Feather.read(temp)
    @test dt[1] == dt2[1]
    @test map(x->x.values,dt[2]) == map(x->x.values,dt2[2])
    @test dt[3].description == dt2[3].description
    @test dt[3].num_rows == dt2[3].num_rows
    @test dt[3].version == dt2[3].version
    @test dt[3].metadata == dt2[3].metadata
    rm(temp)
end
