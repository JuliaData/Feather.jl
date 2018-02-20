using Feather, Base.Test, Missings, WeakRefStrings, CategoricalArrays, DataFrames

if Base.VERSION < v"0.7.0-DEV.2575"
    const Dates = Base.Dates
else
    import Dates
end

testdir = joinpath(dirname(@__FILE__), "data")
testdir2 = joinpath(dirname(@__FILE__), "newdata")
# testdir = joinpath(Pkg.dir("Feather"), "test/data")
# testdir2 = joinpath(Pkg.dir("Feather"), "test/newdata")
files = map(x -> joinpath(testdir, x), readdir(testdir))
append!(files, map(x -> joinpath(testdir2, x), readdir(testdir2)))

temps = []

for f in files
    println("tesing $f...")
    source = Feather.Source(f)
    df = Feather.read(source)
    temp = tempname()
    push!(temps, temp)
    sink = Feather.Sink(temp)
    sink = Feather.write(sink, df)
    df2 = Feather.read(temp)

    for (c1,c2) in zip(df.columns, df2.columns)
        for i = 1:length(c1)
            @test isequal(c1[i], c2[i])
        end
    end

    @test source.ctable.description == sink.ctable.description
    @test source.ctable.num_rows == sink.ctable.num_rows
    @test source.ctable.metadata == sink.ctable.metadata
    for (col1,col2) in zip(source.ctable.columns,sink.ctable.columns)
        @test col1.name == col2.name
        @test col1.metadata_type == col2.metadata_type
        @test typeof(col1.metadata) == typeof(col2.metadata)
        @test col1.user_metadata == col2.user_metadata

        v1 = col1.values; v2 = col2.values
        @test v1.type_ == v2.type_
        @test v1.encoding == v2.encoding
        # @test v1.offset == v2.offset # currently not python/R compatible due to wesm/feather#182
        @test v1.length == v2.length
        @test v1.null_count == v2.null_count
        # @test v1.total_bytes == v2.total_bytes
    end
end

function test_non_string_categories()
    fn = joinpath(testdir2, "int_categories.feather")

    levels_2 = [
        2, 6, 8, 9, 32, 34, 36, 47, 50, 59, 61, 62, 63, 69, 72, 79, 87, 89, 90,
        97, 111, 112, 117, 123, 124, 125, 126, 128, 129, 130, 136, 158, 174,
        182, 184, 185, 194, 199, 201, 210, 212, 221, 223, 236, 248, 257, 265,
        271, 295, 313, 315, 328, 770, 827, 839, 843, 844, 855, 858, 859, 863,
        869, 873, 879, 889, 903, 4901, 4903, 4904, 4918, 4926, 4931, 4954,
        4960, 4999, 5850, 5851, 5917, 5999, 6901, 6904, 6907, 6914, 6918, 8102,
        8103, 8106, 8199, 9104, 9651, 9699
    ]

    source = Feather.Source(fn)
    @test length(source.levels) == 2
    @test source.levels[1] == collect(2006:2015)
    @test source.levels[2] == levels_2  # too long to hard-code
    @test source.orders == Dict(k => false for k in 1:2)

    df = Feather.read(source);
    want = DataFrame(
        year=CategoricalArray{Int64,1}(
            Int8[1, 2, 3, 4, 6, 7, 8, 10],
            CategoricalPool{Int64,Int8}(Int64.(2006:2015), false)
        ),
        id1=CategoricalArray{Int64,1}(
            Int8[87, 21, 77, 77, 81, 81, 82, 67],
            CategoricalPool{Int64,Int8}(levels_2, false)
        ),
        id2=[12493, 4846710, 72400, 2395406, 766873, 4578402, 3387985, 3519757]
    )
    @test want == df

    # Feather.write
    temp = tempname()
    sink = Feather.Sink(temp)
    sink = Feather.write(sink, df)
    df2 = Feather.read(temp)
    @test df == df2 == want
    rm(temp)

end
test_non_string_categories()

gc(); gc()
for t in temps
    rm(t)
end

# issue #34
data = DataFrame(A=Union{Missing, String}[randstring(10) for i ∈ 1:100], B=rand(100))
data[2, :A] = missing
Feather.write("testfile.feather", data)
df = Feather.read("testfile.feather")
@test size(Data.schema(df)) == (100, 2)
gc();
rm("testfile.feather")

# check if valid, non-sudo docker is available
dockercheck = false
try
    global dockercheck = success(`docker images`)
catch
    println("It seems that `docker` is not installed or has to be run with sudo, skipping python roundtrip tests")
end

if dockercheck

DOCKERTAG = "0.1"

println("Running python round-trip tests...")

println("Pulling feather docker image...")
run(`docker pull quinnj/feather:$DOCKERTAG`)

println("Create docker container from feather image...")
run(`docker run -it -d --name feathertest quinnj/feather:$DOCKERTAG /bin/sh`)

try
    println("Generate a test.feather file from python...")
    run(`docker cp runtests.py feathertest:/home/runtests.py`)
    run(`docker exec feathertest python /home/runtests.py`)

    println("Read test.feather into julia...")
    run(`docker cp feathertest:/home/test.feather test.feather`)
    global df = Feather.read("test.feather")

    f64cat = CategoricalArray{Float64,1,Int8}([Inf, 1.0, 0.0])

    @test df[:Autf8] == ["hey","there","sailor"]
    @test df[:Abool] == [true, true, false]
    @test df[:Acat] == CategoricalArray(["a","b","c"])
    @test df[:Acatordered] == CategoricalArray(["d","e","f"])
    @test df[:Adatetime] == [Dates.DateTime(2016,1,1), Dates.DateTime(2016,1,2), Dates.DateTime(2016,1,3)]
    @test isequal(df[:Afloat32], [1.0, missing, 0.0])
    @test isequal(df[:Afloat64cat], f64cat)
    @test df[:Afloat64] == [Inf,1.0,0.0]

    df_ = Feather.read("test.feather"; nullable=false, use_mmap=false)

    println("Writing test2.feather from julia...")
    Feather.write("test2.feather", df)
    df2 = Feather.read("test2.feather")

    @test df2[:Autf8] == ["hey","there","sailor"]
    @test df2[:Abool] == [true, true, false]
    @test df2[:Acat] == CategoricalArrays.CategoricalArray(["a","b","c"])
    @test df2[:Acatordered] == CategoricalArrays.CategoricalArray(["d","e","f"])
    @test df2[:Adatetime] == [Dates.DateTime(2016,1,1), Dates.DateTime(2016,1,2), Dates.DateTime(2016,1,3)]
    @test isequal(df2[:Afloat32], [1.0, missing, 0.0])
    @test isequal(df2[:Afloat64cat], f64cat)
    @test df2[:Afloat64] == [Inf,1.0,0.0]

    println("Read test2.feather into python...")
    run(`docker cp test2.feather feathertest:/home/test2.feather`)
    run(`docker cp runtests2.py feathertest:/home/runtests2.py`)
    run(`docker exec feathertest python /home/runtests2.py`)
finally

    run(`docker stop feathertest`)
    run(`docker rm feathertest`)
    try
        rm("test.feather")
        rm("test2.feather")
    end
end

end
