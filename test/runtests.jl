using Feather, Compat, Missings, WeakRefStrings, CategoricalArrays

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

    for (c1,c2) in zip(df, df2)
        for i = 1:length(c1)
            Compat.Test.@test isequal(c1[i], c2[i])
        end
    end

    Compat.Test.@test source.ctable.description == sink.ctable.description
    Compat.Test.@test source.ctable.num_rows == sink.ctable.num_rows
    Compat.Test.@test source.ctable.metadata == sink.ctable.metadata
    for (col1,col2) in zip(source.ctable.columns,sink.ctable.columns)
        Compat.Test.@test col1.name == col2.name
        Compat.Test.@test col1.metadata_type == col2.metadata_type
        Compat.Test.@test typeof(col1.metadata) == typeof(col2.metadata)
        Compat.Test.@test col1.user_metadata == col2.user_metadata

        v1 = col1.values; v2 = col2.values
        Compat.Test.@test v1.type_ == v2.type_
        Compat.Test.@test v1.encoding == v2.encoding
        # Compat.Test.@test v1.offset == v2.offset # currently not python/R compatible due to wesm/feather#182
        Compat.Test.@test v1.length == v2.length
        Compat.Test.@test v1.null_count == v2.null_count
        # Compat.Test.@test v1.total_bytes == v2.total_bytes
    end
end

gc(); gc()
for t in temps
    rm(t)
end

# issue #34
data = (A=Union{Missing, String}[randstring(10) for i ∈ 1:100], B=rand(100))
data.A[2] = missing
Feather.write("testfile.feather", data)
df = Feather.read("testfile.feather")
Compat.Test.@test size(Data.schema(df)) == (100, 2)
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

    Compat.Test.@test df[:Autf8] == ["hey","there","sailor"]
    Compat.Test.@test df[:Abool] == [true, true, false]
    Compat.Test.@test df[:Acat] == CategoricalArray(["a","b","c"])
    Compat.Test.@test df[:Acatordered] == CategoricalArray(["d","e","f"])
    Compat.Test.@test df[:Adatetime] == [Compat.Dates.DateTime(2016,1,1), Compat.Dates.DateTime(2016,1,2), Compat.Dates.DateTime(2016,1,3)]
    Compat.Test.@test isequal(df[:Afloat32], [1.0, missing, 0.0])
    Compat.Test.@test df[:Afloat64] == [Inf,1.0,0.0]

    df_ = Feather.read("test.feather"; nullable=false, use_mmap=false)

    println("Writing test2.feather from julia...")
    Feather.write("test2.feather", df)
    df2 = Feather.read("test2.feather")

    Compat.Test.@test df2[:Autf8] == ["hey","there","sailor"]
    Compat.Test.@test df2[:Abool] == [true, true, false]
    Compat.Test.@test df2[:Acat] == CategoricalArrays.CategoricalArray(["a","b","c"])
    Compat.Test.@test df2[:Acatordered] == CategoricalArrays.CategoricalArray(["d","e","f"])
    Compat.Test.@test df2[:Adatetime] == [Compat.Dates.DateTime(2016,1,1), Compat.Dates.DateTime(2016,1,2), Compat.Dates.DateTime(2016,1,3)]
    Compat.Test.@test isequal(df2[:Afloat32], [1.0, missing, 0.0])
    Compat.Test.@test df2[:Afloat64] == [Inf,1.0,0.0]

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
